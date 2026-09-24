package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func evaluatorTestConfig(result string) config {
	return config{Fleet: "orders", Partitions: 128, Routes: []route{{Pattern: "orders/*/*", Shard: "orders", KeyEvaluator: &evaluatorConfig{Name: "order-entity", Version: "1.0.0", Result: result}}}, Groups: map[string][]string{"processor": {"orders/>"}}, Ready: []string{"processor"}}
}

func TestEvaluatorWholeMessageIdentityHeadersAndPayload(t *testing.T) {
	registry := NewEvaluatorRegistry()
	payload := map[string]any{"order_id": "é-42", "amount": float64(20)}
	headers := map[string]string{"tenant": "acme"}
	message := &Publication{Topic: "orders/eu/created", Payload: payload, EventID: "evt", Headers: headers}
	var seen *Publication
	if err := registry.Register("order-entity", "1.0.0", EvaluatorFunc(func(got *Publication) (RoutingResult, error) {
		seen = got
		if got.Payload.(map[string]any)["order_id"] != "é-42" || got.Headers["tenant"] != "acme" {
			t.Fatal(got)
		}
		return BusinessKey("acme:é-42"), nil
	})); err != nil {
		t.Fatal(err)
	}
	r, err := evaluatorTestConfig("key").publicationMessage(message, registry)
	if err != nil {
		t.Fatal(err)
	}
	if seen != message || r.RoutingKind != "key" || r.RoutingValue != "acme:é-42" || r.Partition != partitionFor("orders", "acme:é-42", 128) {
		t.Fatalf("%+v", r)
	}
}

func TestEvaluatorRegistryZeroValueAndConcurrentAccess(t *testing.T) {
	var registry EvaluatorRegistry
	if err := registry.Register("base", "1", EvaluatorFunc(func(*Publication) (RoutingResult, error) { return BusinessKey("base"), nil })); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 20)
	for i := 0; i < 10; i++ {
		go func(i int) {
			done <- registry.Register(string(rune('a'+i)), "1", EvaluatorFunc(func(*Publication) (RoutingResult, error) { return BusinessKey("x"), nil }))
		}(i)
		go func() { _, err := registry.require("base", "1"); done <- err }()
	}
	for i := 0; i < 20; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestEvaluatorTypedResultsBoundsAndPanic(t *testing.T) {
	tests := []struct {
		name, format string
		result       RoutingResult
		panic        bool
	}{
		{"wrong-key-type", "key", SHA256Digest{}, false},
		{"empty-key", "key", BusinessKey(""), false},
		{"unicode-key-bytes", "key", BusinessKey(string(make([]rune, 513))), false},
		{"wrong-digest-type", "sha256", BusinessKey("abc"), false},
		{"panic", "key", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := NewEvaluatorRegistry()
			registry.Register("order-entity", "1.0.0", EvaluatorFunc(func(*Publication) (RoutingResult, error) {
				if tt.panic {
					panic("boom")
				}
				return tt.result, nil
			}))
			if _, err := evaluatorTestConfig(tt.format).publicationMessage(&Publication{Topic: "orders/eu/created", EventID: "evt", Payload: map[string]any{}}, registry); err == nil {
				t.Fatal("accepted invalid evaluator result")
			}
		})
	}
}

func TestMissingVersionRejectedAndCallbackDoesNotHoldClientLock(t *testing.T) {
	cfg := evaluatorTestConfig("key")
	server := configServer(t, cfg)
	if _, err := Open(context.Background(), Options{ControllerURL: server.URL, OutboxPath: filepath.Join(t.TempDir(), "missing.db"), Credentials: testCredentials}); err == nil {
		t.Fatal("missing evaluator accepted")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	registry := NewEvaluatorRegistry()
	registry.Register("order-entity", "1.0.0", EvaluatorFunc(func(*Publication) (RoutingResult, error) {
		close(entered)
		<-release
		return BusinessKey("entity"), nil
	}))
	c, err := Open(context.Background(), Options{ControllerURL: server.URL, OutboxPath: filepath.Join(t.TempDir(), "ok.db"), Credentials: testCredentials, RoutingEvaluators: registry})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan error, 1)
	go func() {
		done <- c.PublishMessage(&Publication{Topic: "orders/eu/created", Payload: map[string]any{}, EventID: "evt"})
	}()
	<-entered
	statusDone := make(chan struct{})
	go func() { _ = c.Status(); close(statusDone) }()
	select {
	case <-statusDone:
	case <-time.After(time.Second):
		t.Fatal("Status blocked behind evaluator")
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEvaluatorErrorAndPanicLeaveNoOutboxEntry(t *testing.T) {
	for _, panicCallback := range []bool{false, true} {
		registry := NewEvaluatorRegistry()
		registry.Register("order-entity", "1.0.0", EvaluatorFunc(func(*Publication) (RoutingResult, error) {
			if panicCallback {
				panic("boom")
			}
			return nil, errors.New("bad")
		}))
		cfg := evaluatorTestConfig("key")
		server := configServer(t, cfg)
		c, err := Open(context.Background(), Options{ControllerURL: server.URL, OutboxPath: filepath.Join(t.TempDir(), "o.db"), Credentials: testCredentials, RoutingEvaluators: registry})
		if err != nil {
			t.Fatal(err)
		}
		if err = c.PublishMessage(&Publication{Topic: "orders/eu/created", Payload: map[string]any{}, EventID: "evt"}); err == nil {
			t.Fatal("accepted failure")
		}
		if c.Status().Pending != 0 {
			t.Fatal(c.Status())
		}
		c.Close()
	}
}

func TestLegacyPendingIdentityRemainsIdempotentButChangesReject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	o, err := openOutbox(path, testConfig(), 100000, 100)
	if err != nil {
		t.Fatal(err)
	}
	old := struct {
		ID        string          `json:"event_id"`
		Shard     string          `json:"shard"`
		Partition int             `json:"partition"`
		Topic     string          `json:"topic"`
		Data      json.RawMessage `json:"data"`
		Sequence  uint64          `json:"sequence"`
	}{"evt", "payments", 3, "payments/account/created", json.RawMessage(`{"amount":1}`), 0}
	legacy, _ := json.Marshal(old)
	err = o.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("ids")).Put([]byte("evt"), legacy) })
	if err != nil {
		t.Fatal(err)
	}
	r := record{ID: "evt", Shard: "payments", Partition: 3, Topic: "payments/account/created", Data: json.RawMessage(`{"amount":1}`), RoutingKind: "key", RoutingValue: `["account"]`}
	if err = o.enqueue(r); err != nil {
		t.Fatal(err)
	}
	r.Data = json.RawMessage(`{"amount":2}`)
	if err = o.enqueue(r); err == nil {
		t.Fatal("changed content accepted")
	}
	o.db.Close()
}

func TestCustomerRoutingGoldenVectors(t *testing.T) {
	data, err := os.ReadFile("../testdata/customer-routing.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Name, Shard, Result, Value string
		Tenant                     string `json:"tenant"`
		OrderID                    string `json:"order_id"`
		Partition                  int    `json:"partition128"`
	}
	if err = json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, v := range vectors {
		var got int
		if v.Result == "key" {
			got = partitionFor(v.Shard, v.Value, 128)
		} else {
			raw, e := hex.DecodeString(v.Value)
			if e != nil || len(raw) != 32 {
				t.Fatal(v)
			}
			var d SHA256Digest
			copy(d[:], raw)
			_, _, got, e = resolveEvaluator(EvaluatorFunc(func(*Publication) (RoutingResult, error) { return d, nil }), "sha256", &Publication{}, v.Shard, 128)
			if e != nil {
				t.Fatal(e)
			}
		}
		if got != v.Partition {
			t.Fatalf("%s: %d != %d", v.Name, got, v.Partition)
		}
	}
	for _, v := range vectors {
		if v.Result != "sha256" {
			continue
		}
		tenant, order := []byte(v.Tenant), []byte(v.OrderID)
		canonical := make([]byte, 8+len(tenant)+len(order))
		binary.BigEndian.PutUint32(canonical[:4], uint32(len(tenant)))
		copy(canonical[4:], tenant)
		offset := 4 + len(tenant)
		binary.BigEndian.PutUint32(canonical[offset:offset+4], uint32(len(order)))
		copy(canonical[offset+4:], order)
		d := sha256.Sum256(canonical)
		if hex.EncodeToString(d[:]) != v.Value {
			t.Fatalf("%s digest source mismatch", v.Name)
		}
	}
}

func configServer(t *testing.T, cfg config) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/messaging/config":
			_ = json.NewEncoder(w).Encode(cfg)
		case "/assignment":
			_ = json.NewEncoder(w).Encode(assignment{Broker: "a", Prefix: "a/", Partition: 0, Count: cfg.Partitions, Lease: 60, Endpoints: map[string]string{"amqp": "amqp://localhost:5672"}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}
func testCredentials(string) (string, string, error) { return "u", "p", nil }
