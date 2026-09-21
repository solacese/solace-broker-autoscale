package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
)

func testConfig() config {
	return config{Fleet: "test", Partitions: 8, Routes: []route{{Pattern: "payments/*/*", Shard: "payments", Dispatch: "by-key", KeyLevels: []int{1}}}, Groups: map[string][]string{"ledger": {"payments/>"}}, Ready: []string{"ledger"}}
}
func fixtureAPI(t *testing.T, status *atomic.Int32, owner *atomic.Value) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n := status.Load(); n != 0 {
			w.WriteHeader(int(n))
			return
		}
		switch r.URL.Path {
		case "/messaging/config":
			json.NewEncoder(w).Encode(testConfig())
		case "/assignment":
			var p int
			json.Unmarshal([]byte(r.URL.Query().Get("partition")), &p)
			json.NewEncoder(w).Encode(assignment{Broker: owner.Load().(string), Prefix: owner.Load().(string) + "/", Partition: p, Count: 8, Lease: 60, Endpoints: map[string]string{"amqp": "amqp://localhost:5672"}})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(s.Close)
	return s
}

type testTransport struct {
	mu      sync.Mutex
	sent    []string
	blocked bool
	started chan struct{}
	once    sync.Once
}

func (t *testTransport) Sender(context.Context, string) (dispatch.Sender, error) { return t, nil }
func (t *testTransport) Receiver(context.Context, string, string) (dispatch.Receiver, error) {
	return nil, errors.New("unused")
}
func (t *testTransport) Send(ctx context.Context, m dispatch.Message) error {
	t.once.Do(func() {
		if t.started != nil {
			close(t.started)
		}
	})
	t.mu.Lock()
	blocked := t.blocked
	t.mu.Unlock()
	if blocked {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			return errors.New("fenced")
		}
	}
	var e struct {
		ID string `json:"event_id"`
	}
	json.Unmarshal(m.Body, &e)
	t.mu.Lock()
	t.sent = append(t.sent, m.Address+":"+e.ID)
	t.mu.Unlock()
	return nil
}
func (t *testTransport) Close() error { return nil }
func wait(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func TestDurableAcceptanceRestartOwnerAndOrder(t *testing.T) {
	var status atomic.Int32
	var owner atomic.Value
	owner.Store("a")
	api := fixtureAPI(t, &status, &owner)
	transport := &testTransport{blocked: true, started: make(chan struct{})}
	opts := Options{ControllerURL: api.URL, OutboxPath: filepath.Join(t.TempDir(), "test.outbox.db"), Credentials: func(string) (string, string, error) { return "u", "p", nil }, Transport: transport, PollInterval: 10 * time.Millisecond}
	c, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two", "three"} {
		if err = c.Publish("payments/account/created", map[string]int{"amount": 1}, id); err != nil {
			t.Fatal(err)
		}
	}
	<-transport.started
	if c.Status().Pending != 3 {
		t.Fatal(c.Status())
	}
	if err = c.Publish("payments/account/created", map[string]int{"amount": 1}, "one"); err != nil {
		t.Fatal(err)
	}
	if err = c.Publish("payments/account/created", map[string]int{"amount": 2}, "one"); err == nil {
		t.Fatal("allowed conflicting event ID")
	}
	if _, err = Open(context.Background(), opts); err == nil {
		t.Fatal("allowed second writer")
	}
	c.Close()
	owner.Store("b")
	transport.mu.Lock()
	transport.blocked = false
	transport.mu.Unlock()
	c, err = Open(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	want := []string{"topic://b/payments/account/created:one", "topic://b/payments/account/created:two", "topic://b/payments/account/created:three"}
	if len(transport.sent) != len(want) {
		t.Fatal(transport.sent)
	}
	for i, s := range want {
		if transport.sent[i] != s {
			t.Fatal(transport.sent)
		}
	}
}
func TestRejectionBackpressureAndClose(t *testing.T) {
	var status atomic.Int32
	var owner atomic.Value
	owner.Store("a")
	api := fixtureAPI(t, &status, &owner)
	c, err := Open(context.Background(), Options{ControllerURL: api.URL, OutboxPath: filepath.Join(t.TempDir(), "outbox.db"), Credentials: func(string) (string, string, error) { return "u", "p", nil }, Transport: &testTransport{blocked: true}, MaxOutboxMessages: 1, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.Publish("payments/account/created", 1, "1"); err != nil {
		t.Fatal(err)
	}
	if err = c.Publish("payments/account/created", 2, "2"); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}
	status.Store(401)
	wait(t, func() bool { return c.Status().Paused })
	if err = c.Publish("payments/account/created", 1, "1"); err == nil {
		t.Fatal("accepted after policy rejection")
	}
	status.Store(0)
	wait(t, func() bool { return !c.Status().Paused })
	c.Close()
	if err = c.Publish("payments/account/created", 1, "1"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// A separate process exits without closing bbolt, exercising committed recovery
// rather than a graceful shutdown that could hide missing fsyncs.
func TestCrashRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$")
	cmd.Env = append(os.Environ(), "GO_OUTBOX_CRASH_PATH="+path)
	if err := cmd.Run(); err == nil {
		t.Fatal("child should exit abruptly")
	}
	o, err := openOutbox(path, testConfig(), 100000, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer o.db.Close()
	heads, err := o.heads()
	if err != nil || len(heads) != 1 || heads[0].ID != "durable" {
		t.Fatalf("%v %v", heads, err)
	}
}
func TestCrashChild(t *testing.T) {
	path := os.Getenv("GO_OUTBOX_CRASH_PATH")
	if path == "" {
		return
	}
	o, err := openOutbox(path, testConfig(), 100000, 100)
	if err != nil {
		os.Exit(2)
	}
	r, err := testConfig().publication("payments/account/created", "durable", 1)
	if err != nil {
		os.Exit(3)
	}
	if o.enqueue(r) != nil {
		os.Exit(4)
	}
	os.Exit(7)
}
func TestContractCannotRemapPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	cfg := testConfig()
	o, err := openOutbox(path, cfg, 10000, 10)
	if err != nil {
		t.Fatal(err)
	}
	o.db.Close()
	cfg.Partitions++
	if _, err = openOutbox(path, cfg, 10000, 10); err == nil {
		t.Fatal("accepted partition change")
	}
}

func TestPythonRoutingGoldenVectors(t *testing.T) {
	data, err := os.ReadFile("../testdata/managed-routing.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Account, Key string
		Partition    int
	}
	if err = json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, v := range vectors {
		key := jsonStrings([]string{v.Account}, true)
		if key != v.Key {
			t.Fatalf("key %q got %s want %s", v.Account, key, v.Key)
		}
		if got := partitionFor("payments", key, 128); got != v.Partition {
			t.Fatalf("%q: %d != %d", v.Account, got, v.Partition)
		}
	}
}

type flowTransport struct {
	messages  chan dispatch.Message
	dials     atomic.Int32
	failFirst atomic.Bool
}

func (t *flowTransport) Sender(context.Context, string) (dispatch.Sender, error) {
	return nil, errors.New("unused")
}
func (t *flowTransport) Receiver(context.Context, string, string) (dispatch.Receiver, error) {
	t.dials.Add(1)
	return &fakeReceiver{transport: t}, nil
}

type fakeReceiver struct{ transport *flowTransport }

func (r *fakeReceiver) Receive(ctx context.Context) (dispatch.Message, error) {
	if r.transport.failFirst.CompareAndSwap(true, false) {
		return dispatch.Message{}, errors.New("link disconnected")
	}
	select {
	case m := <-r.transport.messages:
		return m, nil
	case <-ctx.Done():
		return dispatch.Message{}, ctx.Err()
	}
}
func (r *fakeReceiver) Close() error { return nil }
func TestSubscriberRetriesHeadBeforeACKAndFollowsLocations(t *testing.T) {
	var both atomic.Bool
	var onlyB atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/messaging/config":
			json.NewEncoder(w).Encode(testConfig())
		case "/messaging/subscriptions":
			json.NewEncoder(w).Encode(map[string]any{"shards": []string{"payments"}})
		case "/partitions":
			locs := []map[string]any{{"broker_id": "a", "endpoints": map[string]string{"amqp": "amqp://a:5672"}}}
			if both.Load() {
				locs = append(locs, map[string]any{"broker_id": "b", "endpoints": map[string]string{"amqp": "amqp://b:5672"}})
			}
			if onlyB.Load() {
				locs = locs[1:]
			}
			json.NewEncoder(w).Encode(map[string]any{"partitions": []map[string]any{{"partition_id": 0, "queue_name": "q-test", "locations": locs}}})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	tr := &flowTransport{messages: make(chan dispatch.Message, 2)}
	tr.failFirst.Store(true)
	c, err := Open(context.Background(), Options{ControllerURL: server.URL, OutboxPath: filepath.Join(t.TempDir(), "o.db"), Credentials: func(string) (string, string, error) { return "u", "p", nil }, Transport: tr, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var calls, acks atomic.Int32
	err = c.Subscribe(context.Background(), "ledger", func(ctx context.Context, m Message) error {
		if calls.Add(1) == 1 {
			if acks.Load() != 0 {
				t.Error("ack before handler")
			}
			return errors.New("database not available")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	tr.messages <- dispatch.Message{Body: []byte(`{"event_id":"one","payload":{"topic":"payments/a/created","data":1}}`), Ack: func(context.Context) error { acks.Add(1); return nil }}
	wait(t, func() bool { return acks.Load() == 1 })
	if calls.Load() != 2 || tr.dials.Load() < 2 {
		t.Fatalf("calls %d dials %d", calls.Load(), tr.dials.Load())
	}
	both.Store(true)
	wait(t, func() bool { return c.Status().Receivers == 2 })
	onlyB.Store(true)
	wait(t, func() bool { return c.Status().Receivers == 1 })
}

type uncertainTransport struct {
	mu  sync.Mutex
	ids []string
}

func (t *uncertainTransport) Sender(context.Context, string) (dispatch.Sender, error) { return t, nil }
func (t *uncertainTransport) Receiver(context.Context, string, string) (dispatch.Receiver, error) {
	return nil, errors.New("unused")
}
func (t *uncertainTransport) Close() error { return nil }
func (t *uncertainTransport) Send(ctx context.Context, m dispatch.Message) error {
	var e struct {
		ID string `json:"event_id"`
	}
	json.Unmarshal(m.Body, &e)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ids = append(t.ids, e.ID)
	if len(t.ids) == 1 {
		return errors.New("broker stored it but receipt was lost")
	}
	return nil
}
func TestLostReceiptRetriesBeforeNextPublication(t *testing.T) {
	var status atomic.Int32
	var owner atomic.Value
	owner.Store("a")
	api := fixtureAPI(t, &status, &owner)
	tr := &uncertainTransport{}
	c, err := Open(context.Background(), Options{ControllerURL: api.URL, OutboxPath: filepath.Join(t.TempDir(), "o.db"), Credentials: func(string) (string, string, error) { return "u", "p", nil }, Transport: tr})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, id := range []string{"one", "two"} {
		if err = c.Publish("payments/account/created", 1, id); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.ids) != 3 || tr.ids[0] != "one" || tr.ids[1] != "one" || tr.ids[2] != "two" {
		t.Fatal(tr.ids)
	}
}
