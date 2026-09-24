package messaging

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
	amqptransport "github.com/solacese/solace-broker-autoscale/shim/transport/amqp"
)

// TestManagedControlAMQPLive is an opt-in real-transport check. It is designed to run inside an
// explicitly owned broker container when AMQP is not host-mapped. It creates no queues: the source
// and target are one isolated direct topic, and the temporary outbox is removed by testing.TempDir.
func TestManagedControlAMQPLive(t *testing.T) {
	endpoint := os.Getenv("SOLACE_GO_CONTROL_ENDPOINT")
	username := os.Getenv("SOLACE_GO_CONTROL_USERNAME")
	password := os.Getenv("SOLACE_GO_CONTROL_PASSWORD")
	topic := os.Getenv("SOLACE_GO_CONTROL_TOPIC")
	if endpoint == "" || username == "" || password == "" || topic == "" {
		t.Skip("set SOLACE_GO_CONTROL_ENDPOINT, USERNAME, PASSWORD and TOPIC for an owned broker")
	}

	var revision atomic.Uint64
	var configRequests atomic.Int32
	externalPublisher := os.Getenv("SOLACE_GO_CONTROL_EXTERNAL_PUBLISHER") == "1"
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messaging/config" {
			http.NotFound(w, r)
			return
		}
		configRequests.Add(1)
		cfg := testConfig()
		cfg.Revision = revision.Load()
		cfg.Events = eventConfig{
			Enabled:   true,
			Broker:    "owned-control-broker",
			VPN:       "default",
			Topic:     topic,
			Endpoints: map[string]string{"amqp": endpoint},
		}
		if err := json.NewEncoder(w).Encode(cfg); err != nil {
			t.Error(err)
		}
	}))
	defer api.Close()

	credentials := func(string) (string, string, error) { return username, password, nil }
	client, err := Open(context.Background(), Options{
		ControllerURL: api.URL,
		OutboxPath:    filepath.Join(t.TempDir(), "control.outbox.db"),
		Credentials:   credentials,
		PollInterval:  60 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var sender dispatch.Sender
	if !externalPublisher {
		publisherURI, err := client.uri("owned-control-broker", map[string]string{"amqp": endpoint})
		if err != nil {
			t.Fatal(err)
		}
		transport := amqptransport.New()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		sender, err = transport.Sender(ctx, publisherURI)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		defer sender.Close()
	}

	baseline := configRequests.Load()
	revision.Store(1)
	started := time.Now()
	deadline := started.Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if sender != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = sender.Send(ctx, dispatch.Message{
				Address: "topic://" + topic,
				Body:    []byte(`{"untrusted_revision":18446744073709551615}`),
			})
			cancel()
			if err != nil {
				// The broker rejects unroutable guaranteed sends until the production control receiver
				// finishes attaching. Retry inside the overall bound; never treat that NACK as sent.
				time.Sleep(25 * time.Millisecond)
				continue
			}
		}
		for until := time.Now().Add(250 * time.Millisecond); time.Now().Before(until); {
			client.mu.Lock()
			trusted := client.cfg.Revision
			client.mu.Unlock()
			if trusted == 1 && configRequests.Load() > baseline {
				t.Logf(
					"real AMQP hint refreshed trusted HTTP revision in %s (periodic interval 60s)",
					time.Since(started).Round(time.Millisecond),
				)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Fatalf(
		"real AMQP hint did not refresh revision before 10s; requests=%d status=%+v",
		configRequests.Load(), client.Status(),
	)
}
