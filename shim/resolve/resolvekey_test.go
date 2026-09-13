package resolve

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/solacese/solace-broker-autoscale/shim/topology"
)

// A two-broker gen-3 snapshot and a superseding gen-4 that adds a third broker.
const topoGen3 = `{"version":1,"shard":"orders","gen":3,"emitted_at":"t","brokers":[` +
	`{"broker_id":"b1","state":"active","endpoints":{"amqp":"amqp://b1:5672"}},` +
	`{"broker_id":"b2","state":"active","endpoints":{"amqp":"amqp://b2:5672"}}],"handoffs":[]}`

const topoGen4 = `{"version":1,"shard":"orders","gen":4,"emitted_at":"t","brokers":[` +
	`{"broker_id":"b1","state":"active","endpoints":{"amqp":"amqp://b1:5672"}},` +
	`{"broker_id":"b2","state":"active","endpoints":{"amqp":"amqp://b2:5672"}},` +
	`{"broker_id":"b3","state":"active","endpoints":{"amqp":"amqp://b3:5672"}}],"handoffs":[]}`

func mustLoad(t *testing.T, s string) *topology.Topology {
	t.Helper()
	topo, err := topology.LoadTopology([]byte(s))
	if err != nil {
		t.Fatalf("LoadTopology: %v", err)
	}
	return topo
}

func TestApplyEventsWinByGeneration(t *testing.T) {
	r := New("https://assign.example.com")
	if !r.Apply(mustLoad(t, topoGen4)) {
		t.Fatal("first apply should install gen 4")
	}
	if r.Apply(mustLoad(t, topoGen3)) {
		t.Fatal("older gen 3 must not regress gen 4")
	}
	if r.Apply(mustLoad(t, topoGen4)) {
		t.Fatal("equal gen 4 must be ignored (idempotent)")
	}
	// The held topology is still gen 4 with three brokers.
	loc, err := r.ResolveKey(context.Background(), "orders", "key-0", "amqp")
	if err != nil {
		t.Fatal(err)
	}
	if loc.Gen != 4 {
		t.Errorf("gen = %d, want 4", loc.Gen)
	}
}

func TestResolveKeyFromAppliedTopologyDoesNotFetch(t *testing.T) {
	r := New("https://assign.example.com")
	r.Fetch = func(_ context.Context, _ string) ([]byte, error) {
		t.Fatal("ResolveKey must not fetch when a topology is already applied")
		return nil, nil
	}
	r.Apply(mustLoad(t, topoGen4))
	loc, err := r.ResolveKey(context.Background(), "orders", "key-0", "amqp")
	if err != nil {
		t.Fatal(err)
	}
	if loc.BrokerID == "" || !strings.HasPrefix(loc.Endpoint, "amqp://") {
		t.Errorf("bad location %+v", loc)
	}
	if loc.Endpoint != "amqp://"+loc.BrokerID+":5672" {
		t.Errorf("endpoint %q does not match owner %q", loc.Endpoint, loc.BrokerID)
	}
}

func TestResolveKeyColdStartsOverHTTP(t *testing.T) {
	calls := 0
	r := New("https://assign.example.com")
	r.Fetch = func(_ context.Context, u string) ([]byte, error) {
		calls++
		if !strings.Contains(u, "/topology?") || !strings.Contains(u, "shard=orders") {
			t.Errorf("cold start should hit /topology?shard=orders, got %q", u)
		}
		return []byte(topoGen4), nil
	}
	loc, err := r.ResolveKey(context.Background(), "orders", "key-0", "amqp")
	if err != nil {
		t.Fatal(err)
	}
	if loc.Gen != 4 {
		t.Errorf("gen = %d, want 4", loc.Gen)
	}
	// A second lookup uses the now-applied topology, not a second fetch.
	if _, err := r.ResolveKey(context.Background(), "orders", "key-1", "amqp"); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("expected exactly 1 cold-start fetch, got %d", calls)
	}
}

func TestResolveKeyFailsOpenToCachedTopology(t *testing.T) {
	down := false
	r := New("https://assign.example.com")
	r.Fetch = func(_ context.Context, _ string) ([]byte, error) {
		if down {
			return nil, errors.New("bus and service both down")
		}
		return []byte(topoGen4), nil
	}
	// Prime the cache via a cold start.
	if _, err := r.ResolveKey(context.Background(), "orders", "key-0", "amqp"); err != nil {
		t.Fatal(err)
	}
	down = true
	// Even though a fresh fetch would fail, ResolveKey serves the cached topology.
	loc, err := r.ResolveKey(context.Background(), "orders", "key-9", "amqp")
	if err != nil {
		t.Fatalf("expected fail-open to cached topology, got %v", err)
	}
	if loc.Gen != 4 {
		t.Errorf("served gen %d, want cached 4", loc.Gen)
	}
}

func TestResolveKeyColdWithNoCacheErrors(t *testing.T) {
	r := New("https://assign.example.com")
	r.Fetch = func(_ context.Context, _ string) ([]byte, error) {
		return nil, errors.New("connection refused")
	}
	if _, err := r.ResolveKey(context.Background(), "cold", "k", "amqp"); err == nil {
		t.Error("expected error for a cold shard with no cache and fetch down")
	}
}

func TestResolveKeyGoldenParityWithSpineTopology(t *testing.T) {
	// Apply the same golden the topology package's interop test uses; ResolveKey must pick the same
	// owner the Python model computed (rendezvous parity end to end through the resolver).
	r := New("https://assign.example.com")
	golden := mustLoad(t, spineGoldenOrders)
	r.Apply(golden)
	want := map[string]string{"key-0": "broker-d", "key-2": "broker-b", "key-5": "broker-c", "key-42": "broker-a"}
	for key, owner := range want {
		loc, err := r.ResolveKey(context.Background(), "orders", key, "amqp")
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if loc.BrokerID != owner {
			t.Errorf("ResolveKey(%q) owner = %q, want %q", key, loc.BrokerID, owner)
		}
	}
}

// The golden orders snapshot (gen 2, broker-a..d active, broker-old draining), inline so the resolver
// test does not depend on the testdata path layout.
const spineGoldenOrders = `{"version":1,"shard":"orders","gen":2,"emitted_at":"2026-01-01T00:00:02Z","brokers":[` +
	`{"broker_id":"broker-a","state":"active","endpoints":{"amqp":"amqp://broker-a:5672"}},` +
	`{"broker_id":"broker-b","state":"active","endpoints":{"amqp":"amqp://broker-b:5672"}},` +
	`{"broker_id":"broker-c","state":"active","endpoints":{"amqp":"amqp://broker-c:5672"}},` +
	`{"broker_id":"broker-d","state":"active","endpoints":{"amqp":"amqp://broker-d:5672"}},` +
	`{"broker_id":"broker-old","state":"draining","endpoints":{"amqp":"amqp://broker-old:5672"}}],` +
	`"handoffs":[{"from_broker":"broker-a","to_broker":"broker-d","effective_gen":2,"reason":"scale-up"}]}`
