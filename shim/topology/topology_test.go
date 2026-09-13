package topology

import (
	"os"
	"path/filepath"
	"testing"
)

// TestTopologyInteropGolden proves the Go topology package parses the exact snapshot the Python
// control plane emits (testdata/topology_event.json, written by scaling-controller's
// ShardTopology.to_event) and computes identical rendezvous ownership. The expected owners are what
// Python's ShardTopology.owner returns for these keys; if the two implementations ever diverge, this
// fails. Same cross-language contract as the dispatch rule-spec golden.
func TestTopologyInteropGolden(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "topology_event.json"))
	if err != nil {
		t.Fatalf("read golden topology: %v", err)
	}
	topo, err := LoadTopology(data)
	if err != nil {
		t.Fatalf("LoadTopology: %v", err)
	}

	if topo.Shard != "orders" || topo.Gen() != 2 {
		t.Fatalf("shard/gen = %q/%d (want orders/2)", topo.Shard, topo.Gen())
	}
	if len(topo.AllHandoffs()) != 1 {
		t.Fatalf("handoffs = %d (want 1)", len(topo.AllHandoffs()))
	}
	if h := topo.AllHandoffs()[0]; h.FromBroker != "broker-a" || h.ToBroker != "broker-d" || h.EffectiveGen != 2 {
		t.Fatalf("handoff = %+v (want broker-a->broker-d @gen2)", h)
	}

	// Owners produced by the Python model for the same keys (draining broker-old never owns).
	want := map[string]string{
		"key-0":      "broker-d",
		"key-1":      "broker-d",
		"key-2":      "broker-b",
		"key-5":      "broker-c",
		"key-42":     "broker-a",
		"order-eu-1": "broker-c",
		"order-us-9": "broker-a",
		"alpha":      "broker-c",
		"zulu":       "broker-c",
	}
	for key, wantOwner := range want {
		got, ok := topo.Owner(key)
		if !ok {
			t.Errorf("Owner(%q): no owner", key)
			continue
		}
		if got != wantOwner {
			t.Errorf("Owner(%q) = %q, want %q (Go/Python rendezvous divergence)", key, got, wantOwner)
		}
	}

	// The draining broker must never be selected as an owner for any key.
	for key := range want {
		if got, _ := topo.Owner(key); got == "broker-old" {
			t.Errorf("Owner(%q) = broker-old, but a draining broker must not own keys", key)
		}
	}
}

func TestLoadTopologyRejectsUnknownVersion(t *testing.T) {
	_, err := LoadTopology([]byte(`{"version":999,"shard":"s","gen":1,"brokers":[],"handoffs":[]}`))
	if err == nil {
		t.Fatal("expected error for unknown topology version")
	}
}

func TestOwnerNoneWhenNoActiveBroker(t *testing.T) {
	data := []byte(`{"version":1,"shard":"s","gen":1,"emitted_at":"t","brokers":[{"broker_id":"b","state":"draining","endpoints":{}}],"handoffs":[]}`)
	topo, err := LoadTopology(data)
	if err != nil {
		t.Fatalf("LoadTopology: %v", err)
	}
	if _, ok := topo.Owner("k"); ok {
		t.Fatal("expected no owner when no broker is active")
	}
}

func TestOwnerIsDeterministicAndTotal(t *testing.T) {
	data := []byte(`{"version":1,"shard":"s","gen":1,"emitted_at":"t","brokers":[` +
		`{"broker_id":"b1","state":"active","endpoints":{"amqp":"amqp://b1"}},` +
		`{"broker_id":"b2","state":"active","endpoints":{"amqp":"amqp://b2"}},` +
		`{"broker_id":"b3","state":"active","endpoints":{"amqp":"amqp://b3"}}],"handoffs":[]}`)
	topo, err := LoadTopology(data)
	if err != nil {
		t.Fatalf("LoadTopology: %v", err)
	}
	for i := 0; i < 200; i++ {
		key := "k" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		o1, ok1 := topo.Owner(key)
		o2, ok2 := topo.Owner(key)
		if !ok1 || !ok2 || o1 != o2 {
			t.Fatalf("Owner(%q) not deterministic: %q/%v vs %q/%v", key, o1, ok1, o2, ok2)
		}
	}
}
