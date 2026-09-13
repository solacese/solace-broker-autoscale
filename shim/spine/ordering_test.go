package spine

import (
	"fmt"
	"testing"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/topology"
)

// topo builds a topology for a shard at a generation from a list of active broker ids plus optional
// handoffs. Test-only constructor; the wire form is proven by the topology package's golden test.
func topo(shard string, gen int, active []string, handoffs ...topology.Handoff) *topology.Topology {
	brokers := make([]topology.BrokerRef, 0, len(active))
	for _, id := range active {
		brokers = append(brokers, topology.BrokerRef{
			BrokerID:  id,
			State:     topology.StateActive,
			Endpoints: map[string]string{"amqp": "amqp://" + id + ":5672"},
		})
	}
	return &topology.Topology{
		Version:    topology.Version,
		Shard:      shard,
		Generation: gen,
		Brokers:    brokers,
		Handoffs:   handoffs,
	}
}

// findMovedKey returns a key whose owner differs between prev and next, and its two owners. It scans
// a deterministic key space so the test is reproducible. Rendezvous guarantees such a key exists when
// membership grows (a fraction ~1/N moves), so this is not flaky.
func findMovedKey(t *testing.T, prev, next *topology.Topology) (key, from, to string) {
	t.Helper()
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key-%d", i)
		po, ok1 := prev.Owner(k)
		no, ok2 := next.Owner(k)
		if ok1 && ok2 && po != no {
			return k, po, no
		}
	}
	t.Fatalf("no key moved between the two topologies; membership change did not reassign anything")
	return "", "", ""
}

// TestFencerHoldsMovedKeyOnOldBrokerDuringGrace proves drain-before-cutover: during the grace window a
// moved key is still routed to its OLD broker (Draining), and only after the window does it cut to the
// new owner stamped with the new generation.
func TestFencerHoldsMovedKeyOnOldBrokerDuringGrace(t *testing.T) {
	prev := topo("orders", 1, []string{"broker-a", "broker-b", "broker-c"})
	next := topo("orders", 2, []string{"broker-a", "broker-b", "broker-c", "broker-d"})
	key, from, to := findMovedKey(t, prev, next)

	clock := &fakeClock{now: time.Unix(1000, 0)}
	f := NewFencer(30 * time.Second)
	f.Now = clock.get
	// The topology carries a handoff from the moved key's old owner to its new owner at gen 2.
	next.Handoffs = []topology.Handoff{{FromBroker: from, ToBroker: to, EffectiveGen: 2, Reason: "scale-up"}}
	f.Observe(prev, next)

	// Immediately after the event: still within grace, so route to the OLD broker.
	r, ok := f.Route(prev, next, key)
	if !ok {
		t.Fatalf("expected a route for moved key %q", key)
	}
	if r.BrokerID != from || !r.Draining {
		t.Fatalf("during grace, route = %+v; want old broker %q draining", r, from)
	}
	if r.Gen >= next.Gen() {
		t.Fatalf("draining route gen %d must be below the cutover gen %d so the listener orders it first", r.Gen, next.Gen())
	}

	// After the grace window elapses: cut to the new owner at the new generation.
	clock.now = clock.now.Add(31 * time.Second)
	r, ok = f.Route(prev, next, key)
	if !ok {
		t.Fatalf("expected a route after grace")
	}
	if r.BrokerID != to || r.Draining {
		t.Fatalf("after grace, route = %+v; want new broker %q not draining", r, to)
	}
	if r.Gen != next.Gen() {
		t.Fatalf("after cutover, gen = %d; want %d", r.Gen, next.Gen())
	}
}

// TestFencerLeavesUnmovedKeysAlone proves the common case is cheap: a key whose owner did not change
// routes straight to its owner at the current generation, no grace, no draining.
func TestFencerLeavesUnmovedKeysAlone(t *testing.T) {
	prev := topo("orders", 1, []string{"broker-a", "broker-b", "broker-c"})
	next := topo("orders", 2, []string{"broker-a", "broker-b", "broker-c", "broker-d"})

	// Find a key that did NOT move.
	var stable string
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key-%d", i)
		po, _ := prev.Owner(k)
		no, _ := next.Owner(k)
		if po == no {
			stable = k
			break
		}
	}
	if stable == "" {
		t.Fatalf("expected at least one stable key")
	}

	clock := &fakeClock{now: time.Unix(1000, 0)}
	f := NewFencer(30 * time.Second)
	f.Now = clock.get
	f.Observe(prev, next)

	r, ok := f.Route(prev, next, stable)
	if !ok {
		t.Fatalf("expected a route for stable key")
	}
	if r.Draining {
		t.Fatalf("stable key must not drain: %+v", r)
	}
	if r.Gen != next.Gen() {
		t.Fatalf("stable key gen = %d, want %d", r.Gen, next.Gen())
	}
}

// TestFencerIgnoresDuplicateGeneration proves a re-delivered snapshot at the same generation does not
// reopen a grace window (idempotent, matching the sink's generation-win).
func TestFencerIgnoresDuplicateGeneration(t *testing.T) {
	prev := topo("orders", 1, []string{"broker-a", "broker-b", "broker-c"})
	next := topo("orders", 2, []string{"broker-a", "broker-b", "broker-c", "broker-d"})
	key, from, to := findMovedKey(t, prev, next)
	next.Handoffs = []topology.Handoff{{FromBroker: from, ToBroker: to, EffectiveGen: 2, Reason: "scale-up"}}

	clock := &fakeClock{now: time.Unix(1000, 0)}
	f := NewFencer(30 * time.Second)
	f.Now = clock.get
	f.Observe(prev, next)

	// Grace elapses and we cut over.
	clock.now = clock.now.Add(31 * time.Second)
	if r, _ := f.Route(prev, next, key); r.BrokerID != to {
		t.Fatalf("expected cutover to %q, got %+v", to, r)
	}
	// A duplicate of the same-gen snapshot must not reopen the window.
	f.Observe(prev, next)
	if r, _ := f.Route(prev, next, key); r.Draining {
		t.Fatalf("duplicate same-gen event must not reopen a grace window: %+v", r)
	}
}

// TestReordererNoReorderOnScaleUp is the ordering-first proof: across a scale-up handoff, a key's
// messages are released in nondecreasing (gen, seq) order and a post-cutover (higher-gen) message
// never overtakes an in-flight pre-cutover (lower-gen) message for the same key.
func TestReordererNoReorderOnScaleUp(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	r := NewReorderer(30 * time.Second)
	r.Now = clock.get

	key := "order-eu-1"
	var released []FencedMessage

	// Pre-cutover messages arrive at gen 1 (from the old broker), seq 1..3.
	for seq := int64(1); seq <= 3; seq++ {
		released = append(released, r.Admit(FencedMessage{Key: key, Gen: 1, Seq: seq})...)
	}
	// A post-cutover message at gen 2 arrives out of order (new broker is faster) BEFORE the gen-1
	// stream has had time to drain. It must be held.
	released = append(released, r.Admit(FencedMessage{Key: key, Gen: 2, Seq: 4})...)

	// Nothing should have been released yet: the gen-1 head is still within its hold window, so the
	// gen-2 message cannot jump ahead of it.
	if len(released) != 0 {
		t.Fatalf("nothing should release while gen-1 is still in its hold window, got %d", len(released))
	}

	// Time passes past the hold: now the buffer drains in order.
	clock.now = clock.now.Add(31 * time.Second)
	released = append(released, r.Tick()...)

	if len(released) != 4 {
		t.Fatalf("expected all 4 messages released after hold, got %d", len(released))
	}
	assertNondecreasing(t, released)
	// Specifically: the three gen-1 messages precede the gen-2 message.
	if released[3].Gen != 2 || released[3].Seq != 4 {
		t.Fatalf("gen-2 message must be released last, got %+v", released[3])
	}
}

// TestReordererDropsLateStraggler proves a straggler from an already-drained old broker (gen below
// what the key has released) is dropped, not reinserted behind newer traffic.
func TestReordererDropsLateStraggler(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	r := NewReorderer(0) // release immediately in arrival order for this test
	r.Now = clock.get
	key := "order-eu-1"

	// gen-2 message releases immediately (no older gen present, zero hold).
	out := r.Admit(FencedMessage{Key: key, Gen: 2, Seq: 10})
	if len(out) != 1 {
		t.Fatalf("expected the gen-2 message to release, got %d", len(out))
	}
	// A late gen-1 straggler now arrives: it is behind what has been released and must be dropped.
	out = r.Admit(FencedMessage{Key: key, Gen: 1, Seq: 5})
	if len(out) != 0 {
		t.Fatalf("late straggler (gen below released) must be dropped, got %d", len(out))
	}
}

// TestReordererIndependentKeys proves keys are ordered independently: one key's hold does not stall
// another key's stream.
func TestReordererIndependentKeys(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	r := NewReorderer(0)
	r.Now = clock.get

	a := r.Admit(FencedMessage{Key: "key-a", Gen: 1, Seq: 1})
	b := r.Admit(FencedMessage{Key: "key-b", Gen: 1, Seq: 1})
	if len(a) != 1 || a[0].Key != "key-a" {
		t.Fatalf("key-a should release independently, got %+v", a)
	}
	if len(b) != 1 || b[0].Key != "key-b" {
		t.Fatalf("key-b should release independently, got %+v", b)
	}
}

func assertNondecreasing(t *testing.T, msgs []FencedMessage) {
	t.Helper()
	for i := 1; i < len(msgs); i++ {
		prev, cur := msgs[i-1], msgs[i]
		if cur.Gen < prev.Gen || (cur.Gen == prev.Gen && cur.Seq < prev.Seq) {
			t.Fatalf("out-of-order release at %d: %+v then %+v", i, prev, cur)
		}
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) get() time.Time { return c.now }
