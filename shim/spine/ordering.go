package spine

import (
	"sort"
	"sync"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/topology"
)

// Route is the fencer's decision for one message of a partition key: which broker to send it to and
// which generation to stamp as saas_gen. During a handoff the fencer keeps returning the OLD broker
// (still draining) for a bounded grace period, then cuts to the NEW broker - so a key's stream is
// never split across two brokers at once, which is what preserves per-key order.
type Route struct {
	BrokerID string
	Gen      int
	// Draining is true while the key is still being sent to its pre-handoff broker during the grace
	// window (informational; the send still happens, to BrokerID).
	Draining bool
}

// Fencer is the publisher-side ordering guard. Given the current topology and a partition key it
// returns the broker to send to and the generation to stamp. It implements drain-before-cutover: on
// a topology change that moves a key, it holds the key on its old broker for HandoffGrace so in-flight
// messages there are delivered before any message goes to the new broker. Rendezvous hashing keeps
// the set of moved keys small (~1/(N+1) on a scale-up), so few keys ever enter a grace window.
//
// Pure but for the injected clock (Now). Safe for concurrent use.
type Fencer struct {
	// HandoffGrace bounds how long a moved key keeps going to its old broker before cutover. It must
	// exceed the worst-case in-flight delivery time on the old broker. Zero means cut immediately.
	HandoffGrace time.Duration
	// Now is the injected clock; nil -> time.Now. Tests supply a deterministic clock.
	Now func() time.Time

	mu sync.Mutex
	// gen the fencer has observed; a topology with a lower/equal gen is ignored (matches the sink).
	gen int
	// per-key cutover deadline: while now < deadline, keep sending to oldBroker[key].
	deadline  map[string]time.Time
	oldBroker map[string]string
	oldGen    map[string]int
}

// NewFencer builds a fencer with the given grace period.
func NewFencer(grace time.Duration) *Fencer {
	return &Fencer{
		HandoffGrace: grace,
		deadline:     map[string]time.Time{},
		oldBroker:    map[string]string{},
		oldGen:       map[string]int{},
	}
}

func (f *Fencer) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

// Observe records the handoffs a new topology introduces, opening a grace window for each moved key's
// owner change. It is called by the shim whenever a snapshot is applied (typically from the same
// event that updated the resolver). Older/equal generations are ignored so a duplicate event does not
// reopen a window. The mapping from handoff (broker pair) to the specific keys it moves is resolved
// lazily in Route via rendezvous, so Observe only needs the pairs and the effective generation.
func (f *Fencer) Observe(prev, next *topology.Topology) {
	if next == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if next.Gen() <= f.gen {
		return
	}
	f.gen = next.Gen()
	if prev == nil {
		return // cold start: nothing to drain
	}
	deadlineAt := f.now().Add(f.HandoffGrace)
	// A key is "moved" if its owner under prev differs from its owner under next. We do not enumerate
	// keys here; instead we remember, per handoff source broker, that keys formerly owned there must
	// drain. Route recomputes the specific key membership with rendezvous, which is cheap and exact.
	for _, h := range next.AllHandoffs() {
		// Mark the source broker as draining for keys that were on it. We key the grace window on the
		// (from) broker; Route checks whether this key's previous owner was that broker.
		f.deadline["@"+h.FromBroker] = deadlineAt
		f.oldBroker["@"+h.FromBroker] = h.FromBroker
		f.oldGen["@"+h.FromBroker] = h.EffectiveGen
	}
}

// Route returns where to send a message for key and which generation to stamp, given the currently
// applied topology. During a moved key's grace window it returns the old broker (Draining=true) so
// the key's in-flight stream on that broker drains before any message crosses to the new owner.
func (f *Fencer) Route(prev, next *topology.Topology, key string) (Route, bool) {
	if next == nil {
		return Route{}, false
	}
	owner, ok := next.Owner(key)
	if !ok {
		return Route{}, false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Was this key moved by a handoff, and are we still within its grace window?
	if prev != nil {
		prevOwner, hadPrev := prev.Owner(key)
		if hadPrev && prevOwner != owner {
			if dl, marked := f.deadline["@"+prevOwner]; marked && f.now().Before(dl) {
				// Still draining: keep the key on its old broker, stamped with the OLD generation so
				// the listener orders it before any post-cutover message for this key.
				return Route{BrokerID: prevOwner, Gen: f.oldGen["@"+prevOwner] - 1, Draining: true}, true
			}
		}
	}
	// Steady state or grace elapsed: send to the current owner, stamped with the current generation.
	return Route{BrokerID: owner, Gen: next.Gen()}, true
}

// FencedMessage is a message tagged with the generation it was published under, as recovered from the
// wire (the saas_gen property). The Reorderer orders a key's stream by this.
type FencedMessage struct {
	Key string
	Gen int
	Seq int64 // arrival sequence within the shim, breaks ties at equal gen (FIFO)
}

// Reorderer is the listener-side ordering guard. Across a handoff a key may briefly arrive from two
// brokers; the Reorderer releases a key's messages in nondecreasing generation order, holding a
// higher-generation message until no lower-generation message for that key can still be in flight
// (bounded by the same grace the publisher used). This guarantees a consumer sees a key's stream in
// the order the publisher committed it, even though it crossed brokers.
//
// It is a small, pure state machine: Admit records an arrival and returns the messages now safe to
// release, in order. Deterministic; the clock is injected.
type Reorderer struct {
	// HoldGrace bounds how long a key's higher-generation message waits for stragglers from the old
	// broker. It should match or exceed the publisher's HandoffGrace. Zero releases immediately in
	// arrival order (no reordering protection).
	HoldGrace time.Duration
	Now       func() time.Time

	mu sync.Mutex
	// per key: the highest generation released so far, and buffered not-yet-releasable messages.
	released map[string]int
	buffered map[string][]heldMsg
}

type heldMsg struct {
	msg       FencedMessage
	holdUntil time.Time
}

// NewReorderer builds a listener-side reorderer with the given hold grace.
func NewReorderer(grace time.Duration) *Reorderer {
	return &Reorderer{
		HoldGrace: grace,
		released:  map[string]int{},
		buffered:  map[string][]heldMsg{},
	}
}

func (r *Reorderer) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Admit records an arriving message and returns the messages for its key that are now safe to release,
// in nondecreasing (gen, seq) order. A message whose generation is lower than one already released for
// its key is dropped as a late straggler from a drained broker (it was already superseded in order).
func (r *Reorderer) Admit(m FencedMessage) []FencedMessage {
	r.mu.Lock()
	defer r.mu.Unlock()

	rel := r.released[m.Key]
	if m.Gen < rel {
		// A straggler from an old broker for a key that has already advanced. Its ordered position is
		// behind what the consumer has already seen; dropping it preserves per-key order.
		return nil
	}
	r.buffered[m.Key] = append(r.buffered[m.Key], heldMsg{msg: m, holdUntil: r.now().Add(r.HoldGrace)})
	return r.drain(m.Key)
}

// Tick releases any messages whose hold has expired (call periodically so a quiet key still drains).
func (r *Reorderer) Tick() []FencedMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []FencedMessage
	for key := range r.buffered {
		out = append(out, r.drain(key)...)
	}
	return out
}

// drain releases from the front of a key's buffer while the head is safe. The head is safe when its
// hold has expired OR it is at/after the highest generation already released (no older gen can precede
// it). Messages are released in (gen, seq) order.
func (r *Reorderer) drain(key string) []FencedMessage {
	buf := r.buffered[key]
	if len(buf) == 0 {
		return nil
	}
	sort.SliceStable(buf, func(i, j int) bool {
		if buf[i].msg.Gen != buf[j].msg.Gen {
			return buf[i].msg.Gen < buf[j].msg.Gen
		}
		return buf[i].msg.Seq < buf[j].msg.Seq
	})

	now := r.now()
	var out []FencedMessage
	i := 0
	for i < len(buf) {
		head := buf[i]
		// Release the head when its generation is not behind what we've released (in-order), and either
		// its hold elapsed or there is nothing older that could still arrive for this key. Because the
		// buffer is sorted, if the head's hold elapsed it is the earliest possible next message.
		if head.msg.Gen >= r.released[key] && !now.Before(head.holdUntil) {
			out = append(out, head.msg)
			r.released[key] = head.msg.Gen
			i++
			continue
		}
		break
	}
	if i > 0 {
		r.buffered[key] = append([]heldMsg(nil), buf[i:]...)
	} else {
		r.buffered[key] = buf
	}
	return out
}
