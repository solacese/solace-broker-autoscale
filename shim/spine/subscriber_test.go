package spine

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
	"github.com/solacese/solace-broker-autoscale/shim/topology"
	"github.com/solacese/solace-broker-autoscale/shim/transport/memory"
)

// recordingSink is a TopologySink that records what it was asked to apply, with generation-win so it
// behaves like the real resolver: a snapshot at gen <= the last applied is ignored.
type recordingSink struct {
	mu      sync.Mutex
	applied []*topology.Topology
	gen     int
}

func (s *recordingSink) Apply(t *topology.Topology) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.Gen() <= s.gen {
		return false
	}
	s.gen = t.Gen()
	s.applied = append(s.applied, t)
	return true
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

func goldenBody(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../testdata/topology_event.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	return b
}

// TestSubscriberAppliesSnapshotOnEvent proves the control path is a push: a snapshot published to the
// reserved topic reaches the sink without any poll.
func TestSubscriberAppliesSnapshotOnEvent(t *testing.T) {
	tport := memory.New()
	sink := &recordingSink{}
	applied := make(chan bool, 1)
	sub := NewSubscriber(tport, sink)
	sub.OnApply = func(_ string, _ int, ok bool) { applied <- ok }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sub.Run(ctx, "amqp://b1", WildcardTopologySource) }()

	// Publish a topology snapshot on the reserved topic for shard "orders". The memory transport only
	// delivers to receivers already registered, and Run registers asynchronously, so republish (a
	// last-value snapshot is idempotent - generation-win dedups) until the first apply lands.
	sender, err := tport.Sender(ctx, "amqp://b1")
	if err != nil {
		t.Fatalf("sender: %v", err)
	}
	body := goldenBody(t)
	deadline := time.After(2 * time.Second)
	for {
		if err := sender.Send(ctx, dispatch.Message{Address: ControlSource("orders"), Body: body}); err != nil {
			t.Fatalf("send: %v", err)
		}
		select {
		case ok := <-applied:
			if !ok {
				t.Fatalf("expected snapshot to be applied")
			}
			if got := sink.count(); got != 1 {
				t.Fatalf("applied count = %d, want 1", got)
			}
			return
		case <-deadline:
			t.Fatalf("timed out waiting for snapshot to be applied")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestSubscriberFailOpenWhenBusDown proves that if the control link cannot be opened the subscriber
// returns without delivering anything - the sink keeps serving its last applied topology.
func TestSubscriberFailOpenWhenBusDown(t *testing.T) {
	sink := &recordingSink{}
	sub := NewSubscriber(failingTransport{}, sink)
	err := sub.Run(context.Background(), "amqp://down", WildcardTopologySource)
	if err == nil {
		t.Fatalf("expected an error opening the receiver on a down bus")
	}
	if sink.count() != 0 {
		t.Fatalf("nothing should have been applied when the bus is down, got %d", sink.count())
	}
}

// TestSubscriberSkipsMalformedControl proves a bad event is skipped (reported via OnError) and never
// takes the path down: a good event after it is still applied.
func TestSubscriberSkipsMalformedControl(t *testing.T) {
	tport := memory.New()
	sink := &recordingSink{}
	errCh := make(chan struct{}, 64)
	appliedCh := make(chan struct{}, 64)
	sub := NewSubscriber(tport, sink)
	sub.OnError = func(error) { errCh <- struct{}{} }
	sub.OnApply = func(_ string, _ int, applied bool) {
		if applied {
			appliedCh <- struct{}{}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sub.Run(ctx, "amqp://b1", WildcardTopologySource) }()

	sender, _ := tport.Sender(ctx, "amqp://b1")
	malformed := dispatch.Message{Address: ControlSource("orders"), Body: []byte("{not json")}

	// Send a malformed event until the receiver is live and reports it (proves malformed is handled
	// without crashing). Republishing malformed control is harmless - each is independently skipped.
	waitFor(t, errCh, func() { _ = sender.Send(ctx, malformed) }, "malformed event to be reported")

	// Now a valid snapshot must still be applied: a bad event did not take the path down.
	valid := dispatch.Message{Address: ControlSource("orders"), Body: goldenBody(t)}
	waitFor(t, appliedCh, func() { _ = sender.Send(ctx, valid) }, "valid snapshot to be applied after a malformed one")

	if sink.count() != 1 {
		t.Fatalf("applied count = %d, want 1 (the valid snapshot survived the malformed one)", sink.count())
	}
}

// TestSubscriberCleanShutdown proves that cancelling the context ends Run with no error.
func TestSubscriberCleanShutdown(t *testing.T) {
	tport := memory.New()
	sub := NewSubscriber(tport, &recordingSink{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx, "amqp://b1", WildcardTopologySource) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown should return nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after context cancel")
	}
}

// waitFor repeatedly runs send until a signal arrives on ch or the deadline passes. It absorbs the
// memory transport's asynchronous receiver registration without a fixed sleep.
func waitFor(t *testing.T, ch <-chan struct{}, send func(), what string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		send()
		select {
		case <-ch:
			return
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

type failingTransport struct{}

func (failingTransport) Sender(context.Context, string) (dispatch.Sender, error) {
	return nil, errors.New("bus down")
}

func (failingTransport) Receiver(context.Context, string, string) (dispatch.Receiver, error) {
	return nil, errors.New("bus down")
}
