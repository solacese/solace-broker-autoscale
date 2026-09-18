package dispatch_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
)

type blockedTransport struct {
	entered chan dispatch.Message
	release chan struct{}
	fail    bool
}

func (t *blockedTransport) Sender(context.Context, string) (dispatch.Sender, error) { return t, nil }
func (t *blockedTransport) Receiver(context.Context, string, string) (dispatch.Receiver, error) {
	return nil, errors.New("unused")
}
func (t *blockedTransport) Close() error { return nil }
func (t *blockedTransport) Send(ctx context.Context, m dispatch.Message) error {
	select {
	case t.entered <- m:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-t.release:
		if t.fail {
			return errors.New("broker rejected")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func outcome(t *testing.T, ch <-chan dispatch.Outcome) dispatch.Outcome {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(3 * time.Second):
		t.Fatal("missing outcome")
		return dispatch.Outcome{}
	}
}
func TestAsyncBackpressureCopiesInputAndDoesNotWaitForReceipt(t *testing.T) {
	transport := &blockedTransport{entered: make(chan dispatch.Message, 3), release: make(chan struct{})}
	pub := dispatch.NewPublisher(testPlan(t), staticResolver(uriMap), transport, dispatch.RetryPolicy{})
	async, err := dispatch.NewAsyncPublisher(pub, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer async.Close()
	body := []byte("original")
	first, err := async.Publish(context.Background(), "telemetry/a", body, nil)
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 'X'
	sent := <-transport.entered
	if string(sent.Body) != "original" {
		t.Fatal("payload was not copied")
	}
	second, err := async.Publish(context.Background(), "telemetry/b", []byte("next"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := async.Publish(context.Background(), "telemetry/c", nil, nil); !errors.Is(err, dispatch.ErrBackpressure) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-first:
		t.Fatal("receipt completed before broker")
	default:
	}
	close(transport.release)
	if outcome(t, first).Err != nil || outcome(t, second).Err != nil {
		t.Fatal("delivery failed")
	}
}
func TestAsyncFailureStopsPendingWorkAndCloseCompletesOutcomes(t *testing.T) {
	transport := &blockedTransport{entered: make(chan dispatch.Message, 3), release: make(chan struct{}), fail: true}
	async, _ := dispatch.NewAsyncPublisher(dispatch.NewPublisher(testPlan(t), staticResolver(uriMap), transport, dispatch.RetryPolicy{}), 1, 2)
	first, _ := async.Publish(context.Background(), "telemetry/a", nil, nil)
	<-transport.entered
	second, _ := async.Publish(context.Background(), "telemetry/b", nil, nil)
	close(transport.release)
	if outcome(t, first).Err == nil {
		t.Fatal("lost send failure")
	}
	if !errors.Is(outcome(t, second).Err, dispatch.ErrPublisherStopped) {
		t.Fatal("pending work overtook failure")
	}
	if _, err := async.Publish(context.Background(), "telemetry/c", nil, nil); !errors.Is(err, dispatch.ErrPublisherStopped) {
		t.Fatal(err)
	}
	if err := async.Close(); err != nil {
		t.Fatal(err)
	}
}
