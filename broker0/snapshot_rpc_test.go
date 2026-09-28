package broker0

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

type rpcDelivery struct {
	request                       control.SnapshotRequest
	reply, correlation, principal string
	acked, rejected               atomic.Bool
}

func (d *rpcDelivery) Request() control.SnapshotRequest { return d.request }
func (d *rpcDelivery) ReplyTo() string                  { return d.reply }
func (d *rpcDelivery) CorrelationID() string            { return d.correlation }
func (d *rpcDelivery) AuthenticatedParticipant() (string, bool) {
	return d.principal, d.principal != ""
}
func (d *rpcDelivery) Ack(context.Context) error    { d.acked.Store(true); return nil }
func (d *rpcDelivery) Reject(context.Context) error { d.rejected.Store(true); return nil }

type rpcReceiver struct {
	delivery *rpcDelivery
	sent     bool
}

func (r *rpcReceiver) ReceiveSnapshotRequest(ctx context.Context) (SnapshotRequestDelivery, error) {
	if !r.sent {
		r.sent = true
		return r.delivery, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (*rpcReceiver) Close() error { return nil }

type rpcPublisher struct {
	called bool
	reply  string
}

func (p *rpcPublisher) PublishSnapshotResponse(_ context.Context, reply, _ string, _ control.SnapshotResponse) error {
	p.called = true
	p.reply = reply
	return nil
}

type rpcSource struct {
	snapshot control.MembershipSnapshot
	ok       bool
}

func (s rpcSource) SnapshotForGroup(string) (control.MembershipSnapshot, bool) {
	return s.snapshot, s.ok
}

func rpcRequest() control.SnapshotRequest {
	return control.SnapshotRequest{Version: control.BootstrapProtocolVersion, CorrelationID: "request-1", Namespace: "swlb", Group: "orders", Participant: "publisher-1", Role: control.RolePublisher, RequestedAt: time.Unix(1, 0).UTC()}
}
func TestSnapshotServerRejectsCrossGroupReplyTo(t *testing.T) {
	d := &rpcDelivery{request: rpcRequest(), reply: "swlb.ctl.other.snapshot-replies.publisher.publisher-1", correlation: "request-1", principal: "publisher-1"}
	p := &rpcPublisher{}
	s := &SnapshotRequestServer{Receiver: &rpcReceiver{delivery: d}, Publisher: p, Source: rpcSource{}, Authorize: func(control.SnapshotRequest, string) error { return nil }, Replies: SnapshotReplyResolverFunc(func(control.SnapshotRequest) (string, error) {
		return "swlb.ctl.orders.snapshot-replies.publisher.publisher-1", nil
	})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	for !d.rejected.Load() {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if p.called || !d.rejected.Load() {
		t.Fatal("malicious reply-to was published or not rejected")
	}
}
func TestSnapshotServerSettlesUnavailableState(t *testing.T) {
	d := &rpcDelivery{request: rpcRequest(), reply: "reply", correlation: "request-1", principal: "publisher-1"}
	s := &SnapshotRequestServer{Receiver: &rpcReceiver{delivery: d}, Publisher: &rpcPublisher{}, Source: rpcSource{ok: false}, Authorize: func(control.SnapshotRequest, string) error { return nil }, Replies: SnapshotReplyResolverFunc(func(control.SnapshotRequest) (string, error) { return "reply", nil })}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	for !d.rejected.Load() {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run=%v", err)
	}
}
