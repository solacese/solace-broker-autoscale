package broker0

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

func TestCommandHandlerPublishesRecordsThenAcknowledges(t *testing.T) {
	issuedAt := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	command := control.CommandEnvelope{
		Version: control.ProtocolVersion, MessageID: "command-1", Namespace: "swlb", Group: "flight-operations",
		TransitionID: "transition-1", Epoch: 2, Phase: control.PhasePaused, Participant: "publisher-1",
		Role: control.RolePublisher, IssuedAt: issuedAt, Deadline: issuedAt.Add(time.Minute),
	}
	delivery := commandDelivery(t, command)
	var mu sync.Mutex
	var order []string
	operations := &fakeParticipantOperations{onRecord: func() { mu.Lock(); order = append(order, "record"); mu.Unlock() }}
	executor := commandExecutorFunc(func(context.Context, control.CommandEnvelope) error {
		mu.Lock()
		order = append(order, "execute")
		mu.Unlock()
		return nil
	})
	publisher := acknowledgementPublisherFunc(func(_ context.Context, acknowledgement control.AcknowledgementEnvelope) error {
		if err := acknowledgement.ValidateForCommand(command); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		order = append(order, "publish")
		mu.Unlock()
		return nil
	})
	delivery.onAck = func() { mu.Lock(); order = append(order, "ack"); mu.Unlock() }
	handler, err := NewCommandHandler("publisher-1", control.RolePublisher, "swlb", []string{"flight-operations"}, operations, executor, publisher)
	if err != nil {
		t.Fatal(err)
	}
	handler.Now = func() time.Time { return issuedAt.Add(time.Second) }
	if err := handler.Handle(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	want := []string{"execute", "publish", "record", "ack"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for index := range want {
		if order[index] != want[index] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestCommandHandlerFailureLeavesDeliveryUnacknowledged(t *testing.T) {
	issuedAt := time.Now().UTC()
	command := control.CommandEnvelope{
		Version: control.ProtocolVersion, MessageID: "command-2", Namespace: "swlb", Group: "flight-operations",
		TransitionID: "transition-2", Epoch: 2, Phase: control.PhaseActive, Participant: "subscriber-1",
		Role: control.RoleSubscriber, IssuedAt: issuedAt, Deadline: issuedAt.Add(time.Minute),
	}
	delivery := commandDelivery(t, command)
	handler, err := NewCommandHandler("subscriber-1", control.RoleSubscriber, "swlb", []string{"flight-operations"}, &fakeParticipantOperations{}, commandExecutorFunc(func(context.Context, control.CommandEnvelope) error { return nil }), acknowledgementPublisherFunc(func(context.Context, control.AcknowledgementEnvelope) error { return errors.New("publish failed") }))
	if err != nil {
		t.Fatal(err)
	}
	handler.Now = func() time.Time { return issuedAt.Add(time.Second) }
	if err := handler.Handle(context.Background(), delivery); err == nil {
		t.Fatal("expected acknowledgement publication failure")
	}
	if delivery.acked {
		t.Fatal("failed command delivery was acknowledged")
	}
}

func TestCommandInboxRetriesTransientCommandWithoutRedelivery(t *testing.T) {
	issuedAt := time.Now().UTC()
	command := control.CommandEnvelope{
		Version: control.ProtocolVersion, MessageID: "command-retry", Namespace: "swlb", Group: "flight-operations",
		TransitionID: "transition-retry", Epoch: 2, Phase: control.PhaseActive, Participant: "publisher-1",
		Role: control.RolePublisher, IssuedAt: issuedAt, Deadline: issuedAt.Add(time.Minute),
	}
	delivery := commandDelivery(t, command)
	acked := make(chan struct{})
	delivery.onAck = func() { close(acked) }
	receiver := &singleParticipantReceiver{delivery: delivery}
	attempts := 0
	handler, err := NewCommandHandler("publisher-1", control.RolePublisher, "swlb", []string{"flight-operations"}, &fakeParticipantOperations{}, commandExecutorFunc(func(context.Context, control.CommandEnvelope) error {
		attempts++
		if attempts == 1 {
			return errors.New("snapshot not ready")
		}
		return nil
	}), acknowledgementPublisherFunc(func(context.Context, control.AcknowledgementEnvelope) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	handler.Now = func() time.Time { return issuedAt.Add(time.Second) }
	inbox, err := NewCommandInbox(receiver, handler)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- inbox.Run(ctx) }()
	select {
	case <-acked:
	case <-time.After(time.Second):
		t.Fatal("command was not retried and acknowledged")
	}
	cancel()
	<-result
	if attempts != 2 || receiver.receives != 1 {
		t.Fatalf("attempts=%d receives=%d", attempts, receiver.receives)
	}
}

func TestCommandInboxRejectsMalformedDeliveryAndContinues(t *testing.T) {
	malformed := &fakeParticipantDelivery{kind: KindCommand, operation: "bad", payload: []byte(`{"unknown":true}`)}
	issuedAt := time.Now().UTC()
	valid := commandDelivery(t, control.CommandEnvelope{
		Version: control.ProtocolVersion, MessageID: "good", Namespace: "swlb", Group: "flight-operations",
		TransitionID: "transition", Epoch: 2, Phase: control.PhaseActive, Participant: "publisher-1",
		Role: control.RolePublisher, IssuedAt: issuedAt, Deadline: issuedAt.Add(time.Minute),
	})
	acked := make(chan struct{})
	valid.onAck = func() { close(acked) }
	receiver := &sequenceParticipantReceiver{deliveries: []Delivery{malformed, valid}}
	handler, err := NewCommandHandler("publisher-1", control.RolePublisher, "swlb", []string{"flight-operations"}, &fakeParticipantOperations{}, commandExecutorFunc(func(context.Context, control.CommandEnvelope) error { return nil }), acknowledgementPublisherFunc(func(context.Context, control.AcknowledgementEnvelope) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	handler.Now = func() time.Time { return issuedAt.Add(time.Second) }
	inbox, err := NewCommandInbox(receiver, handler)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- inbox.Run(ctx) }()
	select {
	case <-acked:
	case <-time.After(time.Second):
		t.Fatal("valid command after malformed delivery was not processed")
	}
	cancel()
	<-result
	if !malformed.rejected || malformed.acked {
		t.Fatalf("malformed rejected=%t acknowledged=%t", malformed.rejected, malformed.acked)
	}
}

func TestCommandHandlerReplayOnlyAcknowledges(t *testing.T) {
	issuedAt := time.Now().UTC()
	command := control.CommandEnvelope{
		Version: control.ProtocolVersion, MessageID: "command-3", Namespace: "swlb", Group: "flight-operations",
		TransitionID: "transition-3", Epoch: 2, Phase: control.PhaseActive, Participant: "publisher-1",
		Role: control.RolePublisher, IssuedAt: issuedAt, Deadline: issuedAt.Add(time.Minute),
	}
	delivery := commandDelivery(t, command)
	operations := &fakeParticipantOperations{completed: true}
	called := false
	handler, err := NewCommandHandler("publisher-1", control.RolePublisher, "swlb", []string{"flight-operations"}, operations, commandExecutorFunc(func(context.Context, control.CommandEnvelope) error { called = true; return nil }), acknowledgementPublisherFunc(func(context.Context, control.AcknowledgementEnvelope) error { called = true; return nil }))
	if err != nil {
		t.Fatal(err)
	}
	handler.Now = func() time.Time { return issuedAt.Add(time.Second) }
	if err := handler.Handle(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if called || !delivery.acked {
		t.Fatalf("called=%t acknowledged=%t", called, delivery.acked)
	}
}

func TestDurableUpdateSubscriberBindsExactQueueAndDefersAck(t *testing.T) {
	delivery := &fakeParticipantDelivery{kind: KindMembershipSnapshot, operation: "snapshot-1", payload: []byte(`{}`)}
	receiver := &fakeParticipantReceiver{deliveries: make(chan Delivery, 1)}
	receiver.deliveries <- delivery
	factory := &fakeParticipantReceiverFactory{receiver: receiver}
	subscriber := DurableUpdateSubscriber{Factory: factory, Resolver: UpdateQueueResolverFunc(func(_ context.Context, group, participant string) (string, error) {
		if group != "flight-operations" || participant != "publisher-1" {
			t.Fatalf("resolver scope = %q, %q", group, participant)
		}
		return "swlb.membership.publisher-1.flight-operations", nil
	})}
	subscription, err := subscriber.Subscribe(context.Background(), "flight-operations", "publisher-1")
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if factory.binding.Queue != "swlb.membership.publisher-1.flight-operations" {
		t.Fatalf("queue = %q", factory.binding.Queue)
	}
	select {
	case got := <-subscription.Deliveries:
		if got != delivery {
			t.Fatalf("delivery = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivery")
	}
	if delivery.acked {
		t.Fatal("update adapter acknowledged delivery before orchestrator")
	}
}

type commandExecutorFunc func(context.Context, control.CommandEnvelope) error

func (f commandExecutorFunc) Execute(ctx context.Context, command control.CommandEnvelope) error {
	return f(ctx, command)
}

type acknowledgementPublisherFunc func(context.Context, control.AcknowledgementEnvelope) error

func (f acknowledgementPublisherFunc) PublishAcknowledgement(ctx context.Context, acknowledgement control.AcknowledgementEnvelope) error {
	return f(ctx, acknowledgement)
}

type fakeParticipantOperations struct {
	completed bool
	onRecord  func()
}

func (s *fakeParticipantOperations) Contains(context.Context, string) (bool, error) {
	return s.completed, nil
}
func (s *fakeParticipantOperations) Record(context.Context, string) error {
	if s.onRecord != nil {
		s.onRecord()
	}
	s.completed = true
	return nil
}

type fakeParticipantDelivery struct {
	kind      Kind
	operation string
	payload   []byte
	acked     bool
	rejected  bool
	onAck     func()
}

func (d *fakeParticipantDelivery) Kind() Kind          { return d.kind }
func (d *fakeParticipantDelivery) OperationID() string { return d.operation }
func (d *fakeParticipantDelivery) Payload() []byte     { return d.payload }
func (d *fakeParticipantDelivery) Ack(context.Context) error {
	d.acked = true
	if d.onAck != nil {
		d.onAck()
	}
	return nil
}
func (d *fakeParticipantDelivery) Reject(context.Context) error {
	d.rejected = true
	return nil
}

func commandDelivery(t *testing.T, command control.CommandEnvelope) *fakeParticipantDelivery {
	t.Helper()
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeParticipantDelivery{kind: KindCommand, operation: command.MessageID, payload: payload}
}

type sequenceParticipantReceiver struct {
	deliveries []Delivery
	next       int
}

func (r *sequenceParticipantReceiver) Receive(ctx context.Context) (Delivery, error) {
	if r.next < len(r.deliveries) {
		delivery := r.deliveries[r.next]
		r.next++
		return delivery, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (*sequenceParticipantReceiver) Close() error { return nil }

type singleParticipantReceiver struct {
	delivery Delivery
	receives int
}

func (r *singleParticipantReceiver) Receive(ctx context.Context) (Delivery, error) {
	if r.receives == 0 {
		r.receives++
		return r.delivery, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (*singleParticipantReceiver) Close() error { return nil }

type fakeParticipantReceiver struct {
	deliveries chan Delivery
	closed     bool
}

func (r *fakeParticipantReceiver) Receive(ctx context.Context) (Delivery, error) {
	select {
	case delivery := <-r.deliveries:
		return delivery, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (r *fakeParticipantReceiver) Close() error { r.closed = true; return nil }

type fakeParticipantReceiverFactory struct {
	receiver *fakeParticipantReceiver
	binding  ParticipantQueue
}

func (f *fakeParticipantReceiverFactory) BindDurable(_ context.Context, binding ParticipantQueue) (DurableReceiver, error) {
	f.binding = binding
	return f.receiver, nil
}
