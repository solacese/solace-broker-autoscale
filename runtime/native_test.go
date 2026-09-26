package runtime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
	"solace.dev/go/messaging/pkg/solace"
	solaceConfig "solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/message/sdt"
	"solace.dev/go/messaging/pkg/solace/resource"
)

func TestBindControllerInboxBindsAuthenticatedParticipant(t *testing.T) {
	inbound := &stubNativeInboundMessage{
		payload:     []byte(`{"participant":"payload-spoof"}`),
		senderID:    "spoofed-sender-id",
		destination: "swlb/control/orders/acknowledgements/subscriber/subscriber-configured",
		properties: sdt.Map{
			propertyKind:                     string(broker0.KindAcknowledgement),
			propertyOperationID:              "operation-1",
			"swlb.authenticated_participant": "property-spoof",
		},
	}
	persistent := &stubNativePersistentReceiver{inbound: inbound}
	builder := &stubNativeReceiverBuilder{receiver: persistent}
	service := &stubNativeMessagingService{builder: builder}
	binding := broker0.ParticipantQueue{
		Participant: "subscriber-configured", Principal: "subscriber-configured", Role: control.RoleSubscriber,
		Group: "orders", Kind: broker0.KindAcknowledgement, Queue: "controller.inbox", Topic: "swlb/control/orders/acknowledgements/subscriber/subscriber-configured",
	}

	receiver, err := BindControllerInbox(service, binding, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !persistent.started {
		t.Fatal("persistent receiver was not started")
	}
	if builder.queue == nil || builder.queue.GetName() != binding.Queue {
		t.Fatalf("bound queue = %#v, want %q", builder.queue, binding.Queue)
	}

	delivery, err := receiver.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	authenticated, ok := delivery.(broker0.AuthenticatedDelivery)
	if !ok {
		t.Fatalf("delivery type %T does not expose authenticated identity", delivery)
	}
	participant, present := authenticated.AuthenticatedParticipant()
	if !present || participant != binding.Participant {
		t.Fatalf("authenticated participant = %q, %t; want %q, true", participant, present, binding.Participant)
	}
}

func TestNativeDeliveryIgnoresSpoofableSenderID(t *testing.T) {
	inbound := &stubNativeInboundMessage{
		payload:     []byte(`{}`),
		senderID:    "attacker-controlled",
		destination: "swlb/control/orders/telemetry/observer/observer-configured",
		properties: sdt.Map{
			propertyKind:        string(broker0.KindTelemetry),
			propertyOperationID: "operation-1",
		},
	}
	binding := broker0.ParticipantQueue{
		Participant: "observer-configured", Principal: "observer-configured", Role: control.RoleObserver,
		Group: "orders", Kind: broker0.KindTelemetry, Queue: "controller.inbox", Topic: "swlb/control/orders/telemetry/observer/observer-configured",
	}
	delivery, err := newNativeDelivery(&stubNativePersistentReceiver{}, inbound, binding)
	if err != nil {
		t.Fatal(err)
	}
	participant, present := delivery.AuthenticatedParticipant()
	if !present || participant != binding.Participant {
		t.Fatalf("queue-bound participant = %q, %t", participant, present)
	}
}

func TestBindControllerInboxRejectsWildcardBinding(t *testing.T) {
	service := &stubNativeMessagingService{builder: &stubNativeReceiverBuilder{receiver: &stubNativePersistentReceiver{}}}
	binding := broker0.ParticipantQueue{Participant: "*", Principal: "shared", Role: control.RoleSubscriber, Group: "orders", Kind: broker0.KindAcknowledgement, Queue: "shared", Topic: "swlb/control/orders/acknowledgements/subscriber/shared"}
	if _, err := BindControllerInbox(service, binding, time.Millisecond); err == nil {
		t.Fatal("wildcard controller queue binding was accepted")
	}
}

func TestSubscriberParticipantFactoryBridgesRetryAndRelease(t *testing.T) {
	for _, test := range []struct {
		name        string
		directive   string
		wantRetries int
		wantRelease int
	}{
		{name: "retry", directive: "retry", wantRetries: 1},
		{name: "reject", directive: "reject", wantRelease: 1},
		{name: "release", directive: "release", wantRelease: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			delegate := &settlementConsumerFactory{}
			controller := &settlementController{}
			factory := subscriberParticipantFactory{Delegate: delegate, Subscriber: func() subscriberSettlementController { return controller }}
			binding := shimSubscriber.Binding{Group: "orders", BrokerID: "broker-a", Epoch: 1, Destination: "orders/e1"}
			consumer, err := factory.Prepare(context.Background(), binding, func(context.Context, shimSubscriber.Delivery) error {
				return settlementDirective(test.directive)
			})
			if err != nil {
				t.Fatal(err)
			}
			if consumer == nil || delegate.deliver == nil {
				t.Fatal("settlement adapter did not retain delegate callback")
			}
			delivery := &settlementDelivery{metadata: shimSubscriber.RoutingMetadata{Group: "orders", BusinessHash: sha256.Sum256([]byte("one"))}}
			if err := delegate.deliver(context.Background(), delivery); err != nil {
				t.Fatal(err)
			}
			if controller.retries != test.wantRetries || controller.releases != test.wantRelease {
				t.Fatalf("settlements retry/release = %d/%d, want %d/%d", controller.retries, controller.releases, test.wantRetries, test.wantRelease)
			}
			if controller.group != delivery.metadata.Group || controller.hash != delivery.metadata.BusinessHash {
				t.Fatalf("settlement key = %q/%x", controller.group, controller.hash)
			}
		})
	}
}

type settlementDirective string

func (d settlementDirective) Error() string                { return fmt.Sprintf("directive %q", string(d)) }
func (d settlementDirective) SubscriberSettlement() string { return string(d) }

type settlementConsumerFactory struct {
	deliver func(context.Context, shimSubscriber.Delivery) error
}

func (f *settlementConsumerFactory) Prepare(_ context.Context, _ shimSubscriber.Binding, deliver func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
	f.deliver = deliver
	return settlementConsumer{}, nil
}

type settlementConsumer struct{}

func (settlementConsumer) Activate(context.Context) error { return nil }
func (settlementConsumer) Pause(context.Context) error    { return nil }
func (settlementConsumer) Close(context.Context) error    { return nil }

type settlementController struct {
	retries  int
	releases int
	group    string
	hash     customer.BusinessHash
}

func (c *settlementController) Retry(_ context.Context, group string, hash customer.BusinessHash) error {
	c.retries++
	c.group, c.hash = group, hash
	return nil
}

func (c *settlementController) Release(_ context.Context, group string, hash customer.BusinessHash) error {
	c.releases++
	c.group, c.hash = group, hash
	return nil
}

type settlementDelivery struct {
	metadata shimSubscriber.RoutingMetadata
}

func (*settlementDelivery) Message() customer.MessageView { return customer.MessageView{} }
func (*settlementDelivery) Ack(context.Context) error     { return nil }
func (d *settlementDelivery) RoutingMetadata() (shimSubscriber.RoutingMetadata, error) {
	return d.metadata, nil
}

var _ broker0.AuthenticatedDelivery = (*nativeDelivery)(nil)

type stubNativeMessagingService struct {
	solace.MessagingService
	builder solace.PersistentMessageReceiverBuilder
}

func (*stubNativeMessagingService) IsConnected() bool { return true }
func (s *stubNativeMessagingService) CreatePersistentMessageReceiverBuilder() solace.PersistentMessageReceiverBuilder {
	return s.builder
}

type stubNativeReceiverBuilder struct {
	solace.PersistentMessageReceiverBuilder
	receiver solace.PersistentMessageReceiver
	queue    *resource.Queue
}

func (b *stubNativeReceiverBuilder) WithMessageClientAcknowledgement() solace.PersistentMessageReceiverBuilder {
	return b
}
func (b *stubNativeReceiverBuilder) WithMissingResourcesCreationStrategy(solaceConfig.MissingResourcesCreationStrategy) solace.PersistentMessageReceiverBuilder {
	return b
}
func (b *stubNativeReceiverBuilder) WithRequiredMessageOutcomeSupport(...solaceConfig.MessageSettlementOutcome) solace.PersistentMessageReceiverBuilder {
	return b
}
func (b *stubNativeReceiverBuilder) Build(queue *resource.Queue) (solace.PersistentMessageReceiver, error) {
	b.queue = queue
	return b.receiver, nil
}

type stubNativePersistentReceiver struct {
	solace.PersistentMessageReceiver
	inbound message.InboundMessage
	started bool
}

func (r *stubNativePersistentReceiver) Start() error {
	r.started = true
	return nil
}
func (r *stubNativePersistentReceiver) ReceiveMessage(time.Duration) (message.InboundMessage, error) {
	return r.inbound, nil
}

type stubNativeInboundMessage struct {
	message.InboundMessage
	payload     []byte
	properties  sdt.Map
	senderID    string
	destination string
}

func (m *stubNativeInboundMessage) GetDestinationName() string { return m.destination }
func (m *stubNativeInboundMessage) GetPayloadAsBytes() ([]byte, bool) {
	return m.payload, m.payload != nil
}
func (m *stubNativeInboundMessage) GetProperty(key string) (sdt.Data, bool) {
	value, ok := m.properties[key]
	return value, ok
}
func (m *stubNativeInboundMessage) GetSenderID() (string, bool)             { return m.senderID, m.senderID != "" }
func (*stubNativeInboundMessage) GetApplicationMessageType() (string, bool) { return "", false }
func (*stubNativeInboundMessage) GetApplicationMessageID() (string, bool)   { return "", false }
