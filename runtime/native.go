package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/control"
	messaging "solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	solaceConfig "solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

const (
	propertyKind        = "swlb.control.kind"
	propertyOperationID = "swlb.control.operation_id"
)

// SMFConnection is a native Broker 0 connection assembled from resolved
// credentials. It owns the Solace service and disconnects it exactly once.
type SMFConnection struct {
	Service solace.MessagingService
	once    sync.Once
	err     error
}

func ConnectBroker0(ctx context.Context, endpoint, messageVPN, username, password, applicationID string) (*SMFConnection, error) {
	return ConnectBroker0WithTrustStore(ctx, endpoint, messageVPN, username, password, applicationID, "")
}

func ConnectBroker0WithTrustStore(ctx context.Context, endpoint, messageVPN, username, password, applicationID, trustStorePath string) (*SMFConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	builder := messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(solaceConfig.ServicePropertyMap{
			solaceConfig.TransportLayerPropertyHost: endpoint,
			solaceConfig.ServicePropertyVPNName:     messageVPN,
		}).
		WithAuthenticationStrategy(solaceConfig.BasicUserNamePasswordAuthentication(username, password)).
		WithConnectionRetryStrategy(solaceConfig.RetryStrategyNeverRetry()).
		WithReconnectionRetryStrategy(solaceConfig.RetryStrategyForeverRetry())
	if trustStorePath != "" {
		builder = builder.WithTransportSecurityStrategy(solaceConfig.NewTransportSecurityStrategy().WithCertificateValidation(false, true, trustStorePath, ""))
	}
	service, err := builder.BuildWithApplicationID(applicationID)
	if err != nil {
		return nil, fmt.Errorf("runtime: build Broker 0 messaging service: %w", err)
	}
	result := service.ConnectAsync()
	select {
	case <-ctx.Done():
		go func() {
			<-result
			_ = service.Disconnect()
		}()
		return nil, ctx.Err()
	case err := <-result:
		if err != nil {
			return nil, fmt.Errorf("runtime: connect Broker 0 messaging service: %w", err)
		}
		return &SMFConnection{Service: service}, nil
	}
}

func (c *SMFConnection) Close() error {
	if c == nil || c.Service == nil {
		return nil
	}
	c.once.Do(func() { c.err = c.Service.Disconnect() })
	return c.err
}

// NativePublisher implements broker0.NativePersistentPublisher using the
// official Solace Go API and positive broker acknowledgements.
type NativePublisher struct {
	service   solace.MessagingService
	publisher solace.PersistentMessagePublisher
	timeout   time.Duration
	once      sync.Once
	err       error
}

func NewNativePublisher(service solace.MessagingService, timeout time.Duration) (*NativePublisher, error) {
	if service == nil || !service.IsConnected() || timeout <= 0 {
		return nil, errors.New("runtime: connected Broker 0 service and publish timeout are required")
	}
	publisher, err := service.CreatePersistentMessagePublisherBuilder().OnBackPressureReject(0).Build()
	if err != nil {
		return nil, fmt.Errorf("runtime: build Broker 0 publisher: %w", err)
	}
	if err := publisher.Start(); err != nil {
		return nil, fmt.Errorf("runtime: start Broker 0 publisher: %w", err)
	}
	return &NativePublisher{service: service, publisher: publisher, timeout: timeout}, nil
}

func (p *NativePublisher) PublishPersistent(ctx context.Context, publication broker0.Publication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if publication.Destination == "" || publication.Kind == "" || publication.OperationID == "" {
		return errors.New("runtime: incomplete Broker 0 publication")
	}
	outbound, err := p.service.MessageBuilder().
		WithApplicationMessageID(publication.OperationID).
		WithApplicationMessageType(string(publication.Kind)).
		WithProperty(solaceConfig.MessageProperty(propertyKind), string(publication.Kind)).
		WithProperty(solaceConfig.MessageProperty(propertyOperationID), publication.OperationID).
		BuildWithByteArrayPayload(publication.Payload)
	if err != nil {
		return fmt.Errorf("runtime: build Broker 0 message: %w", err)
	}
	defer outbound.Dispose()
	if err := p.publisher.PublishAwaitAcknowledgement(outbound, resource.TopicOf(publication.Destination), p.timeout, nil); err != nil {
		return fmt.Errorf("runtime: publish Broker 0 message: %w", err)
	}
	return nil
}

func (p *NativePublisher) Close() error {
	if p == nil || p.publisher == nil {
		return nil
	}
	p.once.Do(func() { p.err = p.publisher.Terminate(10 * time.Second) })
	return p.err
}

// NativeReceiver binds one existing durable controller queue. It never
// provisions or mutates broker resources.
type NativeReceiver struct {
	receiver solace.PersistentMessageReceiver
	binding  broker0.ParticipantQueue
	poll     time.Duration
	once     sync.Once
	err      error
}

func BindControllerInbox(service solace.MessagingService, binding broker0.ParticipantQueue, poll time.Duration) (*NativeReceiver, error) {
	if service == nil || !service.IsConnected() || binding.Participant == "" || binding.Participant == "*" || binding.Principal == "" || binding.Queue == "" || binding.Topic == "" || binding.Group == "" {
		return nil, errors.New("runtime: connected Broker 0 service and exact participant queue binding are required")
	}
	if err := binding.Role.Validate(); err != nil {
		return nil, fmt.Errorf("runtime: invalid controller queue role: %w", err)
	}
	if binding.Kind != broker0.KindRegistration && binding.Kind != broker0.KindAcknowledgement && binding.Kind != broker0.KindTelemetry {
		return nil, fmt.Errorf("runtime: unsupported controller queue kind %q", binding.Kind)
	}
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	receiver, err := service.CreatePersistentMessageReceiverBuilder().
		WithMessageClientAcknowledgement().
		WithMissingResourcesCreationStrategy(solaceConfig.PersistentReceiverDoNotCreateMissingResources).
		WithRequiredMessageOutcomeSupport(solaceConfig.PersistentReceiverFailedOutcome, solaceConfig.PersistentReceiverRejectedOutcome).
		Build(resource.QueueDurableExclusive(binding.Queue))
	if err != nil {
		return nil, fmt.Errorf("runtime: build Broker 0 controller receiver: %w", err)
	}
	if err := receiver.Start(); err != nil {
		return nil, fmt.Errorf("runtime: start Broker 0 controller receiver: %w", err)
	}
	return &NativeReceiver{receiver: receiver, binding: binding, poll: poll}, nil
}

func (r *NativeReceiver) Receive(ctx context.Context) (broker0.Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		inbound, err := r.receiver.ReceiveMessage(r.poll)
		if err != nil {
			var timeout *solace.TimeoutError
			if errors.As(err, &timeout) {
				continue
			}
			return nil, err
		}
		delivery, err := newNativeDelivery(r.receiver, inbound, r.binding)
		if err != nil {
			_ = r.receiver.Settle(inbound, solaceConfig.PersistentReceiverRejectedOutcome)
			inbound.Dispose()
			return nil, err
		}
		return delivery, nil
	}
}

func (r *NativeReceiver) Close() error {
	if r == nil || r.receiver == nil {
		return nil
	}
	r.once.Do(func() { r.err = r.receiver.Terminate(10 * time.Second) })
	return r.err
}

type nativeDelivery struct {
	mu        sync.Mutex
	receiver  solace.PersistentMessageReceiver
	message   message.InboundMessage
	kind      broker0.Kind
	operation string
	payload   []byte
	binding   broker0.ParticipantQueue
	done      bool
}

func newNativeDelivery(receiver solace.PersistentMessageReceiver, inbound message.InboundMessage, binding broker0.ParticipantQueue) (*nativeDelivery, error) {
	payload, ok := inbound.GetPayloadAsBytes()
	if !ok {
		return nil, errors.New("runtime: Broker 0 message has no byte payload")
	}
	kindText := nativeStringProperty(inbound, propertyKind)
	if kindText == "" {
		kindText, _ = inbound.GetApplicationMessageType()
	}
	operation := nativeStringProperty(inbound, propertyOperationID)
	if operation == "" {
		operation, _ = inbound.GetApplicationMessageID()
	}
	if destination := inbound.GetDestinationName(); destination != binding.Topic {
		return nil, fmt.Errorf("runtime: Broker 0 message destination %q does not match queue binding topic %q", destination, binding.Topic)
	}
	kind := broker0.Kind(kindText)
	if kind != binding.Kind {
		return nil, fmt.Errorf("runtime: Broker 0 message kind %q does not match queue binding %q", kind, binding.Kind)
	}
	if operation == "" {
		return nil, errors.New("runtime: Broker 0 message has no operation ID")
	}
	return &nativeDelivery{
		receiver: receiver, message: inbound, kind: kind, operation: operation,
		payload: append([]byte(nil), payload...), binding: binding,
	}, nil
}

func nativeStringProperty(inbound message.InboundMessage, key string) string {
	value, ok := inbound.GetProperty(key)
	if !ok {
		return ""
	}
	text, _ := value.(string)
	return text
}

func (d *nativeDelivery) Kind() broker0.Kind  { return d.kind }
func (d *nativeDelivery) OperationID() string { return d.operation }
func (d *nativeDelivery) Payload() []byte     { return append([]byte(nil), d.payload...) }
func (d *nativeDelivery) AuthenticatedParticipant() (string, bool) {
	return d.binding.Participant, d.binding.Participant != ""
}
func (d *nativeDelivery) AuthenticatedScope() (broker0.Kind, string, control.ParticipantRole) {
	return d.binding.Kind, d.binding.Group, d.binding.Role
}
func (d *nativeDelivery) Ack(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done {
		return nil
	}
	if err := d.receiver.Ack(d.message); err != nil {
		return err
	}
	d.done = true
	d.message.Dispose()
	return nil
}
