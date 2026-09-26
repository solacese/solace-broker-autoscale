package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"solace.dev/go/messaging/pkg/solace"
	solaceConfig "solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// NativeDurableReceiverFactory binds existing Broker 0 queues without creating
// resources or altering subscriptions.
type NativeDurableReceiverFactory struct {
	Service solace.MessagingService
	Poll    time.Duration
}

func (f NativeDurableReceiverFactory) BindDurable(_ context.Context, binding broker0.ParticipantQueue) (broker0.DurableReceiver, error) {
	if f.Service == nil || !f.Service.IsConnected() || binding.Participant == "" || binding.Queue == "" {
		return nil, errors.New("runtime: connected Broker 0 service, participant, and durable queue are required")
	}
	poll := f.Poll
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	receiver, err := f.Service.CreatePersistentMessageReceiverBuilder().
		WithMessageClientAcknowledgement().
		WithMissingResourcesCreationStrategy(solaceConfig.PersistentReceiverDoNotCreateMissingResources).
		WithRequiredMessageOutcomeSupport(solaceConfig.PersistentReceiverFailedOutcome, solaceConfig.PersistentReceiverRejectedOutcome).
		Build(resource.QueueDurableExclusive(binding.Queue))
	if err != nil {
		return nil, fmt.Errorf("runtime: bind existing Broker 0 queue %q: %w", binding.Queue, err)
	}
	if err := receiver.Start(); err != nil {
		return nil, fmt.Errorf("runtime: start Broker 0 queue %q: %w", binding.Queue, err)
	}
	result := &nativeDurableReceiver{
		service: f.Service, receiver: receiver, binding: binding, poll: poll,
		reconnects: make(chan struct{}, 1), errors: make(chan error, 1), done: make(chan struct{}),
	}
	result.reconnectListener = f.Service.AddReconnectionListener(func(solace.ServiceEvent) {
		select {
		case result.reconnects <- struct{}{}:
		default:
		}
	})
	result.interruptionListener = f.Service.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
		cause := event.GetCause()
		if cause == nil {
			cause = errors.New("unrecoverable Broker 0 service interruption")
		}
		select {
		case result.errors <- cause:
		default:
		}
	})
	return result, nil
}

type nativeDurableReceiver struct {
	service              solace.MessagingService
	receiver             solace.PersistentMessageReceiver
	binding              broker0.ParticipantQueue
	poll                 time.Duration
	reconnectListener    uint64
	interruptionListener uint64
	reconnects           chan struct{}
	errors               chan error
	done                 chan struct{}
	closeOnce            sync.Once
	closeErr             error
}

func (r *nativeDurableReceiver) Receive(ctx context.Context) (broker0.Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case <-r.done:
			return nil, context.Canceled
		default:
		}
		inbound, err := r.receiver.ReceiveMessage(r.poll)
		if err != nil {
			var timeout *solace.TimeoutError
			if errors.As(err, &timeout) {
				continue
			}
			return nil, err
		}
		delivery, err := newParticipantDelivery(r.receiver, inbound, r.binding)
		if err != nil {
			_ = r.receiver.Settle(inbound, solaceConfig.PersistentReceiverRejectedOutcome)
			inbound.Dispose()
			return nil, err
		}
		return delivery, nil
	}
}

func (r *nativeDurableReceiver) Reconnects() <-chan struct{} { return r.reconnects }
func (r *nativeDurableReceiver) Errors() <-chan error        { return r.errors }

func (r *nativeDurableReceiver) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		close(r.done)
		r.service.RemoveReconnectionListener(r.reconnectListener)
		r.service.RemoveServiceInterruptionListener(r.interruptionListener)
		r.closeErr = r.receiver.Terminate(10 * time.Second)
	})
	return r.closeErr
}

type participantDelivery struct {
	mu        sync.Mutex
	receiver  solace.PersistentMessageReceiver
	message   message.InboundMessage
	kind      broker0.Kind
	operation string
	payload   []byte
	done      bool
}

func newParticipantDelivery(receiver solace.PersistentMessageReceiver, inbound message.InboundMessage, binding broker0.ParticipantQueue) (*participantDelivery, error) {
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
	kind := broker0.Kind(kindText)
	if kind != broker0.KindMembershipSnapshot && kind != broker0.KindCommand {
		return nil, fmt.Errorf("runtime: participant queue received unsupported Broker 0 kind %q", kind)
	}
	if binding.Kind != "" && kind != binding.Kind {
		return nil, fmt.Errorf("runtime: Broker 0 message kind %q does not match participant queue binding %q", kind, binding.Kind)
	}
	if binding.Topic != "" && inbound.GetDestinationName() != binding.Topic {
		return nil, fmt.Errorf("runtime: Broker 0 message destination %q does not match participant queue binding topic %q", inbound.GetDestinationName(), binding.Topic)
	}
	if operation == "" {
		return nil, errors.New("runtime: Broker 0 message has no operation ID")
	}
	return &participantDelivery{receiver: receiver, message: inbound, kind: kind, operation: operation, payload: append([]byte(nil), payload...)}, nil
}

func (d *participantDelivery) Kind() broker0.Kind  { return d.kind }
func (d *participantDelivery) OperationID() string { return d.operation }
func (d *participantDelivery) Payload() []byte     { return append([]byte(nil), d.payload...) }
func (d *participantDelivery) Ack(ctx context.Context) error {
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

func (d *participantDelivery) Reject(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done {
		return nil
	}
	if err := d.receiver.Settle(d.message, solaceConfig.PersistentReceiverRejectedOutcome); err != nil {
		return err
	}
	d.done = true
	d.message.Dispose()
	return nil
}
