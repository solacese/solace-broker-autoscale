package broker0

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

// CommandExecutor completes the local, phase-specific action requested by a
// command. Implementations must be idempotent because a process can stop after
// Execute returns but before command completion is durably recorded.
type CommandExecutor interface {
	Execute(context.Context, control.CommandEnvelope) error
}

// RejectableDelivery provides terminal settlement for malformed, expired, or
// mis-scoped control messages. Valid messages are never rejected for transient
// errors.
type RejectableDelivery interface {
	Delivery
	Reject(context.Context) error
}

type permanentCommandError struct{ err error }

func (e *permanentCommandError) Error() string { return e.err.Error() }
func (e *permanentCommandError) Unwrap() error { return e.err }

func permanentCommand(err error) error { return &permanentCommandError{err: err} }

// AcknowledgementPublisher publishes a command-scoped acknowledgement and
// waits for its positive Broker 0 acknowledgement.
type AcknowledgementPublisher interface {
	PublishAcknowledgement(context.Context, control.AcknowledgementEnvelope) error
}

// CommandHandler validates and executes one participant command. Its ordering
// is deliberately local action, acknowledgement publication, durable command
// record, then native delivery acknowledgement.
type CommandHandler struct {
	Participant string
	Role        control.ParticipantRole
	Namespace   string
	Groups      map[string]struct{}
	Operations  OperationStore
	Executor    CommandExecutor
	Publisher   AcknowledgementPublisher
	Now         func() time.Time
}

func NewCommandHandler(participant string, role control.ParticipantRole, namespace string, groups []string, operations OperationStore, executor CommandExecutor, publisher AcknowledgementPublisher) (*CommandHandler, error) {
	if err := validateTransportIdentifier("participant", participant); err != nil {
		return nil, err
	}
	if err := role.Validate(); err != nil {
		return nil, err
	}
	if namespace == "" || len(groups) == 0 || operations == nil || executor == nil || publisher == nil {
		return nil, errors.New("broker0: command handler namespace, groups, operation store, executor, and publisher are required")
	}
	allowed := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		if err := validateTransportIdentifier("group", group); err != nil {
			return nil, err
		}
		if _, exists := allowed[group]; exists {
			return nil, fmt.Errorf("broker0: duplicate command group %q", group)
		}
		allowed[group] = struct{}{}
	}
	return &CommandHandler{
		Participant: participant, Role: role, Namespace: namespace, Groups: allowed,
		Operations: operations, Executor: executor, Publisher: publisher, Now: time.Now,
	}, nil
}

func (h *CommandHandler) Handle(ctx context.Context, delivery Delivery) error {
	if delivery == nil {
		return errors.New("broker0: nil command delivery")
	}
	if delivery.Kind() != KindCommand {
		return permanentCommand(fmt.Errorf("broker0: command inbox received kind %q", delivery.Kind()))
	}
	message, err := parseMessage(delivery.Kind(), delivery.OperationID(), delivery.Payload())
	if err != nil {
		return permanentCommand(err)
	}
	command := *message.Command
	if command.Namespace != h.Namespace || command.Participant != h.Participant || command.Role != h.Role {
		return permanentCommand(errors.New("broker0: command scope does not match participant"))
	}
	if _, allowed := h.Groups[command.Group]; !allowed {
		return permanentCommand(fmt.Errorf("broker0: command group %q is not assigned to participant", command.Group))
	}
	now := h.Now
	if now == nil {
		now = time.Now
	}
	completed, err := h.Operations.Contains(ctx, command.MessageID)
	if err != nil {
		return fmt.Errorf("broker0: check command operation %q: %w", command.MessageID, err)
	}
	if completed {
		return delivery.Ack(ctx)
	}
	if now().After(command.Deadline) {
		return permanentCommand(fmt.Errorf("broker0: command %q deadline has expired", command.MessageID))
	}
	if err := h.Executor.Execute(ctx, command); err != nil {
		return fmt.Errorf("broker0: execute command %q: %w", command.MessageID, err)
	}
	acknowledgement := control.AcknowledgementEnvelope{
		Version: control.ProtocolVersion, MessageID: command.MessageID + "/ack", CommandID: command.MessageID,
		Namespace: command.Namespace, Group: command.Group, TransitionID: command.TransitionID,
		Epoch: command.Epoch, Phase: command.Phase, Participant: command.Participant,
		Role: command.Role, Ready: true, ObservedAt: now().UTC(),
	}
	if err := acknowledgement.ValidateForCommand(command); err != nil {
		return fmt.Errorf("broker0: build acknowledgement for command %q: %w", command.MessageID, err)
	}
	if err := h.Publisher.PublishAcknowledgement(ctx, acknowledgement); err != nil {
		return fmt.Errorf("broker0: publish acknowledgement for command %q: %w", command.MessageID, err)
	}
	if err := h.Operations.Record(ctx, command.MessageID); err != nil {
		return fmt.Errorf("broker0: record command %q: %w", command.MessageID, err)
	}
	if err := delivery.Ack(ctx); err != nil {
		return fmt.Errorf("broker0: acknowledge command delivery %q: %w", command.MessageID, err)
	}
	return nil
}

// CommandInbox consumes one existing durable participant command queue.
type CommandInbox struct {
	receiver DurableReceiver
	handler  *CommandHandler
}

func NewCommandInbox(receiver DurableReceiver, handler *CommandHandler) (*CommandInbox, error) {
	if receiver == nil || handler == nil {
		return nil, errors.New("broker0: command receiver and handler are required")
	}
	return &CommandInbox{receiver: receiver, handler: handler}, nil
}

func (i *CommandInbox) Run(ctx context.Context) error {
	for {
		delivery, err := i.receiver.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			return fmt.Errorf("broker0: receive participant command: %w", err)
		}
		backoff := 250 * time.Millisecond
		for {
			err = i.handler.Handle(ctx, delivery)
			if err == nil {
				break
			}
			var permanent *permanentCommandError
			if errors.As(err, &permanent) {
				rejectable, ok := delivery.(RejectableDelivery)
				if !ok {
					return fmt.Errorf("broker0: terminal command cannot be rejected: %w", err)
				}
				if rejectErr := rejectable.Reject(ctx); rejectErr != nil {
					return errors.Join(err, fmt.Errorf("broker0: reject terminal command: %w", rejectErr))
				}
				break
			}
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ctx.Err()
			case <-timer.C:
			}
			backoff = min(backoff*2, 5*time.Second)
		}
	}
}

func (i *CommandInbox) Close() error { return i.receiver.Close() }

// BindCommandInboxes binds one existing queue per assigned group. If any bind
// fails, already-opened receivers are closed before returning.
func BindCommandInboxes(ctx context.Context, factory DurableReceiverFactory, resolver CommandQueueResolver, participant string, role control.ParticipantRole, groups []string, handler *CommandHandler) (map[string]*CommandInbox, error) {
	if factory == nil || resolver == nil || handler == nil {
		return nil, errors.New("broker0: command receiver factory, queue resolver, and handler are required")
	}
	inboxes := make(map[string]*CommandInbox, len(groups))
	closeAll := func() {
		for _, inbox := range inboxes {
			_ = inbox.Close()
		}
	}
	for _, group := range groups {
		queue, err := resolver.CommandQueue(ctx, group, role, participant)
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("broker0: resolve command queue for %q: %w", group, err)
		}
		if queue == "" {
			closeAll()
			return nil, fmt.Errorf("broker0: resolved command queue for %q is empty", group)
		}
		receiver, err := factory.BindDurable(ctx, ParticipantQueue{Participant: participant, Role: role, Group: group, Kind: KindCommand, Queue: queue})
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("broker0: bind command queue %q: %w", queue, err)
		}
		inbox, err := NewCommandInbox(receiver, handler)
		if err != nil {
			_ = receiver.Close()
			closeAll()
			return nil, err
		}
		inboxes[group] = inbox
	}
	return inboxes, nil
}

// UpdateQueueResolver selects an exact pre-provisioned durable update queue.
type UpdateQueueResolver interface {
	UpdateQueue(context.Context, string, string) (string, error)
}

type UpdateQueueResolverFunc func(context.Context, string, string) (string, error)

func (f UpdateQueueResolverFunc) UpdateQueue(ctx context.Context, group, participant string) (string, error) {
	return f(ctx, group, participant)
}

type CommandQueueResolver interface {
	CommandQueue(context.Context, string, control.ParticipantRole, string) (string, error)
}

type CommandQueueResolverFunc func(context.Context, string, control.ParticipantRole, string) (string, error)

func (f CommandQueueResolverFunc) CommandQueue(ctx context.Context, group string, role control.ParticipantRole, participant string) (string, error) {
	return f(ctx, group, role, participant)
}

// DurableUpdateSubscriber adapts an existing durable receiver factory to the
// channel-based update subscription used by GroupOrchestrator.
type receiverSignals interface {
	Reconnects() <-chan struct{}
	Errors() <-chan error
}

type DurableUpdateSubscriber struct {
	Factory  DurableReceiverFactory
	Resolver UpdateQueueResolver
}

func (s DurableUpdateSubscriber) Subscribe(ctx context.Context, group, participant string) (UpdateSubscription, error) {
	if s.Factory == nil || s.Resolver == nil {
		return UpdateSubscription{}, errors.New("broker0: update receiver factory and queue resolver are required")
	}
	queue, err := s.Resolver.UpdateQueue(ctx, group, participant)
	if err != nil {
		return UpdateSubscription{}, fmt.Errorf("broker0: resolve update queue: %w", err)
	}
	if queue == "" {
		return UpdateSubscription{}, errors.New("broker0: resolved update queue is empty")
	}
	receiver, err := s.Factory.BindDurable(ctx, ParticipantQueue{Participant: participant, Group: group, Kind: KindMembershipSnapshot, Queue: queue})
	if err != nil {
		return UpdateSubscription{}, fmt.Errorf("broker0: bind update queue %q: %w", queue, err)
	}
	pumpCtx, cancel := context.WithCancel(ctx)
	deliveries := make(chan Delivery)
	errorsChannel := make(chan error, 1)
	var reconnects <-chan struct{}
	var receiverErrors <-chan error
	if signals, ok := receiver.(receiverSignals); ok {
		reconnects = signals.Reconnects()
		receiverErrors = signals.Errors()
	}
	var closeOnce sync.Once
	closeFn := func() error {
		var closeErr error
		closeOnce.Do(func() {
			cancel()
			closeErr = receiver.Close()
		})
		return closeErr
	}
	go func() {
		defer close(deliveries)
		defer close(errorsChannel)
		for {
			received := make(chan struct {
				delivery Delivery
				err      error
			}, 1)
			go func() {
				delivery, receiveErr := receiver.Receive(pumpCtx)
				received <- struct {
					delivery Delivery
					err      error
				}{delivery: delivery, err: receiveErr}
			}()
			select {
			case result := <-received:
				if result.err != nil {
					if pumpCtx.Err() == nil && !errors.Is(result.err, context.Canceled) {
						errorsChannel <- result.err
					}
					return
				}
				if result.delivery == nil {
					errorsChannel <- errors.New("broker0: update receiver returned nil delivery")
					return
				}
				select {
				case deliveries <- result.delivery:
				case <-pumpCtx.Done():
					return
				}
			case receiverErr, ok := <-receiverErrors:
				if !ok {
					receiverErrors = nil
					continue
				}
				if receiverErr != nil {
					errorsChannel <- receiverErr
					return
				}
			case <-pumpCtx.Done():
				return
			}
		}
	}()
	return UpdateSubscription{Deliveries: deliveries, Reconnects: reconnects, Errors: errorsChannel, Close: closeFn}, nil
}
