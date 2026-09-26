package broker0

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
)

// ControllerHandler is the narrow controller input consumed from Broker 0. A
// *controller.Controller satisfies this interface.
type ControllerHandler interface {
	Acknowledge(controller.Acknowledgement) error
	Observe(controller.Telemetry) error
}

// RegistrationHandler receives registrations only after the envelope identity has
// been bound to authenticated transport metadata.
type RegistrationHandler interface {
	Register(control.RegistrationEnvelope) error
}

// terminalInboxError classifies deliveries that cannot become valid through
// redelivery. They must be rejected rather than acknowledged so malformed or
// incorrectly scoped input is never silently accepted.
type terminalInboxError struct{ err error }

func (e *terminalInboxError) Error() string { return e.err.Error() }
func (e *terminalInboxError) Unwrap() error { return e.err }

func terminalInbox(err error) error { return &terminalInboxError{err: err} }

// ControllerInbox processes one pre-provisioned durable participant queue. It
// persists operation IDs only after the controller has durably accepted the
// payload, and ACKs the native delivery only after both actions succeed.
type ControllerInbox struct {
	receiver   DurableReceiver
	controller ControllerHandler
	operations OperationStore
}

func NewControllerInbox(receiver DurableReceiver, handler ControllerHandler, operations OperationStore) (*ControllerInbox, error) {
	if receiver == nil || handler == nil || operations == nil {
		return nil, errors.New("broker0: receiver, controller handler, and operation store are required")
	}
	return &ControllerInbox{receiver: receiver, controller: handler, operations: operations}, nil
}

// Run receives until cancellation or a transport failure. Terminal deliveries
// are rejected and transient processing failures are retried against the same
// owned delivery, preserving durable redelivery semantics without stopping the
// controller process.
func (i *ControllerInbox) Run(ctx context.Context) error {
	for {
		delivery, err := i.receiver.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			return fmt.Errorf("broker0: receive controller inbox: %w", err)
		}
		if delivery == nil {
			return errors.New("broker0: durable receiver returned nil delivery")
		}
		rejectable, rejectableDelivery := delivery.(RejectableDelivery)
		backoff := 250 * time.Millisecond
		for {
			err = i.Handle(ctx, delivery)
			if err == nil {
				break
			}
			var terminal *terminalInboxError
			if errors.As(err, &terminal) {
				if !rejectableDelivery {
					return fmt.Errorf("broker0: terminal controller message cannot be rejected: %w", err)
				}
				if rejectErr := rejectable.Reject(ctx); rejectErr != nil {
					return errors.Join(err, fmt.Errorf("broker0: reject terminal controller message: %w", rejectErr))
				}
				break
			}
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			if !rejectableDelivery {
				return err
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

// Handle validates and processes one inbox delivery. It classifies malformed,
// mis-scoped, and controller-rejected messages as terminal; callers must settle
// them with Reject rather than Ack. Store and unclassified controller errors stay
// transient so the same delivery is retried.
func (i *ControllerInbox) Handle(ctx context.Context, delivery Delivery) error {
	if delivery == nil {
		return terminalInbox(errors.New("broker0: nil controller inbox delivery"))
	}
	message, err := parseMessage(delivery.Kind(), delivery.OperationID(), delivery.Payload())
	if err != nil {
		return terminalInbox(fmt.Errorf("broker0: parse controller inbox delivery: %w", err))
	}
	if message.Kind != KindRegistration && message.Kind != KindAcknowledgement && message.Kind != KindTelemetry {
		return terminalInbox(fmt.Errorf("broker0: controller inbox does not accept %q", message.Kind))
	}
	participant, err := validateControllerDeliveryScope(delivery, message)
	if err != nil {
		return terminalInbox(fmt.Errorf("broker0: validate controller inbox binding: %w", err))
	}

	completed, err := i.operations.Contains(ctx, message.OperationID)
	if err != nil {
		return fmt.Errorf("broker0: check inbox operation %q: %w", message.OperationID, err)
	}
	if !completed {
		switch message.Kind {
		case KindRegistration:
			registration := *message.Registration
			handler, ok := i.controller.(RegistrationHandler)
			if !ok {
				return terminalInbox(errors.New("broker0: controller does not support participant registration"))
			}
			err = handler.Register(registration)
		case KindAcknowledgement:
			var acknowledgement controller.Acknowledgement
			acknowledgement, err = acknowledgementForController(*message.Acknowledgement)
			if err == nil {
				acknowledgement.AuthenticatedParticipant = participant
				err = i.controller.Acknowledge(acknowledgement)
			}
		case KindTelemetry:
			err = i.controller.Observe(telemetryForController(*message.Telemetry))
		}
		if err != nil {
			appliedErr := fmt.Errorf("broker0: apply inbox operation %q: %w", message.OperationID, err)
			// The controller deliberately uses ErrStaleMessage for both historical and
			// incorrectly scoped input. Rejecting the authenticated delivery is safe for
			// both cases; recording and ACKing it would silently accept the latter.
			if errors.Is(err, controller.ErrStaleMessage) || errors.Is(err, controller.ErrTransitionNotFound) || errors.Is(err, controller.ErrUnknownParticipant) {
				return terminalInbox(appliedErr)
			}
			return appliedErr
		}
		if err := i.operations.Record(ctx, message.OperationID); err != nil {
			return fmt.Errorf("broker0: record inbox operation %q: %w", message.OperationID, err)
		}
	}
	if err := delivery.Ack(ctx); err != nil {
		return fmt.Errorf("broker0: acknowledge inbox operation %q: %w", message.OperationID, err)
	}
	return nil
}

func validateControllerDeliveryScope(delivery Delivery, message Message) (string, error) {
	scoped, ok := delivery.(ScopedDelivery)
	if !ok {
		return "", errors.New("delivery has no trusted queue binding")
	}
	participant, present := scoped.AuthenticatedParticipant()
	if !present {
		return "", errors.New("delivery has no queue-bound participant")
	}
	if err := validateTransportIdentifier("queue-bound participant", participant); err != nil {
		return "", err
	}
	kind, group, role := scoped.AuthenticatedScope()
	if kind != message.Kind {
		return "", fmt.Errorf("queue kind %q does not match payload kind %q", kind, message.Kind)
	}
	var claimedParticipant, claimedGroup string
	var claimedRole control.ParticipantRole
	switch message.Kind {
	case KindRegistration:
		claimedParticipant, claimedGroup, claimedRole = message.Registration.Participant, message.Registration.Group, message.Registration.Role
	case KindAcknowledgement:
		claimedParticipant, claimedGroup, claimedRole = message.Acknowledgement.Participant, message.Acknowledgement.Group, message.Acknowledgement.Role
	case KindTelemetry:
		claimedParticipant, claimedGroup, claimedRole = message.Telemetry.Participant, message.Telemetry.Group, message.Telemetry.Role
	}
	if participant != claimedParticipant || group != claimedGroup || role != claimedRole {
		return "", fmt.Errorf("queue scope %q/%q/%q does not match payload scope %q/%q/%q", group, role, participant, claimedGroup, claimedRole, claimedParticipant)
	}
	return participant, nil
}

func (i *ControllerInbox) Close() error { return i.receiver.Close() }
