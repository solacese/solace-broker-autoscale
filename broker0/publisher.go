package broker0

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
)

// DestinationResolver maps a typed, group-scoped Broker 0 message to an
// existing native destination. It must not provision resources.
type DestinationResolver interface {
	Destination(Kind, string) (string, error)
}

// DestinationResolverFunc adapts a function to DestinationResolver.
type DestinationResolverFunc func(Kind, string) (string, error)

func (f DestinationResolverFunc) Destination(kind Kind, group string) (string, error) {
	return f(kind, group)
}

// PersistentControlPublisher provides positive-acknowledged, operation-id
// deduplicated Broker 0 publication. It satisfies controller.ControlPublisher.
var _ controller.ControlPublisher = (*PersistentControlPublisher)(nil)

// TargetDestinationResolver maps a participant-scoped reliable message to a
// pre-provisioned topic. Per-participant topics allow broker ACLs and dedicated
// queues to bind a claimed participant to an authenticated principal.
type TargetDestinationResolver interface {
	TargetDestination(Kind, string, control.ParticipantRole, string) (string, error)
}

type TargetDestinationResolverFunc func(Kind, string, control.ParticipantRole, string) (string, error)

func (f TargetDestinationResolverFunc) TargetDestination(kind Kind, group string, role control.ParticipantRole, participant string) (string, error) {
	return f(kind, group, role, participant)
}

type PersistentControlPublisher struct {
	mu           sync.Mutex
	native       NativePersistentPublisher
	operations   OperationStore
	destinations DestinationResolver
	targets      TargetDestinationResolver
}

func NewPersistentControlPublisher(native NativePersistentPublisher, operations OperationStore, destinations DestinationResolver) (*PersistentControlPublisher, error) {
	return NewPersistentControlPublisherWithTargets(native, operations, destinations, nil)
}

func NewPersistentControlPublisherWithTargets(native NativePersistentPublisher, operations OperationStore, destinations DestinationResolver, targets TargetDestinationResolver) (*PersistentControlPublisher, error) {
	if native == nil || operations == nil || destinations == nil {
		return nil, errors.New("broker0: native publisher, operation store, and destination resolver are required")
	}
	return &PersistentControlPublisher{native: native, operations: operations, destinations: destinations, targets: targets}, nil
}

// Publish adapts a controller desired-state update to a strict snapshot
// envelope. The controller's stable OperationID is the idempotency key.
func (p *PersistentControlPublisher) Publish(ctx context.Context, update controller.ControlUpdate) error {
	if update.OperationID == "" || update.Group == "" || update.Group != update.Snapshot.ScalingGroup {
		return errors.New("broker0: invalid controller control update scope")
	}
	return p.publishSnapshot(ctx, update.OperationID, update.Snapshot)
}

// PublishSnapshotOperation publishes a complete snapshot with an explicit
// controller operation ID. It is used by fanout adapters so the retained and
// live-update copies have distinct durable idempotency keys.
func (p *PersistentControlPublisher) PublishSnapshotOperation(ctx context.Context, operationID string, snapshot control.MembershipSnapshot) error {
	return p.publishSnapshot(ctx, operationID, snapshot)
}

// PublishSnapshotTo publishes a snapshot to an exact pre-provisioned topic.
// It is intended for the live-update fanout copy; callers still validate the
// destination through their managed-name resolver.
func (p *PersistentControlPublisher) PublishSnapshotTo(ctx context.Context, destination, operationID string, snapshot control.MembershipSnapshot) error {
	payload, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		return err
	}
	return p.publishTo(ctx, KindMembershipSnapshot, destination, operationID, payload)
}

// PublishSnapshot publishes a complete authoritative membership snapshot. Its
// deterministic operation ID makes exact replay safe for bootstrap and tests.
func (p *PersistentControlPublisher) PublishSnapshot(ctx context.Context, snapshot control.MembershipSnapshot) error {
	operationID := fmt.Sprintf("%s/snapshot/%d/%d/%s", snapshot.ScalingGroup, snapshot.Revision, snapshot.Epoch, snapshot.Phase)
	return p.publishSnapshot(ctx, operationID, snapshot)
}

// PublishRegistration publishes a strictly validated participant registration.
func (p *PersistentControlPublisher) PublishRegistration(ctx context.Context, registration control.RegistrationEnvelope) error {
	payload, err := encodeRegistration(registration)
	if err != nil {
		return err
	}
	if p.targets == nil {
		return errors.New("broker0: registration requires a participant-scoped destination resolver")
	}
	destination, resolveErr := p.targets.TargetDestination(KindRegistration, registration.Group, registration.Role, registration.Participant)
	if resolveErr != nil {
		return resolveErr
	}
	return p.publishTo(ctx, KindRegistration, destination, registration.MessageID, payload)
}

// PublishCommand publishes a strictly validated command to the managed topic
// dedicated to its participant. The command message ID is the idempotency key.
func (p *PersistentControlPublisher) PublishCommand(ctx context.Context, command control.CommandEnvelope) error {
	payload, err := encodeCommand(command)
	if err != nil {
		return err
	}
	var destination string
	if p.targets != nil {
		destination, err = p.targets.TargetDestination(KindCommand, command.Group, command.Role, command.Participant)
	} else {
		var names control.ManagedNames
		names, err = control.NewManagedNames(command.Namespace)
		if err == nil {
			destination, err = names.CommandTopic(command.Group, command.Role, command.Participant)
		}
	}
	if err != nil {
		return err
	}
	return p.publishTo(ctx, KindCommand, destination, command.MessageID, payload)
}

// PublishAcknowledgement publishes a strictly validated acknowledgement to its
// participant-scoped managed topic.
func (p *PersistentControlPublisher) PublishAcknowledgement(ctx context.Context, acknowledgement control.AcknowledgementEnvelope) error {
	payload, err := encodeAcknowledgement(acknowledgement)
	if err != nil {
		return err
	}
	if p.targets == nil {
		return errors.New("broker0: acknowledgement requires a participant-scoped destination resolver")
	}
	destination, resolveErr := p.targets.TargetDestination(KindAcknowledgement, acknowledgement.Group, acknowledgement.Role, acknowledgement.Participant)
	if resolveErr != nil {
		return resolveErr
	}
	return p.publishTo(ctx, KindAcknowledgement, destination, acknowledgement.MessageID, payload)
}

// PublishTelemetry publishes strictly validated telemetry to its
// participant-scoped managed topic.
func (p *PersistentControlPublisher) PublishTelemetry(ctx context.Context, telemetry control.TelemetryEnvelope) error {
	payload, err := encodeTelemetry(telemetry)
	if err != nil {
		return err
	}
	if p.targets == nil {
		return errors.New("broker0: telemetry requires a participant-scoped destination resolver")
	}
	destination, resolveErr := p.targets.TargetDestination(KindTelemetry, telemetry.Group, telemetry.Role, telemetry.Participant)
	if resolveErr != nil {
		return resolveErr
	}
	return p.publishTo(ctx, KindTelemetry, destination, telemetry.MessageID, payload)
}

func (p *PersistentControlPublisher) publishSnapshot(ctx context.Context, operationID string, snapshot control.MembershipSnapshot) error {
	payload, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		return err
	}
	return p.publish(ctx, KindMembershipSnapshot, snapshot.ScalingGroup, operationID, payload)
}

func (p *PersistentControlPublisher) publish(ctx context.Context, kind Kind, group, operationID string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	completed, err := p.operations.Contains(ctx, operationID)
	if err != nil {
		return fmt.Errorf("broker0: check operation %q: %w", operationID, err)
	}
	if completed {
		return nil
	}
	destination, err := p.destinations.Destination(kind, group)
	if err != nil {
		return fmt.Errorf("broker0: resolve %s destination for %q: %w", kind, group, err)
	}
	if destination == "" {
		return fmt.Errorf("broker0: empty %s destination for %q", kind, group)
	}
	return p.publishLocked(ctx, kind, destination, operationID, payload)
}

func (p *PersistentControlPublisher) publishTo(ctx context.Context, kind Kind, destination, operationID string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	completed, err := p.operations.Contains(ctx, operationID)
	if err != nil {
		return fmt.Errorf("broker0: check operation %q: %w", operationID, err)
	}
	if completed {
		return nil
	}
	if destination == "" {
		return fmt.Errorf("broker0: empty %s destination", kind)
	}
	return p.publishLocked(ctx, kind, destination, operationID, payload)
}

func (p *PersistentControlPublisher) publishLocked(ctx context.Context, kind Kind, destination, operationID string, payload []byte) error {
	publication := Publication{Destination: destination, Kind: kind, OperationID: operationID, Payload: append([]byte(nil), payload...)}
	if err := p.native.PublishPersistent(ctx, publication); err != nil {
		return fmt.Errorf("broker0: publish operation %q: %w", operationID, err)
	}
	if err := p.operations.Record(ctx, operationID); err != nil {
		// The broker may already have accepted the operation. A retry uses the same
		// operation ID, so the native destination must deduplicate it.
		return fmt.Errorf("broker0: record published operation %q: %w", operationID, err)
	}
	return nil
}
