package broker0

import (
	"context"
	"errors"

	"github.com/solacese/solace-workload-balancer/control"
)

var (
	// ErrNoAuthoritativeState is used to fail closed before a successful browse.
	ErrNoAuthoritativeState = errors.New("broker0: no authoritative membership state")
	// ErrMembershipStale indicates that Broker 0 has not yielded authoritative
	// state within the configured safety window.
	ErrMembershipStale = errors.New("broker0: authoritative membership is stale")
)

// Publication is an owned, persistent Broker 0 message. Native implementations
// must await a positive broker acknowledgement. They must also make OperationID
// idempotent because a process can crash after broker acknowledgement but before
// recording the operation locally.
type Publication struct {
	Destination string
	Kind        Kind
	OperationID string
	Payload     []byte
}

// NativePersistentPublisher is the deliberately small boundary to a native
// Broker 0 client. It does not provision destinations or manage credentials.
type NativePersistentPublisher interface {
	PublishPersistent(context.Context, Publication) error
}

// Delivery remains owned by its receiver until Ack succeeds or the receive
// context is cancelled. Payload must remain stable for that lifetime.
type Delivery interface {
	Kind() Kind
	OperationID() string
	Payload() []byte
	Ack(context.Context) error
}

// AuthenticatedDelivery exposes the immutable participant scope of the exact
// pre-provisioned queue from which a controller message was delivered. The
// identity is a trusted broker-side binding, never a message header or payload.
type AuthenticatedDelivery interface {
	AuthenticatedParticipant() (string, bool)
}

// ScopedDelivery exposes the rest of the immutable queue binding. Controller
// inboxes use it to prevent payload kind/group/role claims from crossing an ACL-
// protected participant queue.
type ScopedDelivery interface {
	AuthenticatedDelivery
	AuthenticatedScope() (Kind, string, control.ParticipantRole)
}

// DurableReceiver represents an existing durable, client-acknowledged queue.
// A receiver is scoped to one participant; provisioning is outside this package.
type DurableReceiver interface {
	Receive(context.Context) (Delivery, error)
	Close() error
}

// DurableReceiverFactory binds, but never provisions or mutates, an existing
// durable participant queue.
type DurableReceiverFactory interface {
	BindDurable(context.Context, ParticipantQueue) (DurableReceiver, error)
}

// ParticipantQueue identifies one pre-provisioned controller inbox.
type ParticipantQueue struct {
	Participant string
	Principal   string
	Role        control.ParticipantRole
	Group       string
	Kind        Kind
	Queue       string
	Topic       string
}

// OperationStore durably records completed operation IDs. Implementations must
// make Record atomic and durable through process restart. Operation IDs are
// globally scoped by the envelope and controller contracts.
type OperationStore interface {
	Contains(context.Context, string) (bool, error)
	Record(context.Context, string) error
}

// BrowsedMessage is an owned copy of one retained control publication.
type BrowsedMessage struct {
	Kind        Kind
	OperationID string
	Payload     []byte
}

// Browser performs a non-destructive read of retained membership snapshots. It
// must not consume or acknowledge the shared retained record.
type Browser interface {
	Browse(context.Context, string) ([]BrowsedMessage, error)
}

// UpdateSubscription is established before Browser.Browse is called. Deliveries
// are durable and client-acknowledged. Reconnects emits after connectivity has
// been restored; it may be nil when the native adapter cannot signal reconnects.
type UpdateSubscription struct {
	Deliveries <-chan Delivery
	Reconnects <-chan struct{}
	Errors     <-chan error
	Close      func() error
}

// UpdateSubscriber binds an existing durable per-participant update queue. The
// returned subscription is live when Subscribe returns.
type UpdateSubscriber interface {
	Subscribe(context.Context, string, string) (UpdateSubscription, error)
}

// SnapshotApplier applies a complete reconciled state to one local shim. Apply
// and FailClosed must be idempotent. FailClosed must synchronously prevent new
// application traffic for the group before returning.
type SnapshotApplier interface {
	Apply(context.Context, Message) error
	FailClosed(context.Context, string, error) error
}
