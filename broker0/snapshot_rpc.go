package broker0

import (
	"context"
	"errors"
	"fmt"

	"github.com/solacese/solace-workload-balancer/control"
)

// SnapshotRequester performs one correlated request/reply exchange. Implementations
// must validate the transport correlation ID before returning the response.
type SnapshotRequester interface {
	RequestSnapshot(context.Context, control.SnapshotRequest) (control.SnapshotResponse, error)
}

// SnapshotRequestDelivery is one ACL-scoped request delivered to the controller.
// The transport owns it until Ack or Reject succeeds.
type SnapshotRequestDelivery interface {
	Request() control.SnapshotRequest
	ReplyTo() string
	CorrelationID() string
	AuthenticatedParticipant() (string, bool)
	Ack(context.Context) error
	Reject(context.Context) error
}

type SnapshotRequestReceiver interface {
	ReceiveSnapshotRequest(context.Context) (SnapshotRequestDelivery, error)
	Close() error
}

type SnapshotResponsePublisher interface {
	PublishSnapshotResponse(context.Context, string, string, control.SnapshotResponse) error
}

type SnapshotReplyResolver interface {
	SnapshotReply(control.SnapshotRequest) (string, error)
}

type SnapshotReplyResolverFunc func(control.SnapshotRequest) (string, error)

func (f SnapshotReplyResolverFunc) SnapshotReply(request control.SnapshotRequest) (string, error) {
	return f(request)
}

type SnapshotSource interface {
	SnapshotForGroup(string) (control.MembershipSnapshot, bool)
}

// SnapshotRequestServer serves only complete state from the controller's durable
// authoritative catalog. Duplicate requests are harmless and receive the same
// current snapshot with their own correlation ID.
type SnapshotRequestServer struct {
	Receiver  SnapshotRequestReceiver
	Publisher SnapshotResponsePublisher
	Source    SnapshotSource
	Authorize func(control.SnapshotRequest, string) error
	Replies   SnapshotReplyResolver
}

func (s *SnapshotRequestServer) Run(ctx context.Context) error {
	if s == nil || s.Receiver == nil || s.Publisher == nil || s.Source == nil || s.Authorize == nil || s.Replies == nil {
		return errors.New("broker0: snapshot request server is not initialized")
	}
	for {
		delivery, err := s.Receiver.ReceiveSnapshotRequest(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			return fmt.Errorf("broker0: receive snapshot request: %w", err)
		}
		request := delivery.Request()
		principal, authenticated := delivery.AuthenticatedParticipant()
		expectedReply, replyErr := s.Replies.SnapshotReply(request)
		if !authenticated || principal != request.Participant || replyErr != nil ||
			delivery.CorrelationID() != request.CorrelationID || delivery.ReplyTo() != expectedReply {
			if rejectErr := delivery.Reject(ctx); rejectErr != nil {
				return errors.Join(errors.New("broker0: reject untrusted snapshot request"), rejectErr)
			}
			continue
		}
		if err := request.Validate(); err != nil || s.Authorize(request, principal) != nil {
			if rejectErr := delivery.Reject(ctx); rejectErr != nil {
				return errors.Join(err, rejectErr)
			}
			continue
		}
		snapshot, ok := s.Source.SnapshotForGroup(request.Group)
		if !ok {
			// A valid but currently unavailable request is released once. The client
			// has its own bounded timeout/backoff and issues a fresh correlation ID;
			// this prevents one unsettled delivery from exhausting link credit.
			if err := delivery.Reject(ctx); err != nil {
				return fmt.Errorf("broker0: settle unavailable snapshot request: %w", err)
			}
			continue
		}
		response := control.SnapshotResponse{
			Version: control.BootstrapProtocolVersion, CorrelationID: request.CorrelationID,
			Namespace: request.Namespace, Group: request.Group, Participant: request.Participant,
			Role: request.Role, Snapshot: snapshot,
		}
		if err := response.ValidateFor(request); err != nil {
			return fmt.Errorf("broker0: build snapshot response: %w", err)
		}
		if err := s.Publisher.PublishSnapshotResponse(ctx, delivery.ReplyTo(), request.CorrelationID, response); err != nil {
			return fmt.Errorf("broker0: publish snapshot response: %w", err)
		}
		if err := delivery.Ack(ctx); err != nil {
			return fmt.Errorf("broker0: acknowledge snapshot request: %w", err)
		}
	}
}

func (s *SnapshotRequestServer) Close() error {
	if s == nil || s.Receiver == nil {
		return nil
	}
	return s.Receiver.Close()
}
