package broker0

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

// OrchestratorOptions defines one group's request/reply bootstrap and freshness
// policy. Refresh and retry are bounded; no cached state becomes routable before
// a valid controller response has been reconciled with buffered updates.
type OrchestratorOptions struct {
	Namespace       string
	Group           string
	Participant     string
	Role            control.ParticipantRole
	RefreshInterval time.Duration
	MaxStaleness    time.Duration
	RequestTimeout  time.Duration
	RetryInitial    time.Duration
	RetryMaximum    time.Duration
	Now             func() time.Time
}

func (options OrchestratorOptions) withDefaults() OrchestratorOptions {
	if options.RefreshInterval <= 0 {
		options.RefreshInterval = time.Minute
	}
	if options.MaxStaleness <= 0 {
		options.MaxStaleness = 5 * time.Minute
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = 10 * time.Second
	}
	if options.RetryInitial <= 0 {
		options.RetryInitial = 250 * time.Millisecond
	}
	if options.RetryMaximum <= 0 {
		options.RetryMaximum = 5 * time.Second
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return options
}

type pendingDelivery struct {
	delivery  Delivery
	message   Message
	completed bool
}

// GroupOrchestrator subscribes to updates first, requests an authoritative full
// snapshot second, and reconciles revisions before exposing state.
type GroupOrchestrator struct {
	subscriber UpdateSubscriber
	requester  SnapshotRequester
	applier    SnapshotApplier
	operations OperationStore
	options    OrchestratorOptions

	mu      sync.RWMutex
	ready   bool
	failed  bool
	current control.MembershipSnapshot
}

func NewGroupOrchestrator(subscriber UpdateSubscriber, requester SnapshotRequester, applier SnapshotApplier, operations OperationStore, options OrchestratorOptions) (*GroupOrchestrator, error) {
	options = options.withDefaults()
	if subscriber == nil || requester == nil || applier == nil || operations == nil {
		return nil, errors.New("broker0: subscriber, snapshot requester, applier, and operation store are required")
	}
	for field, value := range map[string]string{"namespace": options.Namespace, "group": options.Group, "participant": options.Participant} {
		if err := validateTransportIdentifier(field, value); err != nil {
			return nil, err
		}
	}
	if err := options.Role.Validate(); err != nil {
		return nil, err
	}
	if options.MaxStaleness <= options.RefreshInterval || options.RetryMaximum < options.RetryInitial {
		return nil, errors.New("broker0: invalid refresh, staleness, or retry bounds")
	}
	return &GroupOrchestrator{subscriber: subscriber, requester: requester, applier: applier, operations: operations, options: options}, nil
}

func (o *GroupOrchestrator) Snapshot() (control.MembershipSnapshot, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if !o.ready || o.failed {
		return control.MembershipSnapshot{}, false
	}
	return o.current.Clone(), true
}

func (o *GroupOrchestrator) Run(ctx context.Context) error {
	if err := o.applier.FailClosed(ctx, o.options.Group, ErrNoAuthoritativeState); err != nil {
		return fmt.Errorf("broker0: establish initial fail-closed state: %w", err)
	}
	subscription, err := o.subscriber.Subscribe(ctx, o.options.Group, o.options.Participant)
	if err != nil {
		return fmt.Errorf("broker0: subscribe to group %q updates: %w", o.options.Group, err)
	}
	if subscription.Deliveries == nil {
		return o.fail(ctx, errors.New("broker0: update subscription has no delivery channel"))
	}
	if subscription.Close != nil {
		defer subscription.Close()
	}

	reconciler := control.NewReconciler()
	if err := reconciler.BeginSubscribe(); err != nil {
		return o.fail(ctx, err)
	}
	type requestResult struct {
		request  control.SnapshotRequest
		response control.SnapshotResponse
		err      error
	}
	results := make(chan requestResult, 1)
	requesting := false
	requestAgain := false
	startRequest := func() {
		if requesting {
			requestAgain = true
			return
		}
		requesting = true
		request := control.SnapshotRequest{
			Version: control.BootstrapProtocolVersion, CorrelationID: newCorrelationID(),
			Namespace: o.options.Namespace, Group: o.options.Group,
			Participant: o.options.Participant, Role: o.options.Role,
			RequestedAt: o.options.Now().UTC(),
		}
		go func() {
			requestCtx, cancel := context.WithTimeout(ctx, o.options.RequestTimeout)
			defer cancel()
			response, requestErr := o.requester.RequestSnapshot(requestCtx, request)
			select {
			case results <- requestResult{request: request, response: response, err: requestErr}:
			case <-ctx.Done():
			}
		}()
	}
	startRequest()

	refresh := time.NewTicker(o.options.RefreshInterval)
	defer refresh.Stop()
	stale := time.NewTimer(o.options.MaxStaleness)
	defer stale.Stop()
	var retry <-chan time.Time
	retryDelay := o.options.RetryInitial
	resetStale := func() {
		if !stale.Stop() {
			select {
			case <-stale.C:
			default:
			}
		}
		stale.Reset(o.options.MaxStaleness)
	}

	var pending []pendingDelivery
	ready := false
	deliveries, reconnects, subscriptionErrors := subscription.Deliveries, subscription.Reconnects, subscription.Errors
	for {
		select {
		case <-ctx.Done():
			return o.fail(context.WithoutCancel(ctx), ctx.Err())
		case <-refresh.C:
			startRequest()
		case <-retry:
			retry = nil
			startRequest()
		case <-stale.C:
			return o.fail(ctx, ErrMembershipStale)
		case _, ok := <-reconnects:
			if !ok {
				reconnects = nil
				continue
			}
			// A reconnect creates a gap in update authority. Revoke routing before
			// requesting a fresh full snapshot; live updates buffer until it arrives.
			o.mu.Lock()
			o.failed = true
			o.mu.Unlock()
			ready = false
			reconciler = control.NewReconciler()
			if err := reconciler.BeginSubscribe(); err != nil {
				return o.fail(ctx, err)
			}
			if err := o.applier.FailClosed(ctx, o.options.Group, ErrNoAuthoritativeState); err != nil {
				return o.fail(ctx, err)
			}
			startRequest()
		case receiveErr, ok := <-subscriptionErrors:
			if !ok {
				subscriptionErrors = nil
				continue
			}
			if receiveErr != nil {
				return o.fail(ctx, fmt.Errorf("broker0: update subscription: %w", receiveErr))
			}
		case delivery, ok := <-deliveries:
			if !ok {
				return o.fail(ctx, errors.New("broker0: update delivery channel closed"))
			}
			if delivery == nil {
				return o.fail(ctx, errors.New("broker0: update subscription returned nil delivery"))
			}
			message, parseErr := parseMessage(delivery.Kind(), delivery.OperationID(), delivery.Payload())
			if parseErr != nil || message.Kind != KindMembershipSnapshot || message.Group != o.options.Group {
				return o.fail(ctx, errors.Join(parseErr, errors.New("broker0: invalid membership update scope")))
			}
			completed, operationErr := o.operations.Contains(ctx, message.OperationID)
			if operationErr != nil {
				return o.fail(ctx, operationErr)
			}
			if !ready {
				if _, applyErr := reconciler.ApplyUpdate(*message.Snapshot); applyErr != nil {
					return o.fail(ctx, applyErr)
				}
				pending = append(pending, pendingDelivery{delivery: delivery, message: message, completed: completed})
				continue
			}
			if completed {
				if err := delivery.Ack(ctx); err != nil {
					return o.fail(ctx, err)
				}
				continue
			}
			changed, applyErr := reconciler.ApplyUpdate(*message.Snapshot)
			if applyErr != nil && !isSuperseded(reconciler, *message.Snapshot, applyErr) {
				return o.fail(ctx, applyErr)
			}
			if changed && applyErr == nil {
				if err := o.apply(ctx, message); err != nil {
					return o.fail(ctx, err)
				}
			}
			if err := o.complete(ctx, message.OperationID, delivery); err != nil {
				return o.fail(ctx, err)
			}
		case result := <-results:
			requesting = false
			if result.err != nil {
				if retry == nil {
					retry = time.After(retryDelay)
					retryDelay = min(retryDelay*2, o.options.RetryMaximum)
				}
				if requestAgain {
					requestAgain = false
					startRequest()
				}
				continue
			}
			if err := result.response.ValidateFor(result.request); err != nil {
				return o.fail(ctx, err)
			}
			retryDelay = o.options.RetryInitial
			if !ready {
				if err := reconciler.ApplyBaseline(result.response.Snapshot); err != nil {
					return o.fail(ctx, err)
				}
				final, finishErr := reconciler.FinishBootstrap()
				if finishErr != nil {
					return o.fail(ctx, finishErr)
				}
				message := Message{Kind: KindMembershipSnapshot, OperationID: result.request.CorrelationID, Group: final.ScalingGroup, Snapshot: pointerToSnapshot(final)}
				if err := o.apply(ctx, message); err != nil {
					return o.fail(ctx, err)
				}
				for _, buffered := range pending {
					if buffered.completed {
						err = buffered.delivery.Ack(ctx)
					} else {
						err = o.complete(ctx, buffered.message.OperationID, buffered.delivery)
					}
					if err != nil {
						return o.fail(ctx, err)
					}
				}
				pending = nil
				ready = true
			} else {
				changed, applyErr := reconciler.ApplyUpdate(result.response.Snapshot)
				if applyErr != nil && !isSuperseded(reconciler, result.response.Snapshot, applyErr) {
					return o.fail(ctx, applyErr)
				}
				if changed && applyErr == nil {
					message := Message{Kind: KindMembershipSnapshot, OperationID: result.request.CorrelationID, Group: result.response.Group, Snapshot: pointerToSnapshot(result.response.Snapshot)}
					if err := o.apply(ctx, message); err != nil {
						return o.fail(ctx, err)
					}
				}
			}
			resetStale()
			if requestAgain {
				requestAgain = false
				startRequest()
			}
		}
	}
}

func newCorrelationID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(fmt.Sprintf("broker0: generate correlation ID: %v", err))
	}
	return hex.EncodeToString(value[:])
}

func (o *GroupOrchestrator) apply(ctx context.Context, message Message) error {
	if message.Snapshot == nil {
		return errors.New("broker0: cannot apply empty membership snapshot")
	}
	if err := o.applier.Apply(ctx, message); err != nil {
		return err
	}
	o.mu.Lock()
	o.current, o.ready, o.failed = message.Snapshot.Clone(), true, false
	o.mu.Unlock()
	return nil
}

func (o *GroupOrchestrator) complete(ctx context.Context, operationID string, delivery Delivery) error {
	if err := o.operations.Record(ctx, operationID); err != nil {
		return err
	}
	return delivery.Ack(ctx)
}

func (o *GroupOrchestrator) fail(ctx context.Context, cause error) error {
	o.mu.Lock()
	o.failed = true
	o.mu.Unlock()
	if err := o.applier.FailClosed(ctx, o.options.Group, cause); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func pointerToSnapshot(snapshot control.MembershipSnapshot) *control.MembershipSnapshot {
	clone := snapshot.Clone()
	return &clone
}

func isSuperseded(reconciler *control.Reconciler, update control.MembershipSnapshot, err error) bool {
	if !errors.Is(err, control.ErrStaleUpdate) {
		return false
	}
	current, ok := reconciler.Snapshot()
	return ok && update.Revision < current.Revision && update.Epoch <= current.Epoch
}
