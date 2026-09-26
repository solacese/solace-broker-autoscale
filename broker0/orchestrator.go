package broker0

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

// OrchestratorOptions defines one scaling group's Broker 0 bootstrap and
// freshness policy.
type OrchestratorOptions struct {
	Group            string
	Participant      string
	RebrowseInterval time.Duration
	MaxStaleness     time.Duration
	Now              func() time.Time
}

func (options OrchestratorOptions) withDefaults() OrchestratorOptions {
	if options.RebrowseInterval <= 0 {
		options.RebrowseInterval = time.Minute
	}
	if options.MaxStaleness <= 0 {
		options.MaxStaleness = 5 * time.Minute
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return options
}

// GroupOrchestrator bootstraps and maintains one group's authoritative
// membership. It intentionally owns no native Broker 0 types.
type pendingDelivery struct {
	delivery  Delivery
	message   Message
	completed bool
}

type GroupOrchestrator struct {
	subscriber UpdateSubscriber
	browser    Browser
	applier    SnapshotApplier
	operations OperationStore
	options    OrchestratorOptions

	mu         sync.RWMutex
	ready      bool
	failed     bool
	lastBrowse time.Time
	current    control.MembershipSnapshot
}

func NewGroupOrchestrator(subscriber UpdateSubscriber, browser Browser, applier SnapshotApplier, operations OperationStore, options OrchestratorOptions) (*GroupOrchestrator, error) {
	options = options.withDefaults()
	if subscriber == nil || browser == nil || applier == nil || operations == nil {
		return nil, errors.New("broker0: subscriber, browser, applier, and operation store are required")
	}
	if err := validateTransportIdentifier("group", options.Group); err != nil {
		return nil, err
	}
	if err := validateTransportIdentifier("participant", options.Participant); err != nil {
		return nil, err
	}
	if options.MaxStaleness <= options.RebrowseInterval {
		return nil, errors.New("broker0: max staleness must exceed the rebrowse interval")
	}
	return &GroupOrchestrator{subscriber: subscriber, browser: browser, applier: applier, operations: operations, options: options}, nil
}

// Snapshot returns the last successfully applied snapshot and whether it is
// currently usable. A fail-closed orchestrator never reports usable state.
func (orchestrator *GroupOrchestrator) Snapshot() (control.MembershipSnapshot, bool) {
	orchestrator.mu.RLock()
	defer orchestrator.mu.RUnlock()
	if !orchestrator.ready || orchestrator.failed {
		return control.MembershipSnapshot{}, false
	}
	return orchestrator.current.Clone(), true
}

// Run subscribes before browsing, buffers live deliveries during the browse,
// applies the reconciled state, then acknowledges buffered deliveries. It
// periodically and after every reconnect repeats a non-destructive browse.
func (orchestrator *GroupOrchestrator) Run(ctx context.Context) error {
	if err := orchestrator.applier.FailClosed(ctx, orchestrator.options.Group, ErrNoAuthoritativeState); err != nil {
		return fmt.Errorf("broker0: establish initial fail-closed state: %w", err)
	}
	subscription, err := orchestrator.subscriber.Subscribe(ctx, orchestrator.options.Group, orchestrator.options.Participant)
	if err != nil {
		return fmt.Errorf("broker0: subscribe to group %q updates: %w", orchestrator.options.Group, err)
	}
	if subscription.Deliveries == nil {
		return orchestrator.fail(ctx, errors.New("broker0: update subscription has no delivery channel"))
	}
	if subscription.Close != nil {
		defer subscription.Close()
	}

	reconciler := control.NewReconciler()
	if err := reconciler.BeginSubscribe(); err != nil {
		return orchestrator.fail(ctx, err)
	}

	type browseResult struct {
		messages []Message
		err      error
	}
	browseResults := make(chan browseResult, 1)
	browsing := false
	browseAgain := false
	startBrowse := func() {
		if browsing {
			browseAgain = true
			return
		}
		browsing = true
		go func() {
			payloads, browseErr := orchestrator.browser.Browse(ctx, orchestrator.options.Group)
			result := browseResult{err: browseErr}
			if browseErr == nil {
				result.messages, result.err = parseBrowsed(orchestrator.options.Group, payloads)
			}
			select {
			case browseResults <- result:
			case <-ctx.Done():
			}
		}()
	}
	startBrowse()

	ticker := time.NewTicker(orchestrator.options.RebrowseInterval)
	defer ticker.Stop()
	staleTimer := time.NewTimer(orchestrator.options.MaxStaleness)
	defer staleTimer.Stop()
	resetStaleTimer := func() {
		if !staleTimer.Stop() {
			select {
			case <-staleTimer.C:
			default:
			}
		}
		staleTimer.Reset(orchestrator.options.MaxStaleness)
	}

	var pending []pendingDelivery
	ready := false
	deliveries := subscription.Deliveries
	reconnects := subscription.Reconnects
	subscriptionErrors := subscription.Errors

	for {
		select {
		case <-ctx.Done():
			return orchestrator.fail(context.WithoutCancel(ctx), ctx.Err())
		case <-ticker.C:
			startBrowse()
		case <-staleTimer.C:
			return orchestrator.fail(ctx, ErrMembershipStale)
		case _, ok := <-reconnects:
			if !ok {
				reconnects = nil
				continue
			}
			startBrowse()
		case receiveErr, ok := <-subscriptionErrors:
			if !ok {
				subscriptionErrors = nil
				continue
			}
			if receiveErr != nil {
				return orchestrator.fail(ctx, fmt.Errorf("broker0: update subscription: %w", receiveErr))
			}
		case delivery, ok := <-deliveries:
			if !ok {
				return orchestrator.fail(ctx, errors.New("broker0: update delivery channel closed"))
			}
			if delivery == nil {
				return orchestrator.fail(ctx, errors.New("broker0: update subscription returned nil delivery"))
			}
			message, parseErr := parseMessage(delivery.Kind(), delivery.OperationID(), delivery.Payload())
			if parseErr != nil {
				return orchestrator.fail(ctx, fmt.Errorf("broker0: parse membership update: %w", parseErr))
			}
			if message.Kind != KindMembershipSnapshot || message.Group != orchestrator.options.Group {
				return orchestrator.fail(ctx, fmt.Errorf("broker0: unexpected %q update for group %q", message.Kind, message.Group))
			}
			completed, operationErr := orchestrator.operations.Contains(ctx, message.OperationID)
			if operationErr != nil {
				return orchestrator.fail(ctx, fmt.Errorf("broker0: check update operation %q: %w", message.OperationID, operationErr))
			}
			// Before the initial browse is reconciled, even an operation completed
			// before a crash is a live observation. Include it in bootstrap ordering
			// before acknowledging its redelivery.
			if !ready {
				if _, applyErr := reconciler.ApplyUpdate(*message.Snapshot); applyErr != nil {
					return orchestrator.fail(ctx, fmt.Errorf("broker0: buffer update %q: %w", message.OperationID, applyErr))
				}
				pending = append(pending, pendingDelivery{delivery: delivery, message: message, completed: completed})
				continue
			}
			if completed {
				if ackErr := delivery.Ack(ctx); ackErr != nil {
					return orchestrator.fail(ctx, fmt.Errorf("broker0: acknowledge replayed update %q: %w", message.OperationID, ackErr))
				}
				continue
			}

			changed, applyErr := reconciler.ApplyUpdate(*message.Snapshot)
			if applyErr != nil {
				if isSuperseded(reconciler, *message.Snapshot, applyErr) {
					if err := orchestrator.complete(ctx, message.OperationID, delivery); err != nil {
						return orchestrator.fail(ctx, err)
					}
					continue
				}
				return orchestrator.fail(ctx, fmt.Errorf("broker0: reconcile update %q: %w", message.OperationID, applyErr))
			}
			if changed {
				if err := orchestrator.apply(ctx, message); err != nil {
					return orchestrator.fail(ctx, err)
				}
			}
			if err := orchestrator.complete(ctx, message.OperationID, delivery); err != nil {
				return orchestrator.fail(ctx, err)
			}

		case result := <-browseResults:
			browsing = false
			if result.err != nil {
				if !ready {
					return orchestrator.fail(ctx, fmt.Errorf("broker0: initial authoritative browse: %w", result.err))
				}
				if browseAgain {
					browseAgain = false
					startBrowse()
				}
				continue
			}

			if !ready {
				for _, message := range result.messages {
					if err := reconciler.ApplyBrowse(*message.Snapshot); err != nil {
						return orchestrator.fail(ctx, fmt.Errorf("broker0: reconcile initial browse: %w", err))
					}
				}
				final, finishErr := reconciler.FinishBrowse()
				if finishErr != nil {
					return orchestrator.fail(ctx, fmt.Errorf("broker0: finish initial browse: %w", finishErr))
				}
				message, findErr := messageForSnapshot(final, result.messages, pending)
				if findErr != nil {
					return orchestrator.fail(ctx, findErr)
				}
				if err := orchestrator.apply(ctx, message); err != nil {
					return orchestrator.fail(ctx, err)
				}
				if err := orchestrator.operations.Record(ctx, message.OperationID); err != nil {
					return orchestrator.fail(ctx, fmt.Errorf("broker0: record browsed operation %q: %w", message.OperationID, err))
				}
				for _, buffered := range pending {
					if buffered.completed {
						if err := buffered.delivery.Ack(ctx); err != nil {
							return orchestrator.fail(ctx, fmt.Errorf("broker0: acknowledge replayed update %q: %w", buffered.message.OperationID, err))
						}
						continue
					}
					if err := orchestrator.complete(ctx, buffered.message.OperationID, buffered.delivery); err != nil {
						return orchestrator.fail(ctx, err)
					}
				}
				pending = nil
				ready = true
			} else {
				candidate, selectErr := selectBrowsed(result.messages)
				if selectErr != nil {
					return orchestrator.fail(ctx, fmt.Errorf("broker0: reconcile authoritative browse: %w", selectErr))
				}
				changed, applyErr := reconciler.ApplyUpdate(*candidate.Snapshot)
				if applyErr != nil {
					// A periodic browse can complete after a newer live delivery. That
					// older retained value is a normal race, not loss of authority.
					if !isSuperseded(reconciler, *candidate.Snapshot, applyErr) {
						return orchestrator.fail(ctx, fmt.Errorf("broker0: authoritative browse regressed or conflicted: %w", applyErr))
					}
					changed = false
				}
				if changed {
					if err := orchestrator.apply(ctx, candidate); err != nil {
						return orchestrator.fail(ctx, err)
					}
				}
				if err := orchestrator.operations.Record(ctx, candidate.OperationID); err != nil {
					return orchestrator.fail(ctx, fmt.Errorf("broker0: record browsed operation %q: %w", candidate.OperationID, err))
				}
			}
			orchestrator.markBrowse(orchestrator.options.Now())
			resetStaleTimer()
			if browseAgain {
				browseAgain = false
				startBrowse()
			}
		}
	}
}

func (orchestrator *GroupOrchestrator) apply(ctx context.Context, message Message) error {
	if message.Snapshot == nil {
		return errors.New("broker0: cannot apply empty membership snapshot")
	}
	if err := orchestrator.applier.Apply(ctx, message); err != nil {
		return fmt.Errorf("broker0: apply group %q revision %d: %w", orchestrator.options.Group, message.Snapshot.Revision, err)
	}
	orchestrator.mu.Lock()
	orchestrator.current = message.Snapshot.Clone()
	orchestrator.ready = true
	orchestrator.failed = false
	orchestrator.mu.Unlock()
	return nil
}

func (orchestrator *GroupOrchestrator) complete(ctx context.Context, operationID string, delivery Delivery) error {
	if err := orchestrator.operations.Record(ctx, operationID); err != nil {
		return fmt.Errorf("broker0: record applied operation %q: %w", operationID, err)
	}
	if err := delivery.Ack(ctx); err != nil {
		return fmt.Errorf("broker0: acknowledge applied operation %q: %w", operationID, err)
	}
	return nil
}

func (orchestrator *GroupOrchestrator) fail(ctx context.Context, cause error) error {
	orchestrator.mu.Lock()
	orchestrator.failed = true
	orchestrator.mu.Unlock()
	if err := orchestrator.applier.FailClosed(ctx, orchestrator.options.Group, cause); err != nil {
		return errors.Join(cause, fmt.Errorf("broker0: fail closed group %q: %w", orchestrator.options.Group, err))
	}
	return cause
}

func (orchestrator *GroupOrchestrator) markBrowse(at time.Time) {
	orchestrator.mu.Lock()
	orchestrator.lastBrowse = at
	orchestrator.mu.Unlock()
}

func parseBrowsed(group string, payloads []BrowsedMessage) ([]Message, error) {
	if len(payloads) == 0 {
		return nil, ErrNoAuthoritativeState
	}
	messages := make([]Message, 0, len(payloads))
	for index, payload := range payloads {
		message, err := parseMessage(payload.Kind, payload.OperationID, payload.Payload)
		if err != nil {
			return nil, fmt.Errorf("browse result %d: %w", index, err)
		}
		if message.Kind != KindMembershipSnapshot || message.Group != group {
			return nil, fmt.Errorf("browse result %d is %q for group %q", index, message.Kind, message.Group)
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func selectBrowsed(messages []Message) (Message, error) {
	reconciler := control.NewReconciler()
	if err := reconciler.BeginSubscribe(); err != nil {
		return Message{}, err
	}
	for _, message := range messages {
		if err := reconciler.ApplyBrowse(*message.Snapshot); err != nil {
			return Message{}, err
		}
	}
	selected, err := reconciler.FinishBrowse()
	if err != nil {
		return Message{}, err
	}
	for _, message := range messages {
		if reflect.DeepEqual(*message.Snapshot, selected) {
			message.Snapshot = pointerToSnapshot(selected)
			return message, nil
		}
	}
	return Message{}, errors.New("broker0: selected browse snapshot has no source envelope")
}

func messageForSnapshot(snapshot control.MembershipSnapshot, browsed []Message, pending []pendingDelivery) (Message, error) {
	for _, message := range browsed {
		if reflect.DeepEqual(*message.Snapshot, snapshot) {
			message.Snapshot = pointerToSnapshot(snapshot)
			return message, nil
		}
	}
	for _, item := range pending {
		if reflect.DeepEqual(*item.message.Snapshot, snapshot) {
			message := item.message
			message.Snapshot = pointerToSnapshot(snapshot)
			return message, nil
		}
	}
	return Message{}, errors.New("broker0: reconciled snapshot has no source envelope")
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
