// Package runtime owns process lifecycles and connects transport-neutral control
// plane components. Native adapters are supplied by the executable assembly.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/controller"
)

var ErrNativeAdapterUnavailable = errors.New("runtime: required native adapter is unavailable")

// Component is one long-running process component. Run must stop when ctx is
// cancelled. Close should interrupt blocked I/O and is called during shutdown.
type Component interface {
	Run(context.Context) error
	Close() error
}

// Reconciler is the durable controller boundary used by the process runtime.
type Reconciler interface {
	Reconcile(context.Context, string) (bool, error)
	Snapshot() controller.PersistentState
}

type rollbackRequester interface {
	RequestRollback(string, controller.RollbackProof) error
}

// CompletedTransitionCleaner removes resources only after controller completion
// and durable cleanup evidence are visible in the same snapshot.
type CompletedTransitionCleaner interface {
	CleanupCompleted(context.Context, controller.PersistentState, string) error
}

// Trigger carries an optional group-scoped native event. An empty Group asks the
// runtime to reconcile every group.
type Trigger struct {
	Group  string
	Reason string
}

// ControllerOptions bounds one action attempt and retry behavior. Periodic
// reconciliation also enforces persisted controller phase deadlines.
type ControllerOptions struct {
	// Groups is the configured group inventory. Including groups with no persisted
	// transition lets an event wake them after policy starts new work.
	Groups                  []string
	ReconcileInterval       time.Duration
	ActionTimeout           time.Duration
	RetryInitial            time.Duration
	RetryMaximum            time.Duration
	ShutdownTimeout         time.Duration
	Logger                  *slog.Logger
	Cleaner                 CompletedTransitionCleaner
	Catalog                 *MembershipCatalog
	RollbackFenceReversible bool
	Now                     func() time.Time
	After                   func(time.Duration) <-chan time.Time
}

func (o ControllerOptions) withDefaults() ControllerOptions {
	if o.ReconcileInterval <= 0 {
		o.ReconcileInterval = 5 * time.Second
	}
	if o.ActionTimeout <= 0 {
		o.ActionTimeout = 30 * time.Second
	}
	if o.RetryInitial <= 0 {
		o.RetryInitial = 250 * time.Millisecond
	}
	if o.RetryMaximum <= 0 {
		o.RetryMaximum = 10 * time.Second
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.After == nil {
		o.After = time.After
	}
	return o
}

// ControllerProcess runs inbox/event components and independent per-group
// reconcile loops. A failing group backs off without blocking other groups.
type ControllerProcess struct {
	reconciler Reconciler
	components []Component
	triggers   <-chan Trigger
	options    ControllerOptions
}

func NewControllerProcess(reconciler Reconciler, components []Component, triggers <-chan Trigger, options ControllerOptions) (*ControllerProcess, error) {
	if reconciler == nil {
		return nil, errors.New("runtime: controller reconciler is required")
	}
	for index, component := range components {
		if component == nil {
			return nil, fmt.Errorf("runtime: component %d is nil", index)
		}
	}
	options = options.withDefaults()
	if options.RetryMaximum < options.RetryInitial {
		return nil, errors.New("runtime: maximum retry delay cannot be shorter than initial delay")
	}
	return &ControllerProcess{reconciler: reconciler, components: slices.Clone(components), triggers: triggers, options: options}, nil
}

// Run performs initial reconciliation before entering the long-running event
// loop, then stops all components on cancellation or a fatal component failure.
func (p *ControllerProcess) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	componentErrors := make(chan error, len(p.components))
	var componentWG sync.WaitGroup
	for _, component := range p.components {
		componentWG.Add(1)
		go func(component Component) {
			defer componentWG.Done()
			err := component.Run(ctx)
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			if err == nil {
				err = errors.New("component stopped unexpectedly")
			}
			select {
			case componentErrors <- err:
			case <-ctx.Done():
			}
		}(component)
	}

	groups := p.groupNames()
	groupSignals := make(map[string]chan struct{}, len(groups))
	var groupWG sync.WaitGroup
	for _, group := range groups {
		signal := make(chan struct{}, 1)
		groupSignals[group] = signal
		groupWG.Add(1)
		go func(group string, signal <-chan struct{}) {
			defer groupWG.Done()
			p.runGroup(ctx, group, signal)
		}(group, signal)
	}

	// Initial reconciliation is queued synchronously so no timer or external event
	// is required to resume durable work after restart.
	for _, signal := range groupSignals {
		signal <- struct{}{}
	}

	ticker := time.NewTicker(p.options.ReconcileInterval)
	defer ticker.Stop()
	var runErr error
run:
	for {
		select {
		case <-ctx.Done():
			runErr = ctx.Err()
			break run
		case err := <-componentErrors:
			runErr = fmt.Errorf("runtime: component failed: %w", err)
			break run
		case <-ticker.C:
			p.signalAll(groupSignals)
		case trigger, ok := <-p.triggers:
			if !ok {
				p.triggers = nil
				continue
			}
			if trigger.Group == "" {
				p.signalAll(groupSignals)
				continue
			}
			if signal, exists := groupSignals[trigger.Group]; exists {
				trySignal(signal)
			} else {
				p.options.Logger.Warn("ignoring trigger for unknown group", "group", trigger.Group, "reason", trigger.Reason)
			}
		}
	}

	cancel()
	shutdown := make(chan error, 1)
	go func() {
		closeErr := p.closeComponents()
		groupWG.Wait()
		componentWG.Wait()
		shutdown <- closeErr
	}()
	var closeErr error
	select {
	case closeErr = <-shutdown:
	case <-p.options.After(p.options.ShutdownTimeout):
		closeErr = errors.New("runtime: graceful shutdown deadline exceeded")
	}
	if errors.Is(runErr, context.Canceled) {
		runErr = nil
	}
	return errors.Join(runErr, closeErr)
}

func (p *ControllerProcess) groupNames() []string {
	set := make(map[string]struct{}, len(p.options.Groups))
	for _, group := range p.options.Groups {
		if group != "" {
			set[group] = struct{}{}
		}
	}
	for group := range p.reconciler.Snapshot().Groups {
		set[group] = struct{}{}
	}
	return slices.Sorted(maps.Keys(set))
}

func (p *ControllerProcess) runGroup(ctx context.Context, group string, signal <-chan struct{}) {
	backoff := p.options.RetryInitial
	for {
		select {
		case <-ctx.Done():
			return
		case <-signal:
		}

		for {
			actionCtx, cancel := context.WithTimeout(ctx, p.actionTimeout(group))
			changed, err := p.reconciler.Reconcile(actionCtx, group)
			cancel()
			if errors.Is(err, controller.ErrTransitionNotFound) {
				backoff = p.options.RetryInitial
				break
			}
			if err == nil {
				backoff = p.options.RetryInitial
				if p.options.Cleaner != nil {
					cleanupCtx, cleanupCancel := context.WithTimeout(ctx, p.options.ActionTimeout)
					cleanupErr := p.options.Cleaner.CleanupCompleted(cleanupCtx, p.reconciler.Snapshot(), group)
					cleanupCancel()
					if cleanupErr != nil {
						if ctx.Err() != nil {
							return
						}
						p.options.Logger.Error("completed transition cleanup failed", "group", group, "error", cleanupErr, "retry_in", backoff)
						select {
						case <-ctx.Done():
							return
						case <-p.options.After(backoff):
						}
						backoff = min(backoff*2, p.options.RetryMaximum)
						continue
					}
				}
				if !changed {
					break
				}
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, controller.ErrPhaseDeadline) && p.requestSafeTimeoutRollback(group) {
				backoff = p.options.RetryInitial
				continue
			}
			p.options.Logger.Error("group reconciliation failed", "group", group, "error", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-p.options.After(backoff):
			}
			backoff = min(backoff*2, p.options.RetryMaximum)
		}
	}
}

func (p *ControllerProcess) requestSafeTimeoutRollback(group string) bool {
	requester, ok := p.reconciler.(rollbackRequester)
	if !ok || p.options.Catalog == nil {
		return false
	}
	state := p.reconciler.Snapshot().Groups[group]
	retained, present := p.options.Catalog.Get(group)
	expected, precommit := expectedPrecommitSnapshot(state)
	if !present || !precommit || !membershipSnapshotsEqual(retained, expected) {
		return false
	}
	proof := controller.RollbackProof{
		TransitionID: state.Spec.ID, SourceMembershipIntact: true, ProposedNotActivated: true,
		FenceReversible: !state.FenceAttempted || state.Unfenced || p.options.RollbackFenceReversible,
	}
	if err := requester.RequestRollback(group, proof); err != nil {
		p.options.Logger.Error("safe timeout rollback request failed", "group", group, "error", err)
		return false
	}
	p.options.Logger.Warn("expired pre-commit transition will roll back", "group", group, "transition", state.Spec.ID)
	return true
}

func (p *ControllerProcess) actionTimeout(group string) time.Duration {
	timeout := p.options.ActionTimeout
	state, ok := p.reconciler.Snapshot().Groups[group]
	if !ok || state == nil || state.PhaseDeadline.IsZero() {
		return timeout
	}
	remaining := state.PhaseDeadline.Sub(p.options.Now())
	if remaining > 0 && remaining < timeout {
		return remaining
	}
	return timeout
}

func (p *ControllerProcess) signalAll(signals map[string]chan struct{}) {
	for _, signal := range signals {
		trySignal(signal)
	}
}

func trySignal(signal chan struct{}) {
	select {
	case signal <- struct{}{}:
	default:
	}
}

func (p *ControllerProcess) closeComponents() error {
	var result error
	for index := len(p.components) - 1; index >= 0; index-- {
		if err := p.components[index].Close(); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

// ParticipantProcess runs one or more independently fail-closed group
// orchestrators. The first non-cancellation failure stops the process.
type ParticipantProcess struct {
	groups          map[string]Component
	shutdownTimeout time.Duration
	after           func(time.Duration) <-chan time.Time
}

func NewParticipantProcess(groups map[string]Component, shutdownTimeout time.Duration) (*ParticipantProcess, error) {
	if len(groups) == 0 {
		return nil, errors.New("runtime: participant requires at least one group")
	}
	for group, component := range groups {
		if group == "" || component == nil {
			return nil, errors.New("runtime: participant group names and components are required")
		}
	}
	if shutdownTimeout <= 0 {
		shutdownTimeout = 30 * time.Second
	}
	return &ParticipantProcess{groups: maps.Clone(groups), shutdownTimeout: shutdownTimeout, after: time.After}, nil
}

func (p *ParticipantProcess) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(p.groups))
	var wg sync.WaitGroup
	for group, component := range p.groups {
		wg.Add(1)
		go func(group string, component Component) {
			defer wg.Done()
			err := component.Run(ctx)
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				results <- nil
				return
			}
			if err == nil {
				err = errors.New("component stopped unexpectedly")
			}
			results <- fmt.Errorf("runtime: participant group %q: %w", group, err)
		}(group, component)
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-results:
	}
	cancel()
	shutdown := make(chan error, 1)
	go func() {
		var closeErr error
		for _, component := range p.groups {
			if err := component.Close(); err != nil && !errors.Is(err, io.EOF) {
				closeErr = errors.Join(closeErr, err)
			}
		}
		wg.Wait()
		shutdown <- closeErr
	}()
	var closeErr error
	select {
	case closeErr = <-shutdown:
	case <-p.after(p.shutdownTimeout):
		closeErr = errors.New("runtime: participant shutdown deadline exceeded")
	}
	return errors.Join(runErr, closeErr)
}
