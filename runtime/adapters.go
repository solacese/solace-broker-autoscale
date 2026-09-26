package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/policy"
	"github.com/solacese/solace-workload-balancer/semp"
)

// ControllerDependencies are the native seams required by controller assembly.
// Keeping them explicit prevents a production process from silently using a
// no-op or dry-run implementation.
type ControllerDependencies struct {
	Fence         controller.BrokerFence
	Publisher     controller.ControlPublisher
	Inbox         Component
	Policy        PolicyCycle
	EventTriggers <-chan Trigger
}

// PolicyCycle collects SEMP-derived telemetry and evaluates policy. Applying a
// recommendation remains a separate adapter because creating transition specs
// requires authoritative membership/revision allocation.
type PolicyCycle interface {
	Run(context.Context) error
}

// SEMPQueueClient is the subset of semp.Client used by the runtime fence,
// telemetry, and managed epoch-resource adapters.
type SEMPQueueClient interface {
	EnsureQueue(context.Context, semp.QueueSpec) error
	CreateSubscription(context.Context, string, string, string) error
	FenceQueue(context.Context, string, string) error
	UnfenceQueue(context.Context, string, string) error
	MonitorQueue(context.Context, string, string) (semp.QueueStatus, error)
	MonitorDrain(context.Context, string, string) (semp.QueueStatus, error)
	DeleteQueue(context.Context, string, string) error
	QueueExists(context.Context, string, string) (bool, error)
}

type BrokerTarget struct {
	Client SEMPQueueClient
}

// FenceQueueResolver maps durable transition identity to an explicitly managed,
// epoch-specific ingress queue. A transition Broker.Destination is an application
// topic and must never be guessed to be the queue that enforces the fence.
type ManagedQueue struct {
	MessageVPN string
	Name       string
}

type FenceQueueResolver interface {
	FenceQueues(context.Context, controller.FenceRequest, controller.Broker) ([]ManagedQueue, error)
}

type FenceQueueResolverFunc func(context.Context, controller.FenceRequest, controller.Broker) ([]ManagedQueue, error)

func (f FenceQueueResolverFunc) FenceQueues(ctx context.Context, request controller.FenceRequest, broker controller.Broker) ([]ManagedQueue, error) {
	return f(ctx, request, broker)
}

// SEMPBrokerFence maps stable controller broker IDs to SEMP clients and requires
// an explicit managed-resource resolver; it never derives queue names from an
// application destination.
type SEMPBrokerFence struct {
	Brokers map[string]BrokerTarget
	Queues  FenceQueueResolver
	Now     func() time.Time
}

func (f SEMPBrokerFence) Fence(ctx context.Context, request controller.FenceRequest) error {
	return f.change(ctx, request, false)
}

func (f SEMPBrokerFence) Unfence(ctx context.Context, request controller.FenceRequest) error {
	return f.change(ctx, request, true)
}

func (f SEMPBrokerFence) EnableIngress(ctx context.Context, request controller.FenceRequest) error {
	return f.change(ctx, request, true)
}

func (f SEMPBrokerFence) VerifyFence(ctx context.Context, request controller.FenceRequest) (controller.FenceStatus, error) {
	fenced, observedAt, err := f.verifyIngressState(ctx, request, false)
	if err != nil {
		return controller.FenceStatus{}, err
	}
	return controller.FenceStatus{Fenced: fenced, ObservedAt: observedAt}, nil
}

func (f SEMPBrokerFence) VerifyIngress(ctx context.Context, request controller.FenceRequest) (controller.IngressStatus, error) {
	enabled, observedAt, err := f.verifyIngressState(ctx, request, true)
	if err != nil {
		return controller.IngressStatus{}, err
	}
	return controller.IngressStatus{Enabled: enabled, ObservedAt: observedAt}, nil
}

func (f SEMPBrokerFence) verifyIngressState(ctx context.Context, request controller.FenceRequest, expectedEnabled bool) (bool, time.Time, error) {
	now := f.Now
	if now == nil {
		now = time.Now
	}
	if f.Queues == nil {
		return false, time.Time{}, errors.New("runtime: explicit fence queue resolver is required")
	}
	for _, broker := range request.Brokers {
		target, ok := f.Brokers[broker.ID]
		if !ok || target.Client == nil {
			return false, time.Time{}, fmt.Errorf("runtime: no SEMP adapter for broker %q", broker.ID)
		}
		queues, err := f.Queues.FenceQueues(ctx, request, broker)
		if err != nil || len(queues) == 0 {
			return false, time.Time{}, fmt.Errorf("runtime: resolve managed ingress queues for broker %q: %w", broker.ID, errors.Join(err, ErrNativeAdapterUnavailable))
		}
		for _, queue := range queues {
			if queue.MessageVPN == "" || queue.Name == "" {
				return false, time.Time{}, fmt.Errorf("runtime: managed ingress queue for broker %q is incomplete: %w", broker.ID, ErrNativeAdapterUnavailable)
			}
			status, err := target.Client.MonitorQueue(ctx, queue.MessageVPN, queue.Name)
			if err != nil {
				return false, time.Time{}, fmt.Errorf("runtime: verify broker %q queue %q ingress: %w", broker.ID, queue.Name, err)
			}
			if status.IngressEnabled != expectedEnabled {
				return false, now(), nil
			}
		}
	}
	return true, now(), nil
}

func (f SEMPBrokerFence) change(ctx context.Context, request controller.FenceRequest, enable bool) error {
	if f.Queues == nil {
		return errors.New("runtime: explicit fence queue resolver is required")
	}
	for _, broker := range request.Brokers {
		target, ok := f.Brokers[broker.ID]
		if !ok || target.Client == nil {
			return fmt.Errorf("runtime: no SEMP adapter for broker %q", broker.ID)
		}
		queues, err := f.Queues.FenceQueues(ctx, request, broker)
		if err != nil || len(queues) == 0 {
			return fmt.Errorf("runtime: resolve managed fence queues for broker %q: %w", broker.ID, errors.Join(err, ErrNativeAdapterUnavailable))
		}
		for _, queue := range queues {
			if queue.MessageVPN == "" || queue.Name == "" {
				return fmt.Errorf("runtime: managed fence queue for broker %q is incomplete: %w", broker.ID, ErrNativeAdapterUnavailable)
			}
			if enable {
				err = target.Client.UnfenceQueue(ctx, queue.MessageVPN, queue.Name)
			} else {
				err = target.Client.FenceQueue(ctx, queue.MessageVPN, queue.Name)
			}
			if err != nil {
				return fmt.Errorf("runtime: update broker %q queue %q ingress: %w", broker.ID, queue.Name, err)
			}
		}
	}
	return nil
}

// PolicyRuntime wires a collector, policy.Engine, and recommendation applier.
type TelemetryCollector interface {
	Collect(context.Context) (policy.TelemetrySnapshot, error)
}

type telemetryCollectorValidator interface {
	Validate() error
}

type PolicyStateProvider interface {
	PolicyStateForTelemetry(policy.TelemetrySnapshot, time.Time) ([]policy.Broker, []policy.GroupState, error)
}

type RecommendationApplier interface {
	ApplyRecommendations(context.Context, policy.Decision) error
}

type PolicyRuntime struct {
	Collector    TelemetryCollector
	State        PolicyStateProvider
	Engine       *policy.Engine
	Store        PolicyEngineStateStore
	Applier      RecommendationApplier
	Interval     time.Duration
	RetryInitial time.Duration
	RetryMaximum time.Duration
	Triggers     <-chan struct{}
	Logger       *slog.Logger
	Now          func() time.Time
	After        func(time.Duration) <-chan time.Time
}

func (r *PolicyRuntime) Run(ctx context.Context) error {
	if r == nil || r.Collector == nil || r.State == nil || r.Engine == nil || r.Store == nil || r.Applier == nil {
		return errors.New("runtime: policy collector, state, engine, store, and applier are required")
	}
	interval := r.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	retryInitial := r.RetryInitial
	if retryInitial <= 0 {
		retryInitial = min(250*time.Millisecond, interval)
	}
	retryMaximum := r.RetryMaximum
	if retryMaximum <= 0 {
		retryMaximum = interval
	}
	if retryMaximum < retryInitial {
		return errors.New("runtime: policy maximum retry delay cannot be shorter than initial delay")
	}
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := r.Now
	if now == nil {
		now = time.Now
	}
	after := r.After
	if after == nil {
		after = time.After
	}
	if validator, ok := r.Collector.(telemetryCollectorValidator); ok {
		if err := validator.Validate(); err != nil {
			return fmt.Errorf("runtime: validate policy collector: %w", err)
		}
	}
	evaluate := func(snapshot policy.TelemetrySnapshot, evaluatedAt time.Time) error {
		inventory, groups, err := r.State.PolicyStateForTelemetry(snapshot, evaluatedAt)
		if err != nil {
			return fmt.Errorf("runtime: build policy state: %w", err)
		}
		decision, err := r.Engine.Evaluate(evaluatedAt, snapshot, inventory, groups)
		if err != nil {
			return fmt.Errorf("runtime: evaluate policy: %w", err)
		}
		if err := r.Store.Save(r.Engine.State()); err != nil {
			return fmt.Errorf("runtime: persist policy state before applying recommendations: %w", err)
		}
		if err := r.Applier.ApplyRecommendations(ctx, decision); err != nil {
			return fmt.Errorf("runtime: apply policy recommendations: %w", err)
		}
		return nil
	}
	run := func() (bool, error) {
		snapshot, err := r.Collector.Collect(ctx)
		evaluatedAt := now().UTC()
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			// An empty current snapshot is valid and makes every configured broker and
			// group unknown. Evaluating it resets sustained-pressure evidence rather
			// than allowing a failed collection to pause a scale-out window.
			if failClosedErr := evaluate(policy.TelemetrySnapshot{CapturedAt: evaluatedAt}, evaluatedAt); failClosedErr != nil {
				return false, fmt.Errorf("runtime: fail closed after SEMP telemetry collection failure: %w", failClosedErr)
			}
			return false, nil
		}
		return true, evaluate(snapshot, evaluatedAt)
	}

	backoff := retryInitial
	for {
		succeeded, err := run()
		if err != nil {
			return err
		}
		if !succeeded {
			logger.Warn("SEMP policy telemetry collection failed; scaling decisions are fail-closed", "retry_in", backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-after(backoff):
			}
			backoff = nextBackoff(backoff, retryMaximum)
			continue
		}

		backoff = retryInitial
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-after(interval):
		case _, ok := <-r.Triggers:
			if !ok {
				r.Triggers = nil
			}
		}
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

func (r *PolicyRuntime) Close() error { return nil }

// NewParticipant assembles existing Broker 0 group orchestrators into one
// participant lifecycle without depending on a concrete native client.
func NewParticipant(subscriber broker0.UpdateSubscriber, browser broker0.Browser, applier broker0.SnapshotApplier, operations broker0.OperationStore, groups []config.ScalingGroup, participant string, rebrowseInterval, maxStaleness, shutdownTimeout time.Duration) (*ParticipantProcess, error) {
	components := make(map[string]Component, len(groups))
	for _, group := range groups {
		orchestrator, err := broker0.NewGroupOrchestrator(subscriber, browser, applier, operations, broker0.OrchestratorOptions{
			Group: group.ID, Participant: participant, RebrowseInterval: rebrowseInterval, MaxStaleness: maxStaleness,
		})
		if err != nil {
			return nil, err
		}
		components[group.ID] = orchestratorComponent{orchestrator: orchestrator}
	}
	return NewParticipantProcess(components, shutdownTimeout)
}

type orchestratorComponent struct{ orchestrator *broker0.GroupOrchestrator }

func (c orchestratorComponent) Run(ctx context.Context) error { return c.orchestrator.Run(ctx) }
func (orchestratorComponent) Close() error                    { return nil }

// ComposeController opens durable state and validates that every production
// adapter exists before constructing a long-running process.
func ComposeController(cfg config.Config, dependencies ControllerDependencies, options ControllerOptions) (*ControllerProcess, *controller.Controller, error) {
	missing := make([]string, 0, 4)
	if dependencies.Fence == nil {
		missing = append(missing, "SEMP broker fence")
	}
	if dependencies.Publisher == nil {
		missing = append(missing, "Broker 0 persistent publisher")
	}
	if dependencies.Inbox == nil {
		missing = append(missing, "Broker 0 durable controller inbox")
	}
	if dependencies.Policy == nil {
		missing = append(missing, "SEMP policy collector/applier")
	}
	if len(missing) != 0 {
		return nil, nil, fmt.Errorf("%w: %v", ErrNativeAdapterUnavailable, missing)
	}
	coordinator, err := controller.Open(controller.JSONStore{Path: cfg.Persistence.ControllerState}, dependencies.Fence, dependencies.Publisher, controller.Options{
		TelemetryFreshness:        cfg.Staleness.TelemetryMaxAge.Duration,
		ReadinessFreshness:        cfg.Staleness.ParticipantMaxAge.Duration,
		FenceFreshness:            cfg.Staleness.TelemetryMaxAge.Duration,
		DrainGraceByGroup:         drainGraceByGroup(cfg.Groups),
		TelemetryFreshnessByGroup: telemetryFreshnessByGroup(cfg.Groups),
		PhaseTimeoutByGroup:       phaseTimeoutByGroup(cfg.Groups),
	})
	if err != nil {
		return nil, nil, err
	}
	components := []Component{dependencies.Inbox, policyComponent{cycle: dependencies.Policy}}
	options.Groups = make([]string, 0, len(cfg.Groups))
	for _, group := range cfg.Groups {
		options.Groups = append(options.Groups, group.ID)
	}
	process, err := NewControllerProcess(coordinator, components, dependencies.EventTriggers, options)
	return process, coordinator, err
}

type policyComponent struct{ cycle PolicyCycle }

func (c policyComponent) Run(ctx context.Context) error { return c.cycle.Run(ctx) }
func (policyComponent) Close() error                    { return nil }

func drainGraceByGroup(groups []config.ScalingGroup) map[string]time.Duration {
	result := make(map[string]time.Duration, len(groups))
	for _, group := range groups {
		if group.ID != "" && group.Handover.DrainGrace.Duration > 0 {
			result[group.ID] = group.Handover.DrainGrace.Duration
		}
	}
	return result
}

func telemetryFreshnessByGroup(groups []config.ScalingGroup) map[string]time.Duration {
	result := make(map[string]time.Duration, len(groups))
	for _, group := range groups {
		if group.ID != "" && group.Handover.TelemetryMaxAge.Duration > 0 {
			result[group.ID] = group.Handover.TelemetryMaxAge.Duration
		}
	}
	return result
}

func phaseTimeoutByGroup(groups []config.ScalingGroup) map[string]time.Duration {
	result := make(map[string]time.Duration, len(groups))
	for _, group := range groups {
		if group.ID != "" && group.Handover.TransitionTimeout.Duration > 0 {
			result[group.ID] = group.Handover.TransitionTimeout.Duration
		}
	}
	return result
}

// CloseGroup safely closes a set of closers once and is useful for native
// adapter implementations with multiple SMF/SEMP resources.
type CloseGroup struct {
	Once    sync.Once
	Closers []interface{ Close() error }
	Err     error
}

func (g *CloseGroup) Close() error {
	g.Once.Do(func() {
		for index := len(g.Closers) - 1; index >= 0; index-- {
			g.Err = errors.Join(g.Err, g.Closers[index].Close())
		}
	})
	return g.Err
}
