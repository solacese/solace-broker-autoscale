package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/policy"
)

type staticCollector struct {
	snapshot policy.TelemetrySnapshot
	err      error
}

func (c staticCollector) Collect(context.Context) (policy.TelemetrySnapshot, error) {
	return c.snapshot, c.err
}

type collectionResult struct {
	snapshot policy.TelemetrySnapshot
	err      error
}

type sequenceCollector struct {
	results []collectionResult
	calls   int
}

func (c *sequenceCollector) Collect(context.Context) (policy.TelemetrySnapshot, error) {
	result := c.results[min(c.calls, len(c.results)-1)]
	c.calls++
	return result.snapshot, result.err
}

type staticPolicyState struct {
	brokers []policy.Broker
	groups  []policy.GroupState
}

func (s staticPolicyState) PolicyStateForTelemetry(policy.TelemetrySnapshot, time.Time) ([]policy.Broker, []policy.GroupState, error) {
	return s.brokers, s.groups, nil
}

type capturePolicyState struct {
	called      bool
	snapshot    policy.TelemetrySnapshot
	evaluatedAt time.Time
}

func (s *capturePolicyState) PolicyStateForTelemetry(snapshot policy.TelemetrySnapshot, evaluatedAt time.Time) ([]policy.Broker, []policy.GroupState, error) {
	s.called = true
	s.snapshot = snapshot
	s.evaluatedAt = evaluatedAt
	return nil, nil, errors.New("state stopped cycle")
}

type memoryPolicyStore struct {
	state policy.EngineState
	err   error
	saves int
}

func (s *memoryPolicyStore) Load() (policy.EngineState, error) { return s.state, s.err }
func (s *memoryPolicyStore) Save(state policy.EngineState) error {
	s.saves++
	if s.err != nil {
		return s.err
	}
	s.state = state
	return nil
}

type captureApplier struct {
	decision policy.Decision
	cancel   context.CancelFunc
}

func (a *captureApplier) ApplyRecommendations(_ context.Context, decision policy.Decision) error {
	a.decision = decision
	a.cancel()
	return nil
}

type retryCaptureApplier struct {
	decisions []policy.Decision
	cancel    context.CancelFunc
}

func (a *retryCaptureApplier) ApplyRecommendations(_ context.Context, decision policy.Decision) error {
	a.decisions = append(a.decisions, decision)
	if len(a.decisions) == 2 {
		a.cancel()
	}
	return nil
}

func TestPolicyRuntimePassesExactCurrentSnapshotToState(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	snapshot := policy.TelemetrySnapshot{CapturedAt: now}
	state := &capturePolicyState{}
	runtime := &PolicyRuntime{
		Collector: staticCollector{snapshot: snapshot}, State: state, Engine: &policy.Engine{}, Store: &memoryPolicyStore{}, Applier: &captureApplier{},
		Now: func() time.Time { return now },
	}

	err := runtime.Run(context.Background())
	if err == nil || !state.called {
		t.Fatalf("Run() error = %v, state called = %t", err, state.called)
	}
	if !state.snapshot.CapturedAt.Equal(snapshot.CapturedAt) || !state.evaluatedAt.Equal(now) {
		t.Fatalf("state received snapshot %#v at %v", state.snapshot, state.evaluatedAt)
	}
}

func TestPolicyRuntimeFailsClosedAndRetriesCollection(t *testing.T) {
	first := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	second := first.Add(time.Second)
	catalog, err := policy.NewProfileCatalog([]policy.CapacityProfile{{
		ServiceClass: "class", BrokerVersion: "1", Limits: policy.CapacityLimits{
			IngressBytesPerSecond: 100, EgressBytesPerSecond: 100, SpoolBytes: 100, Connections: 100,
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := policy.NewEngine(catalog, policy.EngineOptions{MaxConcurrentTransitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.RestoreState(policy.EngineState{LastEvaluatedAt: first.Add(-time.Second), PressureSince: map[string]time.Time{"g": first.Add(-time.Minute)}}, first); err != nil {
		t.Fatal(err)
	}
	known := policy.Resources{
		IngressBytesPerSecond: policy.KnownMetric(0), EgressBytesPerSecond: policy.KnownMetric(0),
		SpoolBytes: policy.KnownMetric(0), Connections: policy.KnownMetric(0),
	}
	snapshot := policy.TelemetrySnapshot{
		CapturedAt: second,
		Brokers:    []policy.BrokerSample{{BrokerID: "a", ServiceClass: "class", BrokerVersion: "1", ObservedAt: second, Resources: known}},
		Groups: []policy.GroupSample{{GroupID: "g", BrokerID: "a", ObservedAt: second, Resources: known, Backlog: policy.Backlog{
			QueuedMessages: policy.KnownMetric(0), UnackedMessages: policy.KnownMetric(0),
		}}},
	}
	collector := &sequenceCollector{results: []collectionResult{{err: errors.New("SEMP failed")}, {snapshot: snapshot}}}
	state := staticPolicyState{
		brokers: []policy.Broker{{ID: "a", ServiceClass: "class", BrokerVersion: "1", Ready: true, EligibleGroups: []string{"g"}}},
		groups: []policy.GroupState{{ID: "g", Membership: []string{"a"}, Policy: policy.GroupPolicy{
			MinimumBrokers: 1, MaximumBrokers: 2, HeadroomPercent: 20, PressureWindow: time.Minute,
			TelemetryMaxAge: time.Minute, MaxConcurrentChanges: 1,
		}}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	applier := &retryCaptureApplier{cancel: cancel}
	clockCalls := 0
	runtime := &PolicyRuntime{
		Collector: collector, State: state, Engine: engine, Store: &memoryPolicyStore{}, Applier: applier,
		Interval: time.Hour, RetryInitial: time.Millisecond, RetryMaximum: time.Millisecond,
		Now: func() time.Time {
			value := first.Add(time.Duration(clockCalls) * time.Second)
			clockCalls++
			return value
		},
		After: func(delay time.Duration) <-chan time.Time {
			ready := make(chan time.Time, 1)
			if delay == time.Millisecond {
				ready <- time.Time{}
			}
			return ready
		},
	}
	if err := runtime.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want cancellation after retry succeeds", err)
	}
	if collector.calls != 2 {
		t.Fatalf("collection calls = %d, want failed attempt and retry", collector.calls)
	}
	if len(engine.State().PressureSince) != 0 {
		t.Fatalf("failed telemetry collection did not reset pressure: %#v", engine.State())
	}
	if len(applier.decisions) != 2 || applier.decisions[0].Groups[0].Telemetry != policy.TelemetryUnknown {
		t.Fatalf("failed collection was not evaluated as unknown: %#v", applier.decisions)
	}
	if len(applier.decisions[1].Transitions) != 0 {
		t.Fatalf("retry admitted transition after reset pressure: %#v", applier.decisions[1])
	}
}

func TestPolicyRuntimeStopsBeforeApplyWhenPersistenceFails(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	catalog, err := policy.NewProfileCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := policy.NewEngine(catalog, policy.EngineOptions{MaxConcurrentTransitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	storeErr := errors.New("disk full")
	applied := false
	applier := recommendationApplierFunc(func(context.Context, policy.Decision) error {
		applied = true
		return nil
	})
	runtime := &PolicyRuntime{
		Collector: staticCollector{snapshot: policy.TelemetrySnapshot{CapturedAt: now}},
		State:     staticPolicyState{}, Engine: engine, Store: &memoryPolicyStore{err: storeErr}, Applier: applier,
		Now: func() time.Time { return now },
	}
	if err := runtime.Run(context.Background()); !errors.Is(err, storeErr) {
		t.Fatalf("Run() error = %v, want persistence failure", err)
	}
	if applied {
		t.Fatal("recommendations applied after policy persistence failed")
	}
}

type recommendationApplierFunc func(context.Context, policy.Decision) error

func (fn recommendationApplierFunc) ApplyRecommendations(ctx context.Context, decision policy.Decision) error {
	return fn(ctx, decision)
}

func TestPolicyRuntimeTreatsStaleTelemetryAsUnsafe(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	catalog, err := policy.NewProfileCatalog([]policy.CapacityProfile{{
		ServiceClass: "class", BrokerVersion: "1", Limits: policy.CapacityLimits{
			IngressBytesPerSecond: 100, EgressBytesPerSecond: 100, SpoolBytes: 100, Connections: 100,
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := policy.NewEngine(catalog, policy.EngineOptions{MaxConcurrentTransitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	known := policy.Resources{
		IngressBytesPerSecond: policy.KnownMetric(0), EgressBytesPerSecond: policy.KnownMetric(0),
		SpoolBytes: policy.KnownMetric(0), Connections: policy.KnownMetric(0),
	}
	observed := now.Add(-2 * time.Minute)
	snapshot := policy.TelemetrySnapshot{
		CapturedAt: now,
		Brokers:    []policy.BrokerSample{{BrokerID: "a", ServiceClass: "class", BrokerVersion: "1", ObservedAt: observed, Resources: known}},
		Groups: []policy.GroupSample{{GroupID: "g", BrokerID: "a", ObservedAt: observed, Resources: known, Backlog: policy.Backlog{
			QueuedMessages: policy.KnownMetric(0), UnackedMessages: policy.KnownMetric(0),
		}}},
	}
	state := staticPolicyState{
		brokers: []policy.Broker{{ID: "a", ServiceClass: "class", BrokerVersion: "1", Ready: true, EligibleGroups: []string{"g"}}},
		groups: []policy.GroupState{{ID: "g", Membership: []string{"a"}, Policy: policy.GroupPolicy{
			MinimumBrokers: 1, MaximumBrokers: 2, HeadroomPercent: 20, PressureWindow: time.Second,
			TelemetryMaxAge: time.Minute, MaxConcurrentChanges: 1,
		}}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	applier := &captureApplier{cancel: cancel}
	runtime := &PolicyRuntime{Collector: staticCollector{snapshot: snapshot}, State: state, Engine: engine, Store: &memoryPolicyStore{}, Applier: applier, Now: func() time.Time { return now }}
	err = runtime.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want cancellation after first cycle", err)
	}
	if len(applier.decision.Transitions) != 0 {
		t.Fatalf("stale telemetry admitted transitions: %#v", applier.decision.Transitions)
	}
	if len(applier.decision.Groups) != 1 || applier.decision.Groups[0].Telemetry != policy.TelemetryStale {
		t.Fatalf("decision did not retain stale telemetry: %#v", applier.decision.Groups)
	}
}
