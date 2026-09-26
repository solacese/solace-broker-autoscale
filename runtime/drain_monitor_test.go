package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/semp"
)

type drainCall struct {
	vpn   string
	queue string
}

type drainResult struct {
	status semp.QueueStatus
	err    error
}

type fakeDrainClient struct {
	mu           sync.Mutex
	results      []drainResult
	calls        []drainCall
	monitorQueue int
	started      chan struct{}
	release      <-chan struct{}
}

func (*fakeDrainClient) EnsureQueue(context.Context, semp.QueueSpec) error { return nil }
func (*fakeDrainClient) CreateSubscription(context.Context, string, string, string) error {
	return nil
}
func (*fakeDrainClient) FenceQueue(context.Context, string, string) error   { return nil }
func (*fakeDrainClient) UnfenceQueue(context.Context, string, string) error { return nil }
func (c *fakeDrainClient) MonitorQueue(context.Context, string, string) (semp.QueueStatus, error) {
	c.mu.Lock()
	c.monitorQueue++
	c.mu.Unlock()
	return semp.QueueStatus{}, errors.New("MonitorQueue must not be used for drain polling")
}
func (c *fakeDrainClient) MonitorDrain(ctx context.Context, vpn, queue string) (semp.QueueStatus, error) {
	c.mu.Lock()
	index := len(c.calls)
	c.calls = append(c.calls, drainCall{vpn: vpn, queue: queue})
	var result drainResult
	if len(c.results) != 0 {
		result = c.results[min(index, len(c.results)-1)]
	}
	started := c.started
	release := c.release
	c.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-ctx.Done():
			return semp.QueueStatus{}, ctx.Err()
		case <-release:
		}
	}
	return result.status, result.err
}
func (*fakeDrainClient) DeleteQueue(context.Context, string, string) error { return nil }
func (*fakeDrainClient) QueueExists(context.Context, string, string) (bool, error) {
	return true, nil
}

func (c *fakeDrainClient) recordedCalls() ([]drainCall, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]drainCall(nil), c.calls...), c.monitorQueue
}

type fakeDrainObserver struct {
	mu      sync.Mutex
	state   controller.PersistentState
	samples []controller.Telemetry
}

func (o *fakeDrainObserver) Snapshot() controller.PersistentState {
	o.mu.Lock()
	defer o.mu.Unlock()
	groups := make(map[string]*controller.GroupState, len(o.state.Groups))
	for group, state := range o.state.Groups {
		copy := *state
		copy.Spec.Current = append([]controller.Broker(nil), state.Spec.Current...)
		copy.Spec.CurrentResources = append([]control.EpochResourceIdentity(nil), state.Spec.CurrentResources...)
		groups[group] = &copy
	}
	return controller.PersistentState{Version: o.state.Version, Groups: groups}
}

func (o *fakeDrainObserver) Observe(sample controller.Telemetry) error {
	o.mu.Lock()
	o.samples = append(o.samples, sample)
	o.mu.Unlock()
	return nil
}

func (o *fakeDrainObserver) recordedSamples() []controller.Telemetry {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]controller.Telemetry(nil), o.samples...)
}

func drainState(group, transition, broker, queue string, phase controller.Phase) *controller.GroupState {
	return &controller.GroupState{
		Phase:          phase,
		DrainPublished: phase == controller.PhaseDrain,
		Spec: controller.TransitionSpec{
			ID: transition, Namespace: "swlb", Group: group, FromEpoch: 7, ToEpoch: 8,
			Current:          []controller.Broker{{ID: broker, Destination: "swlb/ingress"}},
			CurrentResources: []control.EpochResourceIdentity{{Epoch: 7, BrokerID: broker, ConsumerSet: "default", QueueName: queue, IngressTopic: "swlb/ingress"}},
		},
	}
}

func drainMonitorForTest(observer *fakeDrainObserver, clients map[string]*fakeDrainClient, now func() time.Time) *SEMPDrainMonitor {
	brokers := make(map[string]BrokerTarget, len(clients))
	vpns := make(map[string]string, len(clients))
	for broker, client := range clients {
		brokers[broker] = BrokerTarget{Client: client}
		vpns[broker] = "vpn-" + broker
	}
	return &SEMPDrainMonitor{
		Controller: observer, Brokers: brokers, MessageVPNs: vpns,
		Interval: time.Millisecond, Participant: "controller-1", Now: now,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestSEMPDrainMonitorReportsNonzeroThenZeroWithUniqueIDs(t *testing.T) {
	observer := &fakeDrainObserver{state: controller.PersistentState{Groups: map[string]*controller.GroupState{
		"orders": drainState("orders", "move-1", "broker-a", "orders-a-e7", controller.PhaseDrain),
	}}}
	times := []time.Time{time.Unix(10, 0).UTC(), time.Unix(11, 0).UTC()}
	var timeMu sync.Mutex
	now := func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		value := times[0]
		times = times[1:]
		return value
	}
	client := &fakeDrainClient{results: []drainResult{
		{status: semp.QueueStatus{MessageVPN: "vpn-broker-a", Name: "orders-a-e7", SpooledMessages: 3, SpoolUsageBytes: 120, UnackedMessages: 2, InProgressAckMessages: 1}},
		// Residual spool allocation is diagnostic only and must not block drain once
		// current message and acknowledgement counts are zero.
		{status: semp.QueueStatus{MessageVPN: "vpn-broker-a", Name: "orders-a-e7", SpoolUsageBytes: 64}},
	}}
	monitor := drainMonitorForTest(observer, map[string]*fakeDrainClient{"broker-a": client}, now)
	if err := monitor.validate(); err != nil {
		t.Fatal(err)
	}
	if err := monitor.observe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := monitor.observe(context.Background()); err != nil {
		t.Fatal(err)
	}

	samples := observer.recordedSamples()
	if len(samples) != 2 {
		t.Fatalf("samples = %d, want 2", len(samples))
	}
	if !samples[0].Queued.Known || samples[0].Queued.Value != 3 || !samples[0].Stored.Known || samples[0].Stored.Value != 3 || samples[0].Unacked.Value != 3 {
		t.Fatalf("nonzero sample = %#v", samples[0])
	}
	if !samples[1].Queued.Known || samples[1].Queued.Value != 0 || !samples[1].Stored.Known || samples[1].Stored.Value != 0 || !samples[1].Unacked.Known {
		t.Fatalf("zero sample with residual spool bytes = %#v", samples[1])
	}
	if samples[0].MessageID == samples[1].MessageID || !samples[0].ObservedAt.Equal(time.Unix(10, 0).UTC()) || !samples[1].ObservedAt.Equal(time.Unix(11, 0).UTC()) {
		t.Fatalf("observation identities/times are not unique and current: %#v", samples)
	}
	calls, monitorQueue := client.recordedCalls()
	if len(calls) != 2 || calls[0] != (drainCall{vpn: "vpn-broker-a", queue: "orders-a-e7"}) || monitorQueue != 0 {
		t.Fatalf("drain calls = %#v, MonitorQueue calls = %d", calls, monitorQueue)
	}
}

func TestSEMPDrainMonitorReportsFailedAndMissingReadsUnknown(t *testing.T) {
	observer := &fakeDrainObserver{state: controller.PersistentState{Groups: map[string]*controller.GroupState{
		"orders": drainState("orders", "move-1", "broker-a", "orders-a-e7", controller.PhaseDrain),
	}}}
	client := &fakeDrainClient{results: []drainResult{{err: errors.New("SEMP unavailable")}}}
	monitor := drainMonitorForTest(observer, map[string]*fakeDrainClient{"broker-a": client}, func() time.Time { return time.Unix(20, 0).UTC() })
	if err := monitor.validate(); err != nil {
		t.Fatal(err)
	}
	if err := monitor.observe(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The same exact resource with its client missing must also produce an unknown
	// observation rather than being omitted or represented as a fabricated zero.
	monitor.Brokers = map[string]BrokerTarget{}
	monitor.MessageVPNs = map[string]string{}
	if err := monitor.observe(context.Background()); err != nil {
		t.Fatal(err)
	}
	samples := observer.recordedSamples()
	if len(samples) != 2 {
		t.Fatalf("samples = %d, want failed and missing observations", len(samples))
	}
	for _, sample := range samples {
		if sample.Queued.Known || sample.Stored.Known || sample.Unacked.Known || sample.Queued.Value != 0 || sample.Stored.Value != 0 || sample.Unacked.Value != 0 {
			t.Fatalf("failed read was not explicitly unknown: %#v", sample)
		}
	}
	if samples[0].MessageID == samples[1].MessageID {
		t.Fatal("failed and missing reads reused a telemetry message ID")
	}
}

func TestSEMPDrainMonitorPreservesEachSourceIdentity(t *testing.T) {
	state := drainState("orders", "move-1", "broker-a", "orders-a-e7", controller.PhaseDrain)
	state.Spec.Current = append(state.Spec.Current, controller.Broker{ID: "broker-b", Destination: "swlb/ingress"})
	state.Spec.CurrentResources = append(state.Spec.CurrentResources, control.EpochResourceIdentity{Epoch: 7, BrokerID: "broker-b", ConsumerSet: "default", QueueName: "orders-b-e7", IngressTopic: "swlb/ingress"})
	observer := &fakeDrainObserver{state: controller.PersistentState{Groups: map[string]*controller.GroupState{"orders": state}}}
	clients := map[string]*fakeDrainClient{
		"broker-a": {results: []drainResult{{status: semp.QueueStatus{MessageVPN: "vpn-broker-a", Name: "orders-a-e7"}}}},
		"broker-b": {results: []drainResult{{status: semp.QueueStatus{MessageVPN: "vpn-broker-b", Name: "orders-b-e7"}}}},
	}
	monitor := drainMonitorForTest(observer, clients, func() time.Time { return time.Unix(30, 0).UTC() })
	if err := monitor.validate(); err != nil {
		t.Fatal(err)
	}
	if err := monitor.observe(context.Background()); err != nil {
		t.Fatal(err)
	}
	samples := observer.recordedSamples()
	if len(samples) != 2 {
		t.Fatalf("samples = %d, want 2", len(samples))
	}
	byBroker := map[string]controller.Telemetry{}
	for _, sample := range samples {
		byBroker[sample.SourceBroker] = sample
		if sample.Group != "orders" || sample.TransitionID != "move-1" || sample.Epoch != 7 || sample.Participant != "controller-1" || sample.Role != control.RoleObserver {
			t.Fatalf("typed telemetry scope = %#v", sample)
		}
	}
	if byBroker["broker-a"].SourceQueue != "orders-a-e7" || byBroker["broker-b"].SourceQueue != "orders-b-e7" {
		t.Fatalf("source identity collapsed: %#v", samples)
	}
}

func TestSEMPDrainMonitorDoesNotPollOutsideActivePublishedDrain(t *testing.T) {
	states := map[string]*controller.GroupState{
		"prepare":     drainState("prepare", "move-1", "broker-a", "prepare-a", controller.PhasePrepare),
		"unpublished": drainState("unpublished", "move-2", "broker-a", "unpublished-a", controller.PhaseDrain),
		"complete":    drainState("complete", "move-3", "broker-a", "complete-a", controller.PhaseDrain),
		"rollback":    drainState("rollback", "move-4", "broker-a", "rollback-a", controller.PhaseDrain),
	}
	states["unpublished"].DrainPublished = false
	states["complete"].Completed = true
	states["rollback"].RollingBack = true
	observer := &fakeDrainObserver{state: controller.PersistentState{Groups: states}}
	client := &fakeDrainClient{}
	monitor := drainMonitorForTest(observer, map[string]*fakeDrainClient{"broker-a": client}, time.Now)
	if err := monitor.validate(); err != nil {
		t.Fatal(err)
	}
	if err := monitor.observe(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls, _ := client.recordedCalls()
	if len(calls) != 0 || len(observer.recordedSamples()) != 0 {
		t.Fatalf("inactive transitions were polled: calls=%#v samples=%#v", calls, observer.recordedSamples())
	}
}

func TestSEMPDrainMonitorPollsGroupsConcurrently(t *testing.T) {
	observer := &fakeDrainObserver{state: controller.PersistentState{Groups: map[string]*controller.GroupState{
		"orders":  drainState("orders", "move-1", "broker-a", "orders-a-e7", controller.PhaseDrain),
		"billing": drainState("billing", "move-2", "broker-b", "billing-b-e7", controller.PhaseDrain),
	}}}
	release := make(chan struct{})
	startedA := make(chan struct{}, 1)
	startedB := make(chan struct{}, 1)
	clients := map[string]*fakeDrainClient{
		"broker-a": {results: []drainResult{{status: semp.QueueStatus{MessageVPN: "vpn-broker-a", Name: "orders-a-e7"}}}, started: startedA, release: release},
		"broker-b": {results: []drainResult{{status: semp.QueueStatus{MessageVPN: "vpn-broker-b", Name: "billing-b-e7"}}}, started: startedB, release: release},
	}
	monitor := drainMonitorForTest(observer, clients, time.Now)
	if err := monitor.validate(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- monitor.observe(context.Background()) }()
	for name, started := range map[string]<-chan struct{}{"orders": startedA, "billing": startedB} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("group %q did not start while the other group was blocked", name)
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(observer.recordedSamples()) != 2 {
		t.Fatalf("samples = %d, want both independent groups", len(observer.recordedSamples()))
	}
}

func TestSEMPDrainMonitorTriggersReconciliationAndStopsOnContext(t *testing.T) {
	observer := &fakeDrainObserver{state: controller.PersistentState{Groups: map[string]*controller.GroupState{
		"orders": drainState("orders", "move-1", "broker-a", "orders-a-e7", controller.PhaseDrain),
	}}}
	client := &fakeDrainClient{results: []drainResult{{status: semp.QueueStatus{MessageVPN: "vpn-broker-a", Name: "orders-a-e7"}}}}
	monitor := drainMonitorForTest(observer, map[string]*fakeDrainClient{"broker-a": client}, time.Now)
	monitor.Interval = time.Hour
	triggers := make(chan Trigger, 1)
	monitor.Triggers = triggers
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- monitor.Run(ctx) }()
	select {
	case trigger := <-triggers:
		if trigger.Group != "orders" {
			t.Fatalf("trigger = %#v", trigger)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted observation did not trigger reconciliation")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain monitor did not stop after context cancellation")
	}
}
