package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/policy"
	"github.com/solacese/solace-workload-balancer/semp"
)

type staticBrowser struct {
	messages []broker0.BrowsedMessage
	err      error
}

func (b staticBrowser) Browse(context.Context, string) ([]broker0.BrowsedMessage, error) {
	return b.messages, b.err
}

type captureMembershipPublisher struct {
	snapshots []control.MembershipSnapshot
	updates   []controller.ControlUpdate
	commands  []control.CommandEnvelope
	err       error
}

func (p *captureMembershipPublisher) Publish(_ context.Context, update controller.ControlUpdate) error {
	if p.err != nil {
		return p.err
	}
	p.updates = append(p.updates, update)
	return nil
}
func (p *captureMembershipPublisher) PublishSnapshot(_ context.Context, snapshot control.MembershipSnapshot) error {
	if p.err != nil {
		return p.err
	}
	p.snapshots = append(p.snapshots, snapshot.Clone())
	return nil
}
func (p *captureMembershipPublisher) PublishCommand(_ context.Context, command control.CommandEnvelope) error {
	if p.err != nil {
		return p.err
	}
	p.commands = append(p.commands, command)
	return nil
}

func productionGroup() config.ScalingGroup {
	return config.ScalingGroup{
		ID: "orders", HashContract: "orders-v1", CustomerLibrary: "routing-v1",
		OrderedBrokerIDs:   []string{"broker-b", "broker-a"},
		Queue:              config.Queue{NamePrefix: "swlb.orders", Type: config.QueueTypeExclusive, Access: config.QueueAccessExclusive, MaxRedeliveries: 5, DeadMessageQueue: "swlb.orders.dmq"},
		Policy:             config.ScalingPolicy{MinimumBrokers: 1, MaximumBrokers: 3, HeadroomPercent: 20, PressureWindow: config.Duration{Duration: time.Minute}, MaxConcurrentChanges: 1},
		Handover:           config.HandoverPolicy{ReadinessTimeout: config.Duration{Duration: time.Minute}, TelemetryMaxAge: config.Duration{Duration: 15 * time.Second}, DrainGrace: config.Duration{Duration: time.Second}, TransitionTimeout: config.Duration{Duration: 5 * time.Minute}},
		RequiredPublishers: []string{"publisher-1"}, RequiredSubscribers: []string{"subscriber-1"},
	}
}

func TestMaximumConcurrentTransitionsAllowsOnePerGroup(t *testing.T) {
	groups := []config.ScalingGroup{productionGroup(), productionGroup()}
	groups[1].ID = "inventory"
	if got := maximumConcurrentTransitions(groups); got != len(groups) {
		t.Fatalf("maximum concurrent transitions = %d, want one per group (%d)", got, len(groups))
	}
	if got := maximumConcurrentTransitions(nil); got != 1 {
		t.Fatalf("empty-group fallback = %d, want 1", got)
	}
}

func TestBootstrapMembershipSeedsOnlyExplicitEmptyState(t *testing.T) {
	cfg := config.Config{Namespace: "swlb", Groups: []config.ScalingGroup{productionGroup()}}
	publisher := &captureMembershipPublisher{}
	catalog := NewMembershipCatalog()
	if err := BootstrapMembership(context.Background(), cfg, controller.PersistentState{Groups: map[string]*controller.GroupState{}}, staticBrowser{err: broker0.ErrNoAuthoritativeState}, publisher, catalog); err != nil {
		t.Fatal(err)
	}
	if len(publisher.snapshots) != 1 {
		t.Fatalf("published %d genesis snapshots", len(publisher.snapshots))
	}
	got, ok := catalog.Get("orders")
	if !ok || !got.CurrentMembership.Equal(control.Membership{"broker-b", "broker-a"}) || got.Revision != 1 || got.Epoch != 1 || got.Phase != control.PhaseActive {
		t.Fatalf("genesis = %#v, present=%t", got, ok)
	}

	publisher.snapshots = nil
	want := errors.New("transport failed")
	if err := BootstrapMembership(context.Background(), cfg, controller.PersistentState{Groups: map[string]*controller.GroupState{}}, staticBrowser{err: want}, publisher, NewMembershipCatalog()); !errors.Is(err, want) {
		t.Fatalf("bootstrap error = %v, want %v", err, want)
	}
	if len(publisher.snapshots) != 0 {
		t.Fatal("arbitrary browser failure seeded membership")
	}
}

func TestBootstrapMembershipPreservesRetainedOrder(t *testing.T) {
	group := productionGroup()
	snapshot, err := GenesisSnapshot("swlb", group)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Revision = 17
	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	catalog := NewMembershipCatalog()
	publisher := &captureMembershipPublisher{}
	if err := BootstrapMembership(context.Background(), config.Config{Namespace: "swlb", Groups: []config.ScalingGroup{group}}, controller.PersistentState{Groups: map[string]*controller.GroupState{}}, staticBrowser{messages: []broker0.BrowsedMessage{{Kind: broker0.KindMembershipSnapshot, OperationID: "retained", Payload: payload}}}, publisher, catalog); err != nil {
		t.Fatal(err)
	}
	got, _ := catalog.Get(group.ID)
	if !reflect.DeepEqual(got.CurrentMembership, snapshot.CurrentMembership) || len(publisher.snapshots) != 0 {
		t.Fatalf("retained membership changed or was republished: %#v", got)
	}
}

func TestBootstrapMembershipRejectsTransitionWithoutDurableState(t *testing.T) {
	group := productionGroup()
	snapshot, err := GenesisSnapshot("swlb", group)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Phase = control.PhasePrepare
	snapshot.ProposedMembership = control.Membership{"broker-b", "broker-a", "broker-c"}
	snapshot.Transition = &control.Transition{ID: "move-1", FromEpoch: 1, ToEpoch: 2}
	snapshot.ProposedResources, err = epochResources(mustNames(t, "swlb"), group.ID, snapshot.ProposedMembership, group.EffectiveConsumerSets(), 2)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(snapshot)
	err = BootstrapMembership(context.Background(), config.Config{Namespace: "swlb", Groups: []config.ScalingGroup{group}}, controller.PersistentState{Groups: map[string]*controller.GroupState{}}, staticBrowser{messages: []broker0.BrowsedMessage{{Kind: broker0.KindMembershipSnapshot, OperationID: "move", Payload: payload}}}, &captureMembershipPublisher{}, NewMembershipCatalog())
	if err == nil {
		t.Fatal("bootstrap accepted retained transition without durable state")
	}
}

func TestBootstrapMembershipRecoversUnpublishedPrepareFromSourceActive(t *testing.T) {
	group := productionGroup()
	cfg := config.Config{Namespace: "swlb", Groups: []config.ScalingGroup{group}}
	baseline, _ := GenesisSnapshot(cfg.Namespace, group)
	spec, err := transitionSpec(cfg.Namespace, group, baseline, policy.Recommendation{
		CurrentMembership: []string{"broker-b", "broker-a"}, ProposedMembership: []string{"broker-b", "broker-a", "broker-c"},
	}, time.Unix(20, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	state := &controller.GroupState{Spec: spec, Phase: controller.PhasePrepare}
	payload, _ := json.Marshal(baseline)
	catalog := NewMembershipCatalog()
	if err := BootstrapMembership(context.Background(), cfg, controller.PersistentState{Groups: map[string]*controller.GroupState{group.ID: state}}, staticBrowser{messages: []broker0.BrowsedMessage{{Kind: broker0.KindMembershipSnapshot, Payload: payload}}}, &captureMembershipPublisher{}, catalog); err != nil {
		t.Fatal(err)
	}
	got, ok := catalog.Get(group.ID)
	if !ok || !membershipSnapshotsEqual(got, baseline) {
		t.Fatalf("recovered source snapshot = %#v, present=%t", got, ok)
	}
}

func TestBootstrapMembershipRecoversPublishedTargetActiveBeforeLocalPersistence(t *testing.T) {
	group := productionGroup()
	cfg := config.Config{Namespace: "swlb", Groups: []config.ScalingGroup{group}}
	baseline, _ := GenesisSnapshot(cfg.Namespace, group)
	spec, err := transitionSpec(cfg.Namespace, group, baseline, policy.Recommendation{
		CurrentMembership: []string{"broker-b", "broker-a"}, ProposedMembership: []string{"broker-b", "broker-a", "broker-c"},
	}, time.Unix(20, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(30, 0).UTC()
	state := &controller.GroupState{
		Spec: spec, Phase: controller.PhaseActivate, FenceAttempted: true, Fenced: true, FenceVerifiedAt: &now,
		CommitPublished: true, TargetIngressEnabled: true, TargetIngressVerified: true, TargetIngressVerifiedAt: &now,
		IssuedCommandIDs: map[controller.Phase]map[string]string{controller.PhaseActivate: {}},
	}
	for _, requirement := range spec.RoleRequirements[controller.PhaseActivate] {
		state.IssuedCommandIDs[controller.PhaseActivate][requirement.Participant] = spec.Group + "/" + spec.ID + "/forward/command/ACTIVATE/" + string(requirement.Role) + "/" + requirement.Participant
	}
	snapshot := targetActiveSnapshot(spec)
	payload, _ := json.Marshal(snapshot)
	catalog := NewMembershipCatalog()
	if err := BootstrapMembership(context.Background(), cfg, controller.PersistentState{Groups: map[string]*controller.GroupState{group.ID: state}}, staticBrowser{messages: []broker0.BrowsedMessage{{Kind: broker0.KindMembershipSnapshot, Payload: payload}}}, &captureMembershipPublisher{}, catalog); err != nil {
		t.Fatal(err)
	}
	got, ok := catalog.Get(group.ID)
	if !ok || !membershipSnapshotsEqual(got, snapshot) {
		t.Fatalf("recovered snapshot = %#v, present=%t", got, ok)
	}
}

func TestBootstrapMembershipRejectsTargetActiveMismatchAndPreCommitState(t *testing.T) {
	group := productionGroup()
	cfg := config.Config{Namespace: "swlb", Groups: []config.ScalingGroup{group}}
	baseline, _ := GenesisSnapshot(cfg.Namespace, group)
	spec, err := transitionSpec(cfg.Namespace, group, baseline, policy.Recommendation{
		CurrentMembership: []string{"broker-b", "broker-a"}, ProposedMembership: []string{"broker-b", "broker-a", "broker-c"},
	}, time.Unix(20, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(30, 0).UTC()
	validState := controller.GroupState{
		Spec: spec, Phase: controller.PhaseActivate, FenceAttempted: true, Fenced: true, FenceVerifiedAt: &now,
		CommitPublished: true, TargetIngressEnabled: true, TargetIngressVerified: true, TargetIngressVerifiedAt: &now,
		IssuedCommandIDs: map[controller.Phase]map[string]string{controller.PhaseActivate: {}},
	}
	for _, requirement := range spec.RoleRequirements[controller.PhaseActivate] {
		validState.IssuedCommandIDs[controller.PhaseActivate][requirement.Participant] = spec.Group + "/" + spec.ID + "/forward/command/ACTIVATE/" + string(requirement.Role) + "/" + requirement.Participant
	}
	for _, test := range []struct {
		name     string
		snapshot control.MembershipSnapshot
		state    controller.GroupState
	}{
		{name: "revision mismatch", snapshot: func() control.MembershipSnapshot {
			snapshot := targetActiveSnapshot(spec)
			snapshot.Revision++
			return snapshot
		}(), state: validState},
		{name: "pre-commit phase", snapshot: targetActiveSnapshot(spec), state: func() controller.GroupState {
			state := validState
			state.Phase = controller.PhaseCommit
			state.CommitPublished = false
			return state
		}()},
		{name: "missing durable command intent", snapshot: targetActiveSnapshot(spec), state: func() controller.GroupState { state := validState; state.IssuedCommandIDs = nil; return state }()},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, _ := json.Marshal(test.snapshot)
			err := BootstrapMembership(context.Background(), cfg, controller.PersistentState{Groups: map[string]*controller.GroupState{group.ID: &test.state}}, staticBrowser{messages: []broker0.BrowsedMessage{{Kind: broker0.KindMembershipSnapshot, Payload: payload}}}, &captureMembershipPublisher{}, NewMembershipCatalog())
			if err == nil {
				t.Fatal("bootstrap accepted conflicting retained ACTIVE snapshot")
			}
		})
	}
}

func TestResolveManagedFenceQueueUsesDurableSourceAtCommittedRestart(t *testing.T) {
	group := productionGroup()
	baseline, _ := GenesisSnapshot("swlb", group)
	spec, err := transitionSpec("swlb", group, baseline, policy.Recommendation{
		CurrentMembership: []string{"broker-b", "broker-a"}, ProposedMembership: []string{"broker-c"},
	}, time.Unix(20, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	store := memoryControllerStore{state: controller.PersistentState{Version: 1, Groups: map[string]*controller.GroupState{
		group.ID: {Spec: spec, Phase: controller.PhaseCommit, FenceAttempted: true, Fenced: true},
	}}}
	coordinator, err := controller.Open(store, passthroughFence{}, &captureMembershipPublisher{}, controller.Options{})
	if err != nil {
		t.Fatal(err)
	}
	catalog := NewMembershipCatalog()
	committed := targetActiveSnapshot(spec)
	committed.Phase = control.PhaseCommitted
	committed.Revision = spec.Revision + 3
	committed.Transition = &control.Transition{ID: spec.ID, FromEpoch: spec.FromEpoch, ToEpoch: spec.ToEpoch}
	if err := catalog.Put(committed); err != nil {
		t.Fatal(err)
	}
	queues, err := resolveManagedFenceQueues(coordinator, catalog, map[string]string{"broker-b": "data-b"}, controller.FenceRequest{
		Group: group.ID, TransitionID: spec.ID, Epoch: spec.FromEpoch,
	}, spec.Current[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 1 || queues[0].MessageVPN != "data-b" || queues[0].Name != spec.CurrentResources[0].QueueName {
		t.Fatalf("resolved %#v, want durable source %q", queues, spec.CurrentResources[0].QueueName)
	}
}

func mustNames(t *testing.T, namespace string) control.ManagedNames {
	t.Helper()
	names, err := control.NewManagedNames(namespace)
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func TestCommandPublisherEmitsTypedCommandsWithStableIDs(t *testing.T) {
	base := &captureMembershipPublisher{}
	catalog := NewMembershipCatalog()
	publisher := &CommandPublisher{Publisher: base, Catalog: catalog, Deadlines: map[string]time.Duration{"orders": time.Minute}, Now: func() time.Time { return time.Unix(10, 0).UTC() }}
	requirements := typedRequirements(productionGroup())
	publisher.SetRequirements("orders", requirements)
	snapshot, err := GenesisSnapshot("swlb", productionGroup())
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Phase = control.PhasePrepare
	snapshot.ProposedMembership = control.Membership{"broker-b", "broker-a", "broker-c"}
	snapshot.Transition = &control.Transition{ID: "move-1", FromEpoch: 1, ToEpoch: 2}
	snapshot.ProposedResources = append([]control.EpochResourceIdentity(nil), snapshot.CurrentResources...)
	for index := range snapshot.ProposedResources {
		snapshot.ProposedResources[index].Epoch = 2
	}
	update := controller.ControlUpdate{OperationID: "orders/move-1/forward/PREPARE", Group: "orders", TransitionID: "move-1", ToEpoch: 2, Phase: controller.PhasePrepare, Snapshot: snapshot, IssuedCommandIDs: map[string]string{"subscriber-1": "orders/move-1/forward/command/PREPARE/subscriber/subscriber-1"}}
	if err := publisher.Publish(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if len(base.updates) != 1 || len(base.commands) != 1 {
		t.Fatalf("updates=%d commands=%d", len(base.updates), len(base.commands))
	}
	command := base.commands[0]
	if command.MessageID != "orders/move-1/forward/command/PREPARE/subscriber/subscriber-1" || command.Participant != "subscriber-1" || command.Role != control.RoleSubscriber {
		t.Fatalf("command = %#v", command)
	}
}

func TestCommandPublisherRequiresDurableIssuanceIntent(t *testing.T) {
	base := &captureMembershipPublisher{}
	publisher := &CommandPublisher{Publisher: base, Catalog: NewMembershipCatalog(), Deadlines: map[string]time.Duration{"orders": time.Minute}}
	publisher.SetRequirements("orders", typedRequirements(productionGroup()))
	snapshot, err := GenesisSnapshot("swlb", productionGroup())
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Phase = control.PhasePrepare
	snapshot.ProposedMembership = control.Membership{"broker-b", "broker-a", "broker-c"}
	snapshot.Transition = &control.Transition{ID: "move-1", FromEpoch: 1, ToEpoch: 2}
	snapshot.ProposedResources, _ = epochResources(mustNames(t, "swlb"), "orders", snapshot.ProposedMembership, productionGroup().EffectiveConsumerSets(), 2)
	update := controller.ControlUpdate{
		OperationID: "orders/move-1/forward/PREPARE", Group: "orders", TransitionID: "move-1", ToEpoch: 2,
		Phase: controller.PhasePrepare, Snapshot: snapshot,
	}
	if err := publisher.Publish(context.Background(), update); err == nil {
		t.Fatal("publisher accepted a command without durable issuance intent")
	}
	if len(base.updates) != 0 || len(base.commands) != 0 {
		t.Fatalf("publication occurred before durable intent: updates=%d commands=%d", len(base.updates), len(base.commands))
	}
}

func TestCommandPublisherDoesNotPublishSnapshotWithoutRequirements(t *testing.T) {
	base := &captureMembershipPublisher{}
	publisher := &CommandPublisher{Publisher: base, Catalog: NewMembershipCatalog(), Deadlines: map[string]time.Duration{"orders": time.Minute}}
	snapshot, err := GenesisSnapshot("swlb", productionGroup())
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Phase = control.PhasePrepare
	snapshot.ProposedMembership = control.Membership{"broker-b", "broker-a", "broker-c"}
	snapshot.Transition = &control.Transition{ID: "move-1", FromEpoch: 1, ToEpoch: 2}
	snapshot.ProposedResources, _ = epochResources(mustNames(t, "swlb"), "orders", snapshot.ProposedMembership, productionGroup().EffectiveConsumerSets(), 2)
	update := controller.ControlUpdate{
		OperationID: "orders/move-1/forward/PREPARE", Group: "orders", TransitionID: "move-1", ToEpoch: 2,
		Phase: controller.PhasePrepare, Snapshot: snapshot,
		IssuedCommandIDs: map[string]string{"subscriber-1": "orders/move-1/forward/command/PREPARE/subscriber/subscriber-1"},
	}
	if err := publisher.Publish(context.Background(), update); err == nil {
		t.Fatal("publisher accepted phase without typed command requirements")
	}
	if len(base.updates) != 0 {
		t.Fatal("snapshot published before command requirements were validated")
	}
}

func TestControllerPolicyStateReadinessIsScopedToCurrentTelemetryCycle(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	group.Policy.WarmBrokers = 1
	cfg := config.Config{
		DataBrokers: []config.DataBroker{
			{ID: "broker-a", ServiceClass: "class", BrokerVersion: "1", EligibleGroups: []string{group.ID}},
			{ID: "broker-b", ServiceClass: "class", BrokerVersion: "1", EligibleGroups: []string{group.ID}},
		},
		Groups:    []config.ScalingGroup{group},
		Staleness: config.StalenessPolicy{TelemetryMaxAge: config.Duration{Duration: 15 * time.Second}},
	}
	membership := NewMembershipCatalog()
	retained, err := GenesisSnapshot("swlb", group)
	if err != nil {
		t.Fatal(err)
	}
	if err := membership.Put(retained); err != nil {
		t.Fatal(err)
	}
	coordinator, err := controller.Open(memoryControllerStore{}, passthroughFence{}, &captureMembershipPublisher{}, controller.Options{})
	if err != nil {
		t.Fatal(err)
	}
	state := &ControllerPolicyState{Config: cfg, Controller: coordinator, Catalog: membership}
	profiles, err := policy.NewProfileCatalog([]policy.CapacityProfile{{
		ServiceClass: "class", BrokerVersion: "1",
		Limits: policy.CapacityLimits{IngressBytesPerSecond: 100, EgressBytesPerSecond: 100, SpoolBytes: 100, Connections: 100},
	}})
	if err != nil {
		t.Fatal(err)
	}
	complete := policy.Resources{
		IngressBytesPerSecond: policy.KnownMetric(1), EgressBytesPerSecond: policy.KnownMetric(1),
		SpoolBytes: policy.KnownMetric(1), Connections: policy.KnownMetric(1),
	}
	brokerSample := func(id string, observedAt time.Time, resources policy.Resources) policy.BrokerSample {
		return policy.BrokerSample{BrokerID: id, ServiceClass: "class", BrokerVersion: "1", ObservedAt: observedAt, Resources: resources}
	}
	cycle := func(candidate *policy.BrokerSample) policy.TelemetrySnapshot {
		brokers := []policy.BrokerSample{brokerSample("broker-a", now, complete)}
		if candidate != nil {
			brokers = append(brokers, *candidate)
		}
		return policy.TelemetrySnapshot{
			CapturedAt: now,
			Brokers:    brokers,
			Groups: []policy.GroupSample{{
				GroupID: group.ID, BrokerID: "broker-a", ObservedAt: now, Resources: complete,
				Backlog: policy.Backlog{QueuedMessages: policy.KnownMetric(0), UnackedMessages: policy.KnownMetric(0)},
			}},
		}
	}
	evaluate := func(snapshot policy.TelemetrySnapshot) policy.Decision {
		t.Helper()
		inventory, groups, err := state.PolicyStateForTelemetry(snapshot, now)
		if err != nil {
			t.Fatal(err)
		}
		engine, err := policy.NewEngine(profiles, policy.EngineOptions{MaxConcurrentTransitions: 1})
		if err != nil {
			t.Fatal(err)
		}
		decision, err := engine.Evaluate(now, snapshot, inventory, groups)
		if err != nil {
			t.Fatal(err)
		}
		return decision
	}
	assertNoScaleOut := func(name string, snapshot policy.TelemetrySnapshot) {
		t.Helper()
		if transitions := evaluate(snapshot).Transitions; len(transitions) != 0 {
			t.Fatalf("%s telemetry admitted scale-out: %#v", name, transitions)
		}
	}

	assertNoScaleOut("missing", cycle(nil))
	partial := complete
	partial.Connections = policy.UnknownMetric()
	partialSample := brokerSample("broker-b", now, partial)
	assertNoScaleOut("partial", cycle(&partialSample))
	exactSample := brokerSample("broker-b", now, complete)
	decision := evaluate(cycle(&exactSample))
	if len(decision.Transitions) != 1 || decision.Transitions[0].BrokerID != "broker-b" {
		t.Fatalf("exact fresh telemetry did not admit broker-b: %#v", decision.Transitions)
	}
	staleSample := brokerSample("broker-b", now.Add(-16*time.Second), complete)
	assertNoScaleOut("stale", cycle(&staleSample))

	inventory, _, err := state.PolicyState()
	if err != nil {
		t.Fatal(err)
	}
	for _, broker := range inventory {
		if broker.Ready {
			t.Fatalf("base policy state retained readiness for broker %q", broker.ID)
		}
	}
}

func TestRecommendationControllerBeginsTypedTransition(t *testing.T) {
	group := productionGroup()
	cfg := config.Config{Namespace: "swlb", Groups: []config.ScalingGroup{group}}
	catalog := NewMembershipCatalog()
	baseline, err := GenesisSnapshot("swlb", group)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Put(baseline); err != nil {
		t.Fatal(err)
	}
	publisher := &captureMembershipPublisher{}
	commands := &CommandPublisher{Publisher: publisher, Catalog: catalog, Deadlines: map[string]time.Duration{"orders": time.Minute}}
	coordinator, err := controller.Open(memoryControllerStore{}, passthroughFence{}, commands, controller.Options{})
	if err != nil {
		t.Fatal(err)
	}
	applier := &RecommendationController{Controller: coordinator, Catalog: catalog, Config: cfg, Publisher: commands}
	decision := policy.Decision{At: time.Unix(20, 0).UTC(), Transitions: []policy.Recommendation{{GroupID: "orders", Action: policy.ActionScaleOut, CurrentMembership: []string{"broker-b", "broker-a"}, ProposedMembership: []string{"broker-b", "broker-a", "broker-c"}}}}
	if err := applier.ApplyRecommendations(context.Background(), decision); err != nil {
		t.Fatal(err)
	}
	state, ok := coordinator.Group("orders")
	if !ok || state.Spec.RoleRequirements[controller.PhasePrepare][0].Role != control.RoleSubscriber || state.Spec.RoleRequirements[controller.PhasePause][0].Role != control.RolePublisher {
		t.Fatalf("transition requirements = %#v", state.Spec.RoleRequirements)
	}
}

type memoryControllerStore struct{ state controller.PersistentState }

func (s memoryControllerStore) Load() (controller.PersistentState, error) {
	if s.state.Groups != nil {
		return s.state, nil
	}
	return controller.PersistentState{Version: 1, Groups: map[string]*controller.GroupState{}, History: map[string][]controller.TransitionRecord{}, CleanupEvidence: map[string][]controller.CleanupEvidence{}}, nil
}
func (memoryControllerStore) Save(controller.PersistentState) error { return nil }

type closeRecorder struct {
	name  string
	order *[]string
}

func (c closeRecorder) Close() error {
	*c.order = append(*c.order, c.name)
	return nil
}

type immediateProcess struct{ err error }

func (p immediateProcess) Run(context.Context) error { return p.err }

func TestSEMPBrokerFenceUsesObservationAfterMonitor(t *testing.T) {
	before := time.Unix(100, 0).UTC()
	after := before.Add(time.Second)
	calls := 0
	client := &fakeQueueClient{status: semp.QueueStatus{IngressEnabled: false}}
	fence := SEMPBrokerFence{
		Brokers: map[string]BrokerTarget{"broker-a": {Client: client}},
		Queues: FenceQueueResolverFunc(func(context.Context, controller.FenceRequest, controller.Broker) ([]ManagedQueue, error) {
			return []ManagedQueue{{MessageVPN: "vpn", Name: "queue"}}, nil
		}),
		Now: func() time.Time {
			calls++
			if calls == 1 {
				return after
			}
			return before
		},
	}
	status, err := fence.VerifyFence(context.Background(), controller.FenceRequest{Brokers: []controller.Broker{{ID: "broker-a", Destination: "topic"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !status.ObservedAt.Equal(after) {
		t.Fatalf("observation time = %v, want post-monitor %v", status.ObservedAt, after)
	}
}

func TestOwnedProcessClosesResourcesInReverseOrder(t *testing.T) {
	var order []string
	process := &OwnedProcess{Process: immediateProcess{}, Close: &CloseGroup{Closers: []interface{ Close() error }{
		closeRecorder{name: "connection", order: &order}, closeRecorder{name: "publisher", order: &order}, closeRecorder{name: "browser", order: &order},
	}}}
	if err := process.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"browser", "publisher", "connection"}) {
		t.Fatalf("close order = %v", order)
	}
}

type assemblyNativePublisher struct{ closeRecorder }

func (*assemblyNativePublisher) PublishPersistent(context.Context, broker0.Publication) error {
	return nil
}

type assemblyBrowser struct {
	staticBrowser
	closeRecorder
}

type assemblyReceiver struct{ closeRecorder }

func (*assemblyReceiver) Receive(ctx context.Context) (broker0.Delivery, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestAssembleControllerClosesPartialResourcesOnPolicyFailure(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := config.Config{
		Namespace: "swlb", Groups: []config.ScalingGroup{group},
		Runtime: config.RuntimeIdentities{Controller: "controller-1", Publishers: []string{"publisher-1"}, Subscribers: []string{"subscriber-1"}, Observers: []string{"observer-1"}},
		Control: config.ControlBroker{ParticipantCredentials: []config.ParticipantControlCredential{
			{Participant: "publisher-1", Principal: "publisher-1"}, {Participant: "subscriber-1", Principal: "subscriber-1"}, {Participant: "observer-1", Principal: "observer-1"},
		}, Resources: config.Broker0Resources{MembershipQueuePrefix: "swlb.membership", RegistrationQueue: "swlb.registration", ReadinessQueue: "swlb.readiness", TelemetryQueue: "swlb.telemetry"}},
		DataBrokers: []config.DataBroker{{ID: "broker-a", MessageVPN: "data"}},
		Publisher:   config.AsyncPublisher{PublishTimeout: config.Duration{Duration: time.Second}, ShutdownTimeout: config.Duration{Duration: time.Second}},
		Staleness:   config.StalenessPolicy{ParticipantMaxAge: config.Duration{Duration: time.Second}, TelemetryMaxAge: config.Duration{Duration: time.Second}},
		Persistence: config.Persistence{ControllerState: t.TempDir() + "/controller.json"},
	}
	var order []string
	connection := &SMFConnection{}
	publisher := &assemblyNativePublisher{closeRecorder{name: "publisher", order: &order}}
	browser := &assemblyBrowser{staticBrowser: staticBrowser{err: broker0.ErrNoAuthoritativeState}, closeRecorder: closeRecorder{name: "browser", order: &order}}
	assembler := ControllerAssembler{
		Connect:      func(context.Context, config.Config, Credentials) (*SMFConnection, error) { return connection, nil },
		NewPublisher: func(*SMFConnection, time.Duration) (NativeControlPublisher, error) { return publisher, nil },
		NewBrowser: func(config.Config, Credentials, broker0.MembershipQueueResolver) (broker0.Browser, error) {
			return browser, nil
		},
		NewSEMPClient: func(config.DataBroker, BrokerCredentials) (SEMPQueueClient, error) { return &fakeQueueClient{}, nil },
		BindInbox: func(*SMFConnection, broker0.ParticipantQueue, time.Duration) (broker0.DurableReceiver, error) {
			return &assemblyReceiver{closeRecorder: closeRecorder{name: "receiver", order: &order}}, nil
		},
		NewPolicy: func(config.Config, *controller.Controller, *MembershipCatalog, map[string]SEMPQueueClient, *CommandPublisher) (PolicyCycle, error) {
			return nil, ErrCapacityPolicyUnavailable
		},
	}
	_, err := AssembleController(context.Background(), cfg, Credentials{DataBrokers: map[string]BrokerCredentials{"broker-a": {}}}, nil, assembler)
	if !errors.Is(err, ErrCapacityPolicyUnavailable) {
		t.Fatalf("assembly error = %v", err)
	}
	if !reflect.DeepEqual(order, []string{"receiver", "receiver", "receiver", "receiver", "receiver", "browser", "publisher"}) {
		t.Fatalf("partial assembly close order = %v", order)
	}
}

type blockingPolicyCycle struct{ started chan struct{} }

func (p *blockingPolicyCycle) Run(ctx context.Context) error {
	close(p.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestAssembleControllerSuccessBindsInboxesAndRunsPolicy(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := config.Config{
		Namespace: "swlb", Groups: []config.ScalingGroup{group},
		Runtime: config.RuntimeIdentities{Controller: "controller-1", Publishers: []string{"publisher-1"}, Subscribers: []string{"subscriber-1"}, Observers: []string{"observer-1"}},
		Control: config.ControlBroker{ParticipantCredentials: []config.ParticipantControlCredential{
			{Participant: "publisher-1", Principal: "publisher-1"}, {Participant: "subscriber-1", Principal: "subscriber-1"}, {Participant: "observer-1", Principal: "observer-1"},
		}, Resources: config.Broker0Resources{MembershipQueuePrefix: "swlb.membership", RegistrationQueue: "swlb.registration", ReadinessQueue: "swlb.readiness", TelemetryQueue: "swlb.telemetry"}},
		DataBrokers: []config.DataBroker{{ID: "broker-a", MessageVPN: "data"}},
		Publisher:   config.AsyncPublisher{PublishTimeout: config.Duration{Duration: time.Second}, ShutdownTimeout: config.Duration{Duration: time.Second}},
		Staleness:   config.StalenessPolicy{ParticipantMaxAge: config.Duration{Duration: time.Second}, TelemetryMaxAge: config.Duration{Duration: time.Second}},
		Persistence: config.Persistence{ControllerState: t.TempDir() + "/controller.json"},
	}
	var order []string
	var bindings []broker0.ParticipantQueue
	publisher := &assemblyNativePublisher{closeRecorder{name: "publisher", order: &order}}
	browser := &assemblyBrowser{staticBrowser: staticBrowser{err: broker0.ErrNoAuthoritativeState}, closeRecorder: closeRecorder{name: "browser", order: &order}}
	policyCycle := &blockingPolicyCycle{started: make(chan struct{})}
	assembler := ControllerAssembler{
		Connect: func(context.Context, config.Config, Credentials) (*SMFConnection, error) {
			return &SMFConnection{}, nil
		},
		NewPublisher: func(*SMFConnection, time.Duration) (NativeControlPublisher, error) { return publisher, nil },
		NewBrowser: func(config.Config, Credentials, broker0.MembershipQueueResolver) (broker0.Browser, error) {
			return browser, nil
		},
		NewSEMPClient: func(config.DataBroker, BrokerCredentials) (SEMPQueueClient, error) { return &fakeQueueClient{}, nil },
		BindInbox: func(_ *SMFConnection, binding broker0.ParticipantQueue, _ time.Duration) (broker0.DurableReceiver, error) {
			bindings = append(bindings, binding)
			return &assemblyReceiver{closeRecorder: closeRecorder{name: binding.Queue, order: &order}}, nil
		},
		NewPolicy: func(config.Config, *controller.Controller, *MembershipCatalog, map[string]SEMPQueueClient, *CommandPublisher) (PolicyCycle, error) {
			return policyCycle, nil
		},
	}
	process, err := AssembleController(context.Background(), cfg, Credentials{DataBrokers: map[string]BrokerCredentials{"broker-a": {}}}, nil, assembler)
	if err != nil {
		t.Fatal(err)
	}
	controllerProcess, ok := process.Process.(*ControllerProcess)
	if !ok {
		t.Fatalf("assembled process = %T, want *ControllerProcess", process.Process)
	}
	if got, want := controllerProcess.groupNames(), []string{group.ID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("assembled reconcile groups = %v, want configured groups %v", got, want)
	}
	wantBindings := []broker0.ParticipantQueue{
		{Participant: "publisher-1", Principal: "publisher-1", Role: control.RolePublisher, Group: "orders", Kind: broker0.KindRegistration, Queue: "swlb.ctl.orders.reg.publisher.publisher-1", Topic: "swlb/control/orders/registrations/publisher/publisher-1"},
		{Participant: "publisher-1", Principal: "publisher-1", Role: control.RolePublisher, Group: "orders", Kind: broker0.KindAcknowledgement, Queue: "swlb.ctl.orders.ack.publisher.publisher-1", Topic: "swlb/control/orders/acknowledgements/publisher/publisher-1"},
		{Participant: "subscriber-1", Principal: "subscriber-1", Role: control.RoleSubscriber, Group: "orders", Kind: broker0.KindRegistration, Queue: "swlb.ctl.orders.reg.subscriber.subscriber-1", Topic: "swlb/control/orders/registrations/subscriber/subscriber-1"},
		{Participant: "subscriber-1", Principal: "subscriber-1", Role: control.RoleSubscriber, Group: "orders", Kind: broker0.KindAcknowledgement, Queue: "swlb.ctl.orders.ack.subscriber.subscriber-1", Topic: "swlb/control/orders/acknowledgements/subscriber/subscriber-1"},
		{Participant: "observer-1", Principal: "observer-1", Role: control.RoleObserver, Group: "orders", Kind: broker0.KindTelemetry, Queue: "swlb.ctl.orders.tel.observer.observer-1", Topic: "swlb/control/orders/telemetry/observer/observer-1"},
	}
	if !reflect.DeepEqual(bindings, wantBindings) {
		t.Fatalf("inbox bindings = %#v", bindings)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	select {
	case <-policyCycle.started:
	case <-time.After(time.Second):
		t.Fatal("policy component did not start")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{
		"swlb.ctl.orders.tel.observer.observer-1",
		"swlb.ctl.orders.ack.subscriber.subscriber-1", "swlb.ctl.orders.reg.subscriber.subscriber-1",
		"swlb.ctl.orders.ack.publisher.publisher-1", "swlb.ctl.orders.reg.publisher.publisher-1",
		"browser", "publisher",
	}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("successful assembly close order = %v, want %v", order, wantOrder)
	}
}

func TestDefaultPolicyAssemblyRejectsMissingCapacityProfile(t *testing.T) {
	assembler := DefaultControllerAssembler()
	cfg := config.Config{DataBrokers: []config.DataBroker{{ID: "broker-a", ServiceClass: "class", BrokerVersion: "1"}}}
	_, err := assembler.NewPolicy(cfg, nil, nil, nil, nil)
	if !errors.Is(err, ErrCapacityPolicyUnavailable) {
		t.Fatalf("policy error = %v", err)
	}
}

type passthroughFence struct{}

func (passthroughFence) Fence(context.Context, controller.FenceRequest) error         { return nil }
func (passthroughFence) Unfence(context.Context, controller.FenceRequest) error       { return nil }
func (passthroughFence) EnableIngress(context.Context, controller.FenceRequest) error { return nil }
func (passthroughFence) VerifyFence(context.Context, controller.FenceRequest) (controller.FenceStatus, error) {
	return controller.FenceStatus{Fenced: true, ObservedAt: time.Now()}, nil
}
func (passthroughFence) VerifyIngress(context.Context, controller.FenceRequest) (controller.IngressStatus, error) {
	return controller.IngressStatus{Enabled: true, ObservedAt: time.Now()}, nil
}
