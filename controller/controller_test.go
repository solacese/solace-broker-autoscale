package controller

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

type fakeFence struct {
	mu                  sync.Mutex
	fences              []FenceRequest
	unfences            []FenceRequest
	enables             []FenceRequest
	verifies            []FenceRequest
	ingress             []FenceRequest
	now                 func() time.Time
	beforeVerifyFence   func()
	beforeVerifyIngress func()
}

func (f *fakeFence) Fence(_ context.Context, request FenceRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fences = append(f.fences, request)
	return nil
}

func (f *fakeFence) Unfence(_ context.Context, request FenceRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unfences = append(f.unfences, request)
	return nil
}

func (f *fakeFence) VerifyFence(_ context.Context, request FenceRequest) (FenceStatus, error) {
	f.mu.Lock()
	f.verifies = append(f.verifies, request)
	hook := f.beforeVerifyFence
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return FenceStatus{Fenced: true, ObservedAt: f.observedAt()}, nil
}

func (f *fakeFence) EnableIngress(_ context.Context, request FenceRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enables = append(f.enables, request)
	return nil
}

func (f *fakeFence) VerifyIngress(_ context.Context, request FenceRequest) (IngressStatus, error) {
	f.mu.Lock()
	f.ingress = append(f.ingress, request)
	hook := f.beforeVerifyIngress
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return IngressStatus{Enabled: true, ObservedAt: f.observedAt()}, nil
}

func (f *fakeFence) observedAt() time.Time {
	if f.now != nil {
		return f.now()
	}
	return time.Now()
}

type fakePublisher struct {
	mu      sync.Mutex
	updates []ControlUpdate
	hook    func(ControlUpdate) error
}

func (p *fakePublisher) Publish(_ context.Context, update ControlUpdate) error {
	p.mu.Lock()
	p.updates = append(p.updates, update)
	hook := p.hook
	p.mu.Unlock()
	if hook != nil {
		return hook(update)
	}
	return nil
}

type blockingPublisher struct {
	blockedGroup string
	started      chan struct{}
	release      chan struct{}
	releaseOnce  sync.Once

	mu      sync.Mutex
	calls   map[string]int
	active  map[string]int
	maximum map[string]int
}

func newBlockingPublisher(group string) *blockingPublisher {
	return &blockingPublisher{
		blockedGroup: group,
		started:      make(chan struct{}, 1),
		release:      make(chan struct{}),
		calls:        make(map[string]int),
		active:       make(map[string]int),
		maximum:      make(map[string]int),
	}
}

func (p *blockingPublisher) Publish(ctx context.Context, update ControlUpdate) error {
	p.mu.Lock()
	p.calls[update.Group]++
	p.active[update.Group]++
	if p.active[update.Group] > p.maximum[update.Group] {
		p.maximum[update.Group] = p.active[update.Group]
	}
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.active[update.Group]--
		p.mu.Unlock()
	}()
	if update.Group != p.blockedGroup {
		return nil
	}
	select {
	case p.started <- struct{}{}:
	default:
	}
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *blockingPublisher) releaseBlocked() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func (p *blockingPublisher) counts(group string) (calls, maximum int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[group], p.maximum[group]
}

type uncertainStore struct {
	state PersistentState
	fail  bool
}

func (s *uncertainStore) Load() (PersistentState, error) { return clonePersistent(s.state), nil }
func (s *uncertainStore) Save(state PersistentState) error {
	if s.fail {
		return errors.New("disk unavailable")
	}
	s.state = clonePersistent(state)
	return nil
}

func testSpec(group, id string) TransitionSpec {
	return TransitionSpec{
		ID: id, Group: group, Revision: 10, FromEpoch: 1, ToEpoch: 2,
		Current:              []Broker{{ID: "old", Destination: "orders"}},
		Proposed:             []Broker{{ID: "new", Destination: "orders"}},
		Queue:                control.QueueInfo{Name: group + "-queue", Durable: true},
		Destination:          control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders"},
		HashContract:         "orders-v1",
		Algorithm:            control.AlgorithmSHA256BigEndianModulo,
		RequiredParticipants: []string{"sub-b", "sub-a"},
	}
}

func openTestController(t *testing.T, path string, now *time.Time, fence BrokerFence, publisher ControlPublisher) *Controller {
	t.Helper()
	if fake, ok := fence.(*fakeFence); ok {
		fake.now = func() time.Time { return *now }
	}
	controller, err := Open(JSONStore{Path: path}, fence, publisher, Options{
		TelemetryFreshness: 10 * time.Second,
		ZeroGrace:          3 * time.Second,
		Now:                func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func reconcileChanged(t *testing.T, controller *Controller, group string) {
	t.Helper()
	changed, err := controller.Reconcile(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatalf("expected reconcile to change %s", group)
	}
}

func acknowledgeAll(t *testing.T, controller *Controller, spec TransitionSpec, phase Phase, at time.Time) {
	t.Helper()
	for _, participant := range spec.RequiredParticipants {
		if err := controller.Acknowledge(Acknowledgement{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.ToEpoch, Phase: phase, Participant: participant, ObservedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
}

func acknowledgeTyped(t *testing.T, controller *Controller, group, participant string, at time.Time) {
	t.Helper()
	state, ok := controller.Group(group)
	if !ok {
		t.Fatalf("group %q not found", group)
	}
	requirement, ok := requirementFor(state.Spec, state.Phase, participant)
	if !ok {
		t.Fatalf("participant %q not required in %s", participant, state.Phase)
	}
	if err := controller.Acknowledge(Acknowledgement{
		CommandID: state.IssuedCommandIDs[state.Phase][participant], Namespace: state.Spec.Namespace,
		Group: state.Spec.Group, TransitionID: state.Spec.ID, Epoch: state.Spec.ToEpoch,
		Phase: state.Phase, Participant: participant, AuthenticatedParticipant: participant,
		Role: requirement.Role, ObservedAt: at,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBlockedGroupDoesNotBlockIndependentGroup(t *testing.T) {
	now := time.Unix(900, 0).UTC()
	publisher := newBlockingPublisher("Flight")
	path := filepath.Join(t.TempDir(), "state.json")
	controller := openTestController(t, path, &now, &fakeFence{}, publisher)
	for _, spec := range []TransitionSpec{testSpec("Flight", "flight-1"), testSpec("Baggage", "baggage-1")} {
		if _, err := controller.Begin(spec); err != nil {
			t.Fatal(err)
		}
	}

	flightDone := make(chan error, 1)
	go func() {
		_, err := controller.Reconcile(context.Background(), "Flight")
		flightDone <- err
	}()
	select {
	case <-publisher.started:
	case <-time.After(time.Second):
		t.Fatal("Flight publication did not block")
	}
	defer publisher.releaseBlocked()

	baggageDone := make(chan error, 1)
	go func() {
		_, err := controller.Reconcile(context.Background(), "Baggage")
		baggageDone <- err
	}()
	select {
	case err := <-baggageDone:
		if err != nil {
			t.Fatalf("Baggage reconcile failed while Flight was blocked: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Baggage reconcile was blocked by Flight publication")
	}
	state, _ := controller.Group("Baggage")
	if !state.PreparePublished {
		t.Fatal("Baggage publication was not persisted")
	}

	publisher.releaseBlocked()
	if err := <-flightDone; err != nil {
		t.Fatalf("Flight reconcile failed after release: %v", err)
	}
	restarted := openTestController(t, path, &now, &fakeFence{}, &fakePublisher{})
	for _, group := range []string{"Flight", "Baggage"} {
		state, ok := restarted.Group(group)
		if !ok || !state.PreparePublished {
			t.Fatalf("persisted %s state was lost: found=%v state=%#v", group, ok, state)
		}
	}
}

func TestSameGroupReconcileRemainsSerialized(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	publisher := newBlockingPublisher("Flight")
	controller := openTestController(t, filepath.Join(t.TempDir(), "state.json"), &now, &fakeFence{}, publisher)
	if _, err := controller.Begin(testSpec("Flight", "flight-1")); err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := controller.Reconcile(context.Background(), "Flight")
		firstDone <- err
	}()
	select {
	case <-publisher.started:
	case <-time.After(time.Second):
		t.Fatal("first Flight publication did not block")
	}
	defer publisher.releaseBlocked()

	secondDone := make(chan error, 1)
	go func() {
		_, err := controller.Reconcile(context.Background(), "Flight")
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("same-group reconcile returned before the first completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if calls, maximum := publisher.counts("Flight"); calls != 1 || maximum != 1 {
		t.Fatalf("concurrent Flight publications: calls=%d maximum=%d", calls, maximum)
	}

	publisher.releaseBlocked()
	if err := <-firstDone; err != nil {
		t.Fatalf("first Flight reconcile failed: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Flight reconcile failed: %v", err)
	}
	if calls, maximum := publisher.counts("Flight"); calls != 1 || maximum != 1 {
		t.Fatalf("same-group publication was not serialized: calls=%d maximum=%d", calls, maximum)
	}
}

func TestAcknowledgementDuringPublishUsesDurableCommandIntent(t *testing.T) {
	now := time.Unix(975, 0).UTC()
	publisher := &fakePublisher{}
	controller := openTestController(t, filepath.Join(t.TempDir(), "state.json"), &now, &fakeFence{}, publisher)
	spec := testSpec("commands", "commands-1")
	spec.Namespace = "acme"
	spec.RoleRequirements = map[Phase][]ParticipantRequirement{
		PhasePrepare:  {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhasePause:    {{Participant: "publisher-a", Role: control.RolePublisher}},
		PhaseDrain:    {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhaseActivate: {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
	}
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}
	publisher.hook = func(update ControlUpdate) error {
		commandID := update.IssuedCommandIDs["subscriber-a"]
		if commandID == "" {
			return errors.New("publish did not receive durable command intent")
		}
		return controller.Acknowledge(Acknowledgement{
			CommandID: commandID, Namespace: spec.Namespace, Group: spec.Group,
			TransitionID: spec.ID, Epoch: spec.ToEpoch, Phase: PhasePrepare,
			Participant: "subscriber-a", AuthenticatedParticipant: "subscriber-a",
			Role: control.RoleSubscriber, ObservedAt: now,
		})
	}
	changed, err := controller.Reconcile(context.Background(), spec.Group)
	if err != nil || !changed {
		t.Fatalf("reconcile with synchronous acknowledgement: changed=%v err=%v", changed, err)
	}
	state, _ := controller.Group(spec.Group)
	if !state.PreparePublished || state.Acknowledgements[PhasePrepare]["subscriber-a"] != now {
		t.Fatalf("publication and acknowledgement were not both persisted: %#v", state)
	}
}

func TestRestartAtCommandIntentBoundaryRetriesStableIDs(t *testing.T) {
	now := time.Unix(990, 0).UTC()
	path := filepath.Join(t.TempDir(), "state.json")
	publisher := &fakePublisher{hook: func(ControlUpdate) error { return errors.New("broker unavailable") }}
	controller := openTestController(t, path, &now, &fakeFence{}, publisher)
	spec := testSpec("intent", "intent-1")
	spec.Namespace = "acme"
	spec.RoleRequirements = map[Phase][]ParticipantRequirement{
		PhasePrepare:  {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhasePause:    {{Participant: "publisher-a", Role: control.RolePublisher}},
		PhaseDrain:    {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhaseActivate: {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
	}
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}
	if changed, err := controller.Reconcile(context.Background(), spec.Group); !changed || err == nil {
		t.Fatalf("failed publication did not report persisted intent: changed=%v err=%v", changed, err)
	}
	intent, _ := controller.Group(spec.Group)
	if intent.PreparePublished || !commandsIssued(&intent, PhasePrepare) {
		t.Fatalf("command intent boundary was not retained: %#v", intent)
	}
	firstIDs := maps.Clone(intent.IssuedCommandIDs[PhasePrepare])
	restartedAtIntent := openTestController(t, path, &now, &fakeFence{}, &fakePublisher{})
	if err := restartedAtIntent.Acknowledge(Acknowledgement{
		CommandID: firstIDs["subscriber-a"], Namespace: spec.Namespace, Group: spec.Group,
		TransitionID: spec.ID, Epoch: spec.ToEpoch, Phase: PhasePrepare,
		Participant: "subscriber-a", AuthenticatedParticipant: "subscriber-a",
		Role: control.RoleSubscriber, ObservedAt: now,
	}); err != nil {
		t.Fatalf("acknowledgement at durable intent boundary was rejected: %v", err)
	}

	retryPublisher := &fakePublisher{}
	restarted := openTestController(t, path, &now, &fakeFence{}, retryPublisher)
	reconcileChanged(t, restarted, spec.Group)
	if len(retryPublisher.updates) != 1 || !maps.Equal(retryPublisher.updates[0].IssuedCommandIDs, firstIDs) {
		t.Fatalf("restart changed issued command IDs: first=%v retry=%#v", firstIDs, retryPublisher.updates)
	}
}

func TestDrainRequiresCommonPostEntryZeroInterval(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	spec := testSpec("overlap", "overlap-1")
	spec.DrainGrace = 10 * time.Second
	spec.Current = []Broker{{ID: "a", Destination: "orders"}, {ID: "b", Destination: "orders"}}
	spec.CurrentResources = []control.EpochResourceIdentity{
		{Epoch: 1, BrokerID: "a", ConsumerSet: "default", QueueName: "a-e1", IngressTopic: "orders/e1/>"},
		{Epoch: 1, BrokerID: "b", ConsumerSet: "default", QueueName: "b-e1", IngressTopic: "orders/e1/>"},
	}
	state := &GroupState{
		Spec: spec, Phase: PhaseDrain, PhaseEnteredAt: now,
		LatestTelemetryBySource: map[string]Telemetry{
			"a\x00a-e1": {ObservedAt: now.Add(10 * time.Second), Queued: Count{Known: true}, Stored: Count{Known: true}, Unacked: Count{Known: true}},
			"b\x00b-e1": {ObservedAt: now.Add(19 * time.Second), Queued: Count{Known: true}, Stored: Count{Known: true}, Unacked: Count{Known: true}},
		},
		ZeroSinceBySource: map[string]time.Time{
			"a\x00a-e1": now,
			"b\x00b-e1": now.Add(9 * time.Second),
		},
	}
	controller := &Controller{options: Options{TelemetryFreshness: time.Minute, Now: func() time.Time { return now.Add(20 * time.Second) }}}
	if controller.drainComplete(state) {
		t.Fatal("per-source grace windows without a common overlap completed drain")
	}
	state.LatestTelemetryBySource["a\x00a-e1"] = Telemetry{ObservedAt: now.Add(19 * time.Second), Queued: Count{Known: true}, Stored: Count{Known: true}, Unacked: Count{Known: true}}
	if !controller.drainComplete(state) {
		t.Fatal("common zero interval did not complete drain")
	}
	state.LatestTelemetryBySource["a\x00a-e1"] = Telemetry{ObservedAt: now.Add(-time.Second), Queued: Count{Known: true}, Stored: Count{Known: true}, Unacked: Count{Known: true}}
	if controller.drainComplete(state) {
		t.Fatal("pre-DRAIN telemetry completed drain")
	}
}

func TestVerificationTimestampCapturedAfterExternalCall(t *testing.T) {
	t.Run("fence", func(t *testing.T) {
		now := time.Unix(1_000, 0).UTC()
		spec := testSpec("fence-time", "fence-time-1")
		store := &uncertainStore{state: PersistentState{Version: stateVersion, Groups: map[string]*GroupState{
			spec.Group: {Spec: spec, Phase: PhaseCommit, Fenced: true, Acknowledgements: make(map[Phase]map[string]time.Time)},
		}}}
		fence := &fakeFence{}
		fence.now = func() time.Time { return now }
		fence.beforeVerifyFence = func() { now = now.Add(time.Second) }
		controller, err := Open(store, fence, &fakePublisher{}, Options{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		reconcileChanged(t, controller, spec.Group)
	})

	t.Run("ingress", func(t *testing.T) {
		now := time.Unix(1_000, 0).UTC()
		spec := testSpec("ingress-time", "ingress-time-1")
		verifiedAt := now
		store := &uncertainStore{state: PersistentState{Version: stateVersion, Groups: map[string]*GroupState{
			spec.Group: {
				Spec: spec, Phase: PhaseActivate, Fenced: true, TargetIngressEnabled: true,
				FenceVerifiedAt: &verifiedAt, Acknowledgements: make(map[Phase]map[string]time.Time),
			},
		}}}
		fence := &fakeFence{}
		fence.now = func() time.Time { return now }
		fence.beforeVerifyIngress = func() { now = now.Add(time.Second) }
		controller, err := Open(store, fence, &fakePublisher{}, Options{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		reconcileChanged(t, controller, spec.Group)
		state, _ := controller.Group(spec.Group)
		if !state.TargetIngressVerified || state.TargetIngressVerifiedAt == nil || *state.TargetIngressVerifiedAt != now {
			t.Fatalf("target ingress verification was not persisted: %#v", state)
		}
	})
}

func TestTransitionPhaseBoundariesAndNoCommitWithUnacked(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	fence := &fakeFence{}
	publisher := &fakePublisher{}
	controller := openTestController(t, filepath.Join(t.TempDir(), "state.json"), &now, fence, publisher)
	spec := testSpec("payments", "move-7")
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}

	reconcileChanged(t, controller, spec.Group) // publish PREPARE
	changed, err := controller.Reconcile(context.Background(), spec.Group)
	if err != nil || changed {
		t.Fatalf("advanced without readiness: changed=%v err=%v", changed, err)
	}
	if err := controller.Acknowledge(Acknowledgement{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.ToEpoch, Phase: PhasePrepare, Participant: "sub-a", ObservedAt: now}); err != nil {
		t.Fatal(err)
	}
	changed, err = controller.Reconcile(context.Background(), spec.Group)
	if err != nil || changed {
		t.Fatalf("advanced without every required participant: changed=%v err=%v", changed, err)
	}
	if err := controller.Acknowledge(Acknowledgement{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.ToEpoch, Phase: PhasePrepare, Participant: "sub-b", ObservedAt: now}); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, controller, spec.Group) // -> PAUSE
	reconcileChanged(t, controller, spec.Group) // publish PAUSE
	acknowledgeAll(t, controller, spec, PhasePause, now)
	reconcileChanged(t, controller, spec.Group) // fence
	reconcileChanged(t, controller, spec.Group) // -> DRAIN
	reconcileChanged(t, controller, spec.Group) // publish DRAIN

	if err := controller.Observe(Telemetry{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.FromEpoch, ObservedAt: now, Queued: Count{Known: false}, Stored: Count{Known: false}, Unacked: Count{Known: false}}); err != nil {
		t.Fatal(err)
	}
	changed, err = controller.Reconcile(context.Background(), spec.Group)
	if err != nil || changed {
		t.Fatalf("unknown telemetry allowed commit: changed=%v err=%v", changed, err)
	}
	now = now.Add(time.Second)
	if err := controller.Observe(Telemetry{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.FromEpoch, ObservedAt: now, Queued: Count{Known: true, Value: 1}, Stored: Count{Known: true, Value: 1}, Unacked: Count{Known: true, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := controller.Observe(Telemetry{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.FromEpoch, ObservedAt: now, Queued: Count{Known: true}, Stored: Count{Known: true}, Unacked: Count{Known: true}}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	changed, err = controller.Reconcile(context.Background(), spec.Group)
	if err != nil || changed {
		t.Fatalf("zero shorter than grace allowed commit: changed=%v err=%v", changed, err)
	}
	now = now.Add(time.Second)
	if err := controller.Observe(Telemetry{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.FromEpoch, ObservedAt: now, Queued: Count{Known: true}, Stored: Count{Known: true}, Unacked: Count{Known: true}}); err != nil {
		t.Fatal(err)
	}
	acknowledgeAll(t, controller, spec, PhaseDrain, now)
	reconcileChanged(t, controller, spec.Group) // persist COMMIT point
	state, _ := controller.Group(spec.Group)
	if state.Phase != PhaseCommit || state.CommitPublished {
		t.Fatalf("unexpected commit boundary: %#v", state)
	}
	reconcileChanged(t, controller, spec.Group) // publish COMMIT
	reconcileChanged(t, controller, spec.Group) // -> ACTIVATE
	reconcileChanged(t, controller, spec.Group) // enable target ingress
	reconcileChanged(t, controller, spec.Group) // persist target ingress verification
	reconcileChanged(t, controller, spec.Group) // freshly verify and publish ACTIVATE
	acknowledgeAll(t, controller, spec, PhaseActivate, now)
	reconcileChanged(t, controller, spec.Group) // complete
	state, _ = controller.Group(spec.Group)
	if !state.Completed {
		t.Fatal("transition did not complete")
	}
}

func TestRestartReplaysUnrecordedSideEffectWithStableOperationID(t *testing.T) {
	now := time.Unix(2_000, 0).UTC()
	fence := &fakeFence{}
	publisher := &fakePublisher{}
	store := &uncertainStore{state: PersistentState{Version: stateVersion, Groups: make(map[string]*GroupState)}}
	controller, err := Open(store, fence, publisher, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	spec := testSpec("orders", "move-8")
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}
	publisher.hook = func(ControlUpdate) error {
		store.fail = true
		return nil
	}
	changed, err := controller.Reconcile(context.Background(), spec.Group)
	if !changed || err == nil {
		t.Fatalf("expected external publish followed by failed persistence, changed=%v err=%v", changed, err)
	}
	if len(publisher.updates) != 1 {
		t.Fatalf("publish count = %d", len(publisher.updates))
	}

	store.fail = false
	publisher.hook = nil
	restarted, err := Open(store, fence, publisher, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, restarted, spec.Group)
	if len(publisher.updates) != 2 || publisher.updates[0].OperationID != publisher.updates[1].OperationID {
		t.Fatalf("recovery did not replay stable operation: %#v", publisher.updates)
	}
}

func TestStaleDuplicateMessagesAndGroupIsolation(t *testing.T) {
	now := time.Unix(3_000, 0).UTC()
	controller := openTestController(t, filepath.Join(t.TempDir(), "state.json"), &now, &fakeFence{}, &fakePublisher{})
	alpha := testSpec("alpha", "a-1")
	beta := testSpec("beta", "b-1")
	for _, spec := range []TransitionSpec{alpha, beta} {
		if _, err := controller.Begin(spec); err != nil {
			t.Fatal(err)
		}
		reconcileChanged(t, controller, spec.Group)
	}
	ack := Acknowledgement{Group: alpha.Group, TransitionID: alpha.ID, Epoch: alpha.ToEpoch, Phase: PhasePrepare, Participant: "sub-a", ObservedAt: now}
	if err := controller.Acknowledge(ack); err != nil {
		t.Fatal(err)
	}
	if err := controller.Acknowledge(ack); err != nil {
		t.Fatalf("duplicate acknowledgement was not idempotent: %v", err)
	}
	stale := ack
	stale.TransitionID = "old"
	if err := controller.Acknowledge(stale); !errors.Is(err, ErrStaleMessage) {
		t.Fatalf("stale acknowledgement error = %v", err)
	}
	unknown := ack
	unknown.Participant = "intruder"
	if err := controller.Acknowledge(unknown); !errors.Is(err, ErrUnknownParticipant) {
		t.Fatalf("unknown participant error = %v", err)
	}
	ack.Participant = "sub-b"
	if err := controller.Acknowledge(ack); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, controller, alpha.Group)
	alphaState, _ := controller.Group(alpha.Group)
	betaState, _ := controller.Group(beta.Group)
	if alphaState.Phase != PhasePause || betaState.Phase != PhasePrepare {
		t.Fatalf("groups were not isolated: alpha=%s beta=%s", alphaState.Phase, betaState.Phase)
	}
}

func TestRollbackPolicyAndForwardOnlyAfterCommit(t *testing.T) {
	now := time.Unix(4_000, 0).UTC()
	controller := openTestController(t, filepath.Join(t.TempDir(), "state.json"), &now, &fakeFence{}, &fakePublisher{})
	spec := testSpec("audit", "x-1")
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}
	if err := controller.RequestRollback(spec.Group, RollbackProof{TransitionID: spec.ID}); !errors.Is(err, ErrRollbackUnproven) {
		t.Fatalf("unproven rollback error = %v", err)
	}
	proof := RollbackProof{TransitionID: spec.ID, SourceMembershipIntact: true, ProposedNotActivated: true, FenceReversible: true}
	if err := controller.RequestRollback(spec.Group, proof); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, controller, spec.Group) // publish rollback
	reconcileChanged(t, controller, spec.Group) // complete (never fenced)
	state, _ := controller.Group(spec.Group)
	if !state.Completed || !state.RollbackPublished {
		t.Fatalf("rollback incomplete: %#v", state)
	}

	// Construct a persisted commit boundary to assert recovery is forward-only.
	commitSpec := testSpec("ledger", "x-2")
	commitStore := &uncertainStore{state: PersistentState{Version: stateVersion, Groups: map[string]*GroupState{
		commitSpec.Group: {Spec: commitSpec, Phase: PhaseCommit, Acknowledgements: make(map[Phase]map[string]time.Time)},
	}}}
	commitFence := &fakeFence{now: func() time.Time { return now }}
	forward, err := Open(commitStore, commitFence, &fakePublisher{}, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := forward.RequestRollback(commitSpec.Group, RollbackProof{TransitionID: commitSpec.ID, SourceMembershipIntact: true, ProposedNotActivated: true, FenceReversible: true}); !errors.Is(err, ErrCommitReached) {
		t.Fatalf("post-commit rollback error = %v", err)
	}
	reconcileChanged(t, forward, commitSpec.Group)
	state, _ = forward.Group(commitSpec.Group)
	if !state.CommitPublished || state.RollingBack {
		t.Fatalf("restart did not continue forward: %#v", state)
	}
}

func TestBeginSnapshotPreservesAuthoritativeMembershipOrder(t *testing.T) {
	now := time.Unix(4_500, 0).UTC()
	publisher := &fakePublisher{}
	controller := openTestController(t, filepath.Join(t.TempDir(), "state.json"), &now, &fakeFence{}, publisher)
	snapshot := control.MembershipSnapshot{
		Version:            control.SnapshotVersion,
		ScalingGroup:       "ordered",
		Revision:           20,
		Epoch:              7,
		Phase:              control.PhasePrepare,
		HashContract:       "ordered-v1",
		Algorithm:          control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership:  control.Membership{"z", "a"},
		ProposedMembership: control.Membership{"m", "b"},
		Transition:         &control.Transition{ID: "order-1", FromEpoch: 7, ToEpoch: 8},
		Queue:              control.QueueInfo{Name: "ordered-queue", Durable: true},
		Destination:        control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders"},
	}
	state, err := controller.BeginSnapshot(snapshot, []string{"subscriber"})
	if err != nil {
		t.Fatal(err)
	}
	if state.Spec.Current[0].ID != "z" || state.Spec.Current[1].ID != "a" || state.Spec.Proposed[0].ID != "m" || state.Spec.Proposed[1].ID != "b" {
		t.Fatalf("membership order changed: current=%#v proposed=%#v", state.Spec.Current, state.Spec.Proposed)
	}
	reconcileChanged(t, controller, snapshot.ScalingGroup)
	if err := publisher.updates[0].Snapshot.Validate(); err != nil {
		t.Fatalf("published shared snapshot is invalid: %v", err)
	}
	if !publisher.updates[0].Snapshot.CurrentMembership.Equal(snapshot.CurrentMembership) || !publisher.updates[0].Snapshot.ProposedMembership.Equal(snapshot.ProposedMembership) {
		t.Fatalf("published membership order changed: %#v", publisher.updates[0].Snapshot)
	}
}

func TestGroupSpecificSafetyWindows(t *testing.T) {
	now := time.Unix(4_700, 0).UTC()
	controller, err := Open(
		&uncertainStore{state: PersistentState{Version: stateVersion, Groups: map[string]*GroupState{}}},
		&fakeFence{}, &fakePublisher{},
		Options{
			Now: func() time.Time { return now },
			PhaseTimeoutByGroup: map[string]time.Duration{
				"Flight":  time.Minute,
				"Baggage": 2 * time.Minute,
			},
			TelemetryFreshness: time.Minute,
			TelemetryFreshnessByGroup: map[string]time.Duration{
				"Flight":  5 * time.Second,
				"Baggage": 15 * time.Second,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for group, timeout := range map[string]time.Duration{"Flight": time.Minute, "Baggage": 2 * time.Minute} {
		state, err := controller.Begin(testSpec(group, group+"-1"))
		if err != nil {
			t.Fatal(err)
		}
		if got := state.PhaseDeadline.Sub(state.PhaseEnteredAt); got != timeout {
			t.Fatalf("%s phase timeout = %v, want %v", group, got, timeout)
		}
	}
	zeroAt := now.Add(-10 * time.Second)
	zero := Telemetry{ObservedAt: zeroAt, Queued: Count{Known: true}, Stored: Count{Known: true}, Unacked: Count{Known: true}}
	for group, wantComplete := range map[string]bool{"Flight": false, "Baggage": true} {
		state := &GroupState{Spec: testSpec(group, group+"-drain"), Phase: PhaseDrain, PhaseEnteredAt: zeroAt.Add(-time.Second), LatestTelemetry: &zero, ZeroSince: &zeroAt}
		state.Spec.DrainGrace = 0
		if got := controller.drainComplete(state); got != wantComplete {
			t.Fatalf("%s drainComplete = %v, want %v", group, got, wantComplete)
		}
	}
}

func TestPhaseSpecificParticipantsAndDeadline(t *testing.T) {
	now := time.Unix(4_750, 0).UTC()
	controller := openTestController(t, filepath.Join(t.TempDir(), "state.json"), &now, &fakeFence{}, &fakePublisher{})
	spec := testSpec("phase-specific", "p-1")
	spec.ParticipantRequirements = map[Phase][]string{
		PhasePrepare: {"subscriber"}, PhasePause: {"publisher"}, PhaseDrain: {"subscriber"}, PhaseActivate: {"publisher", "subscriber"},
	}
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, controller, spec.Group)
	if err := controller.Acknowledge(Acknowledgement{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.ToEpoch, Phase: PhasePrepare, Participant: "publisher", ObservedAt: now}); !errors.Is(err, ErrUnknownParticipant) {
		t.Fatalf("wrong phase participant error = %v", err)
	}
	if err := controller.Acknowledge(Acknowledgement{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.ToEpoch, Phase: PhasePrepare, Participant: "subscriber", ObservedAt: now}); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, controller, spec.Group)
	now = now.Add(6 * time.Minute)
	if changed, err := controller.Reconcile(context.Background(), spec.Group); changed || !errors.Is(err, ErrPhaseDeadline) {
		t.Fatalf("expired phase reconcile = %v, %v", changed, err)
	}
}

func TestRollbackUnfencesBeforeActivePublication(t *testing.T) {
	now := time.Unix(4_800, 0).UTC()
	fence := &fakeFence{}
	publisher := &fakePublisher{}
	store := &uncertainStore{state: PersistentState{Version: stateVersion, Groups: map[string]*GroupState{}}}
	controller, err := Open(store, fence, publisher, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	spec := testSpec("rollback-order", "r-1")
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}
	controller.state.Groups[spec.Group].Fenced = true
	proof := RollbackProof{TransitionID: spec.ID, SourceMembershipIntact: true, ProposedNotActivated: true, FenceReversible: true}
	if err := controller.RequestRollback(spec.Group, proof); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, controller, spec.Group)
	if len(fence.unfences) != 1 || len(publisher.updates) != 0 {
		t.Fatalf("first rollback action: unfences=%d publishes=%d", len(fence.unfences), len(publisher.updates))
	}
	reconcileChanged(t, controller, spec.Group)
	if len(publisher.updates) != 1 || !publisher.updates[0].Rollback {
		t.Fatalf("rollback ACTIVE was not published after unfence: %#v", publisher.updates)
	}
}

func TestOpenRejectsInvalidPersistedInvariant(t *testing.T) {
	now := time.Unix(4_900, 0).UTC()
	spec := testSpec("invalid", "i-1")
	store := &uncertainStore{state: PersistentState{Version: stateVersion, Groups: map[string]*GroupState{
		spec.Group: {Spec: spec, Phase: PhaseDrain, Acknowledgements: make(map[Phase]map[string]time.Time), DrainPublished: true},
	}}}
	if _, err := Open(store, &fakeFence{}, &fakePublisher{}, Options{Now: func() time.Time { return now }}); err == nil {
		t.Fatal("invalid persisted state accepted")
	}
}

func TestVerifiedAcknowledgementScopeAndStableCommandID(t *testing.T) {
	now := time.Unix(4_950, 0).UTC()
	path := filepath.Join(t.TempDir(), "state.json")
	controller := openTestController(t, path, &now, &fakeFence{}, &fakePublisher{})
	spec := testSpec("verified", "verified-1")
	spec.Namespace = "acme"
	spec.RoleRequirements = map[Phase][]ParticipantRequirement{
		PhasePrepare:  {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhasePause:    {{Participant: "publisher-a", Role: control.RolePublisher}},
		PhaseDrain:    {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhaseActivate: {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
	}
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}
	if err := controller.Acknowledge(Acknowledgement{Group: spec.Group, TransitionID: spec.ID, Epoch: spec.ToEpoch, Phase: PhasePrepare, Participant: "subscriber-a", ObservedAt: now}); !errors.Is(err, ErrStaleMessage) {
		t.Fatalf("ack before command publication error = %v", err)
	}
	reconcileChanged(t, controller, spec.Group)
	state, _ := controller.Group(spec.Group)
	commandID := state.IssuedCommandIDs[PhasePrepare]["subscriber-a"]
	if commandID == "" {
		t.Fatal("published command ID was not persisted")
	}
	valid := Acknowledgement{CommandID: commandID, Namespace: spec.Namespace, Group: spec.Group, TransitionID: spec.ID, Epoch: spec.ToEpoch, Phase: PhasePrepare, Participant: "subscriber-a", AuthenticatedParticipant: "subscriber-a", Role: control.RoleSubscriber, ObservedAt: now}
	for name, mutate := range map[string]func(*Acknowledgement){
		"command":                   func(ack *Acknowledgement) { ack.CommandID = "wrong" },
		"namespace":                 func(ack *Acknowledgement) { ack.Namespace = "other" },
		"authenticated participant": func(ack *Acknowledgement) { ack.AuthenticatedParticipant = "intruder" },
		"role":                      func(ack *Acknowledgement) { ack.Role = control.RolePublisher },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := valid
			mutate(&invalid)
			if err := controller.Acknowledge(invalid); !errors.Is(err, ErrStaleMessage) {
				t.Fatalf("scope mismatch error = %v", err)
			}
		})
	}
	if err := controller.Acknowledge(valid); err != nil {
		t.Fatal(err)
	}
	restarted := openTestController(t, path, &now, &fakeFence{}, &fakePublisher{})
	restartedState, _ := restarted.Group(spec.Group)
	if restartedState.IssuedCommandIDs[PhasePrepare]["subscriber-a"] != commandID {
		t.Fatal("command ID changed across restart")
	}
	if err := restarted.Acknowledge(valid); err != nil {
		t.Fatalf("exact acknowledgement replay failed: %v", err)
	}
}

func TestTypedDrainRequiresFreshPhaseSpecificAcknowledgement(t *testing.T) {
	now := time.Unix(4_960, 0).UTC()
	controller := openTestController(t, filepath.Join(t.TempDir(), "state.json"), &now, &fakeFence{}, &fakePublisher{})
	spec := testSpec("drain-ack", "drain-ack-1")
	spec.Namespace = "acme"
	spec.RoleRequirements = map[Phase][]ParticipantRequirement{
		PhasePrepare:  {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhasePause:    {{Participant: "publisher-a", Role: control.RolePublisher}},
		PhaseDrain:    {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhaseActivate: {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
	}
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, controller, spec.Group) // publish PREPARE
	acknowledgeTyped(t, controller, spec.Group, "subscriber-a", now)
	reconcileChanged(t, controller, spec.Group) // -> PAUSE
	reconcileChanged(t, controller, spec.Group) // publish PAUSE
	acknowledgeTyped(t, controller, spec.Group, "publisher-a", now)
	reconcileChanged(t, controller, spec.Group) // fence
	reconcileChanged(t, controller, spec.Group) // -> DRAIN
	reconcileChanged(t, controller, spec.Group) // publish DRAIN

	zero := func(message string, at time.Time) Telemetry {
		return Telemetry{
			MessageID: message, Namespace: spec.Namespace, Group: spec.Group,
			TransitionID: spec.ID, Epoch: spec.FromEpoch, Participant: "monitor",
			Role: control.RoleObserver, SourceBroker: "old", SourceQueue: spec.Queue.Name,
			ObservedAt: at, Queued: Count{Known: true}, Stored: Count{Known: true}, Unacked: Count{Known: true},
		}
	}
	if err := controller.Observe(zero("drain-zero-1", now)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(3 * time.Second)
	if err := controller.Observe(zero("drain-zero-2", now)); err != nil {
		t.Fatal(err)
	}
	if changed, err := controller.Reconcile(context.Background(), spec.Group); changed || !errors.Is(err, ErrReadinessStale) {
		t.Fatalf("fresh PREPARE acknowledgement satisfied typed DRAIN: changed=%v err=%v", changed, err)
	}

	acknowledgeTyped(t, controller, spec.Group, "subscriber-a", now.Add(-time.Minute))
	if changed, err := controller.Reconcile(context.Background(), spec.Group); changed || !errors.Is(err, ErrReadinessStale) {
		t.Fatalf("stale DRAIN acknowledgement allowed commit: changed=%v err=%v", changed, err)
	}

	acknowledgeTyped(t, controller, spec.Group, "subscriber-a", now)
	reconcileChanged(t, controller, spec.Group)
	state, _ := controller.Group(spec.Group)
	if state.Phase != PhaseCommit {
		t.Fatalf("phase = %s, want COMMIT after fresh DRAIN acknowledgement", state.Phase)
	}
}

func TestDrainRequiresEverySourceAndTelemetryReplayIsIdempotent(t *testing.T) {
	now := time.Unix(4_975, 0).UTC()
	path := filepath.Join(t.TempDir(), "state.json")
	controller := openTestController(t, path, &now, &fakeFence{}, &fakePublisher{})
	spec := testSpec("sources", "sources-1")
	spec.Namespace = "acme"
	spec.Current = []Broker{{ID: "broker-a", Destination: "orders"}, {ID: "broker-b", Destination: "orders"}}
	spec.CurrentResources = []control.EpochResourceIdentity{
		{Epoch: spec.FromEpoch, BrokerID: "broker-a", ConsumerSet: "default", QueueName: "orders-a-e1", IngressTopic: "orders/e1/>"},
		{Epoch: spec.FromEpoch, BrokerID: "broker-b", ConsumerSet: "default", QueueName: "orders-b-e1", IngressTopic: "orders/e1/>"},
	}
	spec.RoleRequirements = map[Phase][]ParticipantRequirement{
		PhasePrepare:  {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhasePause:    {{Participant: "publisher-a", Role: control.RolePublisher}},
		PhaseDrain:    {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
		PhaseActivate: {{Participant: "subscriber-a", Role: control.RoleSubscriber}},
	}
	if _, err := controller.Begin(spec); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, controller, spec.Group)
	acknowledgeTyped(t, controller, spec.Group, "subscriber-a", now)
	reconcileChanged(t, controller, spec.Group)
	reconcileChanged(t, controller, spec.Group)
	acknowledgeTyped(t, controller, spec.Group, "publisher-a", now)
	reconcileChanged(t, controller, spec.Group)
	reconcileChanged(t, controller, spec.Group)
	reconcileChanged(t, controller, spec.Group)
	acknowledgeTyped(t, controller, spec.Group, "subscriber-a", now)

	sample := func(message, broker, queue string, at time.Time) Telemetry {
		return Telemetry{MessageID: message, Namespace: spec.Namespace, Group: spec.Group, TransitionID: spec.ID, Epoch: spec.FromEpoch, Participant: "monitor", Role: control.RoleObserver, SourceBroker: broker, SourceQueue: queue, ObservedAt: at, Queued: Count{Known: true}, Stored: Count{Known: true}, Unacked: Count{Known: true}}
	}
	firstA := sample("telemetry-a-1", "broker-a", "orders-a-e1", now)
	if err := controller.Observe(firstA); err != nil {
		t.Fatal(err)
	}
	if changed, err := controller.Reconcile(context.Background(), spec.Group); err != nil || changed {
		t.Fatalf("drain completed without broker-b: changed=%v err=%v", changed, err)
	}
	firstB := sample("telemetry-b-1", "broker-b", "orders-b-e1", now)
	if err := controller.Observe(firstB); err != nil {
		t.Fatal(err)
	}
	restarted := openTestController(t, path, &now, &fakeFence{}, &fakePublisher{})
	if err := restarted.Observe(firstA); err != nil {
		t.Fatalf("exact telemetry replay after restart failed: %v", err)
	}
	different := firstA
	different.Queued.Value = 1
	if err := restarted.Observe(different); !errors.Is(err, ErrStaleMessage) {
		t.Fatalf("same message ID with different sample error = %v", err)
	}
	stale := sample("telemetry-a-stale", "broker-a", "orders-a-e1", now.Add(-time.Second))
	if err := restarted.Observe(stale); !errors.Is(err, ErrStaleMessage) {
		t.Fatalf("stale differing sample error = %v", err)
	}
	persisted, _ := restarted.Group(spec.Group)
	now = now.Add(persisted.Spec.DrainGrace)
	secondA := sample("telemetry-a-2", "broker-a", "orders-a-e1", now)
	if err := restarted.Observe(secondA); err != nil {
		t.Fatal(err)
	}
	if changed, err := restarted.Reconcile(context.Background(), spec.Group); err != nil || changed {
		t.Fatalf("drain completed without a continuous broker-b sample: changed=%v err=%v", changed, err)
	}
	secondB := sample("telemetry-b-2", "broker-b", "orders-b-e1", now)
	if err := restarted.Observe(secondB); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, restarted, spec.Group)
	state, _ := restarted.Group(spec.Group)
	if state.Phase != PhaseCommit {
		t.Fatalf("phase = %s, want COMMIT", state.Phase)
	}
	evidence := restarted.Snapshot().CleanupEvidence[spec.Group]
	if len(evidence) != 1 || len(evidence[0].Sources) != 2 {
		t.Fatalf("cleanup source evidence = %#v", evidence)
	}
	copy := restarted.Snapshot()
	copy.Groups[spec.Group].LatestTelemetryBySource[firstA.SourceBroker+"\x00"+firstA.SourceQueue] = different
	copy.CleanupEvidence[spec.Group][0].Sources[0].Broker = "mutated"
	original := restarted.Snapshot()
	if original.Groups[spec.Group].LatestTelemetryBySource[firstA.SourceBroker+"\x00"+firstA.SourceQueue].Queued.Value != 0 || original.CleanupEvidence[spec.Group][0].Sources[0].Broker == "mutated" {
		t.Fatal("snapshot shared mutable telemetry or cleanup source storage")
	}
}

func TestJSONStoreRestartAtPhaseBoundary(t *testing.T) {
	now := time.Unix(5_000, 0).UTC()
	path := filepath.Join(t.TempDir(), "nested", "controller.json")
	fence := &fakeFence{}
	publisher := &fakePublisher{}
	first := openTestController(t, path, &now, fence, publisher)
	spec := testSpec("shipping", "s-1")
	if _, err := first.Begin(spec); err != nil {
		t.Fatal(err)
	}
	reconcileChanged(t, first, spec.Group)
	acknowledgeAll(t, first, spec, PhasePrepare, now)
	reconcileChanged(t, first, spec.Group) // exact PAUSE boundary

	restarted := openTestController(t, path, &now, fence, publisher)
	state, ok := restarted.Group(spec.Group)
	if !ok || state.Phase != PhasePause || state.Fenced {
		t.Fatalf("restart state = %#v", state)
	}
	reconcileChanged(t, restarted, spec.Group) // publish PAUSE after restart
	acknowledgeAll(t, restarted, spec, PhasePause, now)
	reconcileChanged(t, restarted, spec.Group) // fence only after publisher pause acknowledgement
	if len(fence.fences) != 1 || fence.fences[0].TransitionID != spec.ID {
		t.Fatalf("fence reconciliation = %#v", fence.fences)
	}
}
