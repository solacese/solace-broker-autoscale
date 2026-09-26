package policy

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

var testBase = time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

func TestMetricValidationRejectsUnknownValueAndInvalidKnownValues(t *testing.T) {
	t.Parallel()
	cases := []Metric{
		{Value: 1},
		KnownMetric(-1),
		KnownMetric(math.NaN()),
		KnownMetric(math.Inf(1)),
	}
	for _, metric := range cases {
		if err := metric.validate("test"); err == nil {
			t.Fatalf("Metric %#v unexpectedly valid", metric)
		}
	}
	if err := KnownMetric(0).validate("test"); err != nil {
		t.Fatalf("measured zero rejected: %v", err)
	}
}

func TestProfileCatalogRequiresExactClassAndVersion(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	if _, err := catalog.Lookup("class-a", "10.4.1"); err != nil {
		t.Fatalf("exact lookup: %v", err)
	}
	for _, key := range [][2]string{{"class-a", "10.4.2"}, {"class-b", "10.4.1"}, {"", "10.4.1"}} {
		if _, err := catalog.Lookup(key[0], key[1]); !errors.Is(err, ErrProfileNotFound) {
			t.Fatalf("Lookup(%q,%q) error = %v, want ErrProfileNotFound", key[0], key[1], err)
		}
	}
}

func TestProfileCatalogRejectsDuplicateAndInvalidProfiles(t *testing.T) {
	t.Parallel()
	valid := testProfile("class-a", "10.4.1", 100)
	if _, err := NewProfileCatalog([]CapacityProfile{valid, valid}); err == nil {
		t.Fatal("duplicate profile accepted")
	}
	invalid := valid
	invalid.Limits.SpoolBytes = 0
	if _, err := NewProfileCatalog([]CapacityProfile{invalid}); err == nil {
		t.Fatal("zero profile limit accepted")
	}
}

func TestAggregateSharedBrokerPressureAffectsEveryMemberGroup(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight", "baggage")}
	groups := []GroupState{
		testGroup("baggage", []string{"broker-a"}),
		testGroup("flight", []string{"broker-a"}),
	}
	snapshot := testSnapshot(testBase,
		[]BrokerSample{testBrokerSample("broker-a", testBase, 10)},
		[]GroupSample{
			testGroupSample("flight", "broker-a", testBase, 60, 0, 0),
			testGroupSample("baggage", "broker-a", testBase, 20, 0, 0),
		})

	got, err := Aggregate(testBase, snapshot, catalog, inventory, groups)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Brokers) != 1 || got.Brokers[0].GroupedUtilization.Ingress != .8 || got.Brokers[0].SharedUtilization.Ingress != .8 {
		t.Fatalf("unexpected broker aggregation: %#v", got.Brokers)
	}
	for _, group := range got.Groups {
		if group.Pressure != PressureBrokerSaturation {
			t.Fatalf("group %q pressure = %q, want saturation from shared broker", group.GroupID, group.Pressure)
		}
		if group.SharedUtilization.Ingress != .8 {
			t.Fatalf("group %q shared ingress = %v", group.GroupID, group.SharedUtilization.Ingress)
		}
	}
}

func TestAggregateUsesWholeBrokerWhenLargerThanGroupSum(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight")}
	groups := []GroupState{testGroup("flight", []string{"broker-a"})}
	snapshot := testSnapshot(testBase,
		[]BrokerSample{testBrokerSample("broker-a", testBase, 90)},
		[]GroupSample{testGroupSample("flight", "broker-a", testBase, 10, 0, 0)})

	got, err := Aggregate(testBase, snapshot, catalog, inventory, groups)
	if err != nil {
		t.Fatal(err)
	}
	if got.Brokers[0].SharedUtilization.Ingress != .9 {
		t.Fatalf("shared utilization = %v, want .9", got.Brokers[0].SharedUtilization.Ingress)
	}
	if got.Groups[0].Pressure != PressureBrokerSaturation {
		t.Fatalf("pressure = %q", got.Groups[0].Pressure)
	}
}

func TestAggregateDistinguishesDownstreamBacklog(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight")}
	groups := []GroupState{testGroup("flight", []string{"broker-a"})}
	snapshot := testSnapshot(testBase,
		[]BrokerSample{testBrokerSample("broker-a", testBase, 10)},
		[]GroupSample{testGroupSample("flight", "broker-a", testBase, 10, 500, 4)})

	got, err := Aggregate(testBase, snapshot, catalog, inventory, groups)
	if err != nil {
		t.Fatal(err)
	}
	group := got.Groups[0]
	if group.Pressure != PressureDownstreamBacklog || group.QueuedMessages != 500 || group.UnackedMessages != 4 {
		t.Fatalf("unexpected group assessment: %#v", group)
	}
}

func TestAggregateUnknownIsNotZero(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight")}
	groups := []GroupState{testGroup("flight", []string{"broker-a"})}
	snapshot := testSnapshot(testBase,
		[]BrokerSample{testBrokerSample("broker-a", testBase, 10)},
		[]GroupSample{testGroupSample("flight", "broker-a", testBase, 10, 0, 0)})
	snapshot.Groups[0].Backlog.QueuedMessages = UnknownMetric()

	got, err := Aggregate(testBase, snapshot, catalog, inventory, groups)
	if err != nil {
		t.Fatal(err)
	}
	if got.Groups[0].State != TelemetryUnknown || got.Groups[0].Pressure != PressureUnknown {
		t.Fatalf("assessment = %#v", got.Groups[0])
	}
	if !strings.Contains(got.Groups[0].Reason, "unknown backlog") {
		t.Fatalf("reason = %q", got.Groups[0].Reason)
	}
}

func TestAggregateMissingSharedGroupSampleMakesBrokerAndGroupsUnknown(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight", "baggage")}
	groups := []GroupState{
		testGroup("flight", []string{"broker-a"}),
		testGroup("baggage", []string{"broker-a"}),
	}
	snapshot := testSnapshot(testBase,
		[]BrokerSample{testBrokerSample("broker-a", testBase, 20)},
		[]GroupSample{testGroupSample("flight", "broker-a", testBase, 20, 0, 0)})

	got, err := Aggregate(testBase, snapshot, catalog, inventory, groups)
	if err != nil {
		t.Fatal(err)
	}
	if got.Brokers[0].State != TelemetryUnknown {
		t.Fatalf("broker state = %q", got.Brokers[0].State)
	}
	for _, group := range got.Groups {
		if group.State != TelemetryUnknown {
			t.Fatalf("group %q state = %q", group.GroupID, group.State)
		}
	}
}

func TestAggregateStaleTakesPrecedenceOverUnknown(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight")}
	groups := []GroupState{testGroup("flight", []string{"broker-a"})}
	snapshot := testSnapshot(testBase,
		[]BrokerSample{testBrokerSample("broker-a", testBase.Add(-2*time.Minute), 10)}, nil)

	got, err := Aggregate(testBase, snapshot, catalog, inventory, groups)
	if err != nil {
		t.Fatal(err)
	}
	if got.Brokers[0].State != TelemetryStale || got.Groups[0].State != TelemetryStale {
		t.Fatalf("states = broker %q group %q", got.Brokers[0].State, got.Groups[0].State)
	}
}

func TestAggregateRejectsProfileIdentityMismatch(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight")}
	groups := []GroupState{testGroup("flight", []string{"broker-a"})}
	sample := testBrokerSample("broker-a", testBase, 10)
	sample.BrokerVersion = "10.4.2"
	_, err := Aggregate(testBase, testSnapshot(testBase, []BrokerSample{sample}, nil), catalog, inventory, groups)
	if err == nil || !strings.Contains(err.Error(), "does not match inventory") {
		t.Fatalf("error = %v", err)
	}
}

func TestAggregateCalculatesWarmShortfall(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	inventory := []Broker{
		testBroker("broker-a", "flight"),
		testBroker("broker-b", "flight"),
	}
	inventory[1].Ready = false
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.WarmBrokers = 1
	group.Policy.MaximumBrokers = 3
	got, err := Aggregate(testBase,
		testSnapshot(testBase, []BrokerSample{testBrokerSample("broker-a", testBase, 10)}, []GroupSample{testGroupSample("flight", "broker-a", testBase, 10, 0, 0)}),
		catalog, inventory, []GroupState{group})
	if err != nil {
		t.Fatal(err)
	}
	if got.Groups[0].ReadyWarmBrokers != 0 || got.Groups[0].WarmBrokerShortfall != 1 {
		t.Fatalf("warm fields = %#v", got.Groups[0])
	}
}

func TestEngineRequiresContinuousSustainedPressure(t *testing.T) {
	catalog := mustCatalog(t)
	engine := mustEngine(t, catalog, 1)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MinimumBrokers = 1
	group.Policy.MaximumBrokers = 2
	group.Policy.WarmBrokers = 0

	first := testBase
	decision := evaluate(t, engine, first, inventory, []GroupState{group}, 80, 0)
	assertNoAction(t, decision, "flight", "not been sustained")
	decision = evaluate(t, engine, first.Add(4*time.Minute), inventory, []GroupState{group}, 80, 0)
	assertNoAction(t, decision, "flight", "not been sustained")
	decision = evaluate(t, engine, first.Add(5*time.Minute), inventory, []GroupState{group}, 80, 0)
	assertAction(t, decision, "flight", ActionScaleOut, "broker-b", []string{"broker-a", "broker-b"})
}

func TestEngineStartsPressureWindowAtFirstEvaluation(t *testing.T) {
	catalog := mustCatalog(t)
	engine := mustEngine(t, catalog, 1)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MaximumBrokers = 2
	group.Policy.TelemetryMaxAge = 10 * time.Minute

	first := testBase
	oldButFresh := cycleSnapshot(first, []GroupState{group}, 80, 0)
	oldButFresh.CapturedAt = first.Add(-group.Policy.PressureWindow)
	oldButFresh.Brokers[0].ObservedAt = oldButFresh.CapturedAt
	oldButFresh.Groups[0].ObservedAt = oldButFresh.CapturedAt
	decision, err := engine.Evaluate(first, oldButFresh, inventory, []GroupState{group})
	if err != nil {
		t.Fatal(err)
	}
	assertNoAction(t, decision, "flight", "not been sustained")
	if got := engine.State().PressureSince["flight"]; !got.Equal(first) {
		t.Fatalf("pressure since = %v, want first evaluation %v", got, first)
	}
}

func TestEngineReplayedSamplesCannotSatisfyPressureWindow(t *testing.T) {
	catalog := mustCatalog(t)
	engine := mustEngine(t, catalog, 1)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MaximumBrokers = 2
	group.Policy.TelemetryMaxAge = 10 * time.Minute

	first := testBase
	replayed := cycleSnapshot(first, []GroupState{group}, 80, 0)
	for _, at := range []time.Time{first, first.Add(5 * time.Minute), first.Add(6 * time.Minute)} {
		replayed.CapturedAt = at
		decision, err := engine.Evaluate(at, replayed, inventory, []GroupState{group})
		if err != nil {
			t.Fatal(err)
		}
		assertNoAction(t, decision, "flight", "not been sustained")
	}
	if _, exists := engine.State().PressureSince["flight"]; exists {
		t.Fatal("replayed sample started a pressure window")
	}

	freshAt := first.Add(7 * time.Minute)
	decision := evaluate(t, engine, freshAt, inventory, []GroupState{group}, 80, 0)
	assertNoAction(t, decision, "flight", "not been sustained")
	decision = evaluate(t, engine, freshAt.Add(group.Policy.PressureWindow), inventory, []GroupState{group}, 80, 0)
	assertAction(t, decision, "flight", ActionScaleOut, "broker-b", []string{"broker-a", "broker-b"})
}

func TestEngineOlderSamplesCannotSatisfyPressureWindow(t *testing.T) {
	catalog := mustCatalog(t)
	engine := mustEngine(t, catalog, 1)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MaximumBrokers = 2
	group.Policy.TelemetryMaxAge = 10 * time.Minute

	first := testBase
	evaluate(t, engine, first, inventory, []GroupState{group}, 80, 0)
	for i, at := range []time.Time{first.Add(5 * time.Minute), first.Add(6 * time.Minute)} {
		older := cycleSnapshot(at, []GroupState{group}, 80, 0)
		observedAt := first.Add(-time.Duration(i+1) * time.Second)
		older.Brokers[0].ObservedAt = observedAt
		older.Groups[0].ObservedAt = observedAt
		decision, err := engine.Evaluate(at, older, inventory, []GroupState{group})
		if err != nil {
			t.Fatal(err)
		}
		assertNoAction(t, decision, "flight", "not been sustained")
	}
	if _, exists := engine.State().PressureSince["flight"]; exists {
		t.Fatal("older samples started a pressure window")
	}
}

func TestEngineUnknownCycleResetsPressureWindow(t *testing.T) {
	catalog := mustCatalog(t)
	engine := mustEngine(t, catalog, 1)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MinimumBrokers = 1
	group.Policy.MaximumBrokers = 2

	evaluate(t, engine, testBase, inventory, []GroupState{group}, 80, 0)
	unknownAt := testBase.Add(4 * time.Minute)
	snapshot := cycleSnapshot(unknownAt, []GroupState{group}, 80, 0)
	snapshot.Groups[0].Resources.IngressBytesPerSecond = UnknownMetric()
	decision, err := engine.Evaluate(unknownAt, snapshot, inventory, []GroupState{group})
	if err != nil {
		t.Fatal(err)
	}
	assertNoAction(t, decision, "flight", "telemetry is unknown")

	decision = evaluate(t, engine, testBase.Add(5*time.Minute), inventory, []GroupState{group}, 80, 0)
	assertNoAction(t, decision, "flight", "not been sustained")
	decision = evaluate(t, engine, testBase.Add(10*time.Minute), inventory, []GroupState{group}, 80, 0)
	assertAction(t, decision, "flight", ActionScaleOut, "broker-b", []string{"broker-a", "broker-b"})
}

func TestEngineBacklogDoesNotScaleOut(t *testing.T) {
	catalog := mustCatalog(t)
	engine := mustEngine(t, catalog, 1)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MinimumBrokers = 1
	group.Policy.MaximumBrokers = 2

	decision := evaluate(t, engine, testBase, inventory, []GroupState{group}, 10, 50)
	assertNoAction(t, decision, "flight", "downstream backlog")
	decision = evaluate(t, engine, testBase.Add(10*time.Minute), inventory, []GroupState{group}, 10, 50)
	assertNoAction(t, decision, "flight", "downstream backlog")
}

func TestEngineHeadroomThreshold(t *testing.T) {
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MinimumBrokers = 1
	group.Policy.MaximumBrokers = 2
	for _, test := range []struct {
		usage      float64
		wantAction bool
	}{
		{69.999, false},
		{70, true},
	} {
		engine := mustEngine(t, catalog, 1)
		evaluate(t, engine, testBase, inventory, []GroupState{group}, test.usage, 0)
		decision := evaluate(t, engine, testBase.Add(5*time.Minute), inventory, []GroupState{group}, test.usage, 0)
		gotAction := findRecommendation(t, decision, "flight").Action == ActionScaleOut
		if gotAction != test.wantAction {
			t.Fatalf("usage %v action=%v, want %v", test.usage, gotAction, test.wantAction)
		}
	}
}

func TestEngineDeterministicScaleOutAppendsLexicalEligibleBroker(t *testing.T) {
	catalog := mustCatalog(t)
	engine := mustEngine(t, catalog, 1)
	inventory := []Broker{
		testBroker("broker-z", "flight"),
		testBroker("broker-c", "flight"),
		testBroker("broker-a", "flight"),
		testBroker("broker-b", "other"),
	}
	group := testGroup("flight", []string{"broker-z"})
	group.Policy.MinimumBrokers = 2
	group.Policy.MaximumBrokers = 3
	decision := evaluate(t, engine, testBase, inventory, []GroupState{group}, 10, 0)
	assertAction(t, decision, "flight", ActionScaleOut, "broker-a", []string{"broker-z", "broker-a"})
}

func TestEngineSafelyRefusesWhenNoEligibleScaleOutBroker(t *testing.T) {
	catalog := mustCatalog(t)
	engine := mustEngine(t, catalog, 1)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "other")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MinimumBrokers = 2
	group.Policy.MaximumBrokers = 3
	decision := evaluate(t, engine, testBase, inventory, []GroupState{group}, 10, 0)
	assertNoAction(t, decision, "flight", "no ready broker")
}

func TestEngineScaleInRequiresExplicitSafeChoiceAndPreservesOrder(t *testing.T) {
	catalog := mustCatalog(t)
	inventory := []Broker{
		testBroker("broker-c", "flight"),
		testBroker("broker-a", "flight"),
		testBroker("broker-b", "flight"),
	}
	group := testGroup("flight", []string{"broker-c", "broker-a", "broker-b"})
	group.Policy.MinimumBrokers = 1
	group.Policy.MaximumBrokers = 3
	group.Policy.WarmBrokers = 1

	decision := evaluate(t, mustEngine(t, catalog, 1), testBase, inventory, []GroupState{group}, 10, 0)
	assertNoAction(t, decision, "flight", "explicit safe")

	unsafe := group
	unsafe.ScaleInChoice = &ScaleInChoice{BrokerID: "broker-a", Safe: false}
	decision = evaluate(t, mustEngine(t, catalog, 1), testBase, inventory, []GroupState{unsafe}, 10, 0)
	assertNoAction(t, decision, "flight", "not proven safe")

	group.ScaleInChoice = &ScaleInChoice{BrokerID: "broker-a", Safe: true, Evidence: "drained and no other ownership"}
	decision = evaluate(t, mustEngine(t, catalog, 1), testBase, inventory, []GroupState{group}, 10, 0)
	assertAction(t, decision, "flight", ActionScaleIn, "broker-a", []string{"broker-c", "broker-b"})
	if findRecommendation(t, decision, "flight").Evidence != group.ScaleInChoice.Evidence {
		t.Fatal("scale-in evidence not retained")
	}
}

func TestEngineWarmCountRaisesScaleInFloor(t *testing.T) {
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a", "broker-b"})
	group.Policy.MinimumBrokers = 1
	group.Policy.WarmBrokers = 1
	group.Policy.MaximumBrokers = 3
	group.ScaleInChoice = &ScaleInChoice{BrokerID: "broker-b", Safe: true, Evidence: "safe"}
	decision := evaluate(t, mustEngine(t, catalog, 1), testBase, inventory, []GroupState{group}, 10, 0)
	assertNoAction(t, decision, "flight", "minimum plus warm")
}

func TestEngineHonorsCooldown(t *testing.T) {
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MinimumBrokers = 2
	group.Policy.MaximumBrokers = 3
	group.LastTransitionAt = testBase.Add(-10 * time.Minute)
	decision := evaluate(t, mustEngine(t, catalog, 1), testBase, inventory, []GroupState{group}, 10, 0)
	assertNoAction(t, decision, "flight", "cooldown")
}

func TestEngineHonorsPerGroupTransitionLimit(t *testing.T) {
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MinimumBrokers = 2
	group.Policy.MaximumBrokers = 3
	group.ActiveTransitions = 1
	decision := evaluate(t, mustEngine(t, catalog, 2), testBase, inventory, []GroupState{group}, 10, 0)
	assertNoAction(t, decision, "flight", "group transition limit")
}

func TestEngineGloballyBoundsTransitionsDeterministically(t *testing.T) {
	catalog := mustCatalog(t)
	inventory := []Broker{
		testBroker("broker-a", "zeta", "alpha"),
		testBroker("broker-b", "zeta", "alpha"),
	}
	alpha := testGroup("alpha", []string{"broker-a"})
	alpha.Policy.MinimumBrokers = 2
	alpha.Policy.MaximumBrokers = 2
	zeta := testGroup("zeta", []string{"broker-a"})
	zeta.Policy.MinimumBrokers = 2
	zeta.Policy.MaximumBrokers = 2
	decision := evaluate(t, mustEngine(t, catalog, 1), testBase, inventory, []GroupState{zeta, alpha}, 10, 0)
	if len(decision.Transitions) != 1 || decision.Transitions[0].GroupID != "alpha" {
		t.Fatalf("transitions = %#v", decision.Transitions)
	}
	assertNoAction(t, decision, "zeta", "global transition limit")
}

func TestEngineCountsActiveTransitionsAgainstGlobalLimit(t *testing.T) {
	catalog := mustCatalog(t)
	inventory := []Broker{
		testBroker("broker-a", "busy", "waiting"),
		testBroker("broker-b", "busy", "waiting"),
	}
	busy := testGroup("busy", []string{"broker-a"})
	busy.ActiveTransitions = 1
	waiting := testGroup("waiting", []string{"broker-a"})
	waiting.Policy.MinimumBrokers = 2
	waiting.Policy.MaximumBrokers = 2
	decision := evaluate(t, mustEngine(t, catalog, 1), testBase, inventory, []GroupState{busy, waiting}, 10, 0)
	if len(decision.Transitions) != 0 {
		t.Fatalf("transitions = %#v", decision.Transitions)
	}
	assertNoAction(t, decision, "waiting", "global transition limit")
}

func TestEngineStateRoundTripPreservesPressureWindow(t *testing.T) {
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight"), testBroker("broker-b", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	group.Policy.MinimumBrokers = 1
	group.Policy.MaximumBrokers = 2
	original := mustEngine(t, catalog, 1)
	evaluate(t, original, testBase, inventory, []GroupState{group}, 80, 0)

	restored := mustEngine(t, catalog, 1)
	if err := restored.RestoreState(original.State(), testBase.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	decision := evaluate(t, restored, testBase.Add(5*time.Minute), inventory, []GroupState{group}, 80, 0)
	assertAction(t, decision, "flight", ActionScaleOut, "broker-b", []string{"broker-a", "broker-b"})
}

func TestEngineRejectsNonMonotonicEvaluationWithoutMutation(t *testing.T) {
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-a", "flight")}
	group := testGroup("flight", []string{"broker-a"})
	engine := mustEngine(t, catalog, 1)
	evaluate(t, engine, testBase, inventory, []GroupState{group}, 10, 0)
	before := engine.State()
	_, err := engine.Evaluate(testBase, cycleSnapshot(testBase, []GroupState{group}, 10, 0), inventory, []GroupState{group})
	if err == nil {
		t.Fatal("non-monotonic cycle accepted")
	}
	if !reflect.DeepEqual(before, engine.State()) {
		t.Fatal("invalid cycle mutated engine")
	}
}

func TestAggregateOutputIsDeterministicallySorted(t *testing.T) {
	t.Parallel()
	catalog := mustCatalog(t)
	inventory := []Broker{testBroker("broker-z", "zeta"), testBroker("broker-a", "alpha")}
	groups := []GroupState{testGroup("zeta", []string{"broker-z"}), testGroup("alpha", []string{"broker-a"})}
	snapshot := cycleSnapshot(testBase, groups, 10, 0)
	got, err := Aggregate(testBase, snapshot, catalog, inventory, groups)
	if err != nil {
		t.Fatal(err)
	}
	if got.Brokers[0].BrokerID != "broker-a" || got.Groups[0].GroupID != "alpha" {
		t.Fatalf("not sorted: %#v %#v", got.Brokers, got.Groups)
	}
}

func TestTelemetrySnapshotRejectsDuplicatesAndFutureSamples(t *testing.T) {
	t.Parallel()
	sample := testBrokerSample("broker-a", testBase, 10)
	for name, snapshot := range map[string]TelemetrySnapshot{
		"duplicate broker": testSnapshot(testBase, []BrokerSample{sample, sample}, nil),
		"future sample":    testSnapshot(testBase, []BrokerSample{func() BrokerSample { sample.ObservedAt = testBase.Add(time.Second); return sample }()}, nil),
	} {
		t.Run(name, func(t *testing.T) {
			if err := snapshot.Validate(); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}

func mustCatalog(t *testing.T) ProfileCatalog {
	t.Helper()
	catalog, err := NewProfileCatalog([]CapacityProfile{testProfile("class-a", "10.4.1", 100)})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func testProfile(class, version string, limit float64) CapacityProfile {
	return CapacityProfile{
		ServiceClass: class, BrokerVersion: version,
		Limits: CapacityLimits{limit, limit, limit, limit},
	}
}

func testBroker(id string, groups ...string) Broker {
	return Broker{ID: id, ServiceClass: "class-a", BrokerVersion: "10.4.1", Ready: true, EligibleGroups: groups}
}

func testGroup(id string, membership []string) GroupState {
	return GroupState{
		ID: id, Membership: membership,
		Policy: GroupPolicy{
			MinimumBrokers: 1, MaximumBrokers: 3, HeadroomPercent: 30,
			PressureWindow: 5 * time.Minute, TelemetryMaxAge: time.Minute,
			Cooldown: 15 * time.Minute, MaxConcurrentChanges: 1,
		},
	}
}

func testBrokerSample(id string, at time.Time, usage float64) BrokerSample {
	return BrokerSample{
		BrokerID: id, ServiceClass: "class-a", BrokerVersion: "10.4.1", ObservedAt: at,
		Resources: knownResource(usage),
	}
}

func testGroupSample(group, broker string, at time.Time, usage, queued, unacked float64) GroupSample {
	return GroupSample{
		GroupID: group, BrokerID: broker, ObservedAt: at,
		Resources: knownResource(usage),
		Backlog:   Backlog{KnownMetric(queued), KnownMetric(unacked)},
	}
}

func knownResource(usage float64) Resources {
	return Resources{KnownMetric(usage), KnownMetric(usage), KnownMetric(usage), KnownMetric(usage)}
}

func testSnapshot(at time.Time, brokers []BrokerSample, groups []GroupSample) TelemetrySnapshot {
	return TelemetrySnapshot{CapturedAt: at, Brokers: brokers, Groups: groups}
}

func cycleSnapshot(at time.Time, groups []GroupState, usage, queued float64) TelemetrySnapshot {
	brokerSet := make(map[string]struct{})
	var brokerSamples []BrokerSample
	var groupSamples []GroupSample
	for _, group := range groups {
		for _, broker := range group.Membership {
			if _, exists := brokerSet[broker]; !exists {
				brokerSet[broker] = struct{}{}
				brokerSamples = append(brokerSamples, testBrokerSample(broker, at, usage))
			}
			groupSamples = append(groupSamples, testGroupSample(group.ID, broker, at, usage, queued, 0))
		}
	}
	return testSnapshot(at, brokerSamples, groupSamples)
}

func mustEngine(t *testing.T, catalog ProfileCatalog, global int) *Engine {
	t.Helper()
	engine, err := NewEngine(catalog, EngineOptions{MaxConcurrentTransitions: global})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func evaluate(t *testing.T, engine *Engine, at time.Time, inventory []Broker, groups []GroupState, usage, queued float64) Decision {
	t.Helper()
	decision, err := engine.Evaluate(at, cycleSnapshot(at, groups, usage, queued), inventory, groups)
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func findRecommendation(t *testing.T, decision Decision, group string) Recommendation {
	t.Helper()
	for _, recommendation := range decision.Groups {
		if recommendation.GroupID == group {
			return recommendation
		}
	}
	t.Fatalf("missing recommendation for %q", group)
	return Recommendation{}
}

func assertNoAction(t *testing.T, decision Decision, group, reasonContains string) {
	t.Helper()
	recommendation := findRecommendation(t, decision, group)
	if recommendation.Action != ActionNone || !strings.Contains(recommendation.Reason, reasonContains) {
		t.Fatalf("recommendation = %#v, want NONE containing %q", recommendation, reasonContains)
	}
}

func assertAction(t *testing.T, decision Decision, group string, action Action, broker string, membership []string) {
	t.Helper()
	recommendation := findRecommendation(t, decision, group)
	if recommendation.Action != action || recommendation.BrokerID != broker || !reflect.DeepEqual(recommendation.ProposedMembership, membership) {
		t.Fatalf("recommendation = %#v, want %s %q %#v", recommendation, action, broker, membership)
	}
	if len(decision.Transitions) == 0 {
		t.Fatal("action not admitted to transitions")
	}
}
