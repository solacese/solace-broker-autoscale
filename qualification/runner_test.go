package qualification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/cloud"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/integration"
	"github.com/solacese/solace-workload-balancer/routing"
	"github.com/solacese/solace-workload-balancer/semp"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

type fakeQueueAPI struct {
	mu             sync.Mutex
	queues         map[string]semp.QueueSpec
	deleted        []string
	ensureCalls    int
	failEnsureAt   int
	failVerifyAt   int
	createCalls    int
	monitorCalls   int
	monitorResults []fakeMonitorResult
	subscriptions  map[string]string
	existsResults  []fakeExistsResult
	existsCalls    []string
	deleteErr      error
}

type fakeMonitorResult struct {
	status semp.QueueStatus
	err    error
}

type fakeExistsResult struct {
	exists bool
	err    error
}

func (f *fakeQueueAPI) CreateQueue(_ context.Context, spec semp.QueueSpec) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	if f.queues == nil {
		f.queues = map[string]semp.QueueSpec{}
	}
	if _, exists := f.queues[spec.Name]; exists {
		return false, semp.ErrAlreadyExists
	}
	if f.failEnsureAt > 0 && f.createCalls == f.failEnsureAt {
		return false, errors.New("injected")
	}
	f.queues[spec.Name] = spec
	if f.failVerifyAt > 0 && f.createCalls == f.failVerifyAt {
		return true, errors.New("injected verification failure")
	}
	return true, nil
}

func (f *fakeQueueAPI) EnsureQueue(_ context.Context, spec semp.QueueSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureCalls++
	if f.failEnsureAt > 0 && f.ensureCalls == f.failEnsureAt {
		return errors.New("injected")
	}
	if f.queues == nil {
		f.queues = map[string]semp.QueueSpec{}
	}
	f.queues[spec.Name] = spec
	return nil
}
func (f *fakeQueueAPI) CreateSubscription(_ context.Context, _, queue, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subscriptions == nil {
		f.subscriptions = make(map[string]string)
	}
	f.subscriptions[queue] = topic
	return nil
}
func (f *fakeQueueAPI) FenceQueue(_ context.Context, _, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	spec := f.queues[name]
	spec.IngressEnabled = false
	f.queues[name] = spec
	return nil
}
func (f *fakeQueueAPI) UnfenceQueue(_ context.Context, _, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	spec := f.queues[name]
	spec.IngressEnabled = true
	f.queues[name] = spec
	return nil
}
func (f *fakeQueueAPI) MonitorDrain(ctx context.Context, vpn, name string) (semp.QueueStatus, error) {
	f.mu.Lock()
	f.monitorCalls++
	if len(f.monitorResults) > 0 {
		result := f.monitorResults[0]
		f.monitorResults = f.monitorResults[1:]
		f.mu.Unlock()
		return result.status, result.err
	}
	f.mu.Unlock()
	return f.MonitorQueue(ctx, vpn, name)
}
func (f *fakeQueueAPI) MonitorQueue(_ context.Context, vpn, name string) (semp.QueueStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	spec, ok := f.queues[name]
	if !ok {
		return semp.QueueStatus{}, errors.New("absent")
	}
	return semp.QueueStatus{MessageVPN: vpn, Name: name, IngressEnabled: spec.IngressEnabled}, nil
}
func (f *fakeQueueAPI) DeleteQueue(_ context.Context, _, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, name)
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if len(f.existsResults) == 0 {
		delete(f.queues, name)
	}
	return nil
}
func (f *fakeQueueAPI) QueueExists(_ context.Context, _, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.existsCalls = append(f.existsCalls, name)
	if len(f.existsResults) > 0 {
		result := f.existsResults[0]
		f.existsResults = f.existsResults[1:]
		return result.exists, result.err
	}
	_, exists := f.queues[name]
	return exists, nil
}

type fakeQueueFactory struct{ byID map[string]*fakeQueueAPI }

func (f fakeQueueFactory) New(bundle cloud.ConnectionBundle) (QueueAPI, error) {
	return f.byID[bundle.ServiceID], nil
}

type fakeConsumer struct{ active bool }

func (c *fakeConsumer) Activate(context.Context) error { c.active = true; return nil }
func (c *fakeConsumer) Pause(context.Context) error    { c.active = false; return nil }
func (c *fakeConsumer) Close(context.Context) error    { c.active = false; return nil }

type fakeDataSession struct {
	mu        sync.Mutex
	consumers map[string]struct {
		consumer *fakeConsumer
		deliver  func(context.Context, shimSubscriber.Delivery) error
	}
	queues       map[string]*fakeQueueAPI
	queueNames   map[string]string
	destinations map[string]string
	staleProbes  []string
}

func (s *fakeDataSession) BrokerPublisher() shimPublisher.BrokerPublisher           { return s }
func (s *fakeDataSession) AsyncBrokerPublisher() shimPublisher.AsyncBrokerPublisher { return s }
func (s *fakeDataSession) ConsumerFactory() shimSubscriber.ConsumerFactory          { return s }
func (*fakeDataSession) Close() error                                               { return nil }
func (s *fakeDataSession) Prepare(_ context.Context, binding shimSubscriber.Binding, deliver func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
	c := &fakeConsumer{}
	s.mu.Lock()
	s.consumers[binding.BrokerID+"\x00"+binding.Group+"\x00"+binding.Destination] = struct {
		consumer *fakeConsumer
		deliver  func(context.Context, shimSubscriber.Delivery) error
	}{c, deliver}
	s.queueNames[binding.BrokerID+"\x00"+binding.Group+"\x00"+binding.Destination] = binding.Destination
	s.mu.Unlock()
	return c, nil
}
func (s *fakeDataSession) Publish(ctx context.Context, broker string, message shimPublisher.BrokerMessage) (shimPublisher.PublishOutcome, error) {
	s.mu.Lock()
	if strings.HasPrefix(message.EventID, "stale-") {
		s.staleProbes = append(s.staleProbes, fmt.Sprintf("%d/%s", message.Epoch, broker))
	}
	consumerKey := broker + "\x00" + message.Properties[shimPublisher.PropertyScalingGroup]
	api := s.queues[broker]
	var entry struct {
		consumer *fakeConsumer
		deliver  func(context.Context, shimSubscriber.Delivery) error
	}
	queue := ""
	for key, candidateQueue := range s.queueNames {
		if !strings.HasPrefix(key, consumerKey+"\x00") {
			continue
		}
		api.mu.Lock()
		_, ok := api.queues[candidateQueue]
		subscription := api.subscriptions[candidateQueue]
		api.mu.Unlock()
		if ok && topicMatches(subscription, message.Destination) {
			entry = s.consumers[key]
			queue = candidateQueue
			break
		}
	}
	s.mu.Unlock()
	api.mu.Lock()
	ingress := true
	if message.EventID == "fence-probe" {
		for _, spec := range api.queues {
			if !spec.IngressEnabled {
				ingress = false
				break
			}
		}
	} else if spec, ok := api.queues[queue]; ok {
		ingress = spec.IngressEnabled
	}
	api.mu.Unlock()
	if !ingress {
		return shimPublisher.OutcomeRejected, errors.New("nack")
	}
	if entry.consumer == nil || !entry.consumer.active {
		return shimPublisher.OutcomeRejected, errors.New("inactive")
	}
	view := customer.MessageView{Topic: message.Topic, EventID: message.EventID, Payload: message.Payload, Headers: message.Properties}
	digest, _ := routing.ParseSHA256(message.Properties[shimPublisher.PropertyBusinessHash])
	metadata := shimSubscriber.RoutingMetadata{
		Group: message.Properties[shimPublisher.PropertyScalingGroup], BusinessHash: digest,
		HashContract: message.Properties[shimPublisher.PropertyHashContract], LibraryVersion: message.Properties[shimPublisher.PropertyLibraryVersion],
		Epoch: message.Epoch, OriginalTopic: message.Topic,
	}
	if err := entry.deliver(ctx, &fakeDelivery{view: view, metadata: metadata}); err != nil {
		return shimPublisher.OutcomeUnknown, err
	}
	return shimPublisher.OutcomeAcknowledged, nil
}

func topicMatches(subscription, destination string) bool {
	if strings.HasSuffix(subscription, ">") {
		return strings.HasPrefix(destination, strings.TrimSuffix(subscription, ">"))
	}
	return subscription == destination
}

func (s *fakeDataSession) PublishAsync(ctx context.Context, broker string, message shimPublisher.BrokerMessage, attempt shimPublisher.PublishAttempt) (shimPublisher.AsyncPublishFuture, error) {
	outcome, err := s.Publish(ctx, broker, message)
	return fakeFuture{result: shimPublisher.AsyncPublishResult{Attempt: attempt, Outcome: outcome, Err: err}}, nil
}

type recordingSnapshotPublisher struct {
	mu        sync.Mutex
	snapshots []control.MembershipSnapshot
}

func (p *recordingSnapshotPublisher) PublishSnapshot(_ context.Context, snapshot control.MembershipSnapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshots = append(p.snapshots, snapshot.Clone())
	return nil
}

func (p *recordingSnapshotPublisher) latest(group string) (control.MembershipSnapshot, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var latest control.MembershipSnapshot
	found := false
	for _, snapshot := range p.snapshots {
		if snapshot.ScalingGroup == group && (!found || snapshot.Revision > latest.Revision) {
			latest = snapshot.Clone()
			found = true
		}
	}
	return latest, found
}

type recordingBrowser struct {
	mu        sync.Mutex
	publisher *recordingSnapshotPublisher
	calls     int
	mutate    func(control.MembershipSnapshot, int) control.MembershipSnapshot
}

func (b *recordingBrowser) Browse(_ context.Context, group string) ([]broker0.BrowsedMessage, error) {
	b.mu.Lock()
	b.calls++
	call := b.calls
	b.mu.Unlock()
	snapshot, ok := b.publisher.latest(group)
	if !ok {
		return nil, errors.New("absent")
	}
	if b.mutate != nil {
		snapshot = b.mutate(snapshot, call)
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	return []broker0.BrowsedMessage{{Kind: broker0.KindMembershipSnapshot, OperationID: "browse", Payload: payload}}, nil
}

func (b *recordingBrowser) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

type fakeFuture struct {
	result shimPublisher.AsyncPublishResult
}

type fixedAsyncBroker struct {
	result shimPublisher.AsyncPublishResult
	err    error
}

func (b fixedAsyncBroker) PublishAsync(context.Context, string, shimPublisher.BrokerMessage, shimPublisher.PublishAttempt) (shimPublisher.AsyncPublishFuture, error) {
	if b.err != nil {
		return nil, b.err
	}
	return fakeFuture{result: b.result}, nil
}

func (f fakeFuture) Await(context.Context) (shimPublisher.AsyncPublishResult, error) {
	return f.result, nil
}

type fakeDelivery struct {
	view     customer.MessageView
	metadata shimSubscriber.RoutingMetadata
}

func (d *fakeDelivery) Message() customer.MessageView { return d.view }
func (*fakeDelivery) Ack(context.Context) error       { return nil }
func (d *fakeDelivery) RoutingMetadata() (shimSubscriber.RoutingMetadata, error) {
	return d.metadata, nil
}

func testBundles() Bundles {
	bundle := func(id string) cloud.ConnectionBundle {
		return cloud.ConnectionBundle{ServiceID: id, MessageVPN: "vpn", SMFHosts: []string{"tcps://example.invalid:55443"}, ManagementURLs: []string{"https://example.invalid:943"}, ServiceCredential: cloud.Credential{Username: "app", Password: "secret"}, ManagementCredential: cloud.Credential{Username: "admin", Password: "secret"}}
	}
	return Bundles{Control: bundle("b0"), Data: [3]cloud.ConnectionBundle{bundle("a"), bundle("b"), bundle("c")}}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Millisecond)
	return c.now
}

func TestRunWithFakeAPIs(t *testing.T) {
	bundles := testBundles()
	apis := map[string]*fakeQueueAPI{"b0": {}, "a": {}, "b": {}, "c": {}}
	roles := map[string]*fakeQueueAPI{"broker-a": apis["a"], "broker-b": apis["b"], "broker-c": apis["c"]}
	session := &fakeDataSession{consumers: make(map[string]struct {
		consumer *fakeConsumer
		deliver  func(context.Context, shimSubscriber.Delivery) error
	}), queues: roles, queueNames: make(map[string]string)}
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	result, err := Run(context.Background(), bundles, "qual.test", Options{RunID: "unit", Keys: 12, EventsPerKey: 3, ReceiveTimeout: time.Second, DrainGrace: time.Millisecond, DrainPollInterval: time.Millisecond, AllowSkipped: true, QueueFactory: fakeQueueFactory{byID: apis}, DataPlane: DataPlaneFunc(func(context.Context, Bundles, string) (Session, error) { return session, nil }), Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passed() {
		t.Fatalf("result did not pass: %+v", result)
	}
	if len(result.Scenarios) != 7 {
		t.Fatalf("got %d scenarios", len(result.Scenarios))
	}
	if !result.Passed() {
		t.Fatal("AllowSkipped should permit explicit fake-mode skips")
	}
	var static ValidationResult
	for _, scenario := range result.Scenarios {
		if scenario.Name == ScenarioStatic {
			static = scenario.Validation
		}
	}
	if static.Missing != 0 || static.Duplicates != 0 || static.OutOfOrder != 0 || static.UnexpectedBroker != 0 || static.UniqueEventIDs != 72 {
		t.Fatalf("invalid traffic result: %+v", static)
	}
	if static.Accepted != 72 || static.PositiveACKs != 72 || static.OutboxHighWaterMessages == 0 || static.OutboxHighWaterBytes == 0 {
		t.Fatalf("invalid publisher instrumentation: %+v", static)
	}
	if static.DurableAcceptanceLatency.Max <= 0 || static.BrokerPositiveACKLatency.Max < 0 || static.EndToEndConsumedLatency.Max <= 0 || static.Latency != static.EndToEndConsumedLatency {
		t.Fatalf("invalid latency instrumentation: %+v", static)
	}
	for _, api := range apis {
		api.mu.Lock()
		if len(api.queues) != 0 {
			t.Errorf("queues not cleaned: %v", api.queues)
		}
		api.mu.Unlock()
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, secret := range []string{"secret", "example.invalid", "b0", "unit-flight"} {
		if strings.Contains(text, secret) {
			t.Fatalf("result leaked %q: %s", secret, text)
		}
	}
}

func TestQuantilesUseNearestRank(t *testing.T) {
	values := make([]time.Duration, 100)
	for i := range values {
		values[i] = time.Duration(i+1) * time.Millisecond
	}
	got := quantiles(values)
	want := Quantiles{P50: 50 * time.Millisecond, P95: 95 * time.Millisecond, P99: 99 * time.Millisecond, Max: 100 * time.Millisecond}
	if got != want {
		t.Fatalf("quantiles = %+v, want %+v", got, want)
	}
}

func TestObservationLedgerCountsOnlyExpectedAndValidatesMetadata(t *testing.T) {
	ledger := newObservationLedger()
	sent := time.Unix(10, 0)
	ledger.expect("expected", expectedEvent{group: FlightGroup, hash: "hash", broker: "broker-a", sequence: 1, sentAt: sent})
	if err := ledger.Observe(DeliveryObservation{EventID: "unexpected", Group: FlightGroup, BusinessHash: "hash", BrokerID: "broker-a", Sequence: 1, SentAt: sent, ReceivedAt: sent.Add(time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := ledger.wait(waitCtx); err == nil {
		t.Fatal("unexpected event ID satisfied expected delivery wait")
	}
	if err := ledger.Observe(DeliveryObservation{EventID: "expected", Group: FlightGroup, BusinessHash: "hash", BrokerID: "broker-b", Sequence: 1, SentAt: sent, ReceivedAt: sent.Add(time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	result := ledger.result(sent, sent.Add(time.Second))
	if result.UniqueEventIDs != 1 || result.Missing != 0 || result.UnexpectedBroker != 2 {
		t.Fatalf("result = %+v", result)
	}
}

func TestThroughputOptionsBoundPerBrokerConcurrency(t *testing.T) {
	options := Options{Keys: 2048, MaxInFlight: 6144, MaxInFlightPerBroker: 2048}.withDefaults()
	if err := options.validate(); err != nil {
		t.Fatal(err)
	}
	if options.MaxInFlight != 3*options.MaxInFlightPerBroker {
		t.Fatalf("global pending = %d, per-broker = %d", options.MaxInFlight, options.MaxInFlightPerBroker)
	}
}

func TestWorkloadPacerSpansConfiguredDuration(t *testing.T) {
	started := time.Now()
	pacer := newWorkloadPacer(started, 4, 40*time.Millisecond)
	if err := pacer.Wait(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 18*time.Millisecond {
		t.Fatalf("pacer elapsed %s, want approximately half the interval", elapsed)
	}
}

func TestMessagesInterleaveIndependentKeysWithoutReorderingLanes(t *testing.T) {
	work := messages("run", 3, 2, func() time.Time { return time.Unix(1, 0) })
	if len(work) != 12 {
		t.Fatalf("message count = %d", len(work))
	}
	want := []string{
		"run-flight-000000-000001", "run-baggage-000000-000001",
		"run-flight-000001-000001", "run-baggage-000001-000001",
		"run-flight-000002-000001", "run-baggage-000002-000001",
		"run-flight-000000-000002", "run-baggage-000000-000002",
	}
	for index, eventID := range want {
		if work[index].EventID != eventID {
			t.Fatalf("work[%d] = %q, want %q", index, work[index].EventID, eventID)
		}
	}
}

func TestActiveSnapshotCarriesEpochResourcesAndStableLogicalIdentity(t *testing.T) {
	plan, err := buildPlan(testBundles(), "qual.test", "resources", Options{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	flight := plan.public.Groups[0]
	initial := activeSnapshot(flight)
	initial.Namespace = plan.public.Namespace
	next := activeSnapshotForEpoch(plan.public.Namespace, flight, flight.Epochs[1], 6)
	if len(initial.CurrentResources) != 1 || initial.CurrentResources[0].BrokerID != "broker-a" || initial.CurrentResources[0].ConsumerSet != QualificationConsumerSet {
		t.Fatalf("initial resources = %+v", initial.CurrentResources)
	}
	if initial.Queue != next.Queue || initial.Destination != next.Destination {
		t.Fatalf("logical identity changed across epochs: %+v / %+v", initial, next)
	}
	reconciler := control.NewReconciler()
	if err := reconciler.BeginSubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ApplyBrowse(initial); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.FinishBrowse(); err != nil {
		t.Fatal(err)
	}
	spec := controllerSpecForTest(plan.public.Namespace, flight, flight.Epochs[0], flight.Epochs[1], 2)
	for _, phase := range []controller.Phase{controller.PhasePrepare, controller.PhasePause} {
		if _, err := reconciler.ApplyUpdate(transitionSnapshot(spec, phase)); err != nil {
			t.Fatalf("apply %s: %v", phase, err)
		}
	}
}

func controllerSpecForTest(namespace string, group GroupPlan, source, target EpochPlan, revision uint64) controller.TransitionSpec {
	return controller.TransitionSpec{
		ID: "transition-test", Namespace: namespace, LibraryVersion: "qualification-v1",
		Group: group.Group, Revision: revision, FromEpoch: source.Epoch, ToEpoch: target.Epoch,
		Current: transitionBrokers(source), Proposed: transitionBrokers(target),
		Queue: logicalQueue(group.Group), Destination: logicalDestination(group.Group),
		CurrentResources: epochResources(source), ProposedResources: epochResources(target),
		HashContract: group.Contract, Algorithm: control.AlgorithmSHA256BigEndianModulo,
	}
}

func TestPlanMatchesConfiguredGroupQueueContracts(t *testing.T) {
	const partitionCount = 12
	plan, err := buildPlan(testBundles(), "qual.test", "queue-contracts", Options{PartitionCount: partitionCount}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}

	groups := make(map[string]GroupPlan, len(plan.public.Groups))
	for _, group := range plan.public.Groups {
		groups[group.Group] = group
	}
	tests := []struct {
		group       string
		contract    string
		partitioned bool
		access      string
		partitions  uint32
	}{
		{group: FlightGroup, contract: routing.FlightOperationsDomain, partitioned: true, access: "non-exclusive", partitions: partitionCount},
		{group: BaggageGroup, contract: routing.BaggageDomain, partitioned: false, access: "exclusive", partitions: 0},
	}
	wantBrokers := []string{"broker-a", "broker-b", "broker-c"}
	for _, test := range tests {
		t.Run(test.group, func(t *testing.T) {
			group, ok := groups[test.group]
			if !ok {
				t.Fatalf("group %q is absent", test.group)
			}
			if group.Contract != test.contract || group.Partitioned != test.partitioned {
				t.Fatalf("group contract = %q, partitioned = %v", group.Contract, group.Partitioned)
			}
			widest, err := widestEpoch(group)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(widest.BrokerIDs, wantBrokers) {
				t.Fatalf("widest membership = %v, want %v", widest.BrokerIDs, wantBrokers)
			}
			for _, epoch := range group.Epochs {
				for _, broker := range epoch.BrokerIDs {
					var spec *semp.QueueSpec
					for index := range plan.queues {
						resource := &plan.queues[index]
						if resource.ref.Role == broker && resource.ref.Queue == epoch.Queues[broker] {
							spec = &resource.spec
							break
						}
					}
					if spec == nil {
						t.Fatalf("epoch %d broker %s queue spec is absent", epoch.Epoch, broker)
					}
					if spec.AccessType != test.access || spec.PartitionCount != test.partitions {
						t.Fatalf("epoch %d broker %s access = %q, partitions = %d; want %q, %d", epoch.Epoch, broker, spec.AccessType, spec.PartitionCount, test.access, test.partitions)
					}
				}
			}
		})
	}
}

func TestCustomerLibraryKeepsPerGroupHashRouting(t *testing.T) {
	library := customerLibrary()
	flight := message("routing", "flight", 7, 1, time.Unix(1, 0), trafficPayload{Kind: "flight", Carrier: "QA", Number: "0007", Date: "2026-09-25", Leg: "LEG-0007", Sequence: 1})
	baggage := message("routing", "baggage", 7, 1, time.Unix(1, 0), trafficPayload{Kind: "baggage", Carrier: "QA", Journey: "BAG-00000007", Sequence: 1})
	wantFlight, err := routing.FlightOperationsHash("QA", "0007", "2026-09-25", "LEG-0007")
	if err != nil {
		t.Fatal(err)
	}
	wantBaggage, err := routing.BaggageHash("QA", "BAG-00000007")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		message customer.MessageView
		group   string
		hash    customer.BusinessHash
	}{
		{name: "flight", message: flight, group: FlightGroup, hash: wantFlight},
		{name: "baggage", message: baggage, group: BaggageGroup, hash: wantBaggage},
	} {
		t.Run(test.name, func(t *testing.T) {
			group, err := library.GetScalingGroup(test.message)
			if err != nil {
				t.Fatal(err)
			}
			hash, err := library.GetBusinessHash(test.message)
			if err != nil {
				t.Fatal(err)
			}
			if group != test.group || hash != test.hash {
				t.Fatalf("routing = (%q, %x), want (%q, %x)", group, hash, test.group, test.hash)
			}
		})
	}
}

func TestPlanContainsExactFlightTransitionChain(t *testing.T) {
	bundles := testBundles()
	bundles.Data[0].MessageVPN = "vpn-a"
	bundles.Data[1].MessageVPN = "vpn-b"
	bundles.Data[2].MessageVPN = "vpn-c"
	plan, err := buildPlan(bundles, "qual.test", "chain", Options{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	flight, baggage, err := transitionGroups(plan.public)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"broker-a"}, {"broker-a", "broker-b"}, {"broker-a", "broker-b", "broker-c"}, {"broker-a", "broker-b"}}
	for index, membership := range want {
		if !reflect.DeepEqual(flight.Epochs[index].BrokerIDs, membership) {
			t.Fatalf("epoch %d membership = %v", index+1, flight.Epochs[index].BrokerIDs)
		}
		for _, broker := range membership {
			if flight.Epochs[index].Queues[broker] == "" || flight.Epochs[index].MessageVPNs[broker] != bundles.Data[roleIndex(broker)].MessageVPN {
				t.Fatalf("epoch %d broker %s not mapped exactly", index+1, broker)
			}
		}
	}
	if len(baggage.Epochs) != 1 {
		t.Fatalf("baggage epochs = %d", len(baggage.Epochs))
	}
	if plan.public.MembershipQueues[FlightGroup] == "" || plan.public.MembershipQueues[BaggageGroup] == "" {
		t.Fatal("membership queues were not tracked")
	}
	for _, resource := range plan.queues {
		if resource.ref.Kind == "epoch-queue" && strings.Contains(resource.ref.Topic, "/epoch/") && !strings.Contains(resource.ref.Topic, "/epoch/1/") && resource.spec.IngressEnabled {
			t.Fatalf("future epoch queue %s started unfenced", resource.ref.Queue)
		}
	}
}

func TestFenceCheckRequiresDefinitiveNegativeReceipt(t *testing.T) {
	plan, err := buildPlan(testBundles(), "qual.test", "fence", Options{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	apis := map[string]*fakeQueueAPI{"b0": {}, "a": {}, "b": {}, "c": {}}
	managed, err := provision(context.Background(), plan, fakeQueueFactory{byID: apis})
	if err != nil {
		t.Fatal(err)
	}
	defer managed.cleanup(context.Background())

	attempt := shimPublisher.PublishAttempt{EventID: "fence-probe", Number: 1, Epoch: 1}
	for name, broker := range map[string]fixedAsyncBroker{
		"unknown":  {result: shimPublisher.AsyncPublishResult{Attempt: attempt, Outcome: shimPublisher.OutcomeUnknown, Err: errors.New("timeout")}},
		"rejected": {result: shimPublisher.AsyncPublishResult{Attempt: attempt, Outcome: shimPublisher.OutcomeRejected, Err: errors.New("nack")}},
	} {
		t.Run(name, func(t *testing.T) {
			err := checkFenceSemantics(context.Background(), managed, plan, broker)
			if name == "rejected" && err != nil {
				t.Fatal(err)
			}
			if name == "unknown" && err == nil {
				t.Fatal("ambiguous outcome passed as a broker NACK")
			}
		})
	}
}

func TestDirectTransitionDriverUsesControllerFenceAndRetainsBaggage(t *testing.T) {
	bundles := testBundles()
	apis := map[string]*fakeQueueAPI{"b0": {}, "a": {}, "b": {}, "c": {}}
	// The first transition must not accept the initial zero as a full grace
	// interval: nonzero and failed SEMP samples both reset it before two fresh
	// real zero polls establish continuous drain.
	apis["a"].monitorResults = []fakeMonitorResult{
		{status: semp.QueueStatus{}},
		{status: semp.QueueStatus{SpooledMessages: 1}},
		{err: errors.New("transient SEMP failure")},
		{status: semp.QueueStatus{}},
		{status: semp.QueueStatus{}},
	}
	plan, err := buildPlan(bundles, "qual.test", "transition", Options{DrainGrace: time.Nanosecond}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	managed, err := provision(context.Background(), plan, fakeQueueFactory{byID: apis})
	if err != nil {
		t.Fatal(err)
	}
	defer managed.cleanup(context.Background())
	roles := map[string]*fakeQueueAPI{"broker-a": apis["a"], "broker-b": apis["b"], "broker-c": apis["c"]}
	session := &fakeDataSession{consumers: make(map[string]struct {
		consumer *fakeConsumer
		deliver  func(context.Context, shimSubscriber.Delivery) error
	}), queues: roles, queueNames: make(map[string]string)}
	publisher := &recordingSnapshotPublisher{}
	primaryBrowser := &recordingBrowser{publisher: publisher}
	independentBrowser := &recordingBrowser{publisher: publisher}
	var observationsMu sync.Mutex
	var observations []DeliveryObservation
	driver := DirectTransitionDriver{Publisher: publisher, Browser: primaryBrowser, IndependentBrowser: independentBrowser, Queues: fakeQueueFactory{byID: apis}, DataPlane: DataPlaneFunc(func(context.Context, Bundles, string) (Session, error) { return session, nil }), Options: Options{DrainGrace: time.Nanosecond, DrainPollInterval: time.Nanosecond, ReceiveTimeout: time.Second}}
	if err := driver.RunTransition(context.Background(), plan.public, ObserverFunc(func(observation DeliveryObservation) error {
		observationsMu.Lock()
		defer observationsMu.Unlock()
		observations = append(observations, observation)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	var active []control.MembershipSnapshot
	baggageCount := 0
	for _, snapshot := range publisher.snapshots {
		if snapshot.ScalingGroup == FlightGroup && snapshot.Phase == control.PhaseActive {
			active = append(active, snapshot)
		}
		if snapshot.ScalingGroup == BaggageGroup {
			baggageCount++
		}
	}
	if len(active) != 3 {
		t.Fatalf("active Flight snapshots = %d", len(active))
	}
	if primaryBrowser.callCount() != 2 || independentBrowser.callCount() != 1 {
		t.Fatalf("final browse calls = primary %d, independent %d; want 2 and 1", primaryBrowser.callCount(), independentBrowser.callCount())
	}
	session.mu.Lock()
	staleProbes := append([]string(nil), session.staleProbes...)
	session.mu.Unlock()
	wantStaleProbes := []string{"1/broker-a", "1/broker-a", "2/broker-a", "2/broker-b", "1/broker-a", "2/broker-a", "2/broker-b", "3/broker-a", "3/broker-b", "3/broker-c"}
	if !reflect.DeepEqual(staleProbes, wantStaleProbes) {
		t.Fatalf("stale probes = %v, want %v", staleProbes, wantStaleProbes)
	}
	for index, want := range [][]string{{"broker-a", "broker-b"}, {"broker-a", "broker-b", "broker-c"}, {"broker-a", "broker-b"}} {
		if !reflect.DeepEqual([]string(active[index].CurrentMembership), want) {
			t.Fatalf("active %d = %v", index, active[index].CurrentMembership)
		}
	}
	if baggageCount != 1 {
		t.Fatalf("baggage publications = %d", baggageCount)
	}
	observationsMu.Lock()
	defer observationsMu.Unlock()
	if len(observations) != 6 {
		t.Fatalf("transition observations = %d, want three Flight and three Baggage probes", len(observations))
	}
	for sequence := uint64(1); sequence <= 3; sequence++ {
		seen := map[string]bool{}
		for _, observation := range observations {
			if observation.Sequence == sequence {
				seen[observation.Group] = true
			}
		}
		if !seen[FlightGroup] || !seen[BaggageGroup] {
			t.Fatalf("sequence %d observations = %v", sequence, seen)
		}
	}
	flight, _, _ := transitionGroups(plan.public)
	for index := 0; index < 3; index++ {
		for _, broker := range flight.Epochs[index].BrokerIDs {
			status, _ := apis[bundles.Data[roleIndex(broker)].ServiceID].MonitorQueue(context.Background(), flight.Epochs[index].MessageVPNs[broker], flight.Epochs[index].Queues[broker])
			if status.IngressEnabled {
				t.Fatalf("stale epoch %d broker %s remained enabled", index+1, broker)
			}
		}
	}
}

func TestMonitorEpochSourcesReturnsUnknownForSEMPError(t *testing.T) {
	epoch := EpochPlan{Epoch: 1, BrokerIDs: []string{"broker-a", "broker-b"}, Queues: map[string]string{"broker-a": "qa", "broker-b": "qb"}, MessageVPNs: map[string]string{"broker-a": "vpn", "broker-b": "vpn"}}
	clients := map[string]QueueAPI{
		"broker-a": &fakeQueueAPI{monitorResults: []fakeMonitorResult{{err: errors.New("transient")}}},
		"broker-b": &fakeQueueAPI{monitorResults: []fakeMonitorResult{{status: semp.QueueStatus{SpooledMessages: 3}}}},
	}
	statuses, err := monitorEpochSources(context.Background(), clients, epoch)
	if err == nil {
		t.Fatal("SEMP error was not reported")
	}
	if statuses["broker-a"].Known || !statuses["broker-b"].Known || statuses["broker-b"].SpooledMessages != 3 {
		t.Fatalf("statuses = %#v", statuses)
	}
	unknown := sourceTelemetry("qual.test", FlightGroup, "transition", "observer-1", epoch, "broker-a", statuses["broker-a"], time.Now())
	if unknown.Queued.Known || unknown.Stored.Known || unknown.Unacked.Known {
		t.Fatalf("failed SEMP sample became known zero: %#v", unknown)
	}
}

func TestRetainedActiveRequiresLatestExactSnapshotAndLaterPersistence(t *testing.T) {
	expected := activeSnapshotForEpoch("qual.test", GroupPlan{Group: FlightGroup, Contract: routing.FlightOperationsDomain}, testEpoch(4, "broker-a", "broker-b"), 16)
	publisher := &recordingSnapshotPublisher{snapshots: []control.MembershipSnapshot{expected}}

	for name, mutate := range map[string]func(control.MembershipSnapshot, int) control.MembershipSnapshot{
		"stale revision": func(snapshot control.MembershipSnapshot, _ int) control.MembershipSnapshot {
			snapshot.Revision--
			return snapshot
		},
		"wrong epoch": func(snapshot control.MembershipSnapshot, _ int) control.MembershipSnapshot {
			snapshot.Epoch--
			return snapshot
		},
		"not active": func(snapshot control.MembershipSnapshot, _ int) control.MembershipSnapshot {
			snapshot.Phase = control.PhaseCommitted
			return snapshot
		},
		"reordered": func(snapshot control.MembershipSnapshot, _ int) control.MembershipSnapshot {
			snapshot.CurrentMembership = control.Membership{"broker-b", "broker-a"}
			return snapshot
		},
		"wrong resource": func(snapshot control.MembershipSnapshot, _ int) control.MembershipSnapshot {
			snapshot.CurrentResources[0].QueueName = "other-queue"
			return snapshot
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := assertRetainedActive(context.Background(), &recordingBrowser{publisher: publisher, mutate: mutate}, &recordingBrowser{publisher: publisher}, expected, time.Nanosecond)
			if err == nil {
				t.Fatal("invalid retained snapshot passed")
			}
		})
	}

	t.Run("independent disagrees", func(t *testing.T) {
		mutate := func(snapshot control.MembershipSnapshot, _ int) control.MembershipSnapshot {
			snapshot.Revision--
			return snapshot
		}
		if err := assertRetainedActive(context.Background(), &recordingBrowser{publisher: publisher}, &recordingBrowser{publisher: publisher, mutate: mutate}, expected, time.Nanosecond); err == nil {
			t.Fatal("independent stale browse passed")
		}
	})
	t.Run("later browse regresses", func(t *testing.T) {
		mutate := func(snapshot control.MembershipSnapshot, call int) control.MembershipSnapshot {
			if call > 1 {
				snapshot.Revision--
			}
			return snapshot
		}
		if err := assertRetainedActive(context.Background(), &recordingBrowser{publisher: publisher, mutate: mutate}, &recordingBrowser{publisher: publisher}, expected, time.Nanosecond); err == nil {
			t.Fatal("later stale browse passed")
		}
	})
}

func TestBrowseLatestSnapshotSelectsHighestRevision(t *testing.T) {
	group := GroupPlan{Group: FlightGroup, Contract: routing.FlightOperationsDomain}
	stale := activeSnapshotForEpoch("qual.test", group, testEpoch(3, "broker-a", "broker-b", "broker-c"), 11)
	latest := activeSnapshotForEpoch("qual.test", group, testEpoch(4, "broker-a", "broker-b"), 16)
	messages := make([]broker0.BrowsedMessage, 0, 2)
	for _, snapshot := range []control.MembershipSnapshot{latest, stale} {
		payload, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		messages = append(messages, broker0.BrowsedMessage{Kind: broker0.KindMembershipSnapshot, Payload: payload})
	}
	browser := broker0BrowserFunc(func(context.Context, string) ([]broker0.BrowsedMessage, error) { return messages, nil })
	got, err := browseLatestSnapshot(context.Background(), browser, FlightGroup)
	if err != nil {
		t.Fatal(err)
	}
	if !sameActiveSnapshot(got, latest) {
		t.Fatalf("latest snapshot = %+v, want %+v", got, latest)
	}
}

func testEpoch(epoch uint64, brokers ...string) EpochPlan {
	queues := make(map[string]string, len(brokers))
	vpns := make(map[string]string, len(brokers))
	for _, broker := range brokers {
		queues[broker] = fmt.Sprintf("%s-e%d", broker, epoch)
		vpns[broker] = "vpn"
	}
	return EpochPlan{Epoch: epoch, BrokerIDs: append([]string(nil), brokers...), Queues: queues, MessageVPNs: vpns, IngressTopic: fmt.Sprintf("qual/epoch/%d/>", epoch)}
}

type broker0BrowserFunc func(context.Context, string) ([]broker0.BrowsedMessage, error)

func (f broker0BrowserFunc) Browse(ctx context.Context, group string) ([]broker0.BrowsedMessage, error) {
	return f(ctx, group)
}

func TestNativeSessionUsesPerGroupPublisherAndConsumerPolicies(t *testing.T) {
	const partitionCount = 12
	session := &nativeSession{factory: qualificationConsumerFactory(nil, partitionCount)}

	publisher, ok := session.BrokerPublisher().(integration.PublisherAdapter)
	if !ok {
		t.Fatalf("BrokerPublisher() type = %T", session.BrokerPublisher())
	}
	asyncPublisher, ok := session.AsyncBrokerPublisher().(integration.AsyncPublisherAdapter)
	if !ok {
		t.Fatalf("AsyncBrokerPublisher() type = %T", session.AsyncBrokerPublisher())
	}
	for _, policy := range []integration.PartitionPolicy{publisher.PartitionPolicy, asyncPublisher.PartitionPolicy} {
		if !policy.Partitioned(FlightGroup) || policy.Partitioned(BaggageGroup) {
			t.Fatalf("partition policy = %#v", policy)
		}
	}

	factory := session.NativeConsumerFactory()
	if factory.ExclusiveGroups[FlightGroup] || !factory.ExclusiveGroups[BaggageGroup] {
		t.Fatalf("exclusive groups = %#v", factory.ExclusiveGroups)
	}
	if factory.ConsumersPerBinding[FlightGroup] != partitionCount || factory.ConsumersPerBinding[BaggageGroup] != 1 {
		t.Fatalf("consumer flows = %#v", factory.ConsumersPerBinding)
	}
}

func TestRunRequiresBroker0SidecarByDefault(t *testing.T) {
	_, err := Run(context.Background(), testBundles(), "qual.test", Options{})
	if err == nil || !strings.Contains(err.Error(), "transition hooks are required") {
		t.Fatalf("got %v", err)
	}
}

func TestResultPassedRejectsSkippedScenario(t *testing.T) {
	result := Result{Cleanup: "complete", Scenarios: []ScenarioResult{{Name: ScenarioBroker0Browse, Status: "SKIP"}}}
	if result.Passed() {
		t.Fatal("skipped mandatory scenario must not pass")
	}
}

func TestCleanupWaitsForEventualQueueDisappearance(t *testing.T) {
	api := &fakeQueueAPI{existsResults: []fakeExistsResult{{exists: true}, {exists: true}, {exists: false}}}
	managed := &managedResources{
		clients: map[string]QueueAPI{"broker-a": api},
		created: []queueResource{{ref: ResourceRef{Role: "broker-a", MessageVPN: "vpn", Queue: "owned.queue"}}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := managed.cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if want := []string{"owned.queue", "owned.queue", "owned.queue"}; !reflect.DeepEqual(api.existsCalls, want) {
		t.Fatalf("existence checks = %v, want %v", api.existsCalls, want)
	}
}

func TestCleanupReportsQueueStillPresentAtTimeout(t *testing.T) {
	api := &fakeQueueAPI{existsResults: []fakeExistsResult{{exists: true}}}
	managed := &managedResources{
		clients: map[string]QueueAPI{"broker-a": api},
		created: []queueResource{{ref: ResourceRef{Role: "broker-a", MessageVPN: "vpn", Queue: "owned.queue"}}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := managed.cleanup(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "verify exact queue absent") {
		t.Fatalf("cleanup error = %v", err)
	}
}

func TestCleanupReportsExistenceCheckError(t *testing.T) {
	wrong := errors.New("wrong SEMP error")
	api := &fakeQueueAPI{existsResults: []fakeExistsResult{{err: wrong}}}
	managed := &managedResources{
		clients: map[string]QueueAPI{"broker-a": api},
		created: []queueResource{{ref: ResourceRef{Role: "broker-a", MessageVPN: "vpn", Queue: "owned.queue"}}},
	}

	err := managed.cleanup(context.Background())
	if !errors.Is(err, wrong) || !strings.Contains(err.Error(), "verify exact queue absent") {
		t.Fatalf("cleanup error = %v", err)
	}
}

func TestCleanupAttemptsAllResourcesInReverseOrder(t *testing.T) {
	var calls []string
	clients := make(map[string]QueueAPI, 3)
	created := make([]queueResource, 0, 3)
	for _, name := range []string{"first", "second", "third"} {
		api := &recordingQueueAPI{
			delete: func() error {
				calls = append(calls, "delete:"+name)
				if name == "third" {
					return errors.New("delete failed")
				}
				return nil
			},
			exists: func() (bool, error) {
				calls = append(calls, "exists:"+name)
				return false, nil
			},
		}
		clients[name] = api
		created = append(created, queueResource{ref: ResourceRef{Role: name, MessageVPN: "vpn", Queue: name + ".queue"}})
	}

	err := (&managedResources{clients: clients, created: created}).cleanup(context.Background())
	if err == nil || !strings.Contains(err.Error(), "third") {
		t.Fatalf("cleanup error = %v", err)
	}
	want := []string{"delete:third", "delete:second", "exists:second", "delete:first", "exists:first"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
}

type recordingQueueAPI struct {
	fakeQueueAPI
	delete func() error
	exists func() (bool, error)
}

func (r *recordingQueueAPI) DeleteQueue(context.Context, string, string) error {
	return r.delete()
}

func (r *recordingQueueAPI) QueueExists(context.Context, string, string) (bool, error) {
	return r.exists()
}

func TestProvisionVerificationFailureCleansSuccessfullyCreatedQueue(t *testing.T) {
	bundles := testBundles()
	apis := map[string]*fakeQueueAPI{"b0": {failVerifyAt: 1}, "a": {}, "b": {}, "c": {}}
	plan, err := buildPlan(bundles, "qual.test", "verify-failure", Options{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	managed, err := provision(context.Background(), plan, fakeQueueFactory{byID: apis})
	if err == nil {
		t.Fatal("expected verification failure")
	}
	if cleanupErr := managed.cleanup(context.Background()); cleanupErr != nil {
		t.Fatal(cleanupErr)
	}
	deleted := 0
	for _, api := range apis {
		deleted += len(api.deleted)
	}
	if deleted != 1 {
		t.Fatalf("deleted %d queues, want the one created before verification failed", deleted)
	}
}

func TestProvisionFailureCleansOnlyTrackedQueues(t *testing.T) {
	bundles := testBundles()
	apis := map[string]*fakeQueueAPI{"b0": {failEnsureAt: 2}, "a": {}, "b": {}, "c": {}}
	apis["a"].queues = map[string]semp.QueueSpec{"customer.queue": {Name: "customer.queue"}}
	plan, err := buildPlan(bundles, "qual.test", "failure", Options{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	managed, err := provision(context.Background(), plan, fakeQueueFactory{byID: apis})
	if err == nil {
		t.Fatal("expected provisioning failure")
	}
	if cleanupErr := managed.cleanup(context.Background()); cleanupErr != nil {
		t.Fatal(cleanupErr)
	}
	if _, ok := apis["a"].queues["customer.queue"]; !ok {
		t.Fatal("cleanup touched untracked customer queue")
	}
	if len(apis["b0"].deleted) != 1 {
		t.Fatalf("deleted %d queues, want exactly the one tracked queue", len(apis["b0"].deleted))
	}
}

func TestPlanContainsCompleteBroker0TransportTopology(t *testing.T) {
	plan, err := buildPlan(testBundles(), "qual.test", "topology", Options{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.queues) != 31 {
		t.Fatalf("planned queues = %d, want 31", len(plan.queues))
	}
	if len(plan.public.Broker0Queues) != 18 {
		t.Fatalf("participant Broker 0 queues = %d, want 18", len(plan.public.Broker0Queues))
	}
	seen := make(map[string]struct{}, len(plan.queues))
	counts := make(map[string]int)
	for _, resource := range plan.queues {
		key := resource.ref.Role + "\x00" + resource.ref.MessageVPN + "\x00" + resource.ref.Queue
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("duplicate queue identity %q", resource.ref.Queue)
		}
		seen[key] = struct{}{}
		counts[resource.ref.Kind]++
		if resource.ref.Role != "broker-0" {
			continue
		}
		if resource.ref.Kind == resourceMembershipLVQ {
			if resource.spec.MaxMsgSpoolUsage != 0 || resource.spec.Permission != "read-only" {
				t.Fatalf("membership LVQ spec = %#v", resource.spec)
			}
		} else if resource.spec.MaxMsgSpoolUsage == 0 {
			t.Fatalf("non-LVQ queue %q has last-value spool semantics", resource.ref.Queue)
		}
	}
	wantCounts := map[string]int{
		resourceMembershipLVQ: 2, resourceParticipantUpdateQueue: 4,
		resourceParticipantCommand: 4, resourceParticipantRegistration: 4,
		resourceParticipantAck: 4, resourceParticipantTelemetry: 2, resourceEpochQueue: 11,
	}
	if !reflect.DeepEqual(counts, wantCounts) {
		t.Fatalf("resource counts = %v, want %v", counts, wantCounts)
	}
	for _, binding := range plan.public.Broker0Queues {
		if binding.Participant == "" || binding.Participant == "*" || binding.Principal != testBundles().Control.ServiceCredential.Username || binding.Topic == "" {
			t.Fatalf("unsafe Broker 0 binding: %#v", binding)
		}
		if !strings.Contains(binding.Queue, plan.public.RunID) || !strings.Contains(binding.Topic, "/"+binding.Participant) && binding.Kind != broker0.KindMembershipSnapshot {
			t.Fatalf("binding is not run/participant scoped: %#v", binding)
		}
	}
}

func TestProvisionExistingQueueCollisionIsNeverCleanupOwned(t *testing.T) {
	bundles := testBundles()
	plan, err := buildPlan(bundles, "qual.test", "collision", Options{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	first := plan.queues[0]
	apis := map[string]*fakeQueueAPI{"b0": {queues: map[string]semp.QueueSpec{first.ref.Queue: first.spec}}, "a": {}, "b": {}, "c": {}}
	managed, err := provision(context.Background(), plan, fakeQueueFactory{byID: apis})
	if !errors.Is(err, semp.ErrAlreadyExists) {
		t.Fatalf("provision error = %v, want existing resource collision", err)
	}
	if cleanupErr := managed.cleanup(context.Background()); cleanupErr != nil {
		t.Fatal(cleanupErr)
	}
	if len(apis["b0"].deleted) != 0 {
		t.Fatalf("cleanup deleted pre-existing queue: %v", apis["b0"].deleted)
	}
}
