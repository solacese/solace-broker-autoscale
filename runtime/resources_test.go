package runtime

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/policy"
	"github.com/solacese/solace-workload-balancer/semp"
)

type lifecycleQueueClient struct {
	queues              map[string]semp.QueueSpec
	subscriptions       map[string][]string
	ensured             []semp.QueueSpec
	deleted             []string
	events              *[]string
	failEnsureAt        int
	failSubscription    bool
	subscriptionCreates int
	subscriptionLists   int
	fenced              []string
	unfenced            []string
}

func (c *lifecycleQueueClient) EnsureQueue(_ context.Context, spec semp.QueueSpec) error {
	if c.events != nil {
		*c.events = append(*c.events, "ensure:"+spec.Name)
	}
	c.ensured = append(c.ensured, spec)
	if c.failEnsureAt > 0 && len(c.ensured) == c.failEnsureAt {
		return errors.New("injected ensure failure")
	}
	if c.queues == nil {
		c.queues = make(map[string]semp.QueueSpec)
	}
	if existing, ok := c.queues[spec.Name]; ok && existing != spec {
		return errors.New("incompatible managed queue")
	}
	c.queues[spec.Name] = spec
	return nil
}

func (c *lifecycleQueueClient) ListExactQueueSubscriptions(_ context.Context, _, queue string) ([]string, error) {
	c.subscriptionLists++
	return slices.Clone(c.subscriptions[queue]), nil
}

func (c *lifecycleQueueClient) CreateSubscription(_ context.Context, _, queue, topic string) error {
	if c.events != nil {
		*c.events = append(*c.events, "subscribe:"+queue)
	}
	if c.failSubscription {
		return errors.New("injected subscription failure")
	}
	c.subscriptionCreates++
	if c.subscriptions == nil {
		c.subscriptions = make(map[string][]string)
	}
	if !slices.Contains(c.subscriptions[queue], topic) {
		c.subscriptions[queue] = append(c.subscriptions[queue], topic)
	}
	return nil
}
func (c *lifecycleQueueClient) FenceQueue(_ context.Context, _, queue string) error {
	c.fenced = append(c.fenced, queue)
	spec, ok := c.queues[queue]
	if !ok {
		return errors.New("absent")
	}
	spec.IngressEnabled = false
	c.queues[queue] = spec
	return nil
}
func (c *lifecycleQueueClient) UnfenceQueue(_ context.Context, _, queue string) error {
	c.unfenced = append(c.unfenced, queue)
	spec, ok := c.queues[queue]
	if !ok {
		return errors.New("absent")
	}
	spec.IngressEnabled = true
	c.queues[queue] = spec
	return nil
}
func (c *lifecycleQueueClient) MonitorQueue(_ context.Context, vpn, queue string) (semp.QueueStatus, error) {
	spec, ok := c.queues[queue]
	if !ok {
		return semp.QueueStatus{}, errors.New("absent")
	}
	return semp.QueueStatus{MessageVPN: vpn, Name: queue, IngressEnabled: spec.IngressEnabled}, nil
}
func (c *lifecycleQueueClient) MonitorDrain(ctx context.Context, vpn, queue string) (semp.QueueStatus, error) {
	return c.MonitorQueue(ctx, vpn, queue)
}
func (c *lifecycleQueueClient) DeleteQueue(_ context.Context, _, queue string) error {
	if c.events != nil {
		*c.events = append(*c.events, "delete:"+queue)
	}
	c.deleted = append(c.deleted, queue)
	delete(c.queues, queue)
	return nil
}
func (c *lifecycleQueueClient) QueueExists(_ context.Context, _, queue string) (bool, error) {
	_, ok := c.queues[queue]
	return ok, nil
}

func resourceTestConfig(groups ...config.ScalingGroup) config.Config {
	return config.Config{
		Namespace: "swlb",
		DataBrokers: []config.DataBroker{
			{ID: "broker-a", MessageVPN: "data-a"},
			{ID: "broker-b", MessageVPN: "data-b"},
			{ID: "broker-c", MessageVPN: "data-c"},
		},
		Groups: groups,
	}
}

func TestProductionRecommendationRejectsMissingResourceManager(t *testing.T) {
	group := productionGroup()
	cfg := resourceTestConfig(group)
	cfg.Runtime.Mode = config.RuntimeModeProduction
	catalog := NewMembershipCatalog()
	baseline, _ := GenesisSnapshot(cfg.Namespace, group)
	_ = catalog.Put(baseline)
	publisher := &captureMembershipPublisher{}
	commands := &CommandPublisher{Publisher: publisher, Catalog: catalog, Deadlines: map[string]time.Duration{group.ID: time.Minute}}
	coordinator, err := controller.Open(memoryControllerStore{}, passthroughFence{}, commands, controller.Options{})
	if err != nil {
		t.Fatal(err)
	}
	applier := &RecommendationController{Controller: coordinator, Catalog: catalog, Config: cfg, Publisher: commands}
	err = applier.ApplyRecommendations(context.Background(), policy.Decision{At: time.Unix(20, 0).UTC()})
	if err == nil {
		t.Fatal("production recommendation controller accepted no resource manager")
	}
}

func TestRecommendationPreparesFencedResourcesBeforeBegin(t *testing.T) {
	group := productionGroup()
	cfg := resourceTestConfig(group)
	catalog := NewMembershipCatalog()
	baseline, err := GenesisSnapshot(cfg.Namespace, group)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Put(baseline); err != nil {
		t.Fatal(err)
	}
	publisher := &captureMembershipPublisher{}
	commands := &CommandPublisher{Publisher: publisher, Catalog: catalog, Deadlines: map[string]time.Duration{group.ID: time.Minute}}
	coordinator, err := controller.Open(memoryControllerStore{}, passthroughFence{}, commands, controller.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	clients := map[string]SEMPQueueClient{
		"broker-a": &lifecycleQueueClient{events: &events},
		"broker-b": &lifecycleQueueClient{events: &events},
		"broker-c": &lifecycleQueueClient{events: &events},
	}
	resources := &ManagedEpochResources{Config: cfg, Clients: clients}
	applier := &RecommendationController{Controller: coordinator, Catalog: catalog, Config: cfg, Publisher: commands, Resources: resources}
	decision := policy.Decision{At: time.Unix(20, 0).UTC(), Transitions: []policy.Recommendation{{
		GroupID: group.ID, Action: policy.ActionScaleOut,
		CurrentMembership: []string{"broker-b", "broker-a"}, ProposedMembership: []string{"broker-b", "broker-a", "broker-c"},
	}}}
	if err := applier.ApplyRecommendations(context.Background(), decision); err != nil {
		t.Fatal(err)
	}
	state, ok := coordinator.Group(group.ID)
	if !ok {
		t.Fatal("transition was not persisted")
	}
	if len(events) != 6 {
		t.Fatalf("resource events = %v", events)
	}
	if len(publisher.updates) != 0 {
		t.Fatal("controller reconciled before resource preparation completed")
	}
	for _, brokerID := range []string{"broker-b", "broker-a", "broker-c"} {
		client := clients[brokerID].(*lifecycleQueueClient)
		if len(client.ensured) != 1 {
			t.Fatalf("broker %s ensured %d queues", brokerID, len(client.ensured))
		}
		spec := client.ensured[0]
		if spec.IngressEnabled || !spec.EgressEnabled || spec.AccessType != config.QueueAccessExclusive || spec.PartitionCount != 0 {
			t.Fatalf("broker %s target queue = %#v", brokerID, spec)
		}
		if !slices.Contains(client.subscriptions[spec.Name], state.Spec.ProposedResources[0].IngressTopic) {
			t.Fatalf("broker %s missing exact epoch subscription", brokerID)
		}
	}
}

func TestManagedEpochResourcesPreserveQueueTypes(t *testing.T) {
	flight := productionGroup()
	flight.ID = "flight"
	flight.Queue = config.Queue{NamePrefix: "swlb.flight", Type: config.QueueTypePartitioned, Access: config.QueueAccessNonExclusive, Partitions: 12, MaxRedeliveries: 5, DeadMessageQueue: "swlb.flight.dmq"}
	flight.OrderedBrokerIDs = []string{"broker-a"}
	baggage := productionGroup()
	baggage.ID = "baggage"
	baggage.Queue = config.Queue{NamePrefix: "swlb.baggage", Type: config.QueueTypeExclusive, Access: config.QueueAccessExclusive, MaxRedeliveries: 5, DeadMessageQueue: "swlb.baggage.dmq"}
	baggage.OrderedBrokerIDs = []string{"broker-b"}
	cfg := resourceTestConfig(flight, baggage)
	clients := map[string]SEMPQueueClient{"broker-a": &lifecycleQueueClient{}, "broker-b": &lifecycleQueueClient{}}
	manager := &ManagedEpochResources{Config: cfg, Clients: clients}
	for _, group := range cfg.Groups {
		snapshot, err := GenesisSnapshot(cfg.Namespace, group)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.EnsureCurrent(context.Background(), snapshot); err != nil {
			t.Fatal(err)
		}
	}
	flightSpec := clients["broker-a"].(*lifecycleQueueClient).ensured[0]
	baggageSpec := clients["broker-b"].(*lifecycleQueueClient).ensured[0]
	if flightSpec.AccessType != config.QueueAccessNonExclusive || flightSpec.PartitionCount != 12 {
		t.Fatalf("Flight queue semantics lost: %#v", flightSpec)
	}
	if baggageSpec.AccessType != config.QueueAccessExclusive || baggageSpec.PartitionCount != 0 {
		t.Fatalf("Baggage queue semantics lost: %#v", baggageSpec)
	}
}

func TestPreparePartialFailureDoesNotBeginOrDeleteAdoptedQueues(t *testing.T) {
	group := productionGroup()
	cfg := resourceTestConfig(group)
	catalog := NewMembershipCatalog()
	baseline, _ := GenesisSnapshot(cfg.Namespace, group)
	_ = catalog.Put(baseline)
	publisher := &captureMembershipPublisher{}
	commands := &CommandPublisher{Publisher: publisher, Catalog: catalog, Deadlines: map[string]time.Duration{group.ID: time.Minute}}
	coordinator, err := controller.Open(memoryControllerStore{}, passthroughFence{}, commands, controller.Options{})
	if err != nil {
		t.Fatal(err)
	}
	clients := map[string]SEMPQueueClient{
		"broker-a": &lifecycleQueueClient{},
		"broker-b": &lifecycleQueueClient{failSubscription: true},
		"broker-c": &lifecycleQueueClient{},
	}
	applier := &RecommendationController{Controller: coordinator, Catalog: catalog, Config: cfg, Publisher: commands, Resources: &ManagedEpochResources{Config: cfg, Clients: clients}}
	err = applier.ApplyRecommendations(context.Background(), policy.Decision{At: time.Unix(20, 0).UTC(), Transitions: []policy.Recommendation{{GroupID: group.ID, Action: policy.ActionScaleOut, CurrentMembership: []string{"broker-b", "broker-a"}, ProposedMembership: []string{"broker-b", "broker-a", "broker-c"}}}})
	if err == nil {
		t.Fatal("partial preparation failure was accepted")
	}
	if _, ok := coordinator.Group(group.ID); ok {
		t.Fatal("transition began before all resources were prepared")
	}
	for brokerID, raw := range clients {
		if deleted := raw.(*lifecycleQueueClient).deleted; len(deleted) != 0 {
			t.Fatalf("partial failure deleted possibly adopted queues on %s: %v", brokerID, deleted)
		}
	}
}

func TestBootstrapEnsuresGenesisResourcesBeforePublication(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group)
	client := &lifecycleQueueClient{}
	manager := &ManagedEpochResources{Config: cfg, Clients: map[string]SEMPQueueClient{"broker-a": client}}
	publisher := &captureMembershipPublisher{}
	if err := BootstrapMembership(context.Background(), cfg, controller.PersistentState{Groups: map[string]*controller.GroupState{}}, staticBrowser{err: ErrNoRetainedMembership}, publisher, NewMembershipCatalog(), manager); err != nil {
		t.Fatal(err)
	}
	if len(client.ensured) != 1 || !client.ensured[0].IngressEnabled || !client.ensured[0].EgressEnabled {
		t.Fatalf("genesis queue = %#v", client.ensured)
	}
	if len(publisher.snapshots) != 1 {
		t.Fatalf("published genesis snapshots = %d", len(publisher.snapshots))
	}
}

func TestBootstrapDoesNotPublishGenesisAfterResourceFailure(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group)
	client := &lifecycleQueueClient{failEnsureAt: 1}
	manager := &ManagedEpochResources{Config: cfg, Clients: map[string]SEMPQueueClient{"broker-a": client}}
	publisher := &captureMembershipPublisher{}
	if err := BootstrapMembership(context.Background(), cfg, controller.PersistentState{Groups: map[string]*controller.GroupState{}}, staticBrowser{err: ErrNoRetainedMembership}, publisher, NewMembershipCatalog(), manager); err == nil {
		t.Fatal("bootstrap published without resources")
	}
	if len(publisher.snapshots) != 0 {
		t.Fatal("genesis membership was published after resource failure")
	}
}

func TestManagedResourceRestartIsIdempotentAndRejectsMismatch(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group)
	client := &lifecycleQueueClient{}
	manager := &ManagedEpochResources{Config: cfg, Clients: map[string]SEMPQueueClient{"broker-a": client}}
	snapshot, _ := GenesisSnapshot(cfg.Namespace, group)
	if err := manager.EnsureCurrent(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureCurrent(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if len(client.queues) != 1 || len(client.subscriptions[snapshot.CurrentResources[0].QueueName]) != 1 {
		t.Fatalf("restart created duplicate resources: queues=%d subscriptions=%v", len(client.queues), client.subscriptions)
	}
	queue := snapshot.CurrentResources[0].QueueName
	mismatch := client.queues[queue]
	mismatch.MaxRedeliveryCount++
	client.queues[queue] = mismatch
	if err := manager.EnsureCurrent(context.Background(), snapshot); err == nil {
		t.Fatal("mismatched existing resource was adopted")
	}
}

func TestManagedResourceBootstrapRecoversPartialFenceAndTargetEnable(t *testing.T) {
	group := productionGroup()
	cfg := resourceTestConfig(group)
	clients := map[string]SEMPQueueClient{
		"broker-a": &lifecycleQueueClient{},
		"broker-b": &lifecycleQueueClient{},
		"broker-c": &lifecycleQueueClient{},
	}
	manager := &ManagedEpochResources{Config: cfg, Clients: clients}
	current, _ := expectedEpochResources(cfg.Namespace, group, []string{"broker-b", "broker-a"}, 1)
	proposed, _ := expectedEpochResources(cfg.Namespace, group, []string{"broker-b", "broker-a", "broker-c"}, 2)
	for _, resource := range current {
		broker, _, _ := manager.broker(resource.BrokerID)
		spec, _ := managedEpochQueueSpec(group, broker.MessageVPN, resource.QueueName, resource.BrokerID == "broker-a")
		clients[resource.BrokerID].(*lifecycleQueueClient).queues = map[string]semp.QueueSpec{resource.QueueName: spec}
	}
	for _, resource := range proposed {
		broker, _, _ := manager.broker(resource.BrokerID)
		spec, _ := managedEpochQueueSpec(group, broker.MessageVPN, resource.QueueName, resource.BrokerID == "broker-b")
		client := clients[resource.BrokerID].(*lifecycleQueueClient)
		if client.queues == nil {
			client.queues = make(map[string]semp.QueueSpec)
		}
		client.queues[resource.QueueName] = spec
	}
	transition := &controller.GroupState{Spec: controller.TransitionSpec{
		Current: []controller.Broker{{ID: "broker-b"}, {ID: "broker-a"}}, Proposed: []controller.Broker{{ID: "broker-b"}, {ID: "broker-a"}, {ID: "broker-c"}},
		FromEpoch: 1, ToEpoch: 2, CurrentResources: current, ProposedResources: proposed,
	}, FenceAttempted: true, TargetIngressEnabled: true}
	snapshot := control.MembershipSnapshot{Namespace: cfg.Namespace, ScalingGroup: group.ID}
	if err := manager.EnsureBootstrap(context.Background(), snapshot, transition); err != nil {
		t.Fatal(err)
	}
	for _, resource := range current {
		client := clients[resource.BrokerID].(*lifecycleQueueClient)
		if client.queues[resource.QueueName].IngressEnabled || !slices.Contains(client.fenced, resource.QueueName) {
			t.Fatalf("source %s was not fenced: %#v", resource.BrokerID, client)
		}
	}
	for _, resource := range proposed {
		client := clients[resource.BrokerID].(*lifecycleQueueClient)
		if !client.queues[resource.QueueName].IngressEnabled || !slices.Contains(client.unfenced, resource.QueueName) {
			t.Fatalf("target %s was not enabled: %#v", resource.BrokerID, client)
		}
	}
}

func TestManagedResourceBootstrapRejectsNonIngressMismatchDuringReplay(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group)
	client := &lifecycleQueueClient{}
	manager := &ManagedEpochResources{Config: cfg, Clients: map[string]SEMPQueueClient{"broker-a": client}}
	current, _ := expectedEpochResources(cfg.Namespace, group, []string{"broker-a"}, 1)
	proposed, _ := expectedEpochResources(cfg.Namespace, group, []string{"broker-a"}, 2)
	currentSpec, _ := managedEpochQueueSpec(group, "data-a", current[0].QueueName, true)
	proposedSpec, _ := managedEpochQueueSpec(group, "data-a", proposed[0].QueueName, false)
	currentSpec.MaxRedeliveryCount++
	client.queues = map[string]semp.QueueSpec{current[0].QueueName: currentSpec, proposed[0].QueueName: proposedSpec}
	transition := &controller.GroupState{Spec: controller.TransitionSpec{
		Current: []controller.Broker{{ID: "broker-a"}}, Proposed: []controller.Broker{{ID: "broker-a"}},
		FromEpoch: 1, ToEpoch: 2, CurrentResources: current, ProposedResources: proposed,
	}, FenceAttempted: true}
	if err := manager.EnsureBootstrap(context.Background(), control.MembershipSnapshot{Namespace: cfg.Namespace, ScalingGroup: group.ID}, transition); err == nil {
		t.Fatal("bootstrap accepted a non-ingress source mismatch")
	}
	if len(client.fenced) != 0 {
		t.Fatal("bootstrap replayed fence before validating exact source configuration")
	}
}

func TestCleanupDeletesOnlyExactUnreferencedOldQueue(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	other := productionGroup()
	other.ID = "baggage"
	other.Queue.NamePrefix = "swlb.baggage"
	other.Queue.DeadMessageQueue = "swlb.baggage.dmq"
	other.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group, other)
	oldResources, _ := expectedEpochResources(cfg.Namespace, group, []string{"broker-a"}, 1)
	newResources, _ := expectedEpochResources(cfg.Namespace, group, []string{"broker-a"}, 2)
	old := oldResources[0]
	client := &lifecycleQueueClient{queues: map[string]semp.QueueSpec{old.QueueName: {Name: old.QueueName}}}
	manager := &ManagedEpochResources{Config: cfg, Clients: map[string]SEMPQueueClient{"broker-a": client}}
	now := time.Unix(100, 0).UTC()
	transition := &controller.GroupState{Spec: controller.TransitionSpec{ID: "move", Group: group.ID, FromEpoch: 1, ToEpoch: 2, DrainGrace: time.Second, CurrentResources: oldResources, ProposedResources: newResources}, Completed: true, CompletedAt: &now}
	evidence := controller.CleanupEvidence{TransitionID: "move", SourceEpoch: 1, TargetEpoch: 2, ZeroSince: now.Add(-2 * time.Second), DrainObservedAt: now.Add(-time.Second), FenceVerifiedAt: now.Add(-time.Second), CommittedAt: now, OldEpochFenced: true, Sources: []controller.DrainSourceEvidence{{Broker: old.BrokerID, Queue: old.QueueName, ZeroSince: now.Add(-2 * time.Second), ObservedAt: now.Add(-time.Second)}}}
	state := controller.PersistentState{Groups: map[string]*controller.GroupState{group.ID: transition}, CleanupEvidence: map[string][]controller.CleanupEvidence{group.ID: {evidence}}}
	catalog := NewMembershipCatalog()
	current := control.MembershipSnapshot{Version: control.SnapshotVersion, Namespace: cfg.Namespace, LibraryVersion: group.CustomerLibrary, ScalingGroup: group.ID, Revision: 6, Epoch: 2, Phase: control.PhaseActive, HashContract: group.HashContract, Algorithm: control.AlgorithmSHA256BigEndianModulo, CurrentMembership: []string{"broker-a"}, Queue: control.QueueInfo{Name: group.Queue.NamePrefix, Durable: true}, Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: newResources[0].IngressTopic}, CurrentResources: newResources}
	if err := catalog.Put(current); err != nil {
		t.Fatal(err)
	}
	otherSnapshot, _ := GenesisSnapshot(cfg.Namespace, other)
	if err := catalog.Put(otherSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := manager.CleanupCompleted(context.Background(), state, catalog); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.deleted, []string{old.QueueName}) {
		t.Fatalf("deleted queues = %v", client.deleted)
	}
	if err := manager.CleanupCompleted(context.Background(), state, catalog); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.deleted, []string{old.QueueName, old.QueueName}) {
		t.Fatalf("restart cleanup was not exact/idempotent: %v", client.deleted)
	}
}

func TestCleanupProtectsQueueReferencedByAnotherGroupOnSharedBroker(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	other := productionGroup()
	other.ID = "baggage"
	other.Queue.NamePrefix = "swlb.baggage"
	other.Queue.DeadMessageQueue = "swlb.baggage.dmq"
	other.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group, other)
	oldResources, _ := expectedEpochResources(cfg.Namespace, group, []string{"broker-a"}, 1)
	newResources, _ := expectedEpochResources(cfg.Namespace, group, []string{"broker-a"}, 2)
	old := oldResources[0]
	client := &lifecycleQueueClient{queues: map[string]semp.QueueSpec{old.QueueName: {Name: old.QueueName}}}
	manager := &ManagedEpochResources{Config: cfg, Clients: map[string]SEMPQueueClient{"broker-a": client}}
	now := time.Unix(100, 0).UTC()
	state := controller.PersistentState{
		Groups: map[string]*controller.GroupState{
			group.ID: {
				Spec: controller.TransitionSpec{
					ID: "move", Group: group.ID, FromEpoch: 1, ToEpoch: 2,
					DrainGrace: time.Second, CurrentResources: oldResources, ProposedResources: newResources,
				},
				Completed: true, CompletedAt: &now,
			},
		},
		CleanupEvidence: map[string][]controller.CleanupEvidence{
			group.ID: {{
				TransitionID: "move", SourceEpoch: 1, TargetEpoch: 2,
				ZeroSince: now.Add(-2 * time.Second), DrainObservedAt: now.Add(-time.Second),
				FenceVerifiedAt: now.Add(-time.Second), CommittedAt: now, OldEpochFenced: true,
				Sources: []controller.DrainSourceEvidence{{
					Broker: old.BrokerID, Queue: old.QueueName,
					ZeroSince: now.Add(-2 * time.Second), ObservedAt: now.Add(-time.Second),
				}},
			}},
		},
	}
	catalog := NewMembershipCatalog()
	for _, entry := range []struct {
		group     config.ScalingGroup
		resources []control.EpochResourceIdentity
		epoch     uint64
	}{{group, newResources, 2}, {other, []control.EpochResourceIdentity{old}, 1}} {
		snapshot := control.MembershipSnapshot{Version: control.SnapshotVersion, Namespace: cfg.Namespace, LibraryVersion: entry.group.CustomerLibrary, ScalingGroup: entry.group.ID, Revision: 6, Epoch: entry.epoch, Phase: control.PhaseActive, HashContract: entry.group.HashContract, Algorithm: control.AlgorithmSHA256BigEndianModulo, CurrentMembership: []string{"broker-a"}, Queue: control.QueueInfo{Name: entry.group.Queue.NamePrefix, Durable: true}, Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: entry.resources[0].IngressTopic}, CurrentResources: entry.resources}
		if err := catalog.Put(snapshot); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.CleanupCompleted(context.Background(), state, catalog); err != nil {
		t.Fatal(err)
	}
	if len(client.deleted) != 0 {
		t.Fatalf("cleanup deleted shared referenced queue: %v", client.deleted)
	}
}
