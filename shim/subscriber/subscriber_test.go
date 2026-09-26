package subscriber

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
)

type fakeConsumer struct {
	mu          sync.Mutex
	activated   int
	paused      int
	closed      int
	activateErr error
	pauseErr    error
	closeErr    error
	deliver     func(context.Context, Delivery) error
}

func (c *fakeConsumer) Activate(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activated++
	return c.activateErr
}
func (c *fakeConsumer) Pause(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused++
	return c.pauseErr
}
func (c *fakeConsumer) Close(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed++
	return c.closeErr
}

type fakeFactory struct {
	mu        sync.Mutex
	consumers map[string]*fakeConsumer
}

func (f *fakeFactory) Prepare(_ context.Context, binding Binding, deliver func(context.Context, Delivery) error) (Consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.consumers == nil {
		f.consumers = make(map[string]*fakeConsumer)
	}
	consumer := &fakeConsumer{deliver: deliver}
	f.consumers[binding.key()] = consumer
	return consumer, nil
}

type fakeReporter struct {
	mu      sync.Mutex
	reports []Readiness
}

func (r *fakeReporter) ReportReadiness(_ context.Context, readiness Readiness) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, readiness)
	return nil
}

type fakeDelivery struct {
	message    customer.MessageView
	acks       atomic.Int32
	rejections atomic.Int32
	ackErr     error
	rejectErr  error
	epoch      uint64
}

func (d *fakeDelivery) Message() customer.MessageView { return d.message }
func (d *fakeDelivery) Ack(context.Context) error {
	d.acks.Add(1)
	return d.ackErr
}
func (d *fakeDelivery) Reject(context.Context) error {
	d.rejections.Add(1)
	return d.rejectErr
}
func (d *fakeDelivery) RoutingMetadata() (RoutingMetadata, error) {
	epoch := d.epoch
	if epoch == 0 {
		epoch = 1
	}
	group := d.message.Headers["group"]
	return RoutingMetadata{
		Group: group, BusinessHash: sha256.Sum256([]byte(d.message.Headers["key"])),
		HashContract: group + "-v1", LibraryVersion: "library-v1", Epoch: epoch,
		OriginalTopic: originalTopic(d.message),
	}, nil
}

type legacyDelivery struct {
	message customer.MessageView
	acks    atomic.Int32
}

func (d *legacyDelivery) Message() customer.MessageView { return d.message }
func (d *legacyDelivery) Ack(context.Context) error {
	d.acks.Add(1)
	return nil
}

type fakeRoutedDelivery struct {
	*fakeDelivery
	metadata RoutingMetadata
	err      error
}

func (d *fakeRoutedDelivery) RoutingMetadata() (RoutingMetadata, error) {
	return d.metadata, d.err
}

func testLibrary() customer.CustomerLibrary {
	return customer.CustomerLibraryFuncs{
		ScalingGroup: func(message customer.MessageView) (string, error) { return message.Headers["group"], nil },
		BusinessHash: func(message customer.MessageView) (customer.BusinessHash, error) {
			return sha256.Sum256([]byte(message.Headers["key"])), nil
		},
	}
}

func testMessage(group, key, id string) customer.MessageView {
	return customer.MessageView{Topic: group + "/events", Headers: map[string]string{"group": group, "key": key}, EventID: id}
}

func originalTopic(message customer.MessageView) string {
	if message.Topic != "" {
		return message.Topic
	}
	return message.Headers["group"] + "/events"
}

func newTestShim(t *testing.T, handler Handler) (*Shim, *fakeFactory, *fakeReporter) {
	t.Helper()
	factory := &fakeFactory{}
	reporter := &fakeReporter{}
	shim, err := New(Config{
		Participant: "subscriber-1", Library: testLibrary(), Handler: handler, Factory: factory, Reporter: reporter,
		Contracts: map[string]Contract{
			"orders":  {HashContract: "orders-v1", LibraryVersion: "library-v1"},
			"billing": {HashContract: "billing-v1", LibraryVersion: "library-v1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return shim, factory, reporter
}

func transition(group, id string) Transition {
	return Transition{
		ID: id, Group: group, SourceEpoch: 1, ProposedEpoch: 2,
		Source:   []Binding{{Group: group, BrokerID: "old", Epoch: 1, Destination: group + "/e1"}},
		Proposed: []Binding{{Group: group, BrokerID: "new-a", Epoch: 2, Destination: group + "/e2/a"}, {Group: group, BrokerID: "new-b", Epoch: 2, Destination: group + "/e2/b"}},
	}
}

func TestNewRequiresCompleteContracts(t *testing.T) {
	base := Config{
		Participant: "subscriber-1", Library: testLibrary(),
		Handler: HandlerFunc(func(context.Context, customer.MessageView) error { return nil }),
		Factory: &fakeFactory{}, Reporter: &fakeReporter{},
	}
	for name, contracts := range map[string]map[string]Contract{
		"missing":       nil,
		"empty group":   {"": {HashContract: "orders-v1", LibraryVersion: "library-v1"}},
		"empty hash":    {"orders": {LibraryVersion: "library-v1"}},
		"empty library": {"orders": {HashContract: "orders-v1"}},
	} {
		t.Run(name, func(t *testing.T) {
			config := base
			config.Contracts = contracts
			if _, err := New(config); err == nil {
				t.Fatal("New() accepted incomplete contracts")
			}
		})
	}
}

func TestApplySnapshotLifecyclePreservesMembershipOrder(t *testing.T) {
	shim, factory, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	resolve := func(broker string, epoch uint64, logical control.DestinationInfo) (string, error) {
		return logical.Name + "/" + broker + "/e" + fmt.Sprint(epoch), nil
	}
	prepare := control.MembershipSnapshot{
		Version:            control.SnapshotVersion,
		LibraryVersion:     "library-v1",
		ScalingGroup:       "orders",
		Revision:           1,
		Epoch:              1,
		Phase:              control.PhasePrepare,
		HashContract:       "orders-v1",
		Algorithm:          control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership:  control.Membership{"old-b", "old-a"},
		ProposedMembership: control.Membership{"new-b", "new-a"},
		Transition:         &control.Transition{ID: "t-snapshot", FromEpoch: 1, ToEpoch: 2},
		Queue:              control.QueueInfo{Name: "orders-queue", Durable: true},
		Destination:        control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders"},
	}
	converted, err := TransitionFromSnapshot(prepare, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if converted.Proposed[0].BrokerID != "new-b" || converted.Proposed[1].BrokerID != "new-a" {
		t.Fatalf("proposed order changed: %#v", converted.Proposed)
	}
	if err := shim.ApplySnapshot(context.Background(), prepare, resolve); err != nil {
		t.Fatal(err)
	}
	paused := prepare.Clone()
	paused.Revision++
	paused.Phase = control.PhasePaused
	if err := shim.ApplySnapshot(context.Background(), paused, resolve); err != nil {
		t.Fatal(err)
	}
	active := control.MembershipSnapshot{
		Version:           control.SnapshotVersion,
		LibraryVersion:    prepare.LibraryVersion,
		ScalingGroup:      prepare.ScalingGroup,
		Revision:          3,
		Epoch:             2,
		Phase:             control.PhaseActive,
		HashContract:      prepare.HashContract,
		Algorithm:         prepare.Algorithm,
		CurrentMembership: prepare.ProposedMembership.Clone(),
		Queue:             prepare.Queue,
		Destination:       prepare.Destination,
	}
	if err := shim.ApplySnapshot(context.Background(), active, resolve); err != nil {
		t.Fatal(err)
	}
	for _, binding := range converted.Proposed {
		if factory.consumers[binding.key()].activated != 1 {
			t.Fatalf("consumer %q not activated", binding.BrokerID)
		}
	}
}

func TestApplySnapshotActiveSourceRollsBackPendingTransition(t *testing.T) {
	shim, factory, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	resolve := func(broker string, epoch uint64, logical control.DestinationInfo) (string, error) {
		return logical.Name + "/" + broker + "/e" + fmt.Sprint(epoch), nil
	}
	prepare := control.MembershipSnapshot{
		Version:            control.SnapshotVersion,
		LibraryVersion:     "library-v1",
		ScalingGroup:       "orders",
		Revision:           1,
		Epoch:              1,
		Phase:              control.PhasePrepare,
		HashContract:       "orders-v1",
		Algorithm:          control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership:  control.Membership{"old-b", "old-a"},
		ProposedMembership: control.Membership{"new-b", "new-a"},
		Transition:         &control.Transition{ID: "t-rollback", FromEpoch: 1, ToEpoch: 2},
		Queue:              control.QueueInfo{Name: "orders-queue", Durable: true},
		Destination:        control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders"},
	}
	for _, broker := range prepare.CurrentMembership {
		name, err := resolve(broker, prepare.Epoch, prepare.Destination)
		if err != nil {
			t.Fatal(err)
		}
		if err := shim.AddActive(context.Background(), Binding{Group: prepare.ScalingGroup, BrokerID: broker, Epoch: prepare.Epoch, Destination: name}); err != nil {
			t.Fatal(err)
		}
	}
	if err := shim.ApplySnapshot(context.Background(), prepare, resolve); err != nil {
		t.Fatal(err)
	}
	if err := shim.PauseSource(context.Background(), prepare.ScalingGroup, prepare.Transition.ID); err != nil {
		t.Fatal(err)
	}
	active := prepare.Clone()
	active.Revision++
	active.Phase = control.PhaseActive
	active.ProposedMembership = nil
	active.Transition = nil
	if err := shim.ApplySnapshot(context.Background(), active, resolve); err != nil {
		t.Fatal(err)
	}
	converted, err := TransitionFromSnapshot(prepare, resolve)
	if err != nil {
		t.Fatal(err)
	}
	for _, binding := range converted.Source {
		consumer := factory.consumers[binding.key()]
		if consumer == nil || consumer.closed != 0 || consumer.paused != 1 || consumer.activated != 2 {
			t.Fatalf("source consumer %#v lifecycle = %#v", binding, consumer)
		}
	}
	for _, binding := range converted.Proposed {
		consumer := factory.consumers[binding.key()]
		if consumer == nil || consumer.closed != 1 || consumer.activated != 0 {
			t.Fatalf("proposed consumer %#v lifecycle = %#v", binding, consumer)
		}
	}
	if err := shim.ApplySnapshot(context.Background(), prepare, resolve); err != nil {
		t.Fatalf("new prepare after rollback failed: %v", err)
	}
}

func TestPrepareRetainsSourceAndReportsReadinessThenActivate(t *testing.T) {
	shim, factory, reporter := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	spec := transition("orders", "t-1")
	if err := shim.AddActive(context.Background(), spec.Source[0]); err != nil {
		t.Fatal(err)
	}
	source := factory.consumers[spec.Source[0].key()]
	if source.activated != 1 {
		t.Fatalf("source activation count = %d", source.activated)
	}
	if err := shim.Prepare(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if source.closed != 0 || source.paused != 0 {
		t.Fatalf("source changed during prepare: %#v", source)
	}
	for _, binding := range spec.Proposed {
		consumer := factory.consumers[binding.key()]
		if consumer == nil || consumer.activated != 0 {
			t.Fatalf("proposed consumer activated before commit: %#v", consumer)
		}
	}
	if len(reporter.reports) != 1 || !reporter.reports[0].Ready || len(reporter.reports[0].Bindings) != 2 {
		t.Fatalf("readiness reports = %#v", reporter.reports)
	}
	if err := shim.PauseSource(context.Background(), spec.Group, spec.ID); err != nil {
		t.Fatal(err)
	}
	if source.paused != 1 {
		t.Fatalf("source pause count = %d", source.paused)
	}
	if err := shim.Activate(context.Background(), spec.Group, spec.ID); err != nil {
		t.Fatal(err)
	}
	if source.closed != 1 {
		t.Fatalf("source close count = %d", source.closed)
	}
	for _, binding := range spec.Proposed {
		if got := factory.consumers[binding.key()].activated; got != 1 {
			t.Fatalf("proposed activation count = %d", got)
		}
	}
}

func TestPrepareDuplicateAndStaleTransition(t *testing.T) {
	shim, _, reporter := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	spec := transition("orders", "t-1")
	if err := shim.AddActive(context.Background(), spec.Source[0]); err != nil {
		t.Fatal(err)
	}
	if err := shim.Prepare(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if err := shim.Prepare(context.Background(), spec); err != nil {
		t.Fatalf("duplicate prepare = %v", err)
	}
	if len(reporter.reports) != 2 || !reporter.reports[1].Ready {
		t.Fatalf("duplicate did not replay readiness: %#v", reporter.reports)
	}
	stale := transition("orders", "t-old")
	if err := shim.Prepare(context.Background(), stale); !errors.Is(err, ErrTransitionInProgress) {
		t.Fatalf("overlapping transition error = %v", err)
	}
	if err := shim.Activate(context.Background(), spec.Group, "t-old"); !errors.Is(err, ErrStaleTransition) {
		t.Fatalf("stale activation error = %v", err)
	}
}

func TestPoisonBlocksOnlySameGroupAndKeyUntilRetry(t *testing.T) {
	var mu sync.Mutex
	attempts := make(map[string]int)
	handler := HandlerFunc(func(_ context.Context, message customer.MessageView) error {
		mu.Lock()
		defer mu.Unlock()
		attempts[message.EventID]++
		if message.EventID == "poison" && attempts[message.EventID] == 1 {
			return errors.New("bad payload")
		}
		return nil
	})
	shim, _, _ := newTestShim(t, handler)
	binding := Binding{Group: "orders", BrokerID: "b1", Epoch: 1, Destination: "orders/e1"}
	poison := &fakeDelivery{message: testMessage("orders", "same", "poison")}
	if err := shim.Handle(context.Background(), binding, poison); err == nil || !RetainedDelivery(err) {
		t.Fatalf("poison delivery error = %v, want retained ownership", err)
	}
	later := &fakeDelivery{message: testMessage("orders", "same", "later")}
	laterDone := make(chan error, 1)
	go func() { laterDone <- shim.Handle(context.Background(), binding, later) }()
	select {
	case err := <-laterDone:
		t.Fatalf("same-key delivery returned while poison was blocked: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	mu.Lock()
	laterAttempts := attempts["later"]
	mu.Unlock()
	if laterAttempts != 0 || later.acks.Load() != 0 {
		t.Fatalf("later same-key delivery was processed: attempts=%d acks=%d", laterAttempts, later.acks.Load())
	}

	otherKey := &fakeDelivery{message: testMessage("orders", "other", "other-key")}
	if err := shim.Handle(context.Background(), binding, otherKey); err != nil {
		t.Fatal(err)
	}
	otherGroupBinding := Binding{Group: "billing", BrokerID: "b2", Epoch: 1, Destination: "billing/e1"}
	otherGroup := &fakeDelivery{message: testMessage("billing", "same", "other-group")}
	if err := shim.Handle(context.Background(), otherGroupBinding, otherGroup); err != nil {
		t.Fatal(err)
	}
	if otherKey.acks.Load() != 1 || otherGroup.acks.Load() != 1 {
		t.Fatal("independent lanes did not acknowledge")
	}

	hash := sha256.Sum256([]byte("same"))
	if err := shim.Retry(context.Background(), "orders", hash); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	poisonAttempts := attempts["poison"]
	mu.Unlock()
	if poison.acks.Load() != 1 || poisonAttempts != 2 {
		t.Fatalf("retry counts: app=%d ack=%d", poisonAttempts, poison.acks.Load())
	}
	select {
	case err := <-laterDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("later delivery did not resume after retry")
	}
	if later.acks.Load() != 1 {
		t.Fatal("later delivery did not proceed after retry")
	}
}

func TestCanceledFollowerPreservesSameKeyFIFO(t *testing.T) {
	entered := make(chan string, 3)
	release := make(chan struct{}, 2)
	shim, _, _ := newTestShim(t, HandlerFunc(func(_ context.Context, message customer.MessageView) error {
		entered <- message.EventID
		<-release
		return nil
	}))
	binding := Binding{Group: "orders", BrokerID: "b1", Epoch: 1, Destination: "orders/e1"}
	done := make(chan error, 3)
	go func() {
		done <- shim.Handle(context.Background(), binding, &fakeDelivery{message: testMessage("orders", "same", "first")})
	}()
	select {
	case id := <-entered:
		if id != "first" {
			t.Fatalf("first entry = %q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("first did not enter")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		done <- shim.Handle(ctx, binding, &fakeDelivery{message: testMessage("orders", "same", "canceled")})
	}()
	go func() {
		done <- shim.Handle(context.Background(), binding, &fakeDelivery{message: testMessage("orders", "same", "last")})
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled follower error = %v", err)
	}
	release <- struct{}{}
	select {
	case id := <-entered:
		if id != "last" {
			t.Fatalf("entry after canceled ticket = %q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("last follower hung behind canceled ticket")
	}
	release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCanceledPoisonFollowerReturnsAndLaneStillUnblocks(t *testing.T) {
	shim, _, _ := newTestShim(t, HandlerFunc(func(_ context.Context, message customer.MessageView) error {
		if message.EventID == "poison" {
			return errors.New("bad")
		}
		return nil
	}))
	binding := Binding{Group: "orders", BrokerID: "b1", Epoch: 1, Destination: "orders/e1"}
	poison := &fakeDelivery{message: testMessage("orders", "same", "poison")}
	if err := shim.Handle(context.Background(), binding, poison); !RetainedDelivery(err) {
		t.Fatalf("poison error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- shim.Handle(ctx, binding, &fakeDelivery{message: testMessage("orders", "same", "follower")})
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("follower error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled poison follower did not return")
	}
	poison.rejectErr = nil
	if err := shim.Release(context.Background(), "orders", sha256.Sum256([]byte("same"))); err != nil {
		t.Fatal(err)
	}
	if err := shim.Handle(context.Background(), binding, &fakeDelivery{message: testMessage("orders", "same", "after")}); err != nil {
		t.Fatalf("lane failed after canceled follower: %v", err)
	}
}

func TestCompletedLanesAreReclaimed(t *testing.T) {
	shim, _, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	binding := Binding{Group: "orders", BrokerID: "b1", Epoch: 1, Destination: "orders/e1"}
	for index := range 1000 {
		key := fmt.Sprintf("key-%d", index)
		if err := shim.Handle(context.Background(), binding, &fakeDelivery{message: testMessage("orders", key, key)}); err != nil {
			t.Fatal(err)
		}
	}
	shim.lanesMu.Lock()
	count := len(shim.lanes)
	shim.lanesMu.Unlock()
	if count != 0 {
		t.Fatalf("completed lanes retained = %d", count)
	}
}

func TestRoutedDeliveryMetadataMustMatchLibraryAndBinding(t *testing.T) {
	shim, _, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	binding := Binding{Group: "orders", BrokerID: "b1", Epoch: 7, Destination: "orders/e7"}
	message := testMessage("orders", "one", "event")
	message.Topic = "orders/events"
	hash := sha256.Sum256([]byte("one"))

	valid := &fakeRoutedDelivery{
		fakeDelivery: &fakeDelivery{message: message},
		metadata: RoutingMetadata{
			Group: "orders", BusinessHash: hash, HashContract: "orders-v1",
			LibraryVersion: "library-v1", Epoch: 7, OriginalTopic: "orders/events",
		},
	}
	if err := shim.Handle(context.Background(), binding, valid); err != nil {
		t.Fatal(err)
	}
	if valid.acks.Load() != 1 {
		t.Fatal("valid routed delivery was not acknowledged")
	}

	mismatch := &fakeRoutedDelivery{
		fakeDelivery: &fakeDelivery{message: message},
		metadata: RoutingMetadata{
			Group: "orders", BusinessHash: sha256.Sum256([]byte("other")), HashContract: "orders-v1",
			LibraryVersion: "library-v1", Epoch: 7, OriginalTopic: "orders/events",
		},
	}
	if err := shim.Handle(context.Background(), binding, mismatch); err == nil {
		t.Fatal("mismatched routed hash was accepted")
	}
	if mismatch.acks.Load() != 0 {
		t.Fatal("mismatched routed delivery was acknowledged")
	}

	for name, mutate := range map[string]func(*RoutingMetadata){
		"hash contract":   func(metadata *RoutingMetadata) { metadata.HashContract = "orders-v2" },
		"library version": func(metadata *RoutingMetadata) { metadata.LibraryVersion = "library-v2" },
	} {
		t.Run(name, func(t *testing.T) {
			metadata := valid.metadata
			mutate(&metadata)
			delivery := &fakeRoutedDelivery{fakeDelivery: &fakeDelivery{message: message}, metadata: metadata}
			if err := shim.Handle(context.Background(), binding, delivery); !errors.Is(err, ErrUnexpectedContract) {
				t.Fatalf("Handle() error = %v, want ErrUnexpectedContract", err)
			}
			if delivery.acks.Load() != 0 {
				t.Fatal("delivery with unexpected routing contract was acknowledged")
			}
		})
	}
}

func TestHandleRequiresAuthoritativeRoutingMetadata(t *testing.T) {
	shim, _, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	binding := Binding{Group: "orders", BrokerID: "b1", Epoch: 1, Destination: "orders/e1"}
	delivery := &legacyDelivery{message: testMessage("orders", "one", "event")}
	if err := shim.Handle(context.Background(), binding, delivery); !errors.Is(err, ErrRoutingMetadata) {
		t.Fatalf("Handle() error = %v, want ErrRoutingMetadata", err)
	}
	if delivery.acks.Load() != 0 {
		t.Fatal("metadata-free delivery was acknowledged")
	}
}

func TestApplySnapshotRequiresConfiguredContract(t *testing.T) {
	shim, _, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	resolve := func(broker string, epoch uint64, logical control.DestinationInfo) (string, error) {
		return logical.Name + "/" + broker + "/e" + fmt.Sprint(epoch), nil
	}
	valid := control.MembershipSnapshot{
		Version: control.SnapshotVersion, LibraryVersion: "library-v1", ScalingGroup: "orders", Revision: 1, Epoch: 1,
		Phase: control.PhaseActive, HashContract: "orders-v1", Algorithm: control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: control.Membership{"broker-a"}, Queue: control.QueueInfo{Name: "orders", Durable: true},
		Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders"},
	}
	for name, mutate := range map[string]func(*control.MembershipSnapshot){
		"hash contract":   func(snapshot *control.MembershipSnapshot) { snapshot.HashContract = "orders-v2" },
		"library version": func(snapshot *control.MembershipSnapshot) { snapshot.LibraryVersion = "library-v2" },
		"unknown group":   func(snapshot *control.MembershipSnapshot) { snapshot.ScalingGroup = "unknown" },
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := valid.Clone()
			mutate(&snapshot)
			if err := shim.ApplySnapshot(context.Background(), snapshot, resolve); !errors.Is(err, ErrUnexpectedContract) {
				t.Fatalf("ApplySnapshot() error = %v, want ErrUnexpectedContract", err)
			}
		})
	}
}

func TestSameKeySerializedAndAckAfterApplication(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	handler := HandlerFunc(func(_ context.Context, message customer.MessageView) error {
		current := active.Add(1)
		for {
			max := maxActive.Load()
			if current <= max || maxActive.CompareAndSwap(max, current) {
				break
			}
		}
		entered <- message.EventID
		<-release
		active.Add(-1)
		return nil
	})
	shim, _, _ := newTestShim(t, handler)
	binding := Binding{Group: "orders", BrokerID: "b1", Epoch: 1, Destination: "orders/e1"}
	first := &fakeDelivery{message: testMessage("orders", "one", "first")}
	second := &fakeDelivery{message: testMessage("orders", "one", "second")}
	done := make(chan error, 2)
	go func() { done <- shim.Handle(context.Background(), binding, first) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first handler did not start")
	}
	go func() { done <- shim.Handle(context.Background(), binding, second) }()
	select {
	case id := <-entered:
		t.Fatalf("same-key handler %q overlapped", id)
	case <-time.After(25 * time.Millisecond):
	}
	if first.acks.Load() != 0 {
		t.Fatal("first delivery ACKed before application completed")
	}
	release <- struct{}{}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("second handler did not start after first")
	}
	release <- struct{}{}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if maxActive.Load() != 1 || first.acks.Load() != 1 || second.acks.Load() != 1 {
		t.Fatalf("serialization/ack failure: max=%d acks=%d,%d", maxActive.Load(), first.acks.Load(), second.acks.Load())
	}
}

func TestAckFailureRetryDoesNotRepeatApplication(t *testing.T) {
	var attempts atomic.Int32
	shim, _, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error {
		attempts.Add(1)
		return nil
	}))
	binding := Binding{Group: "orders", BrokerID: "b1", Epoch: 1, Destination: "orders/e1"}
	delivery := &fakeDelivery{message: testMessage("orders", "one", "event"), ackErr: errors.New("transport down")}
	if err := shim.Handle(context.Background(), binding, delivery); err == nil || !RetainedDelivery(err) {
		t.Fatalf("ACK failure error = %v, want retained ownership", err)
	}
	delivery.ackErr = nil
	hash := sha256.Sum256([]byte("one"))
	if err := shim.Retry(context.Background(), "orders", hash); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 1 || delivery.acks.Load() != 2 {
		t.Fatalf("application/ack attempts = %d/%d", attempts.Load(), delivery.acks.Load())
	}
}

func TestActivateCloseFailureKeepsTransitionRetryable(t *testing.T) {
	shim, factory, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	spec := transition("orders", "t-close-retry")
	if err := shim.AddActive(context.Background(), spec.Source[0]); err != nil {
		t.Fatal(err)
	}
	if err := shim.Prepare(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	source := factory.consumers[spec.Source[0].key()]
	source.closeErr = errors.New("close unavailable")
	if err := shim.Activate(context.Background(), spec.Group, spec.ID); err == nil {
		t.Fatal("activation succeeded despite source close failure")
	}
	source.closeErr = nil
	if err := shim.Activate(context.Background(), spec.Group, spec.ID); err != nil {
		t.Fatalf("activation retry failed: %v", err)
	}
	for _, binding := range spec.Proposed {
		consumer := factory.consumers[binding.key()]
		consumer.mu.Lock()
		activated := consumer.activated
		consumer.mu.Unlock()
		if activated != 1 {
			t.Fatalf("proposed %q activated %d times", binding.BrokerID, activated)
		}
	}
}

func TestRollbackCloseFailureKeepsTransitionRetryable(t *testing.T) {
	shim, factory, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	spec := transition("orders", "t-rollback-close")
	if err := shim.AddActive(context.Background(), spec.Source[0]); err != nil {
		t.Fatal(err)
	}
	if err := shim.Prepare(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	failing := factory.consumers[spec.Proposed[0].key()]
	failing.closeErr = errors.New("close unavailable")
	if err := shim.Rollback(context.Background(), spec.Group, spec.ID); err == nil {
		t.Fatal("rollback succeeded despite prepared close failure")
	}
	failing.closeErr = nil
	if err := shim.Rollback(context.Background(), spec.Group, spec.ID); err != nil {
		t.Fatalf("rollback retry failed: %v", err)
	}
}

func TestCloseFailureRetainsConsumerForRetry(t *testing.T) {
	shim, factory, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	binding := Binding{Group: "orders", BrokerID: "a", Epoch: 1, Destination: "orders-a-e1"}
	if err := shim.AddActive(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	consumer := factory.consumers[binding.key()]
	consumer.closeErr = errors.New("close unavailable")
	if err := shim.Close(context.Background()); err == nil {
		t.Fatal("Close() succeeded despite consumer close failure")
	}
	consumer.closeErr = nil
	if err := shim.Close(context.Background()); err != nil {
		t.Fatalf("Close() retry failed: %v", err)
	}
	consumer.mu.Lock()
	closed := consumer.closed
	consumer.mu.Unlock()
	if closed != 2 {
		t.Fatalf("close attempts = %d, want 2", closed)
	}
}

func TestConcurrentSnapshotAndTransitionOperationsAreSafe(t *testing.T) {
	shim, _, _ := newTestShim(t, HandlerFunc(func(context.Context, customer.MessageView) error { return nil }))
	resolve := func(broker string, epoch uint64, logical control.DestinationInfo) (string, error) {
		return logical.Name + "/" + broker + "/e" + fmt.Sprint(epoch), nil
	}
	snapshot := control.MembershipSnapshot{
		Version: control.SnapshotVersion, LibraryVersion: "library-v1", ScalingGroup: "orders", Revision: 1, Epoch: 1,
		Phase: control.PhaseActive, HashContract: "orders-v1", Algorithm: control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: control.Membership{"old"}, Queue: control.QueueInfo{Name: "orders", Durable: true},
		Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders"},
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = shim.ApplySnapshot(context.Background(), snapshot, resolve)
		}()
		go func() {
			defer wg.Done()
			_ = shim.Prepare(context.Background(), transition("orders", "concurrent"))
		}()
	}
	wg.Wait()
}

func TestCloseClosesActiveAndPreparedConsumersOnce(t *testing.T) {
	library := testLibrary()
	factory := &fakeFactory{}
	shim, err := New(Config{
		Participant: "subscriber-1", Library: library, Handler: HandlerFunc(func(context.Context, customer.MessageView) error { return nil }),
		Factory: factory, Reporter: &fakeReporter{}, Contracts: map[string]Contract{"orders": {HashContract: "orders-v1", LibraryVersion: "library-v1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	active := Binding{Group: "orders", BrokerID: "a", Epoch: 1, Destination: "orders-a-e1"}
	if err := shim.AddActive(context.Background(), active); err != nil {
		t.Fatal(err)
	}
	transition := Transition{ID: "t1", Group: "orders", SourceEpoch: 1, ProposedEpoch: 2, Source: []Binding{active}, Proposed: []Binding{{Group: "orders", BrokerID: "a", Epoch: 2, Destination: "orders-a-e2"}}}
	if err := shim.Prepare(context.Background(), transition); err != nil {
		t.Fatal(err)
	}
	if err := shim.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := shim.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(factory.consumers) != 2 {
		t.Fatalf("consumers = %d", len(factory.consumers))
	}
	for _, consumer := range factory.consumers {
		consumer.mu.Lock()
		closed := consumer.closed
		consumer.mu.Unlock()
		if closed != 1 {
			t.Fatalf("consumer closes = %d, want 1", closed)
		}
	}
}

func TestReleaseRejectsPoisonBeforeUnblocking(t *testing.T) {
	shim, _, _ := newTestShim(t, HandlerFunc(func(_ context.Context, message customer.MessageView) error {
		if message.EventID == "poison" {
			return errors.New("poison")
		}
		return nil
	}))
	binding := Binding{Group: "orders", BrokerID: "b1", Epoch: 1, Destination: "orders/e1"}
	delivery := &fakeDelivery{message: testMessage("orders", "one", "poison")}
	if err := shim.Handle(context.Background(), binding, delivery); err == nil {
		t.Fatal("poison delivery unexpectedly succeeded")
	}
	followerDone := make(chan error, 1)
	go func() {
		followerDone <- shim.Handle(context.Background(), binding, &fakeDelivery{message: testMessage("orders", "one", "follower")})
	}()
	hash := sha256.Sum256([]byte("one"))
	delivery.rejectErr = errors.New("transport")
	if err := shim.Release(context.Background(), "orders", hash); err == nil {
		t.Fatal("release succeeded without settlement")
	}
	select {
	case err := <-followerDone:
		t.Fatalf("follower unblocked after failed settlement: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	delivery.rejectErr = nil
	if err := shim.Release(context.Background(), "orders", hash); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-followerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("follower remained blocked after successful settlement")
	}
	if delivery.acks.Load() != 0 || delivery.rejections.Load() != 2 {
		t.Fatalf("release settlements: acks=%d rejects=%d", delivery.acks.Load(), delivery.rejections.Load())
	}
}
