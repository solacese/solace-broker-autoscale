package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	"solace.dev/go/messaging/pkg/solace"
)

func TestParticipantGroupsSelectsOnlyRequiredRoleGroups(t *testing.T) {
	cfg := config.Config{
		Runtime: config.RuntimeIdentities{Publishers: []string{"publisher-1"}, Subscribers: []string{"subscriber-1"}},
		Groups: []config.ScalingGroup{
			{ID: "flight", RequiredPublishers: []string{"publisher-1"}, RequiredSubscribers: []string{"subscriber-1"}},
			{ID: "baggage", RequiredPublishers: []string{"publisher-2"}, RequiredSubscribers: []string{"subscriber-1"}},
		},
	}
	groups, err := ParticipantGroups(cfg, "publisher-1", control.RolePublisher)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].ID != "flight" {
		t.Fatalf("groups = %#v", groups)
	}
}

func TestConnectDataServicesUsesAssignedGroupEligibility(t *testing.T) {
	cfg := config.Config{
		DataBrokers: []config.DataBroker{
			{ID: "initial-a", EligibleGroups: []string{"flight"}},
			{ID: "scale-out-a", EligibleGroups: []string{"flight"}},
			{ID: "shared", EligibleGroups: []string{"flight", "baggage"}},
			{ID: "other", EligibleGroups: []string{"cargo"}},
			{ID: "disabled", EligibleGroups: []string{}},
		},
	}
	groups := []config.ScalingGroup{{ID: "flight", OrderedBrokerIDs: []string{"initial-a"}}, {ID: "baggage"}}
	credentials := Credentials{DataBrokers: map[string]BrokerCredentials{
		"initial-a": {SMFUsername: "initial-user"}, "scale-out-a": {SMFUsername: "scale-user"}, "shared": {SMFUsername: "shared-user"},
	}}
	var connected []string
	assembler := ParticipantAssembler{ConnectData: func(_ context.Context, broker config.DataBroker, credential BrokerCredentials, participant string) (solace.MessagingService, error) {
		if credential.SMFUsername == "" || participant != "publisher-1" {
			t.Fatalf("connection for %q used unexpected credentials or participant", broker.ID)
		}
		connected = append(connected, broker.ID)
		return nil, nil
	}}
	services, err := connectDataServices(context.Background(), cfg, groups, credentials, "publisher-1", assembler, &CloseGroup{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"initial-a", "scale-out-a", "shared"}
	if !slices.Equal(connected, want) {
		t.Fatalf("connected brokers = %v, want %v", connected, want)
	}
	if len(services) != len(want) {
		t.Fatalf("services = %v, want keys %v", maps.Keys(services), want)
	}
	for _, brokerID := range want {
		if _, ok := services[brokerID]; !ok {
			t.Fatalf("missing service for eligible broker %q", brokerID)
		}
	}
}

func TestParticipantRegistrationPublishesAppliedSnapshotScope(t *testing.T) {
	published := make(chan control.RegistrationEnvelope, 1)
	registration := &ParticipantRegistration{
		Participant: "subscriber-1", Role: control.RoleSubscriber, ConsumerSet: "subscriber-1",
		Publisher: registrationPublisherFunc(func(_ context.Context, envelope control.RegistrationEnvelope) error {
			published <- envelope
			return nil
		}),
		Now: func() time.Time { return time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC) },
	}
	snapshot := control.MembershipSnapshot{
		Version: control.SnapshotVersion, Namespace: "swlb", LibraryVersion: "airline-routing-v1",
		ScalingGroup: "flight", Revision: 1, Epoch: 1, Phase: control.PhaseActive,
		HashContract: "flight-operations-v1", Algorithm: control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: control.Membership{"broker-a"}, Queue: control.QueueInfo{Name: "swlb.flight", Durable: true},
		Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "swlb/data/flight/epoch/1"},
	}
	if err := registration.Publish(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	got := <-published
	if got.Group != snapshot.ScalingGroup || got.Epoch != snapshot.Epoch || got.Phase != snapshot.Phase || got.LibraryVersion != snapshot.LibraryVersion || got.Participant != "subscriber-1" || got.Role != control.RoleSubscriber {
		t.Fatalf("registration = %#v", got)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCommandWaitsForMatchingAuthoritativeSnapshot(t *testing.T) {
	state := NewParticipantPhaseState()
	issuedAt := time.Now().UTC()
	command := control.CommandEnvelope{
		Version: control.ProtocolVersion, MessageID: "command-1", Namespace: "swlb", Group: "flight",
		TransitionID: "transition-1", Epoch: 2, Phase: control.PhasePrepare, Participant: "subscriber-1",
		Role: control.RoleSubscriber, IssuedAt: issuedAt, Deadline: issuedAt.Add(time.Second),
	}
	result := make(chan error, 1)
	go func() { result <- state.Wait(context.Background(), command) }()
	select {
	case err := <-result:
		t.Fatalf("wait completed before snapshot: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	state.Record(control.MembershipSnapshot{
		Version: control.SnapshotVersion, Namespace: "swlb", LibraryVersion: "v1", ScalingGroup: "flight",
		Revision: 2, Epoch: 1, Phase: control.PhasePrepare, HashContract: "contract",
		Algorithm: control.AlgorithmSHA256BigEndianModulo, CurrentMembership: control.Membership{"a"},
		ProposedMembership: control.Membership{"a", "b"}, Transition: &control.Transition{ID: "transition-1", FromEpoch: 1, ToEpoch: 2},
		Queue: control.QueueInfo{Name: "queue", Durable: true}, Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "topic"},
	})
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestPublisherRunDispatchRetriesImmediateRejectionAndKeepsOtherLanesRunning(t *testing.T) {
	store, err := outbox.Open(filepath.Join(t.TempDir(), "outbox.db"), outbox.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	library := customer.CustomerLibraryFuncs{
		ScalingGroup: func(customer.MessageView) (string, error) { return "orders", nil },
		BusinessHash: func(message customer.MessageView) (customer.BusinessHash, error) {
			return sha256.Sum256([]byte(message.Headers["key"])), nil
		},
	}
	publisher, err := shimPublisher.New(shimPublisher.Config{
		Outbox: store, CustomerLibrary: library, Broker: runtimeBrokerStub{},
		ScalingGroup: "orders", HashContract: "contract", LibraryVersion: "v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.ApplyMembership(control.MembershipSnapshot{
		Version: control.SnapshotVersion, Namespace: "swlb", LibraryVersion: "v1", ScalingGroup: "orders",
		Revision: 1, Epoch: 1, Phase: control.PhaseActive, HashContract: "contract",
		Algorithm: control.AlgorithmSHA256BigEndianModulo, CurrentMembership: control.Membership{"broker-a"},
		Queue: control.QueueInfo{Name: "queue", Durable: true}, Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "topic"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ id, key string }{{"retry", "lane-a"}, {"success", "lane-b"}} {
		if _, err := publisher.Accept(customer.MessageView{EventID: item.id, Topic: "events", Headers: map[string]string{"key": item.key}, Payload: []byte(item.id)}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	attempts := make(map[string]int)
	retryErr := errors.New("native buffer full")
	broker := runtimeAsyncBrokerFunc(func(_ context.Context, _ string, message shimPublisher.BrokerMessage, attempt shimPublisher.PublishAttempt) (shimPublisher.AsyncPublishFuture, error) {
		mu.Lock()
		attempts[message.EventID]++
		count := attempts[message.EventID]
		mu.Unlock()
		if message.EventID == "retry" && count == 1 {
			return nil, retryErr
		}
		return runtimePublishFuture{result: shimPublisher.AsyncPublishResult{Attempt: attempt, Outcome: shimPublisher.OutcomeAcknowledged}}, nil
	})
	dispatcher, err := shimPublisher.NewAsyncDispatcher(publisher, broker, shimPublisher.AsyncDispatcherConfig{
		MaxInFlight: 2, AckTimeout: time.Second, CompletionBatchSize: 2, CompletionFlushPeriod: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	participant := &PublisherParticipant{Dispatcher: dispatcher, BatchSize: 2, BatchWait: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- participant.runDispatch(ctx) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		retries := attempts["retry"]
		successes := attempts["success"]
		mu.Unlock()
		if retries >= 2 && successes == 1 && store.Stats().Messages == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	status := dispatcher.Status("orders")
	if status.Rejected != 1 || status.Acknowledged != 2 || !errors.Is(status.LastError, shimPublisher.ErrPublishRejected) || !errors.Is(status.LastError, retryErr) {
		t.Fatalf("status = %+v", status)
	}
	if store.Stats().Messages != 0 {
		t.Fatalf("outbox retained %d messages", store.Stats().Messages)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("runDispatch() error = %v", err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := dispatcher.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

type runtimeBrokerStub struct{}

func (runtimeBrokerStub) Publish(context.Context, string, shimPublisher.BrokerMessage) (shimPublisher.PublishOutcome, error) {
	return shimPublisher.OutcomeRejected, errors.New("unused")
}

type runtimeAsyncBrokerFunc func(context.Context, string, shimPublisher.BrokerMessage, shimPublisher.PublishAttempt) (shimPublisher.AsyncPublishFuture, error)

func (f runtimeAsyncBrokerFunc) PublishAsync(ctx context.Context, broker string, message shimPublisher.BrokerMessage, attempt shimPublisher.PublishAttempt) (shimPublisher.AsyncPublishFuture, error) {
	return f(ctx, broker, message, attempt)
}

type runtimePublishFuture struct {
	result shimPublisher.AsyncPublishResult
}

func (f runtimePublishFuture) Await(context.Context) (shimPublisher.AsyncPublishResult, error) {
	return f.result, nil
}

func TestPublisherSnapshotApplierFailClosedRemovesAuthority(t *testing.T) {
	// The publisher shim behavior itself is covered in shim/publisher. This test
	// verifies the runtime adapter rejects a nil/unconfigured transport instead of
	// silently claiming fail-closed success.
	applier := &PublisherSnapshotApplier{}
	if err := applier.FailClosed(context.Background(), "flight", broker0.ErrMembershipStale); err == nil {
		t.Fatal("expected unconfigured publisher applier failure")
	}
}

type registrationPublisherFunc func(context.Context, control.RegistrationEnvelope) error

func (f registrationPublisherFunc) PublishRegistration(ctx context.Context, registration control.RegistrationEnvelope) error {
	return f(ctx, registration)
}
