package publisher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
	"github.com/solacese/solace-workload-balancer/routing"
)

type fixedLibrary struct{}

func (fixedLibrary) GetScalingGroup(message customer.MessageView) (string, error) {
	if group := message.Headers["group"]; group != "" {
		return group, nil
	}
	return "orders", nil
}

func (fixedLibrary) GetBusinessHash(message customer.MessageView) (customer.BusinessHash, error) {
	key, _ := message.Payload.(string)
	return sha256.Sum256([]byte(key)), nil
}

type brokerResult struct {
	outcome PublishOutcome
	err     error
}

type fakeBroker struct {
	results []brokerResult
	calls   []brokerCall
}

type brokerCall struct {
	broker  string
	message BrokerMessage
}

func (b *fakeBroker) Publish(_ context.Context, broker string, message BrokerMessage) (PublishOutcome, error) {
	b.calls = append(b.calls, brokerCall{broker: broker, message: message})
	if len(b.results) == 0 {
		return OutcomeAcknowledged, nil
	}
	result := b.results[0]
	b.results = b.results[1:]
	return result.outcome, result.err
}

func TestAcceptFailsClosedBeforeMembership(t *testing.T) {
	publisher, store, broker := newTestPublisher(t)
	_, err := publisher.Accept(message("event-1", "key-1"))
	if !errors.Is(err, ErrNoValidMembership) {
		t.Fatalf("accept error = %v, want ErrNoValidMembership", err)
	}
	if stats := store.Stats(); stats.Messages != 0 {
		t.Fatalf("outbox contains %d messages after fail-closed accept", stats.Messages)
	}
	if len(broker.calls) != 0 {
		t.Fatalf("Accept made %d broker calls", len(broker.calls))
	}
}

func TestApplyMembershipRejectsWrongHashContract(t *testing.T) {
	publisher, _, _ := newTestPublisher(t)
	snapshot := activeSnapshot(1, 1, "broker-a")
	snapshot.HashContract = "incompatible-v2"
	if err := publisher.ApplyMembership(snapshot); err == nil {
		t.Fatal("incompatible membership contract was accepted")
	}
}

func TestApplyMembershipRejectsWrongLibraryVersion(t *testing.T) {
	publisher, _, _ := newTestPublisher(t)
	snapshot := activeSnapshot(1, 1, "broker-a")
	snapshot.LibraryVersion = "library-v2"
	if err := publisher.ApplyMembership(snapshot); err == nil {
		t.Fatal("incompatible membership library version was accepted")
	}
}

func TestPublisherUsesPerGroupLibraryVersions(t *testing.T) {
	store, err := outbox.Open(filepath.Join(t.TempDir(), "outbox.db"), outbox.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	broker := &fakeBroker{}
	publisher, err := New(Config{
		Outbox: store, CustomerLibrary: fixedLibrary{}, Broker: broker,
		Contracts: map[string]Contract{
			"orders":  {HashContract: "sha256/orders-v1", LibraryVersion: "orders-library-v1"},
			"billing": {HashContract: "sha256/billing-v2", LibraryVersion: "billing-library-v3"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	orders := activeSnapshot(1, 1, "broker-a")
	orders.HashContract = "sha256/orders-v1"
	orders.LibraryVersion = "orders-library-v1"
	billing := orders.Clone()
	billing.ScalingGroup = "billing"
	billing.HashContract = "sha256/billing-v2"
	billing.LibraryVersion = "billing-library-v3"
	billing.Queue.Name = "billing"
	billing.Destination.Name = "billing/input"
	apply(t, publisher, orders)
	apply(t, publisher, billing)

	ordersMessage := messageForGroup("orders-event", "orders-key", "orders")
	billingMessage := messageForGroup("billing-event", "billing-key", "billing")
	if _, err := publisher.Accept(ordersMessage); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Accept(billingMessage); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Dispatch(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	got := make(map[string][2]string, len(broker.calls))
	for _, call := range broker.calls {
		got[call.message.Properties[PropertyScalingGroup]] = [2]string{
			call.message.Properties[PropertyHashContract], call.message.Properties[PropertyLibraryVersion],
		}
	}
	want := map[string][2]string{
		"orders":  {"sha256/orders-v1", "orders-library-v1"},
		"billing": {"sha256/billing-v2", "billing-library-v3"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("published contracts = %#v, want %#v", got, want)
	}

	wrongBilling := billing.Clone()
	wrongBilling.Revision++
	wrongBilling.LibraryVersion = "orders-library-v1"
	if err := publisher.ApplyMembership(wrongBilling); err == nil {
		t.Fatal("billing snapshot with another group's library version was accepted")
	}
}

func TestNewSingleGroupShorthandRequiresExplicitGroup(t *testing.T) {
	store, err := outbox.Open(filepath.Join(t.TempDir(), "outbox.db"), outbox.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{
		Outbox: store, CustomerLibrary: fixedLibrary{}, Broker: &fakeBroker{},
		HashContract: "sha256/customer-v1", LibraryVersion: "library-v1",
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New() error = %v, want ErrInvalidConfig", err)
	}
}

func TestApplyMembershipExactReplayIsIdempotent(t *testing.T) {
	publisher, _, _ := newTestPublisher(t)
	snapshot := activeSnapshot(1, 1, "broker-a")
	apply(t, publisher, snapshot)
	if err := publisher.ApplyMembership(snapshot); err != nil {
		t.Fatalf("exact membership replay was rejected: %v", err)
	}
}

func TestAcceptRejectsReservedHeadersBeforeDurableWrite(t *testing.T) {
	tests := []string{PropertyEventID, "swlb.future_property", PropertyQueuePartition}
	for _, key := range tests {
		t.Run(key, func(t *testing.T) {
			publisher, store, broker := newTestPublisher(t)
			apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
			msg := message("event-1", "key-1")
			msg.Headers[key] = "forged"
			if _, err := publisher.Accept(msg); !errors.Is(err, ErrReservedProperty) {
				t.Fatalf("Accept error = %v, want ErrReservedProperty", err)
			}
			if stats := store.Stats(); stats.Messages != 0 {
				t.Fatalf("outbox contains %d messages after rejected accept", stats.Messages)
			}
			if len(broker.calls) != 0 {
				t.Fatalf("Accept made %d broker calls", len(broker.calls))
			}
		})
	}
}

func TestAcceptBatchRejectsReservedHeaderAtomically(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	valid := message("event-1", "key-1")
	reserved := message("event-2", "key-2")
	reserved.Headers[PropertyQueuePartition] = "forged"
	if _, err := publisher.AcceptBatch([]customer.MessageView{valid, reserved}); !errors.Is(err, ErrReservedProperty) {
		t.Fatalf("AcceptBatch error = %v, want ErrReservedProperty", err)
	}
	if stats := store.Stats(); stats.Messages != 0 {
		t.Fatalf("outbox contains %d messages after rejected batch", stats.Messages)
	}
}

func TestAcceptIsDurableButDoesNotPublish(t *testing.T) {
	publisher, store, broker := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a", "broker-b"))
	receipt, err := publisher.Accept(message("event-1", "key-1"))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.State != outbox.StateReady {
		t.Fatalf("receipt state = %q, want ready", receipt.State)
	}
	if len(broker.calls) != 0 {
		t.Fatalf("Accept made %d broker calls", len(broker.calls))
	}
	if _, err := store.Get(receipt.EventID); err != nil {
		t.Fatalf("accepted event was not durable: %v", err)
	}
}

func TestPauseBuffersAndActivationAssignsWithoutOvertaking(t *testing.T) {
	publisher, store, broker := newTestPublisher(t)
	apply(t, publisher, pausedSnapshot(2, 1))
	first, err := publisher.Accept(message("event-1", "same-key"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := publisher.Accept(message("event-2", "same-key"))
	if err != nil {
		t.Fatal(err)
	}
	if first.State != outbox.StateUnassigned || second.State != outbox.StateUnassigned {
		t.Fatalf("paused receipts = %q, %q; want unassigned", first.State, second.State)
	}
	if report, err := publisher.Dispatch(context.Background(), 10); err != nil || report.Attempted != 0 {
		t.Fatalf("dispatch while paused = %+v, %v", report, err)
	}

	apply(t, publisher, activeSnapshot(3, 2, "broker-b", "broker-c"))
	firstRecord, err := store.Get(first.EventID)
	if err != nil {
		t.Fatal(err)
	}
	secondRecord, err := store.Get(second.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if firstRecord.State != outbox.StateReady || secondRecord.State != outbox.StateReady || firstRecord.Epoch != 2 || secondRecord.Epoch != 2 {
		t.Fatalf("activated records = %#v, %#v", firstRecord, secondRecord)
	}

	report, err := publisher.Dispatch(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if report.Acknowledged != 1 || len(broker.calls) != 1 || broker.calls[0].message.EventID != first.EventID {
		t.Fatalf("first dispatch report=%+v calls=%#v", report, broker.calls)
	}
	report, err = publisher.Dispatch(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if report.Acknowledged != 1 || len(broker.calls) != 2 || broker.calls[1].message.EventID != second.EventID {
		t.Fatalf("second dispatch report=%+v calls=%#v", report, broker.calls)
	}
}

func TestPrepareRoutesCurrentMembershipAndPausedStopsDispatch(t *testing.T) {
	publisher, _, broker := newTestPublisher(t)
	prepare := pausedSnapshot(2, 1)
	prepare.Phase = control.PhasePrepare
	apply(t, publisher, prepare)
	receipt, err := publisher.Accept(message("event-1", "key-1"))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.State != outbox.StateReady || receipt.Epoch != prepare.Epoch {
		t.Fatalf("prepare receipt = %+v", receipt)
	}
	paused := pausedSnapshot(3, 1)
	apply(t, publisher, paused)
	if report, err := publisher.Dispatch(context.Background(), 10); err != nil || report.Attempted != 0 {
		t.Fatalf("dispatch after pause = %+v, %v", report, err)
	}
	if len(broker.calls) != 0 {
		t.Fatalf("paused dispatch made %d broker calls", len(broker.calls))
	}
}

func TestAmbiguousAckRetainsRecordAndOtherKeyProgresses(t *testing.T) {
	publisher, store, broker := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a", "broker-b"))
	first, err := publisher.Accept(message("event-1", "key-a"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = publisher.Accept(message("event-2", "key-a"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := publisher.Accept(message("event-3", "key-b"))
	if err != nil {
		t.Fatal(err)
	}
	broker.results = []brokerResult{{outcome: OutcomeUnknown, err: errors.New("connection lost after send")}, {outcome: OutcomeAcknowledged}}

	report, err := publisher.Dispatch(context.Background(), 10)
	if !errors.Is(err, ErrAckUncertain) {
		t.Fatalf("dispatch error = %v, want ErrAckUncertain", err)
	}
	if report.Attempted != 2 || report.Uncertain != 1 || report.Acknowledged != 1 {
		t.Fatalf("report = %+v", report)
	}
	uncertain, err := store.Get(first.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if uncertain.State != outbox.StateAckUncertain {
		t.Fatalf("state = %q, want ack_uncertain", uncertain.State)
	}
	if _, err := store.Get(other.EventID); !errors.Is(err, outbox.ErrNotFound) {
		t.Fatalf("independent event was not ACK-removed: %v", err)
	}
	if report, err := publisher.Dispatch(context.Background(), 10); err != nil || report.Attempted != 0 {
		t.Fatalf("same-key follower overtook uncertainty: report=%+v err=%v", report, err)
	}
}

func TestRetryAckUncertainReassignsToCurrentEpoch(t *testing.T) {
	publisher, store, broker := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	receipt, err := publisher.Accept(message("event-1", "key-a"))
	if err != nil {
		t.Fatal(err)
	}
	broker.results = []brokerResult{{outcome: OutcomeUnknown, err: errors.New("ACK timeout")}}
	if _, err := publisher.Dispatch(context.Background(), 1); !errors.Is(err, ErrAckUncertain) {
		t.Fatalf("Dispatch() error = %v", err)
	}
	apply(t, publisher, activeSnapshot(2, 2, "broker-b"))
	if err := publisher.RetryAckUncertain(receipt.EventID); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(receipt.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Epoch != 2 || record.Broker != "broker-b" || record.State != outbox.StateReady {
		t.Fatalf("retry assignment = %#v", record)
	}
}

func TestDispatchUsesRoutingAndAttachesMetadata(t *testing.T) {
	publisher, _, broker := newTestPublisher(t)
	snapshot := activeSnapshot(1, 7, "broker-first", "broker-second", "broker-third")
	apply(t, publisher, snapshot)
	msg := message("event-1", "business-key")
	msg.Headers["application"] = "kept"
	receipt, err := publisher.Accept(msg)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("business-key"))
	wantBroker, err := routing.BrokerForHash(digest, snapshot.CurrentMembership)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Broker != wantBroker {
		t.Fatalf("broker = %q, want routing result %q", receipt.Broker, wantBroker)
	}
	if _, err := publisher.Dispatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(broker.calls) != 1 || broker.calls[0].broker != wantBroker {
		t.Fatalf("calls = %#v", broker.calls)
	}
	properties := broker.calls[0].message.Properties
	want := map[string]string{
		"application":          "kept",
		"group":                "orders",
		PropertyEventID:        "event-1",
		PropertyScalingGroup:   "orders",
		PropertyBusinessHash:   receipt.Hash,
		PropertyHashContract:   "sha256/customer-v1",
		PropertyLibraryVersion: "library-v1",
		PropertyEpoch:          "7",
	}
	if !reflect.DeepEqual(properties, want) {
		t.Fatalf("properties = %#v, want %#v", properties, want)
	}
}

func newTestPublisher(t *testing.T) (*Publisher, *outbox.Store, *fakeBroker) {
	t.Helper()
	return newTestPublisherWithLimits(t, outbox.Limits{})
}

func newTestPublisherWithLimits(t *testing.T, limits outbox.Limits) (*Publisher, *outbox.Store, *fakeBroker) {
	t.Helper()
	store, err := outbox.Open(filepath.Join(t.TempDir(), "outbox.db"), limits)
	if err != nil {
		t.Fatal(err)
	}
	broker := &fakeBroker{}
	publisher, err := New(Config{
		Outbox: store, CustomerLibrary: fixedLibrary{}, Broker: broker,
		ScalingGroup: "orders", HashContract: "sha256/customer-v1", LibraryVersion: "library-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return publisher, store, broker
}

func apply(t *testing.T, publisher *Publisher, snapshot control.MembershipSnapshot) {
	t.Helper()
	if err := publisher.ApplyMembership(snapshot); err != nil {
		t.Fatal(err)
	}
}

func message(eventID, key string) customer.MessageView {
	return messageForGroup(eventID, key, "orders")
}

func messageForGroup(eventID, key, group string) customer.MessageView {
	return customer.MessageView{
		EventID: eventID, Topic: group + "/created", Payload: key,
		Headers: map[string]string{"group": group},
	}
}

func TestAssignmentUsesSelectedBrokerEpochResource(t *testing.T) {
	snapshot := activeSnapshot(1, 3, "broker-a", "broker-b")
	snapshot.Destination.Name = "orders/logical"
	snapshot.CurrentResources = []control.EpochResourceIdentity{
		{Epoch: 3, BrokerID: "broker-a", ConsumerSet: "default", QueueName: "orders-a-e3", IngressTopic: "orders/epoch/3/>"},
		{Epoch: 3, BrokerID: "broker-b", ConsumerSet: "default", QueueName: "orders-b-e3", IngressTopic: "orders/epoch/3/>"},
	}
	for key := 0; key < 1000; key++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("key-%d", key)))
		broker, err := routing.BrokerForHash(digest, snapshot.CurrentMembership)
		if err != nil || broker != "broker-b" {
			continue
		}
		assignment, err := assignmentFor(hex.EncodeToString(digest[:]), snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if assignment.Broker != "broker-b" || assignment.Destination != "orders/epoch/3/events" {
			t.Fatalf("assignment = %+v", assignment)
		}
		return
	}
	t.Fatal("no hash selected broker-b")
}

func TestAssignmentDestinationMatchesBoundedIngressSubscription(t *testing.T) {
	names, err := control.NewManagedNames("acme.prod")
	if err != nil {
		t.Fatal(err)
	}
	ingressTopic, err := names.EpochIngressTopic(strings.Repeat("g", 300), 3)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := activeSnapshot(1, 3, "broker-a")
	snapshot.CurrentResources = []control.EpochResourceIdentity{{
		Epoch: 3, BrokerID: "broker-a", ConsumerSet: "default", QueueName: "orders-a-e3", IngressTopic: ingressTopic,
	}}
	digest := sha256.Sum256([]byte("key"))
	assignment, err := assignmentFor(hex.EncodeToString(digest[:]), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	prefix := strings.TrimSuffix(ingressTopic, ">")
	if assignment.Destination != prefix+"events" {
		t.Fatalf("assignment destination = %q, want %q", assignment.Destination, prefix+"events")
	}
	matchesSubscription := assignment.Destination == ingressTopic
	if strings.HasSuffix(ingressTopic, ">") {
		matchesSubscription = strings.HasPrefix(assignment.Destination, prefix)
	}
	if !matchesSubscription || strings.Contains(assignment.Destination, ">") {
		t.Fatalf("concrete destination %q does not match subscription %q", assignment.Destination, ingressTopic)
	}
}

func TestAssignmentRejectsMissingSelectedBrokerResource(t *testing.T) {
	snapshot := activeSnapshot(1, 3, "broker-a", "broker-b")
	snapshot.CurrentResources = []control.EpochResourceIdentity{
		{Epoch: 3, BrokerID: "broker-a", ConsumerSet: "default", QueueName: "orders-a-e3", IngressTopic: "orders/epoch/3/>"},
	}
	for key := 0; key < 1000; key++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("key-%d", key)))
		broker, err := routing.BrokerForHash(digest, snapshot.CurrentMembership)
		if err != nil || broker != "broker-b" {
			continue
		}
		if _, err := assignmentFor(hex.EncodeToString(digest[:]), snapshot); err == nil {
			t.Fatal("missing selected-broker resource was accepted")
		}
		return
	}
	t.Fatal("no hash selected broker-b")
}

func activeSnapshot(revision, epoch uint64, brokers ...string) control.MembershipSnapshot {
	return control.MembershipSnapshot{
		Version: control.SnapshotVersion, LibraryVersion: "library-v1", ScalingGroup: "orders", Revision: revision,
		Epoch: epoch, Phase: control.PhaseActive, HashContract: "sha256/customer-v1", Algorithm: control.AlgorithmSHA256BigEndianModulo, CurrentMembership: brokers,
		Queue:       control.QueueInfo{Name: "orders", Durable: true},
		Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders/input"},
	}
}

func pausedSnapshot(revision, epoch uint64) control.MembershipSnapshot {
	return control.MembershipSnapshot{
		Version: control.SnapshotVersion, LibraryVersion: "library-v1", ScalingGroup: "orders", Revision: revision,
		Epoch: epoch, Phase: control.PhasePaused, HashContract: "sha256/customer-v1", Algorithm: control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: control.Membership{"broker-a"}, ProposedMembership: control.Membership{"broker-b", "broker-c"},
		Transition:  &control.Transition{ID: "transition-1", FromEpoch: epoch, ToEpoch: epoch + 1},
		Queue:       control.QueueInfo{Name: "orders", Durable: true},
		Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders/input"},
	}
}
