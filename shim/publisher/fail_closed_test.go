package publisher

import (
	"context"
	"testing"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
	"github.com/solacese/solace-workload-balancer/routing"
)

func TestFailClosedRevokesOnlyRequestedGroup(t *testing.T) {
	store, err := outbox.Open(t.TempDir()+"/outbox.db", outbox.Limits{MaxMessages: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	library := customer.CustomerLibraryFuncs{
		ScalingGroup: func(message customer.MessageView) (string, error) { return message.Headers["scaling-group"], nil },
		BusinessHash: func(customer.MessageView) (customer.BusinessHash, error) { return routing.SHA256("key") },
	}
	shim, err := New(Config{Outbox: store, CustomerLibrary: library, Broker: failClosedBroker{}, Contracts: map[string]Contract{"flight": {HashContract: "contract", LibraryVersion: "v1"}}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := control.MembershipSnapshot{
		Version: control.SnapshotVersion, ScalingGroup: "flight", Revision: 1, Epoch: 1, Phase: control.PhaseActive,
		HashContract: "contract", LibraryVersion: "v1", Algorithm: control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: control.Membership{"broker-a"}, Queue: control.QueueInfo{Name: "queue", Durable: true},
		Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "topic"},
	}
	if err := shim.ApplyMembership(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := shim.FailClosed("flight"); err != nil {
		t.Fatal(err)
	}
	_, err = shim.Accept(customer.MessageView{EventID: "event-1", Topic: "topic", Headers: map[string]string{"scaling-group": "flight"}, Payload: []byte("body")})
	if err == nil {
		t.Fatal("accept succeeded after fail closed")
	}
}

type failClosedBroker struct{}

func (failClosedBroker) Publish(context.Context, string, BrokerMessage) (PublishOutcome, error) {
	return OutcomeAcknowledged, nil
}
