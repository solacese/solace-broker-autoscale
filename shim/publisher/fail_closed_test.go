package publisher

import (
	"context"
	"testing"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
)

func TestFailClosedRevokesOnlyRequestedGroup(t *testing.T) {
	store, err := outbox.Open(t.TempDir()+"/outbox.db", outbox.Limits{MaxMessages: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	library := customer.CustomerLibraryFuncs{
		Algorithm:    customer.RendezvousSHA256Contract,
		ScalingGroup: func(message customer.MessageView) (string, error) { return message.Headers["scaling-group"], nil },
		BusinessHash: func(customer.MessageView) (customer.BusinessHash, error) { return customer.EntityHash("test", "key") },
	}
	shim, err := New(Config{Outbox: store, CustomerLibrary: library, Broker: failClosedBroker{}, Contracts: map[string]Contract{"events-a": {HashContract: "contract", LibraryVersion: "v1"}}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := control.MembershipSnapshot{
		Version: control.SnapshotVersion, ScalingGroup: "events-a", Revision: 1, Epoch: 1, Phase: control.PhaseActive,
		HashContract: "contract", LibraryVersion: "v1", Algorithm: control.AlgorithmRendezvousV1,
		CurrentMembership: control.Membership{"broker-a"}, CurrentBrokers: []control.BrokerDescriptor{{ID: "broker-a", Endpoint: "amqps://a.example:5671"}}, CurrentResources: []control.EpochResourceIdentity{{Epoch: 1, BrokerID: "broker-a", ConsumerSet: "default", QueueName: "queue.a", IngressTopic: "topic/>"}}, Queue: control.QueueInfo{Name: "queue", Durable: true},
		Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "topic"},
	}
	if err := shim.ApplyMembership(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := shim.FailClosed("events-a"); err != nil {
		t.Fatal(err)
	}
	_, err = shim.Accept(customer.MessageView{EventID: "event-1", Topic: "topic", Headers: map[string]string{"scaling-group": "events-a"}, Payload: []byte("body")})
	if err == nil {
		t.Fatal("accept succeeded after fail closed")
	}
}

type failClosedBroker struct{}

func (failClosedBroker) Publish(context.Context, string, BrokerMessage) (PublishOutcome, error) {
	return OutcomeAcknowledged, nil
}
