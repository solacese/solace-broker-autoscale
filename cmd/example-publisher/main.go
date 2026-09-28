package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
	"github.com/solacese/solace-workload-balancer/shim/publisher"
)

type exampleLibrary struct{}

func (exampleLibrary) GetScalingGroup(message customer.MessageView) (string, error) {
	return message.Headers["scaling-group"], nil
}

func (exampleLibrary) GetBusinessHash(message customer.MessageView) (customer.BusinessHash, error) {
	return customer.EntityHash(message.Headers["scaling-group"], message.Headers["entity_id"])
}
func (exampleLibrary) GetRendezvousScore(input customer.ScoreInput) (customer.RoutingScore, error) {
	return customer.SHA256RendezvousScore(input)
}
func (exampleLibrary) RoutingAlgorithm() string { return customer.RendezvousSHA256Contract }

type loggingBroker struct{}

func (loggingBroker) Publish(_ context.Context, brokerID string, message publisher.BrokerMessage) (publisher.PublishOutcome, error) {
	fmt.Printf("broker=%s epoch=%d destination=%s event=%s payload=%s\n", brokerID, message.Epoch, message.Destination, message.EventID, message.Payload)
	return publisher.OutcomeAcknowledged, nil
}

func main() {
	path := filepath.Join("var", "example-publisher-outbox.db")
	store, err := outbox.Open(path, outbox.Limits{MaxMessages: 100, MaxBytes: 1 << 20})
	if err != nil {
		log.Fatal(err)
	}
	shim, err := publisher.New(publisher.Config{
		Outbox: store, CustomerLibrary: exampleLibrary{}, Broker: loggingBroker{},
		Contracts: map[string]publisher.Contract{
			"events-a": {HashContract: customer.EntityAffinityContract, LibraryVersion: "entity-routing-v1"},
			"events-b": {HashContract: customer.EntityAffinityContract, LibraryVersion: "entity-routing-v1"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, group := range []string{"events-a", "events-b"} {
		if err := shim.ApplyMembership(active(group)); err != nil {
			log.Fatal(err)
		}
	}
	messages := []customer.MessageView{
		{Topic: "synthetic/events", EventID: "event-a-1", Payload: []byte(`{"event_type":"updated","sequence":1}`), Headers: map[string]string{"scaling-group": "events-a", "entity_id": "entity-001"}},
		{Topic: "synthetic/events", EventID: "event-b-1", Payload: []byte(`{"event_type":"observed","sequence":1}`), Headers: map[string]string{"scaling-group": "events-b", "entity_id": "entity-002"}},
	}
	for _, message := range messages {
		receipt, err := shim.Accept(message)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("durably accepted event=%s group=%s broker=%s\n", receipt.EventID, receipt.Group, receipt.Broker)
	}
	report, err := shim.Dispatch(context.Background(), 100)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("dispatch: attempted=%d acknowledged=%d\n", report.Attempted, report.Acknowledged)
}

func contractFor(group string) string {
	if group == "events-a" {
		return customer.EntityAffinityContract
	}
	return customer.EntityAffinityContract
}

func active(group string) control.MembershipSnapshot {
	return control.MembershipSnapshot{
		Version: control.SnapshotVersion, LibraryVersion: "entity-routing-v1", ScalingGroup: group, Revision: 1, Epoch: 1,
		Phase: control.PhaseActive, HashContract: contractFor(group), Algorithm: control.AlgorithmRendezvousV1,
		CurrentMembership: control.Membership{"broker-a", "broker-b", "broker-c"},
		Queue:             control.QueueInfo{Name: "swlb." + group + ".epoch.1", Durable: true},
		Destination:       control.DestinationInfo{Kind: control.DestinationTopic, Name: "_swlb/v1/data/" + group + "/epoch/1"},
	}
}
