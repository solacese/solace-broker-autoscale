package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"path/filepath"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
	"github.com/solacese/solace-workload-balancer/routing"
	"github.com/solacese/solace-workload-balancer/shim/publisher"
)

type exampleLibrary struct{}

func (exampleLibrary) GetScalingGroup(message customer.MessageView) (string, error) {
	return message.Headers["scaling-group"], nil
}

func (exampleLibrary) GetBusinessHash(message customer.MessageView) ([sha256.Size]byte, error) {
	switch message.Headers["scaling-group"] {
	case "flight-operations":
		return routing.FlightOperationsHash(message.Headers["carrier"], message.Headers["flight-number"], message.Headers["departure-date"], message.Headers["leg-id"])
	case "baggage-tracking":
		return routing.BaggageHash(message.Headers["carrier"], message.Headers["bag-journey-id"])
	default:
		return [sha256.Size]byte{}, fmt.Errorf("unknown scaling group %q", message.Headers["scaling-group"])
	}
}

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
			"flight-operations": {HashContract: routing.FlightOperationsDomain, LibraryVersion: "airline-routing-v1"},
			"baggage-tracking":  {HashContract: routing.BaggageDomain, LibraryVersion: "airline-routing-v1"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, group := range []string{"flight-operations", "baggage-tracking"} {
		if err := shim.ApplyMembership(active(group)); err != nil {
			log.Fatal(err)
		}
	}
	messages := []customer.MessageView{
		{Topic: "airline/flight/status", EventID: "flight-event-1", Payload: []byte(`{"status":"boarding"}`), Headers: map[string]string{"scaling-group": "flight-operations", "carrier": "UA", "flight-number": "123", "departure-date": "2026-09-25", "leg-id": "ORD-LAX"}},
		{Topic: "airline/baggage/scan", EventID: "bag-event-1", Payload: []byte(`{"status":"loaded"}`), Headers: map[string]string{"scaling-group": "baggage-tracking", "carrier": "UA", "bag-journey-id": "0123456789"}},
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
	if group == "flight-operations" {
		return routing.FlightOperationsDomain
	}
	return routing.BaggageDomain
}

func active(group string) control.MembershipSnapshot {
	return control.MembershipSnapshot{
		Version: control.SnapshotVersion, LibraryVersion: "airline-routing-v1", ScalingGroup: group, Revision: 1, Epoch: 1,
		Phase: control.PhaseActive, HashContract: contractFor(group), Algorithm: control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: control.Membership{"broker-a", "broker-b", "broker-c"},
		Queue:             control.QueueInfo{Name: "swlb." + group + ".epoch.1", Durable: true},
		Destination:       control.DestinationInfo{Kind: control.DestinationTopic, Name: "_swlb/v1/data/" + group + "/epoch/1"},
	}
}
