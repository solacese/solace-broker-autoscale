package main

import (
	"context"
	"fmt"
	"log"

	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/shim/subscriber"
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

type delivery struct {
	message customer.MessageView
	acked   bool
}

func (d *delivery) Message() customer.MessageView { return d.message }
func (d *delivery) Ack(context.Context) error {
	d.acked = true
	return nil
}
func (d *delivery) Reject(context.Context) error { return nil }
func (d *delivery) RoutingMetadata() (subscriber.RoutingMetadata, error) {
	group, err := exampleLibrary{}.GetScalingGroup(d.message)
	if err != nil {
		return subscriber.RoutingMetadata{}, err
	}
	hash, err := exampleLibrary{}.GetBusinessHash(d.message)
	if err != nil {
		return subscriber.RoutingMetadata{}, err
	}
	contract := customer.EntityAffinityContract
	if group == "events-a" {
		contract = customer.EntityAffinityContract
	}
	return subscriber.RoutingMetadata{Group: group, BusinessHash: hash, HashContract: contract, LibraryVersion: "entity-routing-v1", Epoch: 1, OriginalTopic: d.message.Topic}, nil
}

type consumer struct{}

func (consumer) Activate(context.Context) error { return nil }
func (consumer) Pause(context.Context) error    { return nil }
func (consumer) Close(context.Context) error    { return nil }

type factory struct{}

func (factory) Prepare(context.Context, subscriber.Binding, func(context.Context, subscriber.Delivery) error) (subscriber.Consumer, error) {
	return consumer{}, nil
}

type reporter struct{}

func (reporter) ReportReadiness(_ context.Context, readiness subscriber.Readiness) error {
	fmt.Printf("participant=%s group=%s epoch=%d ready=%t\n", readiness.Participant, readiness.Group, readiness.Epoch, readiness.Ready)
	return nil
}

func main() {
	handler := subscriber.HandlerFunc(func(_ context.Context, message customer.MessageView) error {
		fmt.Printf("processed event=%s topic=%s payload=%s\n", message.EventID, message.Topic, message.Payload)
		return nil
	})
	shim, err := subscriber.New(subscriber.Config{
		Participant: "example-subscriber", Library: exampleLibrary{}, Handler: handler, Factory: factory{}, Reporter: reporter{},
		Contracts: map[string]subscriber.Contract{
			"events-a": {HashContract: customer.EntityAffinityContract, LibraryVersion: "entity-routing-v1"},
			"events-b": {HashContract: customer.EntityAffinityContract, LibraryVersion: "entity-routing-v1"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	binding := subscriber.Binding{Group: "events-a", BrokerID: "broker-a", Epoch: 1, Destination: "swlb.eventsA.epoch.1"}
	if err := shim.AddActive(context.Background(), binding); err != nil {
		log.Fatal(err)
	}
	message := customer.MessageView{Topic: "synthetic/eventsA/status", EventID: "eventsA-event-1", Payload: []byte(`{"status":"boarding"}`), Headers: map[string]string{"scaling-group": "events-a", "entity_id": "UA", "event_type": "123", "timestamp": "2026-09-25", "sequence": "ORD-LAX"}}
	d := &delivery{message: message}
	if err := shim.Handle(context.Background(), binding, d); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("application success; acknowledged=%t\n", d.acked)
}
