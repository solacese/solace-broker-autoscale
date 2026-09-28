//go:build integration

package integration

import (
	"testing"

	amqp "github.com/Azure/go-amqp"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
)

// TestAMQPLocalCapability verifies the protocol-boundary assumptions used by
// the active adapter without contacting a broker.
func TestAMQPLocalCapability(t *testing.T) {
	message, err := amqpBusinessMessage(shimPublisher.BrokerMessage{
		EventID: "event", Topic: "app/topic", Destination: "managed/ingress", BrokerEndpoint: "amqps://broker.example:5671", Epoch: 1,
		Properties: map[string]string{shimPublisher.PropertyScalingGroup: "orders", shimPublisher.PropertyBusinessHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", shimPublisher.PropertyHashContract: "orders-v1", shimPublisher.PropertyLibraryVersion: "v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if message.Header == nil || !message.Header.Durable || message.Properties == nil || message.Properties.To == nil || *message.Properties.To != "topic://managed/ingress" {
		t.Fatalf("message=%#v", message)
	}
	if outcome, err := classifyAMQPSettlement(&amqp.StateAccepted{}, nil); err != nil || outcome != shimPublisher.OutcomeAcknowledged {
		t.Fatalf("settlement=%v %v", outcome, err)
	}
}
