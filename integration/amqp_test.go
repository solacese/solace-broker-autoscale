package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/solacese/solace-workload-balancer/control"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
)

func TestSolaceAMQPAddressKinds(t *testing.T) {
	if got := TopicAddress("swlb/control/orders/updates"); got != "topic://swlb/control/orders/updates" {
		t.Fatalf("topic=%q", got)
	}
	if got := QueueAddress("swlb.ctl.orders.reply"); got != "queue://swlb.ctl.orders.reply" {
		t.Fatalf("queue=%q", got)
	}
	if TopicAddress("topic://x") != "topic://x" || QueueAddress("queue://x") != "queue://x" {
		t.Fatal("address normalization is not idempotent")
	}
}

func TestAMQPBusinessMessageDurabilityPropertiesAndTopicTo(t *testing.T) {
	message, err := amqpBusinessMessage(shimPublisher.BrokerMessage{EventID: "e1", Payload: []byte("body"), Topic: "app/orders", Destination: "swlb/data/orders/e1/events", BrokerEndpoint: "amqps://a.example:5671", Epoch: 1, Properties: map[string]string{shimPublisher.PropertyScalingGroup: "orders", shimPublisher.PropertyBusinessHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", shimPublisher.PropertyHashContract: "orders-v1", shimPublisher.PropertyLibraryVersion: "v1"}})
	if err != nil {
		t.Fatal(err)
	}
	if message.Header == nil || !message.Header.Durable || message.Properties == nil || message.Properties.To == nil || *message.Properties.To != "topic://swlb/data/orders/e1/events" {
		t.Fatalf("message=%#v", message)
	}
	if message.ApplicationProperties[PropertyOriginalTopic] != "app/orders" {
		t.Fatal("original topic missing")
	}
}

func TestAMQPSettlementMapping(t *testing.T) {
	boom := errors.New("disconnect")
	for _, test := range []struct {
		name    string
		state   amqp.DeliveryState
		err     error
		want    shimPublisher.PublishOutcome
		wantErr bool
	}{{"accepted", &amqp.StateAccepted{}, nil, shimPublisher.OutcomeAcknowledged, false}, {"rejected", &amqp.StateRejected{}, nil, shimPublisher.OutcomeRejected, false}, {"released", &amqp.StateReleased{}, nil, shimPublisher.OutcomeRejected, false}, {"unknown", nil, boom, shimPublisher.OutcomeUnknown, true}} {
		t.Run(test.name, func(t *testing.T) {
			got, err := classifyAMQPSettlement(test.state, test.err)
			if got != test.want || (err != nil) != test.wantErr {
				t.Fatalf("got=%v err=%v", got, err)
			}
		})
	}
}

func snapshotRequestAndResponse(t *testing.T) (control.SnapshotRequest, control.SnapshotResponse) {
	t.Helper()
	request := control.SnapshotRequest{Version: control.BootstrapProtocolVersion, CorrelationID: "current", Namespace: "swlb", Group: "orders", Participant: "publisher-1", Role: control.RolePublisher, RequestedAt: time.Unix(1, 0).UTC()}
	snapshot := control.MembershipSnapshot{Version: control.SnapshotVersion, Namespace: "swlb", LibraryVersion: "v1", ScalingGroup: "orders", Revision: 1, Epoch: 1, Phase: control.PhaseActive, HashContract: "orders-v1", Algorithm: control.AlgorithmRendezvousV1, CurrentMembership: control.Membership{"broker-a"}, CurrentBrokers: []control.BrokerDescriptor{{ID: "broker-a", Endpoint: "amqps://a.example:5671"}}, Queue: control.QueueInfo{Name: "orders", Durable: true}, Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders/>"}, CurrentResources: []control.EpochResourceIdentity{{Epoch: 1, BrokerID: "broker-a", ConsumerSet: "default", QueueName: "orders.a", IngressTopic: "orders/>"}}}
	return request, control.SnapshotResponse{Version: control.BootstrapProtocolVersion, CorrelationID: request.CorrelationID, Namespace: request.Namespace, Group: request.Group, Participant: request.Participant, Role: request.Role, Snapshot: snapshot}
}

func TestSnapshotReplyInspectionDiscardsLateThenAcceptsCurrent(t *testing.T) {
	request, response := snapshotRequestAndResponse(t)
	payload, _ := json.Marshal(response)
	late := amqp.NewMessage(payload)
	late.Properties = &amqp.MessageProperties{CorrelationID: "expired"}
	if _, action, err := inspectSnapshotReply(request, late); err != nil || action != snapshotReplyDiscard {
		t.Fatalf("late action=%v err=%v", action, err)
	}
	current := amqp.NewMessage(payload)
	current.Properties = &amqp.MessageProperties{CorrelationID: request.CorrelationID}
	got, action, err := inspectSnapshotReply(request, current)
	if err != nil || action != snapshotReplyAccept || got.Snapshot.Revision != 1 {
		t.Fatalf("current=%#v action=%v err=%v", got, action, err)
	}
	wrong := response
	wrong.Group = "other"
	wrongPayload, _ := json.Marshal(wrong)
	bad := amqp.NewMessage(wrongPayload)
	bad.Properties = &amqp.MessageProperties{CorrelationID: request.CorrelationID}
	if _, action, err := inspectSnapshotReply(request, bad); err == nil || action != snapshotReplyReject {
		t.Fatalf("scope action=%v err=%v", action, err)
	}
}

func TestPoolEndpointChangeRequiresOldLeaseRelease(t *testing.T) {
	dials := []string{}
	pool, _ := NewAMQPConnectionPool(1, time.Minute, func(_ context.Context, _ string, endpoint string) (*AMQPConnection, error) {
		dials = append(dials, endpoint)
		return &AMQPConnection{}, nil
	})
	old, _ := pool.Acquire(context.Background(), "a", "amqps://old:5671")
	next, err := pool.Acquire(context.Background(), "a", "amqps://new:5671")
	if err != nil {
		t.Fatal(err)
	}
	old.Release()
	next.Release()
	if len(dials) != 2 || next.Generation == old.Generation {
		t.Fatalf("dials=%v generations=%d/%d", dials, old.Generation, next.Generation)
	}
}
