package control

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestManagedNamesDeterministicBoundedAndScoped(t *testing.T) {
	names, err := NewManagedNames("acme.prod")
	if err != nil {
		t.Fatal(err)
	}
	first, err := names.CommandTopic("orders", RolePublisher, "pub-1")
	if err != nil {
		t.Fatal(err)
	}
	second, _ := names.CommandTopic("orders", RolePublisher, "pub-1")
	if first != second || first != "acme.prod/control/orders/commands/publisher/pub-1" {
		t.Fatalf("unexpected deterministic command topic %q / %q", first, second)
	}
	membershipQueue, _ := names.MembershipQueue("orders")
	updateQueue, _ := names.UpdateQueue("orders", "pub-1")
	registration, _ := names.RegistrationTopicFor("orders", RolePublisher, "pub-1")
	acknowledgement, _ := names.AcknowledgementTopicFor("orders", RolePublisher, "pub-1")
	telemetry, _ := names.TelemetryTopicFor("orders", RoleObserver, "observer-1")
	registrationQueue, _ := names.RegistrationQueueFor("orders", RolePublisher, "pub-1")
	acknowledgementQueue, _ := names.AcknowledgementQueueFor("orders", RolePublisher, "pub-1")
	telemetryQueue, _ := names.TelemetryQueueFor("orders", RoleObserver, "observer-1")
	if membershipQueue != "acme.prod.ctl.orders.membership" ||
		updateQueue != "acme.prod.ctl.orders.updates.pub-1" ||
		registration != "acme.prod/control/orders/registrations/publisher/pub-1" ||
		acknowledgement != "acme.prod/control/orders/acknowledgements/publisher/pub-1" ||
		telemetry != "acme.prod/control/orders/telemetry/observer/observer-1" ||
		registrationQueue != "acme.prod.ctl.orders.reg.publisher.pub-1" ||
		acknowledgementQueue != "acme.prod.ctl.orders.ack.publisher.pub-1" ||
		telemetryQueue != "acme.prod.ctl.orders.tel.observer.observer-1" {
		t.Fatalf("unexpected participant destinations: %q %q %q %q %q %q %q %q", membershipQueue, updateQueue, registration, acknowledgement, telemetry, registrationQueue, acknowledgementQueue, telemetryQueue)
	}
	queue, err := names.EpochConsumerQueue(strings.Repeat("g", 180), "payments", 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(queue) > MaxManagedQueueBytes {
		t.Fatalf("queue length = %d", len(queue))
	}
	if _, err := names.EpochConsumerQueue("orders", "../shared", 42); err == nil {
		t.Fatal("unsafe consumer set accepted")
	}
	if _, err := NewManagedNames("UPPER"); err == nil {
		t.Fatal("unsafe namespace accepted")
	}
}

func TestEpochIngressTopicPreservesWildcardWhenBounded(t *testing.T) {
	names, err := NewManagedNames("acme.prod")
	if err != nil {
		t.Fatal(err)
	}
	short, err := names.EpochIngressTopic("orders", 42)
	if err != nil {
		t.Fatal(err)
	}
	if short != "acme.prod/data/orders/epoch/42/>" {
		t.Fatalf("short ingress topic = %q", short)
	}

	longGroup := strings.Repeat("g", 300)
	first, err := names.EpochIngressTopic(longGroup, 42)
	if err != nil {
		t.Fatal(err)
	}
	second, err := names.EpochIngressTopic(longGroup, 42)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("bounded ingress topic is not deterministic: %q / %q", first, second)
	}
	if len(first) != MaxManagedTopicBytes {
		t.Fatalf("bounded ingress topic length = %d, want %d", len(first), MaxManagedTopicBytes)
	}
	if !strings.HasSuffix(first, "/>") {
		t.Fatalf("bounded ingress topic lost wildcard suffix: %q", first)
	}

	other, err := names.EpochIngressTopic(longGroup+"x", 42)
	if err != nil {
		t.Fatal(err)
	}
	if first == other {
		t.Fatalf("distinct long ingress topics collided: %q", first)
	}
	if len(other) != MaxManagedTopicBytes || !strings.HasSuffix(other, "/>") {
		t.Fatalf("other bounded ingress topic = %q", other)
	}
}

func TestVersionedEnvelopesStrictValidation(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	command := CommandEnvelope{
		Version: ProtocolVersion, MessageID: "cmd-1", Namespace: "acme", Group: "orders",
		TransitionID: "move-1", Epoch: 2, Phase: PhasePaused, Participant: "pub-1",
		Role: RolePublisher, IssuedAt: now, Deadline: now.Add(time.Minute),
	}
	data, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseCommandEnvelope(data); err != nil {
		t.Fatalf("valid command rejected: %v", err)
	}
	for name, invalid := range map[string][]byte{
		"unknown field":   append(data[:len(data)-1], []byte(`,"extra":true}`)...),
		"duplicate field": []byte(strings.Replace(string(data), `"version":1`, `"version":1,"version":1`, 1)),
		"secret field":    append(data[:len(data)-1], []byte(`,"password":"x"}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseCommandEnvelope(invalid); err == nil {
				t.Fatal("invalid command accepted")
			}
		})
	}

	command.Role = RoleObserver
	if err := command.Validate(); err == nil {
		t.Fatal("observer command role accepted")
	}
	command.Role = RolePublisher
	command.Phase = PhasePrepare
	if err := command.Validate(); err == nil {
		t.Fatal("publisher PREPARE command accepted")
	}

	ack := AcknowledgementEnvelope{
		Version: ProtocolVersion, MessageID: "ack-1", CommandID: "cmd-1", Namespace: "acme", Group: "orders",
		TransitionID: "move-1", Epoch: 2, Phase: PhasePrepare, Participant: "sub-1",
		Role: RoleSubscriber, Ready: true, ObservedAt: now,
	}
	if err := ack.Validate(); err != nil {
		t.Fatalf("valid acknowledgement rejected: %v", err)
	}
	matching := CommandEnvelope{
		Version: ProtocolVersion, MessageID: ack.CommandID, Namespace: ack.Namespace, Group: ack.Group,
		TransitionID: ack.TransitionID, Epoch: ack.Epoch, Phase: ack.Phase, Participant: ack.Participant,
		Role: ack.Role, IssuedAt: now.Add(-time.Minute), Deadline: now.Add(time.Minute),
	}
	if err := ack.ValidateForCommand(matching); err != nil {
		t.Fatalf("matching acknowledgement rejected: %v", err)
	}
	mismatched := matching
	mismatched.MessageID = "cmd-other"
	if err := ack.ValidateForCommand(mismatched); err == nil {
		t.Fatal("acknowledgement for another command accepted")
	}
	mismatched = matching
	mismatched.Role = RoleBroker
	if err := ack.ValidateForCommand(mismatched); err == nil {
		t.Fatal("acknowledgement for another expected role accepted")
	}

	ack.Ready = false
	if err := ack.Validate(); err == nil {
		t.Fatal("implicit readiness accepted")
	}
}

func TestRegistrationAndTelemetryRolePhaseScoping(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	registration := RegistrationEnvelope{
		Version: ProtocolVersion, MessageID: "reg-1", Namespace: "acme", Group: "orders",
		Epoch: 1, Phase: PhaseActive, Participant: "sub-1", Role: RoleSubscriber,
		ConsumerSet: "workers", LibraryVersion: "v1.2.3", ObservedAt: now,
	}
	if err := registration.Validate(); err != nil {
		t.Fatal(err)
	}
	registration.Role = RolePublisher
	if err := registration.Validate(); err == nil {
		t.Fatal("publisher consumer set accepted")
	}

	telemetry := TelemetryEnvelope{
		Version: ProtocolVersion, MessageID: "tel-1", Namespace: "acme", Group: "orders",
		TransitionID: "move-1", Epoch: 1, Phase: PhaseDrain, Participant: "broker-1",
		Role: RoleBroker, SourceBroker: "broker-1", SourceQueue: "orders.e1", ObservedAt: now,
		Queued: MetricCount{Known: true}, Stored: MetricCount{Known: true}, Unacked: MetricCount{Known: true},
	}
	if err := telemetry.Validate(); err != nil {
		t.Fatal(err)
	}
	withoutSource := telemetry
	withoutSource.SourceQueue = ""
	if err := withoutSource.Validate(); err == nil {
		t.Fatal("telemetry without exact source queue accepted")
	}
	telemetry.Phase = PhaseActive
	if err := telemetry.Validate(); err == nil {
		t.Fatal("telemetry outside DRAIN accepted")
	}
}

func TestSnapshotResourceIdentityValidationAndClone(t *testing.T) {
	snapshot := validTransitionSnapshot(PhasePrepare)
	snapshot.Namespace = "acme"
	snapshot.LibraryVersion = "v1.2.3"
	snapshot.CurrentResources = []EpochResourceIdentity{{Epoch: 3, BrokerID: "broker-a", ConsumerSet: "workers", QueueName: "acme.data.orders.workers.e3", IngressTopic: "acme/data/orders/epoch/3/>"}}
	snapshot.ProposedResources = []EpochResourceIdentity{{Epoch: 4, BrokerID: "broker-a", ConsumerSet: "workers", QueueName: "acme.data.orders.workers.e4", IngressTopic: "acme/data/orders/epoch/4/>"}}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
	clone := snapshot.Clone()
	clone.CurrentResources[0].QueueName = "changed"
	if snapshot.CurrentResources[0].QueueName == "changed" {
		t.Fatal("resource identity slice was not cloned")
	}
	snapshot.ProposedResources[0].Epoch = 3
	if err := snapshot.Validate(); err == nil {
		t.Fatal("wrong proposed resource epoch accepted")
	}
}
