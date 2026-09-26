package qualification

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/cloud"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/routing"
	"github.com/solacese/solace-workload-balancer/semp"
)

const (
	resourceMembershipLVQ           = "membership-lvq"
	resourceParticipantUpdateQueue  = "participant-update-queue"
	resourceParticipantCommand      = "participant-command-queue"
	resourceParticipantRegistration = "participant-registration-queue"
	resourceParticipantAck          = "participant-acknowledgement-queue"
	resourceParticipantTelemetry    = "participant-telemetry-queue"
	resourceEpochQueue              = "epoch-queue"
)

type queueResource struct {
	ref          ResourceRef
	spec         semp.QueueSpec
	subscription string
}

type resourcePlan struct {
	public ProvisionedPlan
	queues []queueResource
}

func buildPlan(bundles Bundles, namespace, runID string, options Options) (resourcePlan, error) {
	if err := validateNamespace(namespace); err != nil {
		return resourcePlan{}, err
	}
	if err := validateBundle("broker-0", bundles.Control, true); err != nil {
		return resourcePlan{}, err
	}
	roles := [3]string{"broker-a", "broker-b", "broker-c"}
	seen := map[string]bool{bundles.Control.ServiceID: true}
	for index, bundle := range bundles.Data {
		if err := validateBundle(roles[index], bundle, true); err != nil {
			return resourcePlan{}, err
		}
		if seen[bundle.ServiceID] {
			return resourcePlan{}, errors.New("qualification: four distinct service IDs are required")
		}
		seen[bundle.ServiceID] = true
	}
	if runID == "" {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s", namespace, bundles.Control.ServiceID, options.Now().UTC().Format("20060102150405.000000000"))))
		runID = hex.EncodeToString(digest[:6])
	}
	if !validComponent(runID) {
		return resourcePlan{}, errors.New("qualification: run ID must contain lowercase letters, digits, dash, underscore, or dot")
	}

	names, err := control.NewManagedNames(namespace)
	if err != nil {
		return resourcePlan{}, err
	}
	participants := QualificationParticipants{
		Publisher:  "qualification-publisher-" + runID,
		Subscriber: "qualification-subscriber-" + runID,
		Observer:   "qualification-observer-" + runID,
	}
	public := ProvisionedPlan{
		Namespace: namespace, RunID: runID, Bundles: bundles,
		MembershipQueues: make(map[string]string, 2), Participants: participants,
	}
	plan := resourcePlan{public: public}
	for _, group := range []string{FlightGroup, BaggageGroup} {
		snapshotTopic, _ := names.SnapshotTopic(group)
		updateTopic, _ := names.UpdateTopic(group)
		baseLVQ, _ := names.MembershipQueue(group)
		lvqName := boundedQueue(baseLVQ + "." + runID)
		plan.public.MembershipQueues[group] = lvqName
		lvqSpec := membershipLVQSpec(bundles.Control.MessageVPN, lvqName, options)
		lvqSpec.Owner = bundles.Control.ServiceCredential.Username
		lvqSpec.Permission = "read-only"
		plan.addQueue("broker-0", bundles.Control.MessageVPN, resourceMembershipLVQ, lvqName, snapshotTopic, lvqSpec)

		for _, participant := range []struct {
			identity string
			role     control.ParticipantRole
		}{{participants.Publisher, control.RolePublisher}, {participants.Subscriber, control.RoleSubscriber}} {
			updateName, err := names.UpdateQueue(group, participant.identity)
			if err != nil {
				return resourcePlan{}, err
			}
			plan.addBroker0Queue(resourceParticipantUpdateQueue, broker0.ParticipantQueue{Participant: participant.identity, Principal: bundles.Control.ServiceCredential.Username, Role: participant.role, Group: group, Kind: broker0.KindMembershipSnapshot, Queue: updateName, Topic: updateTopic}, options)
			commandName, err := names.CommandQueue(group, participant.role, participant.identity)
			if err != nil {
				return resourcePlan{}, err
			}
			commandTopic, err := names.CommandTopic(group, participant.role, participant.identity)
			if err != nil {
				return resourcePlan{}, err
			}
			plan.addBroker0Queue(resourceParticipantCommand, broker0.ParticipantQueue{Participant: participant.identity, Principal: bundles.Control.ServiceCredential.Username, Role: participant.role, Group: group, Kind: broker0.KindCommand, Queue: commandName, Topic: commandTopic}, options)
			registrationName, err := names.RegistrationQueueFor(group, participant.role, participant.identity)
			if err != nil {
				return resourcePlan{}, err
			}
			registrationTopic, err := names.RegistrationTopicFor(group, participant.role, participant.identity)
			if err != nil {
				return resourcePlan{}, err
			}
			plan.addBroker0Queue(resourceParticipantRegistration, broker0.ParticipantQueue{Participant: participant.identity, Principal: bundles.Control.ServiceCredential.Username, Role: participant.role, Group: group, Kind: broker0.KindRegistration, Queue: registrationName, Topic: registrationTopic}, options)
			ackName, err := names.AcknowledgementQueueFor(group, participant.role, participant.identity)
			if err != nil {
				return resourcePlan{}, err
			}
			ackTopic, err := names.AcknowledgementTopicFor(group, participant.role, participant.identity)
			if err != nil {
				return resourcePlan{}, err
			}
			plan.addBroker0Queue(resourceParticipantAck, broker0.ParticipantQueue{Participant: participant.identity, Principal: bundles.Control.ServiceCredential.Username, Role: participant.role, Group: group, Kind: broker0.KindAcknowledgement, Queue: ackName, Topic: ackTopic}, options)
		}
		telemetryName, err := names.TelemetryQueueFor(group, control.RoleObserver, participants.Observer)
		if err != nil {
			return resourcePlan{}, err
		}
		telemetryTopic, err := names.TelemetryTopicFor(group, control.RoleObserver, participants.Observer)
		if err != nil {
			return resourcePlan{}, err
		}
		plan.addBroker0Queue(resourceParticipantTelemetry, broker0.ParticipantQueue{Participant: participants.Observer, Principal: bundles.Control.ServiceCredential.Username, Role: control.RoleObserver, Group: group, Kind: broker0.KindTelemetry, Queue: telemetryName, Topic: telemetryTopic}, options)
	}
	definitions := []struct {
		group, contract string
		partitioned     bool
		memberships     [][]string
	}{
		{FlightGroup, routing.FlightOperationsDomain, true, [][]string{{"broker-a"}, {"broker-a", "broker-b"}, {"broker-a", "broker-b", "broker-c"}, {"broker-a", "broker-b"}}},
		{BaggageGroup, routing.BaggageDomain, false, [][]string{{"broker-a", "broker-b", "broker-c"}}},
	}
	for _, definition := range definitions {
		groupPlan := GroupPlan{Group: definition.group, Contract: definition.contract, Partitioned: definition.partitioned}
		for epochIndex, membership := range definition.memberships {
			epoch := uint64(epochIndex + 1)
			ingressPattern, err := names.EpochIngressTopic(definition.group, epoch)
			if err != nil {
				return resourcePlan{}, err
			}
			destination := strings.TrimSuffix(ingressPattern, ">") + "events"
			epochPlan := EpochPlan{Epoch: epoch, Destination: destination, IngressTopic: ingressPattern, BrokerIDs: append([]string(nil), membership...), Queues: make(map[string]string, len(membership)), MessageVPNs: make(map[string]string, len(membership))}
			for _, role := range membership {
				index := roleIndex(role)
				queue, err := names.EpochConsumerQueueForBroker(definition.group, role, QualificationConsumerSet+"-"+runID, epoch)
				if err != nil {
					return resourcePlan{}, err
				}
				queue = boundedQueue(queue)
				access := "exclusive"
				partitions := uint32(0)
				if definition.partitioned {
					access, partitions = "non-exclusive", options.PartitionCount
				}
				spec := queueSpec(bundles.Data[index].MessageVPN, queue, access, options.QueueMaxSpoolMB, partitions, options)
				// Only epoch 1 accepts ingress at bootstrap. Every transition target is
				// provisioned fenced and is enabled by the controller after commit.
				spec.IngressEnabled = epoch == 1
				plan.addQueue(role, bundles.Data[index].MessageVPN, resourceEpochQueue, queue, ingressPattern, spec)
				epochPlan.Queues[role] = queue
				epochPlan.MessageVPNs[role] = bundles.Data[index].MessageVPN
			}
			groupPlan.Epochs = append(groupPlan.Epochs, epochPlan)
		}
		first := groupPlan.Epochs[0]
		groupPlan.Epoch = first.Epoch
		groupPlan.Destination = first.Destination
		groupPlan.BrokerIDs = append([]string(nil), first.BrokerIDs...)
		groupPlan.Queues = cloneStringMap(first.Queues)
		plan.public.Groups = append(plan.public.Groups, groupPlan)
	}
	return plan, nil
}

func roleIndex(role string) int {
	switch role {
	case "broker-a":
		return 0
	case "broker-b":
		return 1
	case "broker-c":
		return 2
	default:
		panic("qualification: unknown broker role " + role)
	}
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func (p *resourcePlan) addQueue(role, vpn, kind, queue, topic string, spec semp.QueueSpec) {
	ref := ResourceRef{Role: role, Kind: kind, MessageVPN: vpn, Queue: queue, Topic: topic}
	p.queues = append(p.queues, queueResource{ref: ref, spec: spec, subscription: topic})
	p.public.Resources = append(p.public.Resources, ref)
}

func (p *resourcePlan) addBroker0Queue(kind string, binding broker0.ParticipantQueue, options Options) {
	vpn := p.public.Bundles.Control.MessageVPN
	spec := queueSpec(vpn, binding.Queue, "exclusive", options.QueueMaxSpoolMB, 0, options)
	// Cloud bundles expose one existing service credential. Qualification assigns
	// it as owner so the run proves exact durable routing without creating or
	// rotating principals; this does not claim production identity isolation.
	spec.Owner = binding.Principal
	spec.Permission = "no-access"
	p.addQueue("broker-0", vpn, kind, binding.Queue, binding.Topic, spec)
	p.public.Broker0Queues = append(p.public.Broker0Queues, binding)
}

func membershipLVQSpec(vpn, name string, options Options) semp.QueueSpec {
	// Solace defines maxMsgSpoolUsage=0 as a last-value queue: only the most
	// recently received message is spooled and quota checking is disabled.
	return queueSpec(vpn, name, "exclusive", 0, 0, options)
}

func queueSpec(vpn, name, access string, spool uint64, partitions uint32, options Options) semp.QueueSpec {
	return semp.QueueSpec{
		MessageVPN: vpn, Name: name, AccessType: access, Permission: "delete",
		IngressEnabled: true, EgressEnabled: true, MaxMsgSpoolUsage: spool,
		MaxRedeliveryCount: options.QueueMaxRedeliveries, PartitionCount: partitions,
		ConsumerAckPropagationEnabled: true, MaxDeliveredUnackedMsgsPerFlow: 1000,
		RejectMsgToSenderOnDiscardBehavior: "always",
	}
}

func validateBundle(role string, bundle cloud.ConnectionBundle, requireManagement bool) error {
	if bundle.ServiceID == "" || bundle.MessageVPN == "" || len(bundle.SMFHosts) == 0 || bundle.ServiceCredential.Username == "" || bundle.ServiceCredential.Password == "" {
		return fmt.Errorf("qualification: %s bundle requires service ID, VPN, SMF endpoint, and service username/password", role)
	}
	if requireManagement && (len(bundle.ManagementURLs) == 0 || bundle.ManagementCredential.Username == "" || bundle.ManagementCredential.Password == "") {
		return fmt.Errorf("qualification: %s bundle requires SEMP URL and basic management credentials", role)
	}
	return nil
}

func boundedQueue(value string) string {
	if len(value) <= control.MaxManagedQueueBytes {
		return value
	}
	digest := sha256.Sum256([]byte(value))
	return value[:control.MaxManagedQueueBytes-17] + "-" + hex.EncodeToString(digest[:8])
}

func validComponent(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}
