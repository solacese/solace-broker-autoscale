package control

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const (
	// MaxManagedTopicBytes and MaxManagedQueueBytes are conservative bounds for
	// controller-managed Solace resource names.
	MaxManagedTopicBytes = 250
	MaxManagedQueueBytes = 200
)

// ManagedNames constructs deterministic names below one explicitly managed
// namespace. Inputs are validated rather than silently normalized so distinct
// customer identities cannot collapse onto the same resource.
type ManagedNames struct {
	namespace string
}

func NewManagedNames(namespace string) (ManagedNames, error) {
	if err := validateNameComponent("namespace", namespace); err != nil {
		return ManagedNames{}, err
	}
	return ManagedNames{namespace: namespace}, nil
}

func (n ManagedNames) Namespace() string { return n.namespace }

func (n ManagedNames) SnapshotTopic(group string) (string, error) {
	return n.topic(group, "snapshot")
}

// MembershipQueue is the durable last-value queue browsed for authoritative
// membership. Its queue policy, including a zero message-spool quota, is set by
// the provisioner rather than encoded in the name.
func (n ManagedNames) MembershipQueue(group string) (string, error) {
	return n.queue(group, "membership")
}

func (n ManagedNames) UpdateTopic(group string) (string, error) {
	return n.topic(group, "updates")
}

// UpdateQueue is a durable live-update queue dedicated to one participant.
// Updates remain hints; participants reconcile them against MembershipQueue.
func (n ManagedNames) UpdateQueue(group, participant string) (string, error) {
	if err := validateNameComponent("participant", participant); err != nil {
		return "", err
	}
	return boundedName(MaxManagedQueueBytes, ".", n.namespace, "ctl", group, "updates", participant)
}

func (n ManagedNames) RegistrationTopic(group string) (string, error) {
	return n.topic(group, "registrations")
}

func (n ManagedNames) RegistrationTopicFor(group string, role ParticipantRole, participant string) (string, error) {
	return n.participantTopic(group, "registrations", role, participant)
}

func (n ManagedNames) AcknowledgementTopic(group string) (string, error) {
	return n.topic(group, "acknowledgements")
}

func (n ManagedNames) AcknowledgementTopicFor(group string, role ParticipantRole, participant string) (string, error) {
	return n.participantTopic(group, "acknowledgements", role, participant)
}

func (n ManagedNames) TelemetryTopic(group string) (string, error) {
	return n.topic(group, "telemetry")
}

func (n ManagedNames) TelemetryTopicFor(group string, role ParticipantRole, participant string) (string, error) {
	return n.participantTopic(group, "telemetry", role, participant)
}

func (n ManagedNames) CommandTopic(group string, role ParticipantRole, participant string) (string, error) {
	return n.participantTopic(group, "commands", role, participant)
}

func (n ManagedNames) participantTopic(group, purpose string, role ParticipantRole, participant string) (string, error) {
	if err := role.Validate(); err != nil {
		return "", err
	}
	if err := validateNameComponent("participant", participant); err != nil {
		return "", err
	}
	return boundedName(MaxManagedTopicBytes, "/", n.namespace, "control", group, purpose, string(role), participant)
}

func (n ManagedNames) RegistrationQueue(group string) (string, error) {
	return n.queue(group, "registrations")
}

func (n ManagedNames) RegistrationQueueFor(group string, role ParticipantRole, participant string) (string, error) {
	return n.participantQueue(group, "reg", role, participant)
}

func (n ManagedNames) AcknowledgementQueue(group string) (string, error) {
	return n.queue(group, "acknowledgements")
}

func (n ManagedNames) AcknowledgementQueueFor(group string, role ParticipantRole, participant string) (string, error) {
	return n.participantQueue(group, "ack", role, participant)
}

func (n ManagedNames) TelemetryQueue(group string) (string, error) {
	return n.queue(group, "telemetry")
}

func (n ManagedNames) TelemetryQueueFor(group string, role ParticipantRole, participant string) (string, error) {
	return n.participantQueue(group, "tel", role, participant)
}

func (n ManagedNames) CommandQueue(group string, role ParticipantRole, participant string) (string, error) {
	return n.participantQueue(group, "cmd", role, participant)
}

func (n ManagedNames) participantQueue(group, purpose string, role ParticipantRole, participant string) (string, error) {
	if err := role.Validate(); err != nil {
		return "", err
	}
	if err := validateNameComponent("participant", participant); err != nil {
		return "", err
	}
	return boundedName(MaxManagedQueueBytes, ".", n.namespace, "ctl", group, purpose, string(role), participant)
}

// EpochIngressTopic is an internal, epoch-specific ingress identity. Application
// topics remain in message metadata and are not replaced by this name.
func (n ManagedNames) EpochIngressTopic(group string, epoch uint64) (string, error) {
	if epoch == 0 {
		return "", fmt.Errorf("control: epoch must be greater than zero")
	}
	return boundedNamePreservingSuffix(MaxManagedTopicBytes, "/", "/>", n.namespace, "data", group, "epoch", strconv.FormatUint(epoch, 10))
}

// EpochConsumerQueue preserves the v1 single-set name shape for callers that
// manage one queue per group/consumer-set/epoch.
func (n ManagedNames) EpochConsumerQueue(group, consumerSet string, epoch uint64) (string, error) {
	if err := validateNameComponent("consumer set", consumerSet); err != nil {
		return "", err
	}
	if epoch == 0 {
		return "", fmt.Errorf("control: epoch must be greater than zero")
	}
	return boundedName(MaxManagedQueueBytes, ".", n.namespace, "data", group, consumerSet, "e"+strconv.FormatUint(epoch, 10))
}

// EpochConsumerQueueForBroker names the independent queue for one broker and
// consumer set. Broker identity is explicit so neither dimension is overloaded.
func (n ManagedNames) EpochConsumerQueueForBroker(group, broker, consumerSet string, epoch uint64) (string, error) {
	if err := validateNameComponent("broker", broker); err != nil {
		return "", err
	}
	if err := validateNameComponent("consumer set", consumerSet); err != nil {
		return "", err
	}
	if epoch == 0 {
		return "", fmt.Errorf("control: epoch must be greater than zero")
	}
	return boundedName(MaxManagedQueueBytes, ".", n.namespace, "data", group, broker, consumerSet, "e"+strconv.FormatUint(epoch, 10))
}

func (n ManagedNames) topic(group, purpose string) (string, error) {
	return boundedName(MaxManagedTopicBytes, "/", n.namespace, "control", group, purpose)
}

func (n ManagedNames) queue(group, purpose string) (string, error) {
	return boundedName(MaxManagedQueueBytes, ".", n.namespace, "ctl", group, purpose)
}

func boundedName(limit int, separator string, components ...string) (string, error) {
	for index, component := range components {
		if err := validateNameComponent(fmt.Sprintf("name component %d", index), component); err != nil {
			return "", err
		}
	}
	return boundName(limit, strings.Join(components, separator), "")
}

func boundedNamePreservingSuffix(limit int, separator, preservedSuffix string, components ...string) (string, error) {
	for index, component := range components {
		if err := validateNameComponent(fmt.Sprintf("name component %d", index), component); err != nil {
			return "", err
		}
	}
	return boundName(limit, strings.Join(components, separator)+preservedSuffix, preservedSuffix)
}

func boundName(limit int, name, preservedSuffix string) (string, error) {
	if len(name) <= limit {
		return name, nil
	}
	digest := sha256.Sum256([]byte(name))
	hashSuffix := "-" + hex.EncodeToString(digest[:8])
	keep := limit - len(hashSuffix) - len(preservedSuffix)
	if keep <= 0 {
		return "", fmt.Errorf("control: managed name limit %d is too small", limit)
	}
	return strings.TrimSuffix(name, preservedSuffix)[:keep] + hashSuffix + preservedSuffix, nil
}
