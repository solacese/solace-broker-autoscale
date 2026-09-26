// Package broker0 defines the transport-neutral Broker 0 control channel.
package broker0

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
)

// Kind identifies the shared control payload expected on a native destination.
type Kind string

const (
	KindMembershipSnapshot Kind = "membership_snapshot"
	KindRegistration       Kind = "registration"
	KindCommand            Kind = "command"
	KindAcknowledgement    Kind = "acknowledgement"
	KindTelemetry          Kind = "telemetry"
)

// Message is a validated typed Broker 0 delivery. Exactly one payload pointer is
// non-nil. Wire schemas and strict parsers are owned by package control; Broker 0
// only supplies transport metadata such as OperationID and destination kind.
type Message struct {
	Kind            Kind
	OperationID     string
	Group           string
	Snapshot        *control.MembershipSnapshot
	Registration    *control.RegistrationEnvelope
	Command         *control.CommandEnvelope
	Acknowledgement *control.AcknowledgementEnvelope
	Telemetry       *control.TelemetryEnvelope
}

func encodeMembershipSnapshot(snapshot control.MembershipSnapshot) ([]byte, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	return marshalPayload(KindMembershipSnapshot, snapshot)
}

func encodeRegistration(registration control.RegistrationEnvelope) ([]byte, error) {
	if err := registration.Validate(); err != nil {
		return nil, err
	}
	return marshalPayload(KindRegistration, registration)
}

func encodeCommand(command control.CommandEnvelope) ([]byte, error) {
	if err := command.Validate(); err != nil {
		return nil, err
	}
	return marshalPayload(KindCommand, command)
}

func encodeAcknowledgement(acknowledgement control.AcknowledgementEnvelope) ([]byte, error) {
	if err := acknowledgement.Validate(); err != nil {
		return nil, err
	}
	return marshalPayload(KindAcknowledgement, acknowledgement)
}

func encodeTelemetry(telemetry control.TelemetryEnvelope) ([]byte, error) {
	if err := telemetry.Validate(); err != nil {
		return nil, err
	}
	return marshalPayload(KindTelemetry, telemetry)
}

func marshalPayload(kind Kind, payload any) ([]byte, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("broker0: encode %s payload: %w", kind, err)
	}
	return data, nil
}

// parseMessage delegates strict JSON validation to package control. operationID
// is native transport metadata for membership snapshots and must match MessageID
// for reliable control envelopes.
func parseMessage(kind Kind, operationID string, data []byte) (Message, error) {
	message := Message{Kind: kind, OperationID: operationID}
	switch kind {
	case KindMembershipSnapshot:
		if operationID != "" {
			if err := validateOperationID(operationID); err != nil {
				return Message{}, err
			}
		}
		snapshot, err := control.ParseMembershipSnapshot(data)
		if err != nil {
			return Message{}, fmt.Errorf("broker0: membership payload: %w", err)
		}
		message.Group = snapshot.ScalingGroup
		message.Snapshot = &snapshot
	case KindRegistration:
		registration, err := control.ParseRegistrationEnvelope(data)
		if err != nil {
			return Message{}, fmt.Errorf("broker0: registration payload: %w", err)
		}
		if operationID != "" && operationID != registration.MessageID {
			return Message{}, fmt.Errorf("broker0: transport operation %q does not match registration message %q", operationID, registration.MessageID)
		}
		message.OperationID = registration.MessageID
		message.Group = registration.Group
		message.Registration = &registration
	case KindCommand:
		command, err := control.ParseCommandEnvelope(data)
		if err != nil {
			return Message{}, fmt.Errorf("broker0: command payload: %w", err)
		}
		if operationID != "" && operationID != command.MessageID {
			return Message{}, fmt.Errorf("broker0: transport operation %q does not match command message %q", operationID, command.MessageID)
		}
		message.OperationID = command.MessageID
		message.Group = command.Group
		message.Command = &command
	case KindAcknowledgement:
		acknowledgement, err := control.ParseAcknowledgementEnvelope(data)
		if err != nil {
			return Message{}, fmt.Errorf("broker0: acknowledgement payload: %w", err)
		}
		if operationID != "" && operationID != acknowledgement.MessageID {
			return Message{}, fmt.Errorf("broker0: transport operation %q does not match acknowledgement message %q", operationID, acknowledgement.MessageID)
		}
		message.OperationID = acknowledgement.MessageID
		message.Group = acknowledgement.Group
		message.Acknowledgement = &acknowledgement
	case KindTelemetry:
		telemetry, err := control.ParseTelemetryEnvelope(data)
		if err != nil {
			return Message{}, fmt.Errorf("broker0: telemetry payload: %w", err)
		}
		if operationID != "" && operationID != telemetry.MessageID {
			return Message{}, fmt.Errorf("broker0: transport operation %q does not match telemetry message %q", operationID, telemetry.MessageID)
		}
		message.OperationID = telemetry.MessageID
		message.Group = telemetry.Group
		message.Telemetry = &telemetry
	default:
		return Message{}, fmt.Errorf("broker0: unsupported payload kind %q", kind)
	}
	return message, nil
}

func validateOperationID(operationID string) error {
	return validateTransportIdentifier("operation ID", operationID)
}

func validateTransportIdentifier(field, value string) error {
	if value == "" || len(value) > 1024 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("broker0: %s must be 1-1024 valid UTF-8 bytes without NUL", field)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("broker0: %s contains a control character", field)
		}
	}
	return nil
}

func acknowledgementForController(envelope control.AcknowledgementEnvelope) (controller.Acknowledgement, error) {
	phase, err := controllerPhase(envelope.Phase)
	if err != nil {
		return controller.Acknowledgement{}, err
	}
	return controller.Acknowledgement{
		CommandID:    envelope.CommandID,
		Namespace:    envelope.Namespace,
		Group:        envelope.Group,
		TransitionID: envelope.TransitionID,
		Epoch:        envelope.Epoch,
		Phase:        phase,
		Participant:  envelope.Participant,
		Role:         envelope.Role,
		ObservedAt:   envelope.ObservedAt,
	}, nil
}

func telemetryForController(envelope control.TelemetryEnvelope) controller.Telemetry {
	return controller.Telemetry{
		MessageID:    envelope.MessageID,
		Namespace:    envelope.Namespace,
		Group:        envelope.Group,
		TransitionID: envelope.TransitionID,
		Epoch:        envelope.Epoch,
		Participant:  envelope.Participant,
		Role:         envelope.Role,
		SourceBroker: envelope.SourceBroker,
		SourceQueue:  envelope.SourceQueue,
		ObservedAt:   envelope.ObservedAt,
		Queued:       controller.Count{Known: envelope.Queued.Known, Value: envelope.Queued.Value},
		Stored:       controller.Count{Known: envelope.Stored.Known, Value: envelope.Stored.Value},
		Unacked:      controller.Count{Known: envelope.Unacked.Known, Value: envelope.Unacked.Value},
	}
}

func controllerPhase(phase control.Phase) (controller.Phase, error) {
	switch phase {
	case control.PhasePrepare:
		return controller.PhasePrepare, nil
	case control.PhasePaused:
		return controller.PhasePause, nil
	case control.PhaseDrain:
		return controller.PhaseDrain, nil
	case control.PhaseCommitted:
		return controller.PhaseCommit, nil
	case control.PhaseActive:
		return controller.PhaseActivate, nil
	default:
		return "", fmt.Errorf("broker0: unsupported controller phase %q", phase)
	}
}
