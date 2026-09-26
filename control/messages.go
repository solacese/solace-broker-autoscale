package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const ProtocolVersion uint32 = 1

type ParticipantRole string

const (
	RolePublisher  ParticipantRole = "publisher"
	RoleSubscriber ParticipantRole = "subscriber"
	RoleBroker     ParticipantRole = "broker"
	RoleObserver   ParticipantRole = "observer"
)

func (r ParticipantRole) Validate() error {
	switch r {
	case RolePublisher, RoleSubscriber, RoleBroker, RoleObserver:
		return nil
	default:
		return fmt.Errorf("control: unsupported participant role %q", r)
	}
}

// RegistrationEnvelope announces a participant's identity and role for one
// exact group epoch and observed phase. Registrations are durable, idempotent
// messages keyed by MessageID.
type RegistrationEnvelope struct {
	Version        uint32          `json:"version"`
	MessageID      string          `json:"message_id"`
	Namespace      string          `json:"namespace"`
	Group          string          `json:"group"`
	TransitionID   string          `json:"transition_id,omitempty"`
	Epoch          uint64          `json:"epoch"`
	Phase          Phase           `json:"phase"`
	Participant    string          `json:"participant"`
	Role           ParticipantRole `json:"role"`
	ConsumerSet    string          `json:"consumer_set,omitempty"`
	LibraryVersion string          `json:"library_version"`
	ObservedAt     time.Time       `json:"observed_at"`
}

func (e RegistrationEnvelope) Validate() error {
	if err := validateEnvelopeScope(e.Version, e.MessageID, e.Namespace, e.Group, e.TransitionID, e.Epoch, e.Phase, e.Participant, e.Role, e.ObservedAt, false); err != nil {
		return err
	}
	if e.LibraryVersion == "" {
		return errors.New("control: registration library version is required")
	}
	if err := validateIdentifier("library version", e.LibraryVersion); err != nil {
		return err
	}
	if e.Role == RoleSubscriber {
		if err := validateNameComponent("consumer set", e.ConsumerSet); err != nil {
			return err
		}
	} else if e.ConsumerSet != "" {
		return errors.New("control: consumer set is valid only for subscriber registrations")
	}
	return nil
}

// CommandEnvelope requests one phase-specific action from one participant.
type CommandEnvelope struct {
	Version      uint32          `json:"version"`
	MessageID    string          `json:"message_id"`
	Namespace    string          `json:"namespace"`
	Group        string          `json:"group"`
	TransitionID string          `json:"transition_id"`
	Epoch        uint64          `json:"epoch"`
	Phase        Phase           `json:"phase"`
	Participant  string          `json:"participant"`
	Role         ParticipantRole `json:"role"`
	IssuedAt     time.Time       `json:"issued_at"`
	Deadline     time.Time       `json:"deadline"`
}

func (e CommandEnvelope) Validate() error {
	if err := validateEnvelopeScope(e.Version, e.MessageID, e.Namespace, e.Group, e.TransitionID, e.Epoch, e.Phase, e.Participant, e.Role, e.IssuedAt, true); err != nil {
		return err
	}
	if !roleAllowed(e.Role, e.Phase, false) {
		return fmt.Errorf("control: role %q cannot receive %s commands", e.Role, e.Phase)
	}
	if e.Deadline.IsZero() || !e.Deadline.After(e.IssuedAt) {
		return errors.New("control: command deadline must follow issued_at")
	}
	return nil
}

// AcknowledgementEnvelope confirms one exact command and cannot be reused for
// another role, phase, participant, transition, or epoch.
type AcknowledgementEnvelope struct {
	Version      uint32          `json:"version"`
	MessageID    string          `json:"message_id"`
	CommandID    string          `json:"command_id"`
	Namespace    string          `json:"namespace"`
	Group        string          `json:"group"`
	TransitionID string          `json:"transition_id"`
	Epoch        uint64          `json:"epoch"`
	Phase        Phase           `json:"phase"`
	Participant  string          `json:"participant"`
	Role         ParticipantRole `json:"role"`
	Ready        bool            `json:"ready"`
	ObservedAt   time.Time       `json:"observed_at"`
}

func (e AcknowledgementEnvelope) Validate() error {
	if err := validateEnvelopeScope(e.Version, e.MessageID, e.Namespace, e.Group, e.TransitionID, e.Epoch, e.Phase, e.Participant, e.Role, e.ObservedAt, true); err != nil {
		return err
	}
	if err := validateIdentifier("command ID", e.CommandID); err != nil {
		return err
	}
	if !roleAllowed(e.Role, e.Phase, false) {
		return fmt.Errorf("control: role %q cannot acknowledge %s commands", e.Role, e.Phase)
	}
	if !e.Ready {
		return errors.New("control: acknowledgement must explicitly report ready")
	}
	return nil
}

// ValidateForCommand binds an acknowledgement to the exact command that
// authorized it. Checking the phase alone is insufficient because command IDs,
// namespaces, participants, and roles may otherwise be replayed across scopes.
func (e AcknowledgementEnvelope) ValidateForCommand(command CommandEnvelope) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if err := command.Validate(); err != nil {
		return fmt.Errorf("control: invalid acknowledged command: %w", err)
	}
	if e.CommandID != command.MessageID || e.Namespace != command.Namespace ||
		e.Group != command.Group || e.TransitionID != command.TransitionID ||
		e.Epoch != command.Epoch || e.Phase != command.Phase ||
		e.Participant != command.Participant || e.Role != command.Role {
		return errors.New("control: acknowledgement scope does not match issued command")
	}
	return nil
}

type MetricCount struct {
	Known bool   `json:"known"`
	Value uint64 `json:"value"`
}

// TelemetryEnvelope reports source-epoch drain evidence. SourceBroker and
// SourceQueue identify the exact resource measured so independent sources cannot
// be collapsed into one group-wide proof. It is deliberately valid only in
// DRAIN and from broker or observer roles.
type TelemetryEnvelope struct {
	Version      uint32          `json:"version"`
	MessageID    string          `json:"message_id"`
	Namespace    string          `json:"namespace"`
	Group        string          `json:"group"`
	TransitionID string          `json:"transition_id"`
	Epoch        uint64          `json:"epoch"`
	Phase        Phase           `json:"phase"`
	Participant  string          `json:"participant"`
	Role         ParticipantRole `json:"role"`
	SourceBroker string          `json:"source_broker"`
	SourceQueue  string          `json:"source_queue"`
	ObservedAt   time.Time       `json:"observed_at"`
	Queued       MetricCount     `json:"queued"`
	Stored       MetricCount     `json:"stored"`
	Unacked      MetricCount     `json:"unacked"`
}

func (e TelemetryEnvelope) Validate() error {
	if err := validateEnvelopeScope(e.Version, e.MessageID, e.Namespace, e.Group, e.TransitionID, e.Epoch, e.Phase, e.Participant, e.Role, e.ObservedAt, true); err != nil {
		return err
	}
	if e.Phase != PhaseDrain || !roleAllowed(e.Role, e.Phase, true) {
		return errors.New("control: telemetry is valid only for broker or observer roles in DRAIN")
	}
	if err := validateIdentifier("source broker", e.SourceBroker); err != nil {
		return err
	}
	if err := validateIdentifier("source queue", e.SourceQueue); err != nil {
		return err
	}
	for name, count := range map[string]MetricCount{"queued": e.Queued, "stored": e.Stored, "unacked": e.Unacked} {
		if !count.Known && count.Value != 0 {
			return fmt.Errorf("control: unknown %s count must not carry a value", name)
		}
	}
	return nil
}

func ParseRegistrationEnvelope(data []byte) (RegistrationEnvelope, error) {
	return parseEnvelope(data, func(e RegistrationEnvelope) error { return e.Validate() })
}

func ParseCommandEnvelope(data []byte) (CommandEnvelope, error) {
	return parseEnvelope(data, func(e CommandEnvelope) error { return e.Validate() })
}

func ParseAcknowledgementEnvelope(data []byte) (AcknowledgementEnvelope, error) {
	return parseEnvelope(data, func(e AcknowledgementEnvelope) error { return e.Validate() })
}

func ParseTelemetryEnvelope(data []byte) (TelemetryEnvelope, error) {
	return parseEnvelope(data, func(e TelemetryEnvelope) error { return e.Validate() })
}

func parseEnvelope[T any](data []byte, validate func(T) error) (T, error) {
	var envelope T
	if len(data) > maxSnapshotBytes {
		return envelope, errors.New("control: envelope exceeds one MiB")
	}
	if err := rejectDuplicateOrSecretFields(data); err != nil {
		return envelope, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return envelope, fmt.Errorf("control: decode envelope: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return envelope, errors.New("control: envelope contains trailing JSON value")
		}
		return envelope, fmt.Errorf("control: decode trailing envelope data: %w", err)
	}
	if err := validate(envelope); err != nil {
		return envelope, err
	}
	return envelope, nil
}

func validateEnvelopeScope(version uint32, messageID, namespace, group, transitionID string, epoch uint64, phase Phase, participant string, role ParticipantRole, observedAt time.Time, transitionRequired bool) error {
	if version != ProtocolVersion {
		return fmt.Errorf("control: unsupported protocol version %d", version)
	}
	for field, value := range map[string]string{"message ID": messageID, "group": group, "participant": participant} {
		if err := validateIdentifier(field, value); err != nil {
			return err
		}
	}
	if err := validateNameComponent("namespace", namespace); err != nil {
		return err
	}
	if transitionRequired || transitionID != "" {
		if err := validateIdentifier("transition ID", transitionID); err != nil {
			return err
		}
	}
	if epoch == 0 {
		return errors.New("control: envelope epoch must be greater than zero")
	}
	if !validPhase(phase) {
		return fmt.Errorf("control: unsupported phase %q", phase)
	}
	if err := role.Validate(); err != nil {
		return err
	}
	if observedAt.IsZero() {
		return errors.New("control: envelope timestamp is required")
	}
	return nil
}

func validPhase(phase Phase) bool {
	switch phase {
	case PhaseActive, PhasePrepare, PhasePaused, PhaseDrain, PhaseCommitted:
		return true
	default:
		return false
	}
}

func roleAllowed(role ParticipantRole, phase Phase, telemetry bool) bool {
	if telemetry {
		return phase == PhaseDrain && (role == RoleBroker || role == RoleObserver)
	}
	switch role {
	case RolePublisher:
		return phase == PhasePaused || phase == PhaseActive
	case RoleSubscriber:
		return phase == PhasePrepare || phase == PhaseDrain || phase == PhaseActive
	case RoleBroker:
		return validPhase(phase)
	default:
		return false
	}
}
