package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const BootstrapProtocolVersion uint32 = 1

// SnapshotRequest asks the controller for its authoritative durable state for
// one participant and scaling group. CorrelationID is unique per attempt.
type SnapshotRequest struct {
	Version       uint32          `json:"version"`
	CorrelationID string          `json:"correlation_id"`
	Namespace     string          `json:"namespace"`
	Group         string          `json:"group"`
	Participant   string          `json:"participant"`
	Role          ParticipantRole `json:"role"`
	RequestedAt   time.Time       `json:"requested_at"`
}

func (r SnapshotRequest) Validate() error {
	if r.Version != BootstrapProtocolVersion {
		return fmt.Errorf("control: unsupported snapshot request version %d", r.Version)
	}
	for field, value := range map[string]string{
		"correlation ID": r.CorrelationID,
		"namespace":      r.Namespace,
		"group":          r.Group,
		"participant":    r.Participant,
	} {
		if err := validateIdentifier(field, value); err != nil {
			return err
		}
	}
	if err := r.Role.Validate(); err != nil {
		return err
	}
	if r.RequestedAt.IsZero() {
		return errors.New("control: snapshot request timestamp is required")
	}
	return nil
}

// SnapshotResponse returns the complete controller state for the request scope.
// The AMQP correlation-id must equal CorrelationID as an independent transport
// check; the duplicate JSON field keeps fake and future transports equally safe.
type SnapshotResponse struct {
	Version       uint32             `json:"version"`
	CorrelationID string             `json:"correlation_id"`
	Namespace     string             `json:"namespace"`
	Group         string             `json:"group"`
	Participant   string             `json:"participant"`
	Role          ParticipantRole    `json:"role"`
	Snapshot      MembershipSnapshot `json:"snapshot"`
}

func (r SnapshotResponse) ValidateFor(request SnapshotRequest) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("control: invalid snapshot request: %w", err)
	}
	if r.Version != BootstrapProtocolVersion {
		return fmt.Errorf("control: unsupported snapshot response version %d", r.Version)
	}
	if r.CorrelationID != request.CorrelationID || r.Namespace != request.Namespace ||
		r.Group != request.Group || r.Participant != request.Participant || r.Role != request.Role {
		return errors.New("control: snapshot response scope does not match request")
	}
	if err := r.Snapshot.Validate(); err != nil {
		return err
	}
	if r.Snapshot.Namespace != r.Namespace || r.Snapshot.ScalingGroup != r.Group {
		return errors.New("control: snapshot response contains cross-scope state")
	}
	return nil
}

func ParseSnapshotRequest(data []byte) (SnapshotRequest, error) {
	return parseStrictBootstrap(data, func(value SnapshotRequest) error { return value.Validate() })
}

func ParseSnapshotResponse(data []byte) (SnapshotResponse, error) {
	return parseStrictBootstrap(data, func(value SnapshotResponse) error {
		if value.Version != BootstrapProtocolVersion {
			return fmt.Errorf("control: unsupported snapshot response version %d", value.Version)
		}
		if err := value.Snapshot.Validate(); err != nil {
			return err
		}
		return nil
	})
}

func parseStrictBootstrap[T any](data []byte, validate func(T) error) (T, error) {
	var value T
	if len(data) > maxSnapshotBytes {
		return value, errors.New("control: bootstrap envelope exceeds one MiB")
	}
	if err := rejectDuplicateOrSecretFields(data); err != nil {
		return value, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("control: decode bootstrap envelope: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return value, errors.New("control: bootstrap envelope contains trailing JSON")
		}
		return value, fmt.Errorf("control: decode trailing bootstrap data: %w", err)
	}
	if err := validate(value); err != nil {
		return value, err
	}
	return value, nil
}
