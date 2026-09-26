// Package control defines the versioned control-plane snapshot contract and
// the client-side bootstrap reconciler.
package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// SnapshotVersion is the only wire version understood by this package.
	SnapshotVersion uint32 = 2
	// AlgorithmSHA256BigEndianModulo is the only broker-selection algorithm in v1.
	AlgorithmSHA256BigEndianModulo = "sha256-unsigned-big-endian-modulo"
	maxSnapshotBytes               = 1 << 20
	maxIdentifierBytes             = 1024
	maxMembershipSize              = 1 << 16
)

// Phase describes whether the ordered membership is serving normally or is in
// a controlled transition.
type Phase string

const (
	PhaseActive    Phase = "ACTIVE"
	PhasePrepare   Phase = "PREPARE"
	PhasePaused    Phase = "PAUSED"
	PhaseDrain     Phase = "DRAIN"
	PhaseCommitted Phase = "COMMITTED"
)

// Membership is an authoritative ordered list of broker IDs. Its order is part
// of the routing contract. Clients must never sort or deduplicate it.
type Membership []string

// Clone returns an independent copy preserving the authoritative order.
func (m Membership) Clone() Membership {
	return append(Membership(nil), m...)
}

// Equal reports order-sensitive equality.
func (m Membership) Equal(other Membership) bool {
	return slicesEqual(m, other)
}

// Transition identifies the membership generation being prepared or paused.
type Transition struct {
	ID        string `json:"id"`
	FromEpoch uint64 `json:"from_epoch"`
	ToEpoch   uint64 `json:"to_epoch"`
}

// QueueInfo contains only non-secret queue identity and delivery semantics.
type QueueInfo struct {
	Name    string `json:"name"`
	Durable bool   `json:"durable"`
}

// DestinationKind identifies the non-secret destination syntax.
type DestinationKind string

const (
	DestinationTopic DestinationKind = "TOPIC"
	DestinationQueue DestinationKind = "QUEUE"
)

// DestinationInfo contains a destination name, never connection endpoints or
// credentials.
type DestinationInfo struct {
	Kind DestinationKind `json:"kind"`
	Name string          `json:"name"`
}

// EpochResourceIdentity names controller-managed resources for one consumer set
// and routing epoch. These fields are identities only; endpoints and credentials
// never belong in a control document.
type EpochResourceIdentity struct {
	Epoch        uint64 `json:"epoch"`
	BrokerID     string `json:"broker_id"`
	ConsumerSet  string `json:"consumer_set"`
	QueueName    string `json:"queue_name"`
	IngressTopic string `json:"ingress_topic"`
}

// MembershipSnapshot is a complete versioned control-plane state. Revision is
// the monotonic publication revision. Epoch changes only when a proposed
// membership becomes current. Membership slices are ordered and authoritative.
type MembershipSnapshot struct {
	Version            uint32                  `json:"version"`
	Namespace          string                  `json:"namespace,omitempty"`
	LibraryVersion     string                  `json:"library_version,omitempty"`
	ScalingGroup       string                  `json:"scaling_group"`
	Revision           uint64                  `json:"revision"`
	Epoch              uint64                  `json:"epoch"`
	Phase              Phase                   `json:"phase"`
	HashContract       string                  `json:"hash_contract"`
	Algorithm          string                  `json:"algorithm"`
	CurrentMembership  Membership              `json:"current_membership"`
	ProposedMembership Membership              `json:"proposed_membership,omitempty"`
	Transition         *Transition             `json:"transition,omitempty"`
	Queue              QueueInfo               `json:"queue"`
	Destination        DestinationInfo         `json:"destination"`
	CurrentResources   []EpochResourceIdentity `json:"current_resources,omitempty"`
	ProposedResources  []EpochResourceIdentity `json:"proposed_resources,omitempty"`
}

// Clone returns a snapshot that shares no mutable membership or transition
// storage with the receiver.
func (s MembershipSnapshot) Clone() MembershipSnapshot {
	clone := s
	clone.CurrentMembership = s.CurrentMembership.Clone()
	clone.ProposedMembership = s.ProposedMembership.Clone()
	clone.CurrentResources = append([]EpochResourceIdentity(nil), s.CurrentResources...)
	clone.ProposedResources = append([]EpochResourceIdentity(nil), s.ProposedResources...)
	if s.Transition != nil {
		transition := *s.Transition
		clone.Transition = &transition
	}
	return clone
}

// Validate rejects malformed or ambiguous snapshots. It does not normalize any
// value: membership order, spelling, and destination names remain byte-exact.
func (s MembershipSnapshot) Validate() error {
	if s.Version != SnapshotVersion {
		return fmt.Errorf("control: unsupported snapshot version %d", s.Version)
	}
	if s.Namespace != "" {
		if err := validateNameComponent("namespace", s.Namespace); err != nil {
			return err
		}
	}
	if s.LibraryVersion != "" {
		if err := validateIdentifier("library version", s.LibraryVersion); err != nil {
			return err
		}
	}
	if err := validateIdentifier("scaling group", s.ScalingGroup); err != nil {
		return err
	}
	if s.Revision == 0 {
		return errors.New("control: revision must be greater than zero")
	}
	if s.Epoch == 0 {
		return errors.New("control: epoch must be greater than zero")
	}
	if err := validateIdentifier("hash contract", s.HashContract); err != nil {
		return err
	}
	if s.Algorithm != AlgorithmSHA256BigEndianModulo {
		return fmt.Errorf("control: unsupported routing algorithm %q", s.Algorithm)
	}
	if err := validateMembership("current membership", s.CurrentMembership, true); err != nil {
		return err
	}
	if err := validateMembership("proposed membership", s.ProposedMembership, false); err != nil {
		return err
	}
	if err := validateIdentifier("queue name", s.Queue.Name); err != nil {
		return err
	}
	if !s.Queue.Durable {
		return errors.New("control: queue must be durable")
	}
	if s.Destination.Kind != DestinationTopic {
		return fmt.Errorf("control: v1 application ingress requires a topic destination, got %q", s.Destination.Kind)
	}
	if err := validateIdentifier("destination name", s.Destination.Name); err != nil {
		return err
	}
	if err := validateResources("current resources", s.CurrentResources, s.Epoch, s.CurrentMembership); err != nil {
		return err
	}

	switch s.Phase {
	case PhaseActive:
		if len(s.ProposedMembership) != 0 || s.Transition != nil || len(s.ProposedResources) != 0 {
			return errors.New("control: ACTIVE snapshot must not contain proposed state or transition")
		}
	case PhaseCommitted:
		if len(s.ProposedMembership) != 0 || s.Transition == nil || len(s.ProposedResources) != 0 {
			return errors.New("control: COMMITTED snapshot requires transition fields and current committed state only")
		}
		if err := validateIdentifier("transition ID", s.Transition.ID); err != nil {
			return err
		}
		if s.Transition.FromEpoch == ^uint64(0) || s.Transition.ToEpoch != s.Transition.FromEpoch+1 || s.Epoch != s.Transition.ToEpoch {
			return errors.New("control: COMMITTED epoch must equal the transition to_epoch")
		}
	case PhasePrepare, PhasePaused, PhaseDrain:
		if len(s.ProposedMembership) == 0 {
			return fmt.Errorf("control: %s snapshot requires a proposed membership", s.Phase)
		}
		if s.Transition != nil {
			if err := validateResources("proposed resources", s.ProposedResources, s.Transition.ToEpoch, s.ProposedMembership); err != nil {
				return err
			}
		}
		if s.CurrentMembership.Equal(s.ProposedMembership) {
			return fmt.Errorf("control: %s proposed membership must differ from current membership", s.Phase)
		}
		if s.Transition == nil {
			return fmt.Errorf("control: %s snapshot requires transition fields", s.Phase)
		}
		if err := validateIdentifier("transition ID", s.Transition.ID); err != nil {
			return err
		}
		if s.Transition.FromEpoch != s.Epoch {
			return errors.New("control: transition from_epoch must equal snapshot epoch")
		}
		if s.Epoch == ^uint64(0) || s.Transition.ToEpoch != s.Epoch+1 {
			return errors.New("control: transition to_epoch must immediately follow snapshot epoch")
		}
	default:
		return fmt.Errorf("control: unsupported phase %q", s.Phase)
	}
	return nil
}

// ParseMembershipSnapshot strictly decodes and validates one JSON object.
// Unknown fields (including credential, token, and endpoint fields), duplicate
// fields, trailing values, and inputs over one MiB are rejected.
func ParseMembershipSnapshot(data []byte) (MembershipSnapshot, error) {
	var snapshot MembershipSnapshot
	if len(data) > maxSnapshotBytes {
		return snapshot, errors.New("control: snapshot exceeds one MiB")
	}
	if err := rejectDuplicateOrSecretFields(data); err != nil {
		return snapshot, err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return MembershipSnapshot{}, fmt.Errorf("control: decode snapshot: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return MembershipSnapshot{}, errors.New("control: snapshot contains trailing JSON value")
		}
		return MembershipSnapshot{}, fmt.Errorf("control: decode trailing data: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return MembershipSnapshot{}, err
	}
	return snapshot, nil
}

func validateResources(field string, resources []EpochResourceIdentity, epoch uint64, membership Membership) error {
	if len(resources) == 0 {
		return fmt.Errorf("control: %s must not be empty", field)
	}
	brokers := make(map[string]struct{}, len(membership))
	for _, broker := range membership {
		brokers[broker] = struct{}{}
	}
	seen := make(map[string]struct{}, len(resources))
	queues := make(map[string]struct{}, len(resources))
	consumerSets := make(map[string]struct{})
	brokerSets := make(map[string]map[string]struct{}, len(membership))
	for index, resource := range resources {
		if resource.Epoch != epoch {
			return fmt.Errorf("control: %s resource %d epoch %d does not match %d", field, index, resource.Epoch, epoch)
		}
		if err := validateNameComponent(fmt.Sprintf("%s broker %d", field, index), resource.BrokerID); err != nil {
			return err
		}
		if _, member := brokers[resource.BrokerID]; !member {
			return fmt.Errorf("control: %s resource %d references broker %q outside membership", field, index, resource.BrokerID)
		}
		if err := validateNameComponent(fmt.Sprintf("%s consumer set %d", field, index), resource.ConsumerSet); err != nil {
			return err
		}
		if err := validateIdentifier(fmt.Sprintf("%s queue name %d", field, index), resource.QueueName); err != nil {
			return err
		}
		if err := validateIdentifier(fmt.Sprintf("%s ingress topic %d", field, index), resource.IngressTopic); err != nil {
			return err
		}
		key := resource.BrokerID + "\x00" + resource.ConsumerSet
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("control: %s contains duplicate broker %q consumer set %q", field, resource.BrokerID, resource.ConsumerSet)
		}
		seen[key] = struct{}{}
		consumerSets[resource.ConsumerSet] = struct{}{}
		if brokerSets[resource.BrokerID] == nil {
			brokerSets[resource.BrokerID] = make(map[string]struct{})
		}
		brokerSets[resource.BrokerID][resource.ConsumerSet] = struct{}{}
		if _, duplicate := queues[resource.QueueName]; duplicate {
			return fmt.Errorf("control: %s contains duplicate queue name %q", field, resource.QueueName)
		}
		queues[resource.QueueName] = struct{}{}
	}
	for _, broker := range membership {
		sets := brokerSets[broker]
		if len(sets) != len(consumerSets) {
			return fmt.Errorf("control: %s broker %q does not cover every consumer set", field, broker)
		}
		for consumerSet := range consumerSets {
			if _, ok := sets[consumerSet]; !ok {
				return fmt.Errorf("control: %s broker %q is missing consumer set %q", field, broker, consumerSet)
			}
		}
	}
	return nil
}

func validateMembership(field string, membership Membership, required bool) error {
	if required && len(membership) == 0 {
		return fmt.Errorf("control: %s must not be empty", field)
	}
	if len(membership) > maxMembershipSize {
		return fmt.Errorf("control: %s exceeds %d brokers", field, maxMembershipSize)
	}
	seen := make(map[string]struct{}, len(membership))
	for index, broker := range membership {
		if err := validateIdentifier(fmt.Sprintf("%s broker %d", field, index), broker); err != nil {
			return err
		}
		if _, exists := seen[broker]; exists {
			return fmt.Errorf("control: %s contains duplicate broker %q; clients must not deduplicate membership", field, broker)
		}
		seen[broker] = struct{}{}
	}
	return nil
}

func validateNameComponent(field, value string) error {
	if err := validateIdentifier(field, value); err != nil {
		return err
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '-' && r != '_' && r != '.' {
			return fmt.Errorf("control: %s must contain only lowercase letters, digits, dash, underscore, or dot", field)
		}
	}
	return nil
}

func validateIdentifier(field, value string) error {
	if value == "" || len(value) > maxIdentifierBytes || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("control: %s must be 1-%d valid UTF-8 bytes without NUL", field, maxIdentifierBytes)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("control: %s contains a control character", field)
		}
	}
	// Control data contains identities, not connection endpoints. In
	// particular, URL userinfo must never become an accidental credential
	// distribution channel.
	if parsed, err := url.Parse(value); err == nil && parsed.IsAbs() && parsed.User != nil {
		return fmt.Errorf("control: %s must not contain URL credentials", field)
	}
	return nil
}

func slicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

var secretFieldNames = map[string]struct{}{
	"apikey": {}, "authorization": {}, "credential": {}, "credentials": {},
	"password": {}, "secret": {}, "token": {}, "username": {},
}

func rejectDuplicateOrSecretFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder); err != nil {
		return fmt.Errorf("control: inspect snapshot fields: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("control: snapshot contains trailing JSON value")
		}
		return fmt.Errorf("control: inspect trailing data: %w", err)
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate field %q", key)
			}
			seen[key] = struct{}{}
			normalized := strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(key))
			if _, secret := secretFieldNames[normalized]; secret {
				return fmt.Errorf("secret-bearing field %q is forbidden", key)
			}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return errors.New("malformed object")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return errors.New("malformed array")
		}
	default:
		return errors.New("unexpected closing delimiter")
	}
	return nil
}

func snapshotsEqual(left, right MembershipSnapshot) bool {
	if left.Version != right.Version || left.Namespace != right.Namespace || left.LibraryVersion != right.LibraryVersion || left.ScalingGroup != right.ScalingGroup ||
		left.Revision != right.Revision || left.Epoch != right.Epoch ||
		left.Phase != right.Phase || left.HashContract != right.HashContract || left.Algorithm != right.Algorithm ||
		!left.CurrentMembership.Equal(right.CurrentMembership) ||
		!left.ProposedMembership.Equal(right.ProposedMembership) || left.Queue != right.Queue ||
		left.Destination != right.Destination || !resourcesEqual(left.CurrentResources, right.CurrentResources) ||
		!resourcesEqual(left.ProposedResources, right.ProposedResources) {
		return false
	}
	if left.Transition == nil || right.Transition == nil {
		return left.Transition == nil && right.Transition == nil
	}
	return *left.Transition == *right.Transition
}

func resourcesEqual(left, right []EpochResourceIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
