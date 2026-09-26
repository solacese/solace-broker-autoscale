package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

// Phase is the controller's durable transition phase. Commit is the point of no
// return: a transition in Commit or Activate must only move forward.
type Phase string

const (
	PhasePrepare  Phase = "PREPARE"
	PhasePause    Phase = "PAUSE"
	PhaseDrain    Phase = "DRAIN"
	PhaseCommit   Phase = "COMMIT"
	PhaseActivate Phase = "ACTIVATE"
)

var (
	ErrTransitionInProgress = errors.New("membership transition already in progress")
	ErrTransitionNotFound   = errors.New("membership transition not found")
	ErrStaleMessage         = errors.New("stale or incorrectly scoped message")
	ErrUnknownParticipant   = errors.New("participant is not required by this transition")
	ErrCommitReached        = errors.New("transition has reached the commit point")
	ErrRollbackUnproven     = errors.New("safe rollback has not been proven")
	ErrPhaseDeadline        = errors.New("transition phase deadline exceeded")
	ErrReadinessStale       = errors.New("required readiness is missing or stale")
)

// Broker identifies one data-plane broker used by a scaling group.
type Broker struct {
	ID          string `json:"id"`
	Destination string `json:"destination"`
}

// TransitionSpec is the immutable input to a membership transition.
type TransitionSpec struct {
	ID                string                          `json:"id"`
	Namespace         string                          `json:"namespace,omitempty"`
	LibraryVersion    string                          `json:"library_version,omitempty"`
	Group             string                          `json:"group"`
	Revision          uint64                          `json:"revision"`
	FromEpoch         uint64                          `json:"from_epoch"`
	ToEpoch           uint64                          `json:"to_epoch"`
	Current           []Broker                        `json:"current"`
	Proposed          []Broker                        `json:"proposed"`
	Queue             control.QueueInfo               `json:"queue"`
	Destination       control.DestinationInfo         `json:"destination"`
	CurrentResources  []control.EpochResourceIdentity `json:"current_resources,omitempty"`
	ProposedResources []control.EpochResourceIdentity `json:"proposed_resources,omitempty"`
	HashContract      string                          `json:"hash_contract"`
	Algorithm         string                          `json:"algorithm"`
	// DrainGrace is durable and group-specific so independent transitions never
	// inherit another group's shorter safety window.
	DrainGrace time.Duration `json:"drain_grace,omitempty"`
	// RequiredParticipants and ParticipantRequirements are legacy, untyped
	// fallbacks. New callers should use RoleRequirements so acknowledgements can
	// be bound to the role for which a command was issued.
	RequiredParticipants    []string                           `json:"required_participants,omitempty"`
	ParticipantRequirements map[Phase][]string                 `json:"participant_requirements,omitempty"`
	RoleRequirements        map[Phase][]ParticipantRequirement `json:"role_requirements,omitempty"`
}

// ParticipantRequirement binds one participant identity to its command role.
// Participant identities remain unique within a phase.
type ParticipantRequirement struct {
	Participant string                  `json:"participant"`
	Role        control.ParticipantRole `json:"role"`
}

// TransitionSpecFromSnapshot adapts the shared control-plane contract to the
// controller's durable transition input while preserving authoritative broker
// order byte-for-byte.
func TransitionSpecFromSnapshot(snapshot control.MembershipSnapshot, requiredParticipants []string) (TransitionSpec, error) {
	if err := snapshot.Validate(); err != nil {
		return TransitionSpec{}, err
	}
	if snapshot.Phase != control.PhasePrepare || snapshot.Transition == nil {
		return TransitionSpec{}, errors.New("controller: transition must start from a PREPARE snapshot")
	}
	spec := TransitionSpec{
		ID:                   snapshot.Transition.ID,
		Namespace:            snapshot.Namespace,
		LibraryVersion:       snapshot.LibraryVersion,
		Group:                snapshot.ScalingGroup,
		Revision:             snapshot.Revision,
		FromEpoch:            snapshot.Transition.FromEpoch,
		ToEpoch:              snapshot.Transition.ToEpoch,
		Queue:                snapshot.Queue,
		Destination:          snapshot.Destination,
		CurrentResources:     append([]control.EpochResourceIdentity(nil), snapshot.CurrentResources...),
		ProposedResources:    append([]control.EpochResourceIdentity(nil), snapshot.ProposedResources...),
		HashContract:         snapshot.HashContract,
		Algorithm:            snapshot.Algorithm,
		RequiredParticipants: slices.Clone(requiredParticipants),
		ParticipantRequirements: map[Phase][]string{
			PhasePrepare:  slices.Clone(requiredParticipants),
			PhasePause:    slices.Clone(requiredParticipants),
			PhaseDrain:    slices.Clone(requiredParticipants),
			PhaseActivate: slices.Clone(requiredParticipants),
		},
	}
	for _, broker := range snapshot.CurrentMembership {
		spec.Current = append(spec.Current, Broker{ID: broker, Destination: snapshot.Destination.Name})
	}
	for _, broker := range snapshot.ProposedMembership {
		spec.Proposed = append(spec.Proposed, Broker{ID: broker, Destination: snapshot.Destination.Name})
	}
	return spec, spec.Validate()
}

func (s TransitionSpec) Validate() error {
	if s.ID == "" || s.Group == "" {
		return errors.New("transition id and group are required")
	}
	if s.ToEpoch != s.FromEpoch+1 || s.FromEpoch == ^uint64(0) {
		return fmt.Errorf("to epoch %d must immediately follow from epoch %d", s.ToEpoch, s.FromEpoch)
	}
	if s.Revision == 0 || s.Revision > ^uint64(0)-5 {
		return errors.New("transition revision must allow five forward publications")
	}
	if s.HashContract == "" || s.Algorithm != control.AlgorithmSHA256BigEndianModulo {
		return errors.New("supported hash contract and routing algorithm are required")
	}
	if s.Queue.Name == "" || !s.Queue.Durable {
		return errors.New("a durable queue is required")
	}
	if s.Destination.Name == "" || s.Destination.Kind != control.DestinationTopic {
		return errors.New("v1 requires a topic destination")
	}
	if len(s.Current) == 0 || len(s.Proposed) == 0 {
		return errors.New("current and proposed broker memberships are required")
	}
	requirements, err := normalizedRoleRequirements(s)
	if err != nil {
		return err
	}
	if len(requirements[PhasePrepare]) == 0 || len(requirements[PhasePause]) == 0 || len(requirements[PhaseDrain]) == 0 || len(requirements[PhaseActivate]) == 0 {
		return errors.New("prepare, pause, drain and activate participant requirements are required")
	}
	if err := validateBrokers(s.Current, s.Proposed); err != nil {
		return err
	}
	return controlSnapshot(s, PhasePrepare, false).Validate()
}

func normalizedRequirements(s TransitionSpec) (map[Phase][]string, error) {
	typed, err := normalizedRoleRequirements(s)
	if err != nil {
		return nil, err
	}
	result := make(map[Phase][]string, len(typed))
	for phase, requirements := range typed {
		participants := make([]string, len(requirements))
		for index, requirement := range requirements {
			participants[index] = requirement.Participant
		}
		result[phase] = participants
	}
	return result, nil
}

func normalizedRoleRequirements(s TransitionSpec) (map[Phase][]ParticipantRequirement, error) {
	result := make(map[Phase][]ParticipantRequirement)
	if len(s.RoleRequirements) != 0 {
		for phase, requirements := range s.RoleRequirements {
			if !validRequirementPhase(phase) {
				return nil, fmt.Errorf("unsupported participant requirement phase %q", phase)
			}
			result[phase] = slices.Clone(requirements)
		}
	} else {
		legacy := s.ParticipantRequirements
		if len(legacy) == 0 {
			legacy = make(map[Phase][]string, 4)
			for _, phase := range []Phase{PhasePrepare, PhasePause, PhaseDrain, PhaseActivate} {
				legacy[phase] = slices.Clone(s.RequiredParticipants)
			}
		}
		for phase, participants := range legacy {
			if !validRequirementPhase(phase) {
				return nil, fmt.Errorf("unsupported participant requirement phase %q", phase)
			}
			for _, participant := range participants {
				result[phase] = append(result[phase], ParticipantRequirement{Participant: participant})
			}
		}
	}
	for phase, requirements := range result {
		seen := make(map[string]struct{}, len(requirements))
		for _, requirement := range requirements {
			if requirement.Participant == "" {
				return nil, errors.New("participant identities cannot be empty")
			}
			if _, ok := seen[requirement.Participant]; ok {
				return nil, fmt.Errorf("duplicate participant identity %q for phase %s", requirement.Participant, phase)
			}
			seen[requirement.Participant] = struct{}{}
			if requirement.Role != "" {
				if err := requirement.Role.Validate(); err != nil {
					return nil, err
				}
				if !roleAllowedForPhase(requirement.Role, phase) {
					return nil, fmt.Errorf("participant role %q cannot acknowledge %s", requirement.Role, phase)
				}
			}
		}
	}
	return result, nil
}

func validRequirementPhase(phase Phase) bool {
	switch phase {
	case PhasePrepare, PhasePause, PhaseDrain, PhaseCommit, PhaseActivate:
		return true
	default:
		return false
	}
}

func roleAllowedForPhase(role control.ParticipantRole, phase Phase) bool {
	switch role {
	case control.RolePublisher:
		return phase == PhasePause || phase == PhaseActivate
	case control.RoleSubscriber:
		return phase == PhasePrepare || phase == PhaseDrain || phase == PhaseActivate
	case control.RoleBroker:
		return validRequirementPhase(phase)
	default:
		return false
	}
}

func validateBrokers(sets ...[]Broker) error {
	for _, brokers := range sets {
		seen := make(map[string]struct{}, len(brokers))
		for _, broker := range brokers {
			if broker.ID == "" || broker.Destination == "" {
				return errors.New("broker id and destination are required")
			}
			if _, ok := seen[broker.ID]; ok {
				return fmt.Errorf("duplicate broker identity %q", broker.ID)
			}
			seen[broker.ID] = struct{}{}
		}
	}
	return nil
}

// Acknowledgement is scoped to one transition, epoch, phase and participant.
// Replaying an identical acknowledgement is safe.
type Acknowledgement struct {
	CommandID                string                  `json:"command_id,omitempty"`
	Namespace                string                  `json:"namespace,omitempty"`
	Group                    string                  `json:"group"`
	TransitionID             string                  `json:"transition_id"`
	Epoch                    uint64                  `json:"epoch"`
	Phase                    Phase                   `json:"phase"`
	Participant              string                  `json:"participant"`
	AuthenticatedParticipant string                  `json:"authenticated_participant,omitempty"`
	Role                     control.ParticipantRole `json:"role,omitempty"`
	ObservedAt               time.Time               `json:"observed_at"`
}

// Count distinguishes an observed zero from an unknown value.
type Count struct {
	Known bool   `json:"known"`
	Value uint64 `json:"value"`
}

// Telemetry reports every source-epoch drain dimension. Samples must be scoped
// to the active transition and monotonically newer. Each Count distinguishes a
// measured zero from unavailable or stale data.
type Telemetry struct {
	MessageID    string                  `json:"message_id,omitempty"`
	Namespace    string                  `json:"namespace,omitempty"`
	Group        string                  `json:"group"`
	TransitionID string                  `json:"transition_id"`
	Epoch        uint64                  `json:"epoch"`
	Participant  string                  `json:"participant,omitempty"`
	Role         control.ParticipantRole `json:"role,omitempty"`
	SourceBroker string                  `json:"source_broker,omitempty"`
	SourceQueue  string                  `json:"source_queue,omitempty"`
	ObservedAt   time.Time               `json:"observed_at"`
	Queued       Count                   `json:"queued"`
	Stored       Count                   `json:"stored"`
	Unacked      Count                   `json:"unacked"`
}

func (t Telemetry) isKnownZero() bool {
	return t.Queued.Known && t.Queued.Value == 0 &&
		t.Stored.Known && t.Stored.Value == 0 &&
		t.Unacked.Known && t.Unacked.Value == 0
}

// FenceRequest contains a stable OperationID, allowing a broker implementation
// to make retries idempotent across controller restarts.
type FenceRequest struct {
	OperationID  string
	Group        string
	TransitionID string
	Epoch        uint64
	Brokers      []Broker
}

// FenceStatus is fresh broker-side proof that source-epoch ingress remains
// fenced immediately before the durable commit point.
type FenceStatus struct {
	Fenced     bool
	ObservedAt time.Time
}

// IngressStatus is fresh broker-side proof of the requested epoch ingress
// state. ObservedAt is broker-adapter observation time, not controller state
// persistence time.
type IngressStatus struct {
	Enabled    bool
	ObservedAt time.Time
}

// FenceVerifier provides fresh broker-side proof of the source fence. It is
// retained as a separate interface so simple/test fences can conservatively
// reapply the idempotent fence when verification is unavailable.
type FenceVerifier interface {
	VerifyFence(context.Context, FenceRequest) (FenceStatus, error)
}

// BrokerFence controls and verifies source and target epoch ingress.
// Implementations must treat OperationID idempotently. Verification is part of
// the required interface because the controller never commits from a cached or
// assumed fence state.
type BrokerFence interface {
	Fence(context.Context, FenceRequest) error
	Unfence(context.Context, FenceRequest) error
	VerifyFence(context.Context, FenceRequest) (FenceStatus, error)
	EnableIngress(context.Context, FenceRequest) error
	VerifyIngress(context.Context, FenceRequest) (IngressStatus, error)
}

// ControlUpdate is a durable desired-state publication. OperationID remains
// stable when a publish is retried after a crash.
type ControlUpdate struct {
	OperationID      string
	Group            string
	TransitionID     string
	FromEpoch        uint64
	ToEpoch          uint64
	Phase            Phase
	Current          []Broker
	Proposed         []Broker
	Rollback         bool
	Snapshot         control.MembershipSnapshot
	IssuedCommandIDs map[string]string
}

// ControlPublisher publishes membership intent. Implementations must treat
// OperationID idempotently.
type ControlPublisher interface {
	Publish(context.Context, ControlUpdate) error
}

// RollbackProof records the facts required for a safe pre-commit rollback.
// The controller deliberately refuses rollback if either fact is unknown.
type RollbackProof struct {
	TransitionID           string
	SourceMembershipIntact bool
	ProposedNotActivated   bool
	FenceReversible        bool
}

// Options controls telemetry safety windows.
type Options struct {
	TelemetryFreshness time.Duration
	ReadinessFreshness time.Duration
	FenceFreshness     time.Duration
	// ZeroGrace is the backward-compatible default. DrainGraceByGroup overrides
	// it and is copied into each new TransitionSpec before persistence.
	ZeroGrace                 time.Duration
	DrainGraceByGroup         map[string]time.Duration
	PhaseTimeout              time.Duration
	PhaseTimeoutByGroup       map[string]time.Duration
	TelemetryFreshnessByGroup map[string]time.Duration
	HistoryLimit              int
	Now                       func() time.Time
}

func (o Options) telemetryFreshness(group string) time.Duration {
	if configured := o.TelemetryFreshnessByGroup[group]; configured > 0 {
		return configured
	}
	return o.TelemetryFreshness
}

func (o Options) phaseTimeout(group string) time.Duration {
	if configured := o.PhaseTimeoutByGroup[group]; configured > 0 {
		return configured
	}
	return o.PhaseTimeout
}

func (o Options) withDefaults() Options {
	if o.TelemetryFreshness <= 0 {
		o.TelemetryFreshness = 15 * time.Second
	}
	if o.ReadinessFreshness <= 0 {
		o.ReadinessFreshness = 30 * time.Second
	}
	if o.FenceFreshness <= 0 {
		o.FenceFreshness = 15 * time.Second
	}
	if o.ZeroGrace <= 0 {
		o.ZeroGrace = 5 * time.Second
	}
	if o.PhaseTimeout <= 0 {
		o.PhaseTimeout = 5 * time.Minute
	}
	if o.HistoryLimit <= 0 {
		o.HistoryLimit = 64
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

func containsParticipant(participants []string, participant string) bool {
	return slices.Contains(participants, participant)
}

func requirementFor(spec TransitionSpec, phase Phase, participant string) (ParticipantRequirement, bool) {
	for _, requirement := range spec.RoleRequirements[phase] {
		if requirement.Participant == participant {
			return requirement, true
		}
	}
	return ParticipantRequirement{}, false
}
