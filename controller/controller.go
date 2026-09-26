package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

// PersistentState is the JSON representation of all independent scaling-group
// transitions.
type PersistentState struct {
	Version         int                           `json:"version"`
	Groups          map[string]*GroupState        `json:"groups"`
	History         map[string][]TransitionRecord `json:"history,omitempty"`
	CleanupEvidence map[string][]CleanupEvidence  `json:"cleanup_evidence,omitempty"`
}

// PhaseEvent is retained evidence of when a durable phase was entered and left.
type PhaseEvent struct {
	Phase     Phase      `json:"phase"`
	EnteredAt time.Time  `json:"entered_at"`
	ExitedAt  *time.Time `json:"exited_at,omitempty"`
}

// TransitionRecord is an immutable completion summary retained after a later
// transition replaces the group's active state.
type TransitionRecord struct {
	TransitionID string       `json:"transition_id"`
	FromEpoch    uint64       `json:"from_epoch"`
	ToEpoch      uint64       `json:"to_epoch"`
	StartedAt    time.Time    `json:"started_at"`
	CompletedAt  time.Time    `json:"completed_at"`
	Outcome      string       `json:"outcome"`
	Phases       []PhaseEvent `json:"phases"`
}

// CleanupEvidence retains the drain and fence facts needed to decide when old
// epoch resources may eventually be removed.
type CleanupEvidence struct {
	TransitionID    string                `json:"transition_id"`
	SourceEpoch     uint64                `json:"source_epoch"`
	TargetEpoch     uint64                `json:"target_epoch"`
	ZeroSince       time.Time             `json:"zero_since"`
	DrainObservedAt time.Time             `json:"drain_observed_at"`
	FenceVerifiedAt time.Time             `json:"fence_verified_at"`
	CommittedAt     time.Time             `json:"committed_at"`
	OldEpochFenced  bool                  `json:"old_epoch_fenced"`
	Sources         []DrainSourceEvidence `json:"sources,omitempty"`
}

// DrainSourceEvidence retains the exact continuously-zero interval proven for
// one source broker and queue at commit.
type DrainSourceEvidence struct {
	Broker     string    `json:"broker"`
	Queue      string    `json:"queue"`
	MessageID  string    `json:"message_id,omitempty"`
	ZeroSince  time.Time `json:"zero_since"`
	ObservedAt time.Time `json:"observed_at"`
}

// GroupState contains all facts needed to resume a transition after restart.
type GroupState struct {
	Spec                    TransitionSpec                 `json:"spec"`
	Phase                   Phase                          `json:"phase"`
	Acknowledgements        map[Phase]map[string]time.Time `json:"acknowledgements,omitempty"`
	IssuedCommandIDs        map[Phase]map[string]string    `json:"issued_command_ids,omitempty"`
	LatestTelemetryBySource map[string]Telemetry           `json:"latest_telemetry_by_source,omitempty"`
	ZeroSinceBySource       map[string]time.Time           `json:"zero_since_by_source,omitempty"`
	TelemetryMessages       map[string]Telemetry           `json:"telemetry_messages,omitempty"`
	PreparePublished        bool                           `json:"prepare_published,omitempty"`
	FenceAttempted          bool                           `json:"fence_attempted,omitempty"`
	Fenced                  bool                           `json:"fenced,omitempty"`
	PausePublished          bool                           `json:"pause_published,omitempty"`
	DrainPublished          bool                           `json:"drain_published,omitempty"`
	CommitPublished         bool                           `json:"commit_published,omitempty"`
	TargetIngressEnabled    bool                           `json:"target_ingress_enabled,omitempty"`
	TargetIngressVerified   bool                           `json:"target_ingress_verified,omitempty"`
	ActivatePublished       bool                           `json:"activate_published,omitempty"`
	LatestTelemetry         *Telemetry                     `json:"latest_telemetry,omitempty"`
	ZeroSince               *time.Time                     `json:"zero_since,omitempty"`
	RollingBack             bool                           `json:"rolling_back,omitempty"`
	RollbackPublished       bool                           `json:"rollback_published,omitempty"`
	Unfenced                bool                           `json:"unfenced,omitempty"`
	Completed               bool                           `json:"completed,omitempty"`
	StartedAt               time.Time                      `json:"started_at,omitempty"`
	PhaseEnteredAt          time.Time                      `json:"phase_entered_at,omitempty"`
	PhaseDeadline           time.Time                      `json:"phase_deadline,omitempty"`
	CompletedAt             *time.Time                     `json:"completed_at,omitempty"`
	PhaseHistory            []PhaseEvent                   `json:"phase_history,omitempty"`
	FenceVerifiedAt         *time.Time                     `json:"fence_verified_at,omitempty"`
	TargetIngressVerifiedAt *time.Time                     `json:"target_ingress_verified_at,omitempty"`
	CleanupRecorded         bool                           `json:"cleanup_recorded,omitempty"`
}

// Controller coordinates durable, restartable membership transitions.
type Controller struct {
	stateMu    sync.RWMutex
	commitMu   sync.Mutex
	groupsMu   sync.Mutex
	groupLocks map[string]*groupLock
	store      Store
	fence      BrokerFence
	publisher  ControlPublisher
	options    Options
	state      PersistentState
}

type groupLock struct {
	mu   sync.Mutex
	refs int
}

var errNoStateChange = errors.New("controller: no state change")

func Open(store Store, fence BrokerFence, publisher ControlPublisher, options Options) (*Controller, error) {
	if store == nil || fence == nil || publisher == nil {
		return nil, errors.New("store, broker fence and control publisher are required")
	}
	options = options.withDefaults()
	state, err := store.Load()
	if err != nil {
		return nil, err
	}
	if state.Groups == nil {
		state.Groups = make(map[string]*GroupState)
	}
	if state.History == nil {
		state.History = make(map[string][]TransitionRecord)
	}
	if state.CleanupEvidence == nil {
		state.CleanupEvidence = make(map[string][]CleanupEvidence)
	}
	now := options.Now()
	for group, transition := range state.Groups {
		if transition == nil {
			return nil, fmt.Errorf("group %q has nil transition state", group)
		}
		if err := transition.Spec.Validate(); err != nil {
			return nil, fmt.Errorf("group %q has invalid persisted transition: %w", group, err)
		}
		if transition.Acknowledgements == nil {
			transition.Acknowledgements = make(map[Phase]map[string]time.Time)
		}
		roleRequirements, roleErr := normalizedRoleRequirements(transition.Spec)
		if roleErr != nil {
			return nil, fmt.Errorf("group %q has invalid participant requirements: %w", group, roleErr)
		}
		transition.Spec.RoleRequirements = roleRequirements
		transition.Spec.ParticipantRequirements, err = normalizedRequirements(transition.Spec)
		if err != nil {
			return nil, fmt.Errorf("group %q has invalid participant requirements: %w", group, err)
		}
		initializeMessageState(transition)
		if transition.Spec.DrainGrace == 0 {
			transition.Spec.DrainGrace = options.ZeroGrace
			if configured := options.DrainGraceByGroup[group]; configured > 0 {
				transition.Spec.DrainGrace = configured
			}
		}
		if transition.Fenced {
			transition.FenceAttempted = true
		}
		if transition.StartedAt.IsZero() {
			transition.StartedAt = now
		}
		if transition.PhaseEnteredAt.IsZero() {
			transition.PhaseEnteredAt = transition.StartedAt
		}
		if transition.PhaseDeadline.IsZero() && !transition.Completed {
			transition.PhaseDeadline = transition.PhaseEnteredAt.Add(options.phaseTimeout(group))
		}
		if len(transition.PhaseHistory) == 0 {
			transition.PhaseHistory = []PhaseEvent{{Phase: transition.Phase, EnteredAt: transition.PhaseEnteredAt}}
		}
		if transition.Completed && transition.CompletedAt == nil {
			completedAt := now
			transition.CompletedAt = &completedAt
			transition.PhaseDeadline = time.Time{}
			transition.PhaseHistory[len(transition.PhaseHistory)-1].ExitedAt = &completedAt
		}
		if err := validateGroupState(group, transition); err != nil {
			return nil, err
		}
	}
	return &Controller{
		groupLocks: make(map[string]*groupLock),
		store:      store,
		fence:      fence,
		publisher:  publisher,
		options:    options,
		state:      state,
	}, nil
}

func (c *Controller) lockGroup(group string) func() {
	c.groupsMu.Lock()
	lock := c.groupLocks[group]
	if lock == nil {
		lock = &groupLock{}
		c.groupLocks[group] = lock
	}
	lock.refs++
	c.groupsMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		c.groupsMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(c.groupLocks, group)
		}
		c.groupsMu.Unlock()
	}
}

func (c *Controller) groupState(group string) (GroupState, bool) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	state, ok := c.state.Groups[group]
	if !ok {
		return GroupState{}, false
	}
	return cloneGroup(state), true
}

func (c *Controller) updateState(update func(*PersistentState) error) error {
	// Store replaces the complete state document, so saves remain ordered even
	// though group operations and their external side effects run independently.
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	c.stateMu.RLock()
	next := clonePersistent(c.state)
	c.stateMu.RUnlock()
	if err := update(&next); err != nil {
		if errors.Is(err, errNoStateChange) {
			return nil
		}
		return err
	}
	if err := c.store.Save(next); err != nil {
		return fmt.Errorf("persist controller state: %w", err)
	}
	c.stateMu.Lock()
	c.state = next
	c.stateMu.Unlock()
	return nil
}

// Begin starts a transition. An exact replay returns the existing state; a
// different transition for the same group is rejected until the prior one has
// completed. The next transition must continue its committed membership/epoch.
// BeginSnapshot starts a transition from the shared control-plane PREPARE
// snapshot contract.
func (c *Controller) BeginSnapshot(snapshot control.MembershipSnapshot, requiredParticipants []string) (GroupState, error) {
	spec, err := TransitionSpecFromSnapshot(snapshot, requiredParticipants)
	if err != nil {
		return GroupState{}, err
	}
	return c.Begin(spec)
}

func (c *Controller) Begin(spec TransitionSpec) (GroupState, error) {
	if spec.DrainGrace == 0 {
		spec.DrainGrace = c.options.ZeroGrace
		if configured := c.options.DrainGraceByGroup[spec.Group]; configured > 0 {
			spec.DrainGrace = configured
		}
	}
	if err := spec.Validate(); err != nil {
		return GroupState{}, err
	}
	spec.Current = slices.Clone(spec.Current)
	spec.Proposed = slices.Clone(spec.Proposed)
	spec.RequiredParticipants = slices.Clone(spec.RequiredParticipants)
	slices.Sort(spec.RequiredParticipants)
	roleRequirements, err := normalizedRoleRequirements(spec)
	if err != nil {
		return GroupState{}, err
	}
	for phase := range roleRequirements {
		slices.SortFunc(roleRequirements[phase], func(left, right ParticipantRequirement) int {
			return strings.Compare(left.Participant, right.Participant)
		})
	}
	spec.RoleRequirements = roleRequirements
	requirements, err := normalizedRequirements(spec)
	if err != nil {
		return GroupState{}, err
	}
	spec.ParticipantRequirements = requirements

	unlock := c.lockGroup(spec.Group)
	defer unlock()
	if existing, ok := c.groupState(spec.Group); ok {
		if equalSpec(existing.Spec, spec) {
			return existing, nil
		}
		if !canReplaceCompleted(&existing, spec) {
			return GroupState{}, ErrTransitionInProgress
		}
	}
	now := c.options.Now()
	created := GroupState{
		Spec:                    spec,
		Phase:                   PhasePrepare,
		Acknowledgements:        make(map[Phase]map[string]time.Time),
		IssuedCommandIDs:        make(map[Phase]map[string]string),
		LatestTelemetryBySource: make(map[string]Telemetry),
		ZeroSinceBySource:       make(map[string]time.Time),
		TelemetryMessages:       make(map[string]Telemetry),
		StartedAt:               now,
		PhaseEnteredAt:          now,
		PhaseDeadline:           now.Add(c.options.phaseTimeout(spec.Group)),
		PhaseHistory:            []PhaseEvent{{Phase: PhasePrepare, EnteredAt: now}},
	}
	if err := c.updateState(func(next *PersistentState) error {
		copy := cloneGroup(&created)
		next.Groups[spec.Group] = &copy
		return nil
	}); err != nil {
		return GroupState{}, err
	}
	return cloneGroup(&created), nil
}

// Acknowledge records a participant acknowledgement only when all scope fields
// match the transition's current phase. Exact duplicate messages are no-ops.
func (c *Controller) Acknowledge(ack Acknowledgement) error {
	if ack.ObservedAt.IsZero() {
		ack.ObservedAt = c.options.Now()
	}
	// Acknowledgements may be delivered synchronously by Publish. Validate and
	// persist them under the state commit lock rather than the reconcile lock so a
	// participant can answer as soon as the durable issuance intent exists.
	return c.updateState(func(next *PersistentState) error {
		state, ok := next.Groups[ack.Group]
		if !ok {
			return ErrTransitionNotFound
		}
		if state.Completed || state.RollingBack || !commandsIssued(state, ack.Phase) || ack.TransitionID != state.Spec.ID || ack.Epoch != state.Spec.ToEpoch || ack.Phase != state.Phase {
			return ErrStaleMessage
		}
		requirement, required := requirementFor(state.Spec, ack.Phase, ack.Participant)
		if !required {
			return ErrUnknownParticipant
		}
		if ack.Namespace != state.Spec.Namespace {
			return ErrStaleMessage
		}
		// Untyped requirements are retained solely for compatibility with legacy
		// in-process callers that omit all identity fields. A caller that supplies any
		// verified identity field must use a typed requirement so partial verification
		// cannot accidentally be accepted.
		if requirement.Role == "" {
			if ack.CommandID != "" || ack.AuthenticatedParticipant != "" || ack.Role != "" {
				return ErrStaleMessage
			}
		} else if ack.CommandID != state.IssuedCommandIDs[ack.Phase][ack.Participant] || ack.AuthenticatedParticipant != ack.Participant || ack.Role != requirement.Role {
			return ErrStaleMessage
		}
		if existing, ok := state.Acknowledgements[ack.Phase][ack.Participant]; ok && !ack.ObservedAt.After(existing) {
			return errNoStateChange
		}
		phaseAcks := state.Acknowledgements[ack.Phase]
		if phaseAcks == nil {
			phaseAcks = make(map[string]time.Time)
			state.Acknowledgements[ack.Phase] = phaseAcks
		}
		phaseAcks[ack.Participant] = ack.ObservedAt
		return nil
	})
}

// Observe records drain telemetry. Unknown is not zero, and samples are
// ordered independently for each required source broker/queue. An exact
// MessageID replay is idempotent even after restart; reusing that ID with a
// different payload or submitting an older sample is rejected.
func (c *Controller) Observe(sample Telemetry) error {
	unlock := c.lockGroup(sample.Group)
	defer unlock()
	stateCopy, ok := c.groupState(sample.Group)
	if !ok {
		return ErrTransitionNotFound
	}
	state := &stateCopy
	if sample.MessageID != "" {
		if existing, exists := state.TelemetryMessages[sample.MessageID]; exists {
			if telemetryEqual(existing, sample) {
				return nil
			}
			return ErrStaleMessage
		}
	}
	if state.Completed || state.RollingBack || state.Phase != PhaseDrain || !state.DrainPublished || sample.TransitionID != state.Spec.ID || sample.Epoch != state.Spec.FromEpoch || sample.ObservedAt.IsZero() || sample.ObservedAt.Before(state.PhaseEnteredAt) || sample.ObservedAt.After(c.options.Now()) {
		return ErrStaleMessage
	}
	sources, aggregate := telemetrySourcesForSample(state.Spec, sample)
	if len(sources) == 0 || sample.Namespace != state.Spec.Namespace && !aggregate {
		return ErrStaleMessage
	}
	if sample.Role != "" && sample.Role != control.RoleBroker && sample.Role != control.RoleObserver {
		return ErrStaleMessage
	}
	if sample.Participant == "" && sample.Role != "" || sample.Participant != "" && sample.Role == "" {
		return ErrStaleMessage
	}
	if hasTypedRequirements(state.Spec) && !aggregate && (sample.MessageID == "" || sample.Participant == "" || sample.Role == "") {
		return ErrStaleMessage
	}
	for _, source := range sources {
		if previous, exists := state.LatestTelemetryBySource[source.key()]; exists && !sample.ObservedAt.After(previous.ObservedAt) {
			return ErrStaleMessage
		}
	}

	return c.updateGroup(sample.Group, func(group *GroupState) {
		for _, source := range sources {
			key := source.key()
			previous, exists := group.LatestTelemetryBySource[key]
			if !sample.isKnownZero() {
				delete(group.ZeroSinceBySource, key)
			} else if !exists || !previous.isKnownZero() || sample.ObservedAt.Sub(previous.ObservedAt) > c.options.telemetryFreshness(sample.Group) {
				group.ZeroSinceBySource[key] = sample.ObservedAt
			}
			canonical := sample
			canonical.SourceBroker = source.Broker
			canonical.SourceQueue = source.Queue
			group.LatestTelemetryBySource[key] = canonical
		}
		if sample.MessageID != "" {
			group.TelemetryMessages[sample.MessageID] = sample
		}
		// Preserve the legacy aggregate fields for persisted-state compatibility and
		// diagnostics. Drain completion never relies on them when sources are known.
		copySample := sample
		group.LatestTelemetry = &copySample
		group.ZeroSince = commonZeroSince(group.ZeroSinceBySource)
	})
}

// RequestRollback starts a durable rollback. It is accepted only before commit
// and only with an explicit proof that restoring the source is safe.
func (c *Controller) RequestRollback(group string, proof RollbackProof) error {
	unlock := c.lockGroup(group)
	defer unlock()
	stateCopy, ok := c.groupState(group)
	if !ok {
		return ErrTransitionNotFound
	}
	state := &stateCopy
	if state.Phase == PhaseCommit || state.Phase == PhaseActivate || state.CommitPublished || state.ActivatePublished {
		return ErrCommitReached
	}
	if proof.TransitionID != state.Spec.ID || !proof.SourceMembershipIntact || !proof.ProposedNotActivated || ((state.FenceAttempted || state.Fenced) && !proof.FenceReversible) {
		return ErrRollbackUnproven
	}
	if state.RollingBack {
		return nil
	}
	return c.updateGroup(group, func(s *GroupState) { s.RollingBack = true })
}

// Reconcile performs one phase action sequence or phase advance for a group.
// Calling it repeatedly is safe; command intent is persisted before publication,
// and after a restart unfinished actions replay with the same operation ID.
func (c *Controller) Reconcile(ctx context.Context, group string) (bool, error) {
	unlock := c.lockGroup(group)
	defer unlock()
	stateCopy, ok := c.groupState(group)
	if !ok {
		return false, ErrTransitionNotFound
	}
	state := &stateCopy
	if state.Completed {
		return false, nil
	}
	if state.RollingBack {
		return c.reconcileRollback(ctx, group, state)
	}
	if !state.PhaseDeadline.IsZero() && c.options.Now().After(state.PhaseDeadline) && state.Phase != PhaseCommit && state.Phase != PhaseActivate {
		return false, fmt.Errorf("%w: %s deadline was %s", ErrPhaseDeadline, state.Phase, state.PhaseDeadline.Format(time.RFC3339Nano))
	}

	switch state.Phase {
	case PhasePrepare:
		if !state.PreparePublished {
			state, intentChanged, err := c.ensureCommandIntent(group, state, PhasePrepare)
			if err != nil {
				return false, err
			}
			if err := c.publish(ctx, state, PhasePrepare, false); err != nil {
				return intentChanged, err
			}
			return true, c.updateGroup(group, func(s *GroupState) { s.PreparePublished = true })
		}
		if allAcknowledged(state, PhasePrepare) {
			return true, c.updateGroup(group, func(s *GroupState) { enterPhase(s, PhasePause, c.options.Now(), c.options.phaseTimeout(group)) })
		}
	case PhasePause:
		// Publishers first close their durable dispatch gates and resolve
		// in-flight sends. Fencing before those acknowledgements could reject a
		// send that was valid when it began and make its outcome ambiguous.
		if !state.PausePublished {
			state, intentChanged, err := c.ensureCommandIntent(group, state, PhasePause)
			if err != nil {
				return false, err
			}
			if err := c.publish(ctx, state, PhasePause, false); err != nil {
				return intentChanged, err
			}
			return true, c.updateGroup(group, func(s *GroupState) { s.PausePublished = true })
		}
		if !allAcknowledged(state, PhasePause) {
			return false, nil
		}
		// Persist intent before touching any broker. A failed multi-broker fence may
		// have fenced an arbitrary subset, so rollback must unfence every source
		// broker once any attempt has begun.
		if !state.FenceAttempted {
			if err := c.updateGroup(group, func(s *GroupState) { s.FenceAttempted = true }); err != nil {
				return false, err
			}
			updated, ok := c.groupState(group)
			if !ok {
				return false, ErrTransitionNotFound
			}
			state = &updated
		}
		if !state.Fenced {
			if err := c.fence.Fence(ctx, fenceRequest(state, "fence")); err != nil {
				return false, fmt.Errorf("fence source brokers: %w", err)
			}
			return true, c.updateGroup(group, func(s *GroupState) { s.Fenced = true })
		}
		return true, c.updateGroup(group, func(s *GroupState) { enterPhase(s, PhaseDrain, c.options.Now(), c.options.phaseTimeout(group)) })
	case PhaseDrain:
		if !state.DrainPublished {
			state, intentChanged, err := c.ensureCommandIntent(group, state, PhaseDrain)
			if err != nil {
				return false, err
			}
			if err := c.publish(ctx, state, PhaseDrain, false); err != nil {
				return intentChanged, err
			}
			return true, c.updateGroup(group, func(s *GroupState) { s.DrainPublished = true })
		}
		if c.drainComplete(state) {
			if !allAcknowledged(state, PhaseDrain) || !c.readinessFresh(state) {
				return false, ErrReadinessStale
			}
			status, err := c.fence.VerifyFence(ctx, fenceRequest(state, "verify-fence"))
			if err != nil {
				return false, fmt.Errorf("verify source fence: %w", err)
			}
			verifiedAt := c.options.Now()
			if !status.Fenced || status.ObservedAt.IsZero() || status.ObservedAt.After(verifiedAt) || verifiedAt.Sub(status.ObservedAt) > c.options.FenceFreshness {
				return false, fmt.Errorf("%w: source fence is not freshly verified", ErrReadinessStale)
			}
			verifiedAt = status.ObservedAt
			// Persisting COMMIT before its side effect establishes the point of no
			// return. Recovery must publish commit or advance, never roll back.
			return true, c.updateGroup(group, func(s *GroupState) {
				s.FenceVerifiedAt = &verifiedAt
				enterPhase(s, PhaseCommit, c.options.Now(), c.options.phaseTimeout(group))
			})
		}
	case PhaseCommit:
		if !state.CommitPublished {
			// COMMIT always depends on a verification performed by this attempt. A
			// persisted timestamp is audit evidence only and is never trusted after a
			// restart or retry.
			status, err := c.fence.VerifyFence(ctx, fenceRequest(state, "commit-verify-fence"))
			if err != nil {
				return false, fmt.Errorf("verify source fence before commit publication: %w", err)
			}
			now := c.options.Now()
			if !status.Fenced || status.ObservedAt.IsZero() || status.ObservedAt.After(now) || now.Sub(status.ObservedAt) > c.options.FenceFreshness {
				return false, fmt.Errorf("%w: source fence is not freshly verified", ErrReadinessStale)
			}
			if err := c.publish(ctx, state, PhaseCommit, false); err != nil {
				return false, err
			}
			return true, c.updateGroup(group, func(s *GroupState) {
				s.FenceAttempted = true
				s.Fenced = true
				observedAt := status.ObservedAt
				s.FenceVerifiedAt = &observedAt
				s.CommitPublished = true
			})
		}
		return true, c.updateGroup(group, func(s *GroupState) { enterPhase(s, PhaseActivate, c.options.Now(), c.options.phaseTimeout(group)) })
	case PhaseActivate:
		if !state.TargetIngressEnabled {
			if err := c.fence.EnableIngress(ctx, targetIngressRequest(state, "enable-target-ingress")); err != nil {
				return false, fmt.Errorf("enable target ingress: %w", err)
			}
			return true, c.updateGroup(group, func(s *GroupState) { s.TargetIngressEnabled = true })
		}
		if !state.TargetIngressVerified {
			status, err := c.fence.VerifyIngress(ctx, targetIngressRequest(state, "verify-target-ingress"))
			if err != nil {
				return false, fmt.Errorf("verify target ingress: %w", err)
			}
			now := c.options.Now()
			if !status.Enabled || status.ObservedAt.IsZero() || status.ObservedAt.After(now) || now.Sub(status.ObservedAt) > c.options.FenceFreshness {
				return false, fmt.Errorf("%w: target ingress is not freshly verified", ErrReadinessStale)
			}
			return true, c.updateGroup(group, func(s *GroupState) {
				s.TargetIngressVerified = true
				observedAt := status.ObservedAt
				s.TargetIngressVerifiedAt = &observedAt
			})
		}
		if !state.ActivatePublished {
			// Like COMMIT, ACTIVE publication obtains fresh proof on every attempt;
			// the persisted verification above is the durable prerequisite, not a
			// substitute for checking the brokers after restart.
			status, err := c.fence.VerifyIngress(ctx, targetIngressRequest(state, "activate-verify-target-ingress"))
			if err != nil {
				return false, fmt.Errorf("verify target ingress before ACTIVE publication: %w", err)
			}
			now := c.options.Now()
			if !status.Enabled || status.ObservedAt.IsZero() || status.ObservedAt.After(now) || now.Sub(status.ObservedAt) > c.options.FenceFreshness {
				return false, fmt.Errorf("%w: target ingress is not freshly verified", ErrReadinessStale)
			}
			state, intentChanged, err := c.ensureCommandIntent(group, state, PhaseActivate)
			if err != nil {
				return false, err
			}
			if err := c.publish(ctx, state, PhaseActivate, false); err != nil {
				return intentChanged, err
			}
			return true, c.updateGroup(group, func(s *GroupState) {
				observedAt := status.ObservedAt
				s.TargetIngressVerifiedAt = &observedAt
				s.ActivatePublished = true
			})
		}
		if allAcknowledged(state, PhaseActivate) {
			return true, c.updateGroup(group, func(s *GroupState) { completeState(s, c.options.Now()) })
		}
	default:
		return false, fmt.Errorf("unknown phase %q", state.Phase)
	}
	return false, nil
}

// ReconcileAll reconciles each group independently in lexical order. An error
// in one group does not prevent attempts for the remaining groups.
func (c *Controller) ReconcileAll(ctx context.Context) map[string]error {
	c.stateMu.RLock()
	groups := slices.Sorted(maps.Keys(c.state.Groups))
	c.stateMu.RUnlock()
	errs := make(map[string]error)
	for _, group := range groups {
		if _, err := c.Reconcile(ctx, group); err != nil {
			errs[group] = err
		}
	}
	return errs
}

func (c *Controller) Snapshot() PersistentState {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return clonePersistent(c.state)
}

func (c *Controller) Group(group string) (GroupState, bool) {
	return c.groupState(group)
}

func (c *Controller) reconcileRollback(ctx context.Context, group string, state *GroupState) (bool, error) {
	// Restore the enforceable source ingress path before advertising ACTIVE. The
	// old ordering could publish ACTIVE while the source remained fenced.
	if (state.FenceAttempted || state.Fenced) && !state.Unfenced {
		if err := c.fence.Unfence(ctx, fenceRequest(state, "unfence")); err != nil {
			return false, fmt.Errorf("unfence source brokers: %w", err)
		}
		return true, c.updateGroup(group, func(s *GroupState) { s.Unfenced = true })
	}
	if !state.RollbackPublished {
		if err := c.publish(ctx, state, PhaseActivate, true); err != nil {
			return false, err
		}
		return true, c.updateGroup(group, func(s *GroupState) { s.RollbackPublished = true })
	}
	return true, c.updateGroup(group, func(s *GroupState) { completeState(s, c.options.Now()) })
}

func (c *Controller) drainComplete(state *GroupState) bool {
	sources := requiredTelemetrySources(state.Spec)
	if len(sources) == 0 {
		// Legacy specs predate source identities and retain their single aggregate
		// telemetry contract.
		telemetry := state.LatestTelemetry
		if telemetry == nil || state.ZeroSince == nil || !telemetry.isKnownZero() {
			return false
		}
		now := c.options.Now()
		if telemetry.ObservedAt.Before(state.PhaseEnteredAt) || telemetry.ObservedAt.After(now) || now.Sub(telemetry.ObservedAt) > c.options.telemetryFreshness(state.Spec.Group) {
			return false
		}
		return telemetry.ObservedAt.Sub(*state.ZeroSince) >= state.Spec.DrainGrace
	}

	now := c.options.Now()
	latestZeroSince := state.PhaseEnteredAt
	var earliestObservedAt time.Time
	for _, source := range sources {
		telemetry, ok := state.LatestTelemetryBySource[source.key()]
		zeroSince, zero := state.ZeroSinceBySource[source.key()]
		if !ok || !zero || !telemetry.isKnownZero() || telemetry.ObservedAt.Before(state.PhaseEnteredAt) || telemetry.ObservedAt.After(now) || now.Sub(telemetry.ObservedAt) > c.options.telemetryFreshness(state.Spec.Group) {
			return false
		}
		if zeroSince.After(latestZeroSince) {
			latestZeroSince = zeroSince
		}
		if earliestObservedAt.IsZero() || telemetry.ObservedAt.Before(earliestObservedAt) {
			earliestObservedAt = telemetry.ObservedAt
		}
	}
	return earliestObservedAt.Sub(latestZeroSince) >= state.Spec.DrainGrace
}

type telemetrySource struct {
	Broker string
	Queue  string
}

func (s telemetrySource) key() string {
	return s.Broker + "\x00" + s.Queue
}

func requiredTelemetrySources(spec TransitionSpec) []telemetrySource {
	if len(spec.CurrentResources) == 0 && !hasTypedRequirements(spec) {
		return nil
	}
	brokerIDs := make(map[string]struct{}, len(spec.Current))
	for _, broker := range spec.Current {
		brokerIDs[broker.ID] = struct{}{}
	}

	// Every resource explicitly names its broker/queue pair. If that mapping is
	// incomplete, conservatively require every named source queue on every current
	// broker.
	exact := len(spec.CurrentResources) > 0
	queuesByBroker := make(map[string][]string, len(spec.Current))
	for _, resource := range spec.CurrentResources {
		if _, ok := brokerIDs[resource.BrokerID]; !ok {
			exact = false
		}
		queuesByBroker[resource.BrokerID] = append(queuesByBroker[resource.BrokerID], resource.QueueName)
	}
	if exact {
		for _, broker := range spec.Current {
			if len(queuesByBroker[broker.ID]) == 0 {
				exact = false
				break
			}
		}
	}

	var sources []telemetrySource
	if exact {
		for _, broker := range spec.Current {
			for _, queue := range queuesByBroker[broker.ID] {
				sources = append(sources, telemetrySource{Broker: broker.ID, Queue: queue})
			}
		}
	} else {
		queues := []string{spec.Queue.Name}
		if len(spec.CurrentResources) > 0 {
			queues = queues[:0]
			seen := make(map[string]struct{}, len(spec.CurrentResources))
			for _, resource := range spec.CurrentResources {
				if _, duplicate := seen[resource.QueueName]; !duplicate {
					seen[resource.QueueName] = struct{}{}
					queues = append(queues, resource.QueueName)
				}
			}
		}
		for _, broker := range spec.Current {
			for _, queue := range queues {
				sources = append(sources, telemetrySource{Broker: broker.ID, Queue: queue})
			}
		}
	}
	slices.SortFunc(sources, func(left, right telemetrySource) int {
		if comparison := strings.Compare(left.Broker, right.Broker); comparison != 0 {
			return comparison
		}
		return strings.Compare(left.Queue, right.Queue)
	})
	return sources
}

func hasTypedRequirements(spec TransitionSpec) bool {
	for _, requirements := range spec.RoleRequirements {
		for _, requirement := range requirements {
			if requirement.Role != "" {
				return true
			}
		}
	}
	return false
}

func telemetrySourcesForSample(spec TransitionSpec, sample Telemetry) ([]telemetrySource, bool) {
	required := requiredTelemetrySources(spec)
	if len(required) == 0 {
		return []telemetrySource{{Broker: sample.SourceBroker, Queue: sample.SourceQueue}}, false
	}
	for _, source := range required {
		if source.Broker == sample.SourceBroker && source.Queue == sample.SourceQueue {
			return []telemetrySource{source}, false
		}
	}
	// Retain the qualification driver's explicit aggregate observation. It is a
	// trusted in-process compatibility path rather than an externally valid wire
	// sample, and expands atomically to every required source.
	if sample.MessageID == "" && sample.Namespace == "" && sample.Role == control.RoleObserver && sample.SourceBroker == "aggregate" {
		for _, source := range required {
			if source.Queue != sample.SourceQueue && resourceIngressTopic(spec, source.Queue) != sample.SourceQueue {
				return nil, false
			}
		}
		return required, true
	}
	return nil, false
}

func telemetrySourceKey(spec TransitionSpec, sample Telemetry) (string, bool) {
	sources, _ := telemetrySourcesForSample(spec, sample)
	if len(sources) != 1 {
		return "", false
	}
	return sources[0].key(), true
}

func resourceIngressTopic(spec TransitionSpec, queue string) string {
	var topic string
	for _, resource := range spec.CurrentResources {
		if resource.QueueName != queue {
			continue
		}
		if topic != "" && topic != resource.IngressTopic {
			return ""
		}
		topic = resource.IngressTopic
	}
	return topic
}

func commonZeroSince(zeroSince map[string]time.Time) *time.Time {
	var latest time.Time
	for _, observedAt := range zeroSince {
		if observedAt.After(latest) {
			latest = observedAt
		}
	}
	if latest.IsZero() {
		return nil
	}
	return &latest
}

func telemetryEqual(left, right Telemetry) bool {
	return left == right
}

func (c *Controller) publish(ctx context.Context, state *GroupState, phase Phase, rollback bool) error {
	current, proposed := state.Spec.Current, state.Spec.Proposed
	fromEpoch, toEpoch := state.Spec.FromEpoch, state.Spec.ToEpoch
	if rollback {
		proposed = state.Spec.Current
		toEpoch = state.Spec.FromEpoch
	}
	update := ControlUpdate{
		OperationID:      operationID(state.Spec, string(phase), rollback),
		Group:            state.Spec.Group,
		TransitionID:     state.Spec.ID,
		FromEpoch:        fromEpoch,
		ToEpoch:          toEpoch,
		Phase:            phase,
		Current:          slices.Clone(current),
		Proposed:         slices.Clone(proposed),
		Rollback:         rollback,
		Snapshot:         controlSnapshot(state.Spec, phase, rollback),
		IssuedCommandIDs: maps.Clone(state.IssuedCommandIDs[phase]),
	}
	if err := c.publisher.Publish(ctx, update); err != nil {
		return fmt.Errorf("publish %s control update: %w", phase, err)
	}
	return nil
}

func controlSnapshot(spec TransitionSpec, phase Phase, rollback bool) control.MembershipSnapshot {
	snapshot := control.MembershipSnapshot{
		Version:        control.SnapshotVersion,
		Namespace:      spec.Namespace,
		LibraryVersion: spec.LibraryVersion,
		ScalingGroup:   spec.Group,
		Queue:          spec.Queue,
		Destination:    spec.Destination,
		HashContract:   spec.HashContract,
		Algorithm:      spec.Algorithm,
	}
	if rollback {
		snapshot.Revision = revisionFor(spec.Revision, 5)
		snapshot.Epoch = spec.FromEpoch
		snapshot.Phase = control.PhaseActive
		snapshot.CurrentMembership = membership(spec.Current)
		snapshot.CurrentResources = slices.Clone(spec.CurrentResources)
		return snapshot
	}

	switch phase {
	case PhasePrepare:
		snapshot.Revision = revisionFor(spec.Revision, 0)
		snapshot.Epoch = spec.FromEpoch
		snapshot.Phase = control.PhasePrepare
		snapshot.CurrentMembership = membership(spec.Current)
		snapshot.ProposedMembership = membership(spec.Proposed)
		snapshot.CurrentResources = slices.Clone(spec.CurrentResources)
		snapshot.ProposedResources = slices.Clone(spec.ProposedResources)
		snapshot.Transition = &control.Transition{ID: spec.ID, FromEpoch: spec.FromEpoch, ToEpoch: spec.ToEpoch}
	case PhasePause, PhaseDrain:
		offset := uint64(1)
		snapshot.Phase = control.PhasePaused
		if phase == PhaseDrain {
			offset = 2
			snapshot.Phase = control.PhaseDrain
		}
		snapshot.Revision = revisionFor(spec.Revision, offset)
		snapshot.Epoch = spec.FromEpoch
		snapshot.CurrentMembership = membership(spec.Current)
		snapshot.ProposedMembership = membership(spec.Proposed)
		snapshot.CurrentResources = slices.Clone(spec.CurrentResources)
		snapshot.ProposedResources = slices.Clone(spec.ProposedResources)
		snapshot.Transition = &control.Transition{ID: spec.ID, FromEpoch: spec.FromEpoch, ToEpoch: spec.ToEpoch}
	case PhaseCommit:
		snapshot.Revision = revisionFor(spec.Revision, 3)
		snapshot.Epoch = spec.ToEpoch
		snapshot.Phase = control.PhaseCommitted
		snapshot.CurrentMembership = membership(spec.Proposed)
		snapshot.CurrentResources = slices.Clone(spec.ProposedResources)
		snapshot.Transition = &control.Transition{ID: spec.ID, FromEpoch: spec.FromEpoch, ToEpoch: spec.ToEpoch}
	case PhaseActivate:
		snapshot.Revision = revisionFor(spec.Revision, 4)
		snapshot.Epoch = spec.ToEpoch
		snapshot.Phase = control.PhaseActive
		snapshot.CurrentMembership = membership(spec.Proposed)
		snapshot.CurrentResources = slices.Clone(spec.ProposedResources)
	}
	return snapshot
}

func membership(brokers []Broker) control.Membership {
	result := make(control.Membership, len(brokers))
	for index, broker := range brokers {
		result[index] = broker.ID
	}
	return result
}

func revisionFor(base, offset uint64) uint64 {
	if base == 0 {
		base = 1
	}
	return base + offset
}

func (c *Controller) ensureCommandIntent(group string, state *GroupState, phase Phase) (*GroupState, bool, error) {
	if commandsIssued(state, phase) {
		return state, false, nil
	}
	if err := c.updateGroup(group, func(s *GroupState) { recordIssuedCommands(s, phase) }); err != nil {
		return nil, false, err
	}
	updated, ok := c.groupState(group)
	if !ok {
		return nil, false, ErrTransitionNotFound
	}
	return &updated, true, nil
}

func (c *Controller) updateGroup(group string, update func(*GroupState)) error {
	return c.updateState(func(next *PersistentState) error {
		state, ok := next.Groups[group]
		if !ok {
			return ErrTransitionNotFound
		}
		beforePhase := state.Phase
		wasCompleted := state.Completed
		update(state)
		if beforePhase != PhaseCommit && state.Phase == PhaseCommit {
			recordCleanupEvidence(*next, group, state)
		}
		if !wasCompleted && state.Completed {
			recordCompletion(*next, group, state, c.options.HistoryLimit)
		}
		return nil
	})
}

func allAcknowledged(state *GroupState, phase Phase) bool {
	acks := state.Acknowledgements[phase]
	for _, participant := range state.Spec.ParticipantRequirements[phase] {
		if _, ok := acks[participant]; !ok {
			return false
		}
	}
	return true
}

func (c *Controller) readinessFresh(state *GroupState) bool {
	now := c.options.Now()
	legacyPrepareFallback := !hasTypedRequirements(state.Spec)
	participants := state.Spec.ParticipantRequirements[PhaseDrain]
	for _, participant := range participants {
		observedAt, ok := state.Acknowledgements[PhaseDrain][participant]
		if ok && !observedAt.Before(state.PhaseEnteredAt) && !observedAt.After(now) && now.Sub(observedAt) <= c.options.ReadinessFreshness {
			continue
		}
		// Untyped in-process transitions predate phase-specific DRAIN commands and
		// may have reported readiness only during PREPARE. Never apply this fallback
		// to a transition with any role-bound requirement: typed acknowledgements
		// must prove readiness in response to the issued DRAIN command.
		if !legacyPrepareFallback {
			return false
		}
		observedAt, ok = state.Acknowledgements[PhasePrepare][participant]
		if !ok || observedAt.After(now) || now.Sub(observedAt) > c.options.ReadinessFreshness {
			return false
		}
	}
	return true
}

func fenceRequest(state *GroupState, action string) FenceRequest {
	return FenceRequest{
		OperationID:  operationID(state.Spec, action, false),
		Group:        state.Spec.Group,
		TransitionID: state.Spec.ID,
		Epoch:        state.Spec.FromEpoch,
		Brokers:      slices.Clone(state.Spec.Current),
	}
}

func targetIngressRequest(state *GroupState, action string) FenceRequest {
	return FenceRequest{
		OperationID:  operationID(state.Spec, action, false),
		Group:        state.Spec.Group,
		TransitionID: state.Spec.ID,
		Epoch:        state.Spec.ToEpoch,
		Brokers:      slices.Clone(state.Spec.Proposed),
	}
}

func operationID(spec TransitionSpec, action string, rollback bool) string {
	direction := "forward"
	if rollback {
		direction = "rollback"
	}
	return spec.Group + "/" + spec.ID + "/" + direction + "/" + action
}

func commandID(spec TransitionSpec, phase Phase, requirement ParticipantRequirement) string {
	return operationID(spec, "command/"+string(phase)+"/"+string(requirement.Role)+"/"+requirement.Participant, false)
}

func expectedCommandIDs(spec TransitionSpec) map[Phase]map[string]string {
	result := make(map[Phase]map[string]string, len(spec.RoleRequirements))
	for phase, requirements := range spec.RoleRequirements {
		result[phase] = make(map[string]string, len(requirements))
		for _, requirement := range requirements {
			result[phase][requirement.Participant] = commandID(spec, phase, requirement)
		}
	}
	return result
}

func initializeMessageState(state *GroupState) {
	if state.IssuedCommandIDs == nil {
		state.IssuedCommandIDs = make(map[Phase]map[string]string)
	}
	for phase, commands := range expectedCommandIDs(state.Spec) {
		if !phasePublished(state, phase) {
			continue
		}
		if state.IssuedCommandIDs[phase] == nil {
			state.IssuedCommandIDs[phase] = make(map[string]string, len(commands))
		}
		for participant, command := range commands {
			if state.IssuedCommandIDs[phase][participant] == "" {
				state.IssuedCommandIDs[phase][participant] = command
			}
		}
	}
	if state.LatestTelemetryBySource == nil {
		state.LatestTelemetryBySource = make(map[string]Telemetry)
	}
	if state.ZeroSinceBySource == nil {
		state.ZeroSinceBySource = make(map[string]time.Time)
	}
	if state.TelemetryMessages == nil {
		state.TelemetryMessages = make(map[string]Telemetry)
	}
}

func recordIssuedCommands(state *GroupState, phase Phase) {
	if state.IssuedCommandIDs == nil {
		state.IssuedCommandIDs = make(map[Phase]map[string]string)
	}
	commands := expectedCommandIDs(state.Spec)[phase]
	state.IssuedCommandIDs[phase] = maps.Clone(commands)
}

func commandsIssued(state *GroupState, phase Phase) bool {
	expected := expectedCommandIDs(state.Spec)[phase]
	return len(expected) > 0 && maps.Equal(state.IssuedCommandIDs[phase], expected)
}

func phasePublished(state *GroupState, phase Phase) bool {
	switch phase {
	case PhasePrepare:
		return state.PreparePublished
	case PhasePause:
		return state.PausePublished
	case PhaseDrain:
		return state.DrainPublished
	case PhaseCommit:
		return state.CommitPublished
	case PhaseActivate:
		return state.ActivatePublished
	default:
		return false
	}
}

func equalSpec(a, b TransitionSpec) bool {
	return a.ID == b.ID && a.Namespace == b.Namespace && a.LibraryVersion == b.LibraryVersion && a.Group == b.Group && a.Revision == b.Revision && a.FromEpoch == b.FromEpoch && a.ToEpoch == b.ToEpoch && a.Queue == b.Queue && a.Destination == b.Destination && a.HashContract == b.HashContract && a.Algorithm == b.Algorithm && a.DrainGrace == b.DrainGrace && slices.Equal(a.Current, b.Current) && slices.Equal(a.Proposed, b.Proposed) && resourcesEqual(a.CurrentResources, b.CurrentResources) && resourcesEqual(a.ProposedResources, b.ProposedResources) && requirementsEqual(a.ParticipantRequirements, b.ParticipantRequirements) && roleRequirementsEqual(a.RoleRequirements, b.RoleRequirements)
}

func canReplaceCompleted(existing *GroupState, next TransitionSpec) bool {
	if !existing.Completed || next.ID == existing.Spec.ID {
		return false
	}
	if existing.RollingBack {
		return next.FromEpoch == existing.Spec.FromEpoch && next.Revision > revisionFor(existing.Spec.Revision, 5) && slices.Equal(existing.Spec.Current, next.Current)
	}
	return next.FromEpoch == existing.Spec.ToEpoch && next.Revision > revisionFor(existing.Spec.Revision, 4) && slices.Equal(existing.Spec.Proposed, next.Current)
}

func clonePersistent(state PersistentState) PersistentState {
	clone := PersistentState{
		Version:         state.Version,
		Groups:          make(map[string]*GroupState, len(state.Groups)),
		History:         make(map[string][]TransitionRecord, len(state.History)),
		CleanupEvidence: make(map[string][]CleanupEvidence, len(state.CleanupEvidence)),
	}
	for group, transition := range state.Groups {
		copy := cloneGroup(transition)
		clone.Groups[group] = &copy
	}
	for group, records := range state.History {
		clone.History[group] = cloneRecords(records)
	}
	for group, evidence := range state.CleanupEvidence {
		clone.CleanupEvidence[group] = cloneCleanupEvidence(evidence)
	}
	return clone
}

func cloneGroup(state *GroupState) GroupState {
	clone := *state
	clone.Spec.Current = slices.Clone(state.Spec.Current)
	clone.Spec.Proposed = slices.Clone(state.Spec.Proposed)
	clone.Spec.CurrentResources = slices.Clone(state.Spec.CurrentResources)
	clone.Spec.ProposedResources = slices.Clone(state.Spec.ProposedResources)
	clone.Spec.RequiredParticipants = slices.Clone(state.Spec.RequiredParticipants)
	clone.Spec.ParticipantRequirements = cloneRequirements(state.Spec.ParticipantRequirements)
	clone.Spec.RoleRequirements = cloneRoleRequirements(state.Spec.RoleRequirements)
	clone.PhaseHistory = slices.Clone(state.PhaseHistory)
	clone.Acknowledgements = make(map[Phase]map[string]time.Time, len(state.Acknowledgements))
	for phase, acknowledgements := range state.Acknowledgements {
		clone.Acknowledgements[phase] = maps.Clone(acknowledgements)
	}
	clone.IssuedCommandIDs = make(map[Phase]map[string]string, len(state.IssuedCommandIDs))
	for phase, commands := range state.IssuedCommandIDs {
		clone.IssuedCommandIDs[phase] = maps.Clone(commands)
	}
	clone.LatestTelemetryBySource = maps.Clone(state.LatestTelemetryBySource)
	clone.ZeroSinceBySource = maps.Clone(state.ZeroSinceBySource)
	clone.TelemetryMessages = maps.Clone(state.TelemetryMessages)
	if state.LatestTelemetry != nil {
		telemetry := *state.LatestTelemetry
		clone.LatestTelemetry = &telemetry
	}
	if state.ZeroSince != nil {
		zeroSince := *state.ZeroSince
		clone.ZeroSince = &zeroSince
	}
	if state.CompletedAt != nil {
		completedAt := *state.CompletedAt
		clone.CompletedAt = &completedAt
	}
	if state.FenceVerifiedAt != nil {
		verifiedAt := *state.FenceVerifiedAt
		clone.FenceVerifiedAt = &verifiedAt
	}
	if state.TargetIngressVerifiedAt != nil {
		verifiedAt := *state.TargetIngressVerifiedAt
		clone.TargetIngressVerifiedAt = &verifiedAt
	}
	return clone
}

func cloneRequirements(requirements map[Phase][]string) map[Phase][]string {
	clone := make(map[Phase][]string, len(requirements))
	for phase, participants := range requirements {
		clone[phase] = slices.Clone(participants)
	}
	return clone
}

func cloneRoleRequirements(requirements map[Phase][]ParticipantRequirement) map[Phase][]ParticipantRequirement {
	clone := make(map[Phase][]ParticipantRequirement, len(requirements))
	for phase, participants := range requirements {
		clone[phase] = slices.Clone(participants)
	}
	return clone
}

func requirementsEqual(left, right map[Phase][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for phase, participants := range left {
		if !slices.Equal(participants, right[phase]) {
			return false
		}
	}
	return true
}

func roleRequirementsEqual(left, right map[Phase][]ParticipantRequirement) bool {
	if len(left) != len(right) {
		return false
	}
	for phase, requirements := range left {
		if !slices.Equal(requirements, right[phase]) {
			return false
		}
	}
	return true
}

func resourcesEqual(left, right []control.EpochResourceIdentity) bool {
	return slices.Equal(left, right)
}

func enterPhase(state *GroupState, phase Phase, now time.Time, timeout time.Duration) {
	if len(state.PhaseHistory) > 0 && state.PhaseHistory[len(state.PhaseHistory)-1].ExitedAt == nil {
		exitedAt := now
		state.PhaseHistory[len(state.PhaseHistory)-1].ExitedAt = &exitedAt
	}
	state.Phase = phase
	state.PhaseEnteredAt = now
	state.PhaseDeadline = now.Add(timeout)
	state.PhaseHistory = append(state.PhaseHistory, PhaseEvent{Phase: phase, EnteredAt: now})
}

func completeState(state *GroupState, now time.Time) {
	state.Completed = true
	state.CompletedAt = &now
	state.PhaseDeadline = time.Time{}
	if len(state.PhaseHistory) > 0 && state.PhaseHistory[len(state.PhaseHistory)-1].ExitedAt == nil {
		exitedAt := now
		state.PhaseHistory[len(state.PhaseHistory)-1].ExitedAt = &exitedAt
	}
}

func recordCompletion(state PersistentState, group string, completed *GroupState, limit int) {
	if completed.CompletedAt == nil {
		return
	}
	outcome := "committed"
	if completed.RollingBack {
		outcome = "rolled_back"
	}
	record := TransitionRecord{
		TransitionID: completed.Spec.ID,
		FromEpoch:    completed.Spec.FromEpoch,
		ToEpoch:      completed.Spec.ToEpoch,
		StartedAt:    completed.StartedAt,
		CompletedAt:  *completed.CompletedAt,
		Outcome:      outcome,
		Phases:       slices.Clone(completed.PhaseHistory),
	}
	state.History[group] = append(state.History[group], record)
	if len(state.History[group]) > limit {
		state.History[group] = slices.Clone(state.History[group][len(state.History[group])-limit:])
	}
}

func recordCleanupEvidence(state PersistentState, group string, transition *GroupState) {
	if transition.CleanupRecorded || transition.FenceVerifiedAt == nil {
		return
	}
	evidence := CleanupEvidence{
		TransitionID:    transition.Spec.ID,
		SourceEpoch:     transition.Spec.FromEpoch,
		TargetEpoch:     transition.Spec.ToEpoch,
		FenceVerifiedAt: *transition.FenceVerifiedAt,
		CommittedAt:     transition.PhaseEnteredAt,
		OldEpochFenced:  true,
	}
	sources := requiredTelemetrySources(transition.Spec)
	if len(sources) == 0 {
		if transition.ZeroSince == nil || transition.LatestTelemetry == nil {
			return
		}
		evidence.ZeroSince = *transition.ZeroSince
		evidence.DrainObservedAt = transition.LatestTelemetry.ObservedAt
	} else {
		for _, source := range sources {
			telemetry, ok := transition.LatestTelemetryBySource[source.key()]
			zeroSince, zero := transition.ZeroSinceBySource[source.key()]
			if !ok || !zero {
				return
			}
			evidence.Sources = append(evidence.Sources, DrainSourceEvidence{
				Broker: source.Broker, Queue: source.Queue, MessageID: telemetry.MessageID,
				ZeroSince: zeroSince, ObservedAt: telemetry.ObservedAt,
			})
			if zeroSince.After(evidence.ZeroSince) {
				evidence.ZeroSince = zeroSince
			}
			if evidence.DrainObservedAt.IsZero() || telemetry.ObservedAt.Before(evidence.DrainObservedAt) {
				evidence.DrainObservedAt = telemetry.ObservedAt
			}
		}
	}
	state.CleanupEvidence[group] = append(state.CleanupEvidence[group], evidence)
	transition.CleanupRecorded = true
}

func cloneRecords(records []TransitionRecord) []TransitionRecord {
	clone := slices.Clone(records)
	for index := range clone {
		clone[index].Phases = slices.Clone(records[index].Phases)
	}
	return clone
}

func cloneCleanupEvidence(evidence []CleanupEvidence) []CleanupEvidence {
	clone := slices.Clone(evidence)
	for index := range clone {
		clone[index].Sources = slices.Clone(evidence[index].Sources)
	}
	return clone
}

func validateGroupState(group string, state *GroupState) error {
	if state.Spec.Group != group {
		return fmt.Errorf("group %q persisted under mismatched key %q", state.Spec.Group, group)
	}
	rank := map[Phase]int{PhasePrepare: 1, PhasePause: 2, PhaseDrain: 3, PhaseCommit: 4, PhaseActivate: 5}
	if rank[state.Phase] == 0 {
		return fmt.Errorf("group %q has unknown phase %q", group, state.Phase)
	}
	if state.PausePublished && !state.PreparePublished || state.DrainPublished && (!state.PausePublished || !state.Fenced) || state.CommitPublished && state.Phase != PhaseCommit && state.Phase != PhaseActivate || state.ActivatePublished && state.Phase != PhaseActivate {
		return fmt.Errorf("group %q has inconsistent publication/fence state", group)
	}
	if state.ActivatePublished && (state.FenceVerifiedAt == nil || !state.TargetIngressEnabled || !state.TargetIngressVerified || state.TargetIngressVerifiedAt == nil) {
		return fmt.Errorf("group %q activated without retained source fence and target ingress verification", group)
	}
	for phase, acknowledgements := range state.Acknowledgements {
		for participant := range acknowledgements {
			if !containsParticipant(state.Spec.ParticipantRequirements[phase], participant) {
				return fmt.Errorf("group %q has acknowledgement by unrequired participant %q in %s", group, participant, phase)
			}
			if !commandsIssued(state, phase) || state.IssuedCommandIDs[phase][participant] == "" {
				return fmt.Errorf("group %q has acknowledgement without an issued command for participant %q in %s", group, participant, phase)
			}
		}
	}
	expectedCommands := expectedCommandIDs(state.Spec)
	for phase, commands := range state.IssuedCommandIDs {
		if len(commands) == 0 {
			continue
		}
		if !maps.Equal(commands, expectedCommands[phase]) {
			return fmt.Errorf("group %q has invalid issued commands for %s", group, phase)
		}
		if !phasePublished(state, phase) && phase != state.Phase {
			return fmt.Errorf("group %q has command intent outside its current phase %s", group, phase)
		}
	}
	for phase := range expectedCommands {
		if phasePublished(state, phase) && !commandsIssued(state, phase) {
			return fmt.Errorf("group %q has incomplete issued commands for %s", group, phase)
		}
	}
	if (state.Phase == PhaseDrain && state.DrainPublished || state.Phase == PhaseCommit && state.CommitPublished || state.Phase == PhaseActivate) && !state.Fenced && !state.RollingBack {
		return fmt.Errorf("group %q reached durable %s actions without a source fence", group, state.Phase)
	}
	if state.Completed && state.CompletedAt == nil {
		return fmt.Errorf("group %q is complete without a completion timestamp", group)
	}
	if state.Completed && !state.RollingBack && !state.ActivatePublished {
		return fmt.Errorf("group %q is complete without ACTIVE publication", group)
	}
	if state.Fenced && !state.FenceAttempted {
		return fmt.Errorf("group %q is fenced without a durable fence attempt", group)
	}
	if state.TargetIngressVerified && (!state.TargetIngressEnabled || state.TargetIngressVerifiedAt == nil) {
		return fmt.Errorf("group %q has inconsistent target ingress verification", group)
	}
	if state.RollbackPublished && (state.FenceAttempted || state.Fenced) && !state.Unfenced {
		return fmt.Errorf("group %q advertised rollback before source unfence", group)
	}
	for key, telemetry := range state.LatestTelemetryBySource {
		expected, ok := telemetrySourceKey(state.Spec, telemetry)
		if !ok || expected != key {
			return fmt.Errorf("group %q has telemetry for unknown source %q", group, key)
		}
		if telemetry.MessageID != "" {
			original, exists := state.TelemetryMessages[telemetry.MessageID]
			if !exists || original.MessageID != telemetry.MessageID || original.Namespace != telemetry.Namespace || original.Group != telemetry.Group || original.TransitionID != telemetry.TransitionID || original.Epoch != telemetry.Epoch || original.Participant != telemetry.Participant || original.Role != telemetry.Role || original.ObservedAt != telemetry.ObservedAt || original.Queued != telemetry.Queued || original.Stored != telemetry.Stored || original.Unacked != telemetry.Unacked {
				return fmt.Errorf("group %q has telemetry without matching message identity %q", group, telemetry.MessageID)
			}
		}
		if _, zero := state.ZeroSinceBySource[key]; zero && !telemetry.isKnownZero() {
			return fmt.Errorf("group %q has zero interval for non-zero source %q", group, key)
		}
	}
	for key := range state.ZeroSinceBySource {
		if _, ok := state.LatestTelemetryBySource[key]; !ok {
			return fmt.Errorf("group %q has zero interval without telemetry for source %q", group, key)
		}
	}
	return nil
}
