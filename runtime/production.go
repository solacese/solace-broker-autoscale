package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/policy"
)

var (
	ErrNoRetainedMembership      = errors.New("runtime: no retained membership snapshot")
	ErrCapacityPolicyUnavailable = errors.New("runtime: capacity policy is not configured with measured broker profiles")
)

// MembershipPublisher is the production Broker 0 publication boundary used by
// bootstrap and transition command fanout.
type MembershipPublisher interface {
	controller.ControlPublisher
	PublishSnapshot(context.Context, control.MembershipSnapshot) error
	PublishCommand(context.Context, control.CommandEnvelope) error
}

// MembershipCatalog is the controller's synchronized view of retained routing
// state. Every update is schema validated and preserves broker order.
type MembershipCatalog struct {
	mu        sync.RWMutex
	snapshots map[string]control.MembershipSnapshot
}

func NewMembershipCatalog() *MembershipCatalog {
	return &MembershipCatalog{snapshots: make(map[string]control.MembershipSnapshot)}
}

func (c *MembershipCatalog) Put(snapshot control.MembershipSnapshot) error {
	if c == nil {
		return errors.New("runtime: membership catalog is required")
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	c.snapshots[snapshot.ScalingGroup] = snapshot.Clone()
	c.mu.Unlock()
	return nil
}

func (c *MembershipCatalog) Get(group string) (control.MembershipSnapshot, bool) {
	if c == nil {
		return control.MembershipSnapshot{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	snapshot, ok := c.snapshots[group]
	return snapshot.Clone(), ok
}

// BootstrapMembership browses every group before the policy loop starts. It
// seeds a deterministic ACTIVE snapshot only after the browser positively
// reports ErrNoRetainedMembership; every other browse failure is fatal.
func BootstrapMembership(ctx context.Context, cfg config.Config, durable controller.PersistentState, browser broker0.Browser, publisher interface {
	PublishSnapshot(context.Context, control.MembershipSnapshot) error
}, catalog *MembershipCatalog, resources ...*ManagedEpochResources) error {
	if browser == nil || publisher == nil || catalog == nil {
		return errors.New("runtime: membership browser, publisher, and catalog are required")
	}
	for _, group := range cfg.Groups {
		messages, err := browser.Browse(ctx, group.ID)
		if errors.Is(err, ErrNoRetainedMembership) || errors.Is(err, broker0.ErrNoAuthoritativeState) {
			if state := durable.Groups[group.ID]; state != nil {
				return fmt.Errorf("runtime: group %q has durable transition history but no retained membership", group.ID)
			}
			snapshot, buildErr := GenesisSnapshot(cfg.Namespace, group)
			if buildErr != nil {
				return buildErr
			}
			if len(resources) != 0 {
				if resources[0] == nil {
					return errors.New("runtime: managed epoch resources are required")
				}
				if err := resources[0].EnsureBootstrap(ctx, snapshot, nil); err != nil {
					return fmt.Errorf("runtime: ensure initial epoch resources for %q: %w", group.ID, err)
				}
			}
			if err := publisher.PublishSnapshot(ctx, snapshot); err != nil {
				return fmt.Errorf("runtime: publish initial membership for %q: %w", group.ID, err)
			}
			if err := catalog.Put(snapshot); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("runtime: browse retained membership for %q: %w", group.ID, err)
		}
		snapshot, err := selectRetainedSnapshot(group.ID, messages)
		if err != nil {
			return err
		}
		if err := validateConfiguredSnapshot(group, snapshot); err != nil {
			return err
		}
		state := durable.Groups[group.ID]
		if snapshot.Phase != control.PhaseActive && state == nil {
			return fmt.Errorf("runtime: retained transition for %q has no durable controller state", group.ID)
		}
		if state != nil {
			if snapshot.Transition != nil && snapshot.Transition.ID != state.Spec.ID {
				return fmt.Errorf("runtime: retained transition for %q conflicts with durable controller state", group.ID)
			}
			if !state.Completed && snapshot.Phase == control.PhaseActive && !matchesUnpublishedPrepareSourceActive(snapshot, state) && !matchesPublishedTargetActive(snapshot, state) {
				return fmt.Errorf("runtime: retained ACTIVE membership for %q conflicts with an incomplete durable transition", group.ID)
			}
		}
		if len(resources) != 0 {
			if resources[0] == nil {
				return errors.New("runtime: managed epoch resources are required")
			}
			if err := resources[0].EnsureBootstrap(ctx, snapshot, state); err != nil {
				return fmt.Errorf("runtime: verify retained epoch resources for %q: %w", group.ID, err)
			}
		}
		if err := catalog.Put(snapshot); err != nil {
			return err
		}
	}
	return nil
}

func selectRetainedSnapshot(group string, messages []broker0.BrowsedMessage) (control.MembershipSnapshot, error) {
	if len(messages) == 0 {
		return control.MembershipSnapshot{}, ErrNoRetainedMembership
	}
	var selected control.MembershipSnapshot
	for index, message := range messages {
		if message.Kind != broker0.KindMembershipSnapshot {
			return control.MembershipSnapshot{}, fmt.Errorf("runtime: retained message %d for %q has kind %q", index, group, message.Kind)
		}
		snapshot, err := control.ParseMembershipSnapshot(message.Payload)
		if err != nil {
			return control.MembershipSnapshot{}, fmt.Errorf("runtime: parse retained membership for %q: %w", group, err)
		}
		if snapshot.ScalingGroup != group {
			return control.MembershipSnapshot{}, fmt.Errorf("runtime: retained membership for %q contains group %q", group, snapshot.ScalingGroup)
		}
		if selected.Revision == 0 || snapshot.Revision > selected.Revision {
			selected = snapshot
		} else if snapshot.Revision == selected.Revision && !membershipSnapshotsEqual(snapshot, selected) {
			return control.MembershipSnapshot{}, fmt.Errorf("runtime: conflicting retained revision %d for %q", snapshot.Revision, group)
		}
	}
	return selected, nil
}

func membershipSnapshotsEqual(left, right control.MembershipSnapshot) bool {
	return left.Version == right.Version && left.Namespace == right.Namespace && left.LibraryVersion == right.LibraryVersion && left.ScalingGroup == right.ScalingGroup && left.Revision == right.Revision && left.Epoch == right.Epoch && left.Phase == right.Phase && left.HashContract == right.HashContract && left.Algorithm == right.Algorithm && left.Queue == right.Queue && left.Destination == right.Destination && left.CurrentMembership.Equal(right.CurrentMembership) && left.ProposedMembership.Equal(right.ProposedMembership) && slices.Equal(left.CurrentResources, right.CurrentResources) && slices.Equal(left.ProposedResources, right.ProposedResources) && ((left.Transition == nil && right.Transition == nil) || (left.Transition != nil && right.Transition != nil && *left.Transition == *right.Transition))
}

func matchesUnpublishedPrepareSourceActive(snapshot control.MembershipSnapshot, state *controller.GroupState) bool {
	if state == nil || state.Phase != controller.PhasePrepare || state.RollingBack || state.Completed || state.PreparePublished || state.FenceAttempted || state.Fenced || state.CommitPublished || state.TargetIngressEnabled || state.ActivatePublished {
		return false
	}
	return membershipSnapshotsEqual(snapshot, sourceActiveSnapshot(state.Spec))
}

func sourceActiveSnapshot(spec controller.TransitionSpec) control.MembershipSnapshot {
	membership := make(control.Membership, len(spec.Current))
	for index, broker := range spec.Current {
		membership[index] = broker.ID
	}
	return control.MembershipSnapshot{
		Version: control.SnapshotVersion, Namespace: spec.Namespace, LibraryVersion: spec.LibraryVersion,
		ScalingGroup: spec.Group, Revision: spec.Revision - 1, Epoch: spec.FromEpoch, Phase: control.PhaseActive,
		HashContract: spec.HashContract, Algorithm: spec.Algorithm, CurrentMembership: membership,
		Queue: spec.Queue, Destination: spec.Destination, CurrentResources: slices.Clone(spec.CurrentResources),
	}
}

func matchesPublishedTargetActive(snapshot control.MembershipSnapshot, state *controller.GroupState) bool {
	if state == nil || state.Phase != controller.PhaseActivate || state.RollingBack || state.Completed ||
		!state.CommitPublished || !state.Fenced || state.FenceVerifiedAt == nil ||
		!state.TargetIngressEnabled || !state.TargetIngressVerified || state.TargetIngressVerifiedAt == nil {
		return false
	}
	issued := state.IssuedCommandIDs[controller.PhaseActivate]
	for _, requirement := range state.Spec.RoleRequirements[controller.PhaseActivate] {
		if issued[requirement.Participant] != state.Spec.Group+"/"+state.Spec.ID+"/forward/command/"+string(controller.PhaseActivate)+"/"+string(requirement.Role)+"/"+requirement.Participant {
			return false
		}
	}
	return membershipSnapshotsEqual(snapshot, targetActiveSnapshot(state.Spec))
}

func expectedPrecommitSnapshot(state *controller.GroupState) (control.MembershipSnapshot, bool) {
	if state == nil || state.RollingBack || state.Completed || state.CommitPublished || state.TargetIngressEnabled || state.ActivatePublished {
		return control.MembershipSnapshot{}, false
	}
	spec := state.Spec
	if state.Phase == controller.PhasePrepare && !state.PreparePublished {
		return sourceActiveSnapshot(spec), true
	}
	snapshot := control.MembershipSnapshot{
		Version: control.SnapshotVersion, Namespace: spec.Namespace, LibraryVersion: spec.LibraryVersion,
		ScalingGroup: spec.Group, Epoch: spec.FromEpoch, HashContract: spec.HashContract, Algorithm: spec.Algorithm,
		CurrentMembership: brokerMembership(spec.Current), ProposedMembership: brokerMembership(spec.Proposed),
		Transition: &control.Transition{ID: spec.ID, FromEpoch: spec.FromEpoch, ToEpoch: spec.ToEpoch},
		Queue:      spec.Queue, Destination: spec.Destination,
		CurrentResources: slices.Clone(spec.CurrentResources), ProposedResources: slices.Clone(spec.ProposedResources),
	}
	switch state.Phase {
	case controller.PhasePrepare:
		if !state.PreparePublished {
			return control.MembershipSnapshot{}, false
		}
		snapshot.Revision = spec.Revision
		snapshot.Phase = control.PhasePrepare
	case controller.PhasePause:
		if state.PausePublished {
			snapshot.Revision = spec.Revision + 1
			snapshot.Phase = control.PhasePaused
		} else if state.PreparePublished {
			snapshot.Revision = spec.Revision
			snapshot.Phase = control.PhasePrepare
		} else {
			return control.MembershipSnapshot{}, false
		}
	case controller.PhaseDrain:
		if state.DrainPublished {
			snapshot.Revision = spec.Revision + 2
			snapshot.Phase = control.PhaseDrain
		} else if state.PausePublished {
			snapshot.Revision = spec.Revision + 1
			snapshot.Phase = control.PhasePaused
		} else {
			return control.MembershipSnapshot{}, false
		}
	default:
		return control.MembershipSnapshot{}, false
	}
	return snapshot, true
}

func brokerMembership(brokers []controller.Broker) control.Membership {
	membership := make(control.Membership, len(brokers))
	for index, broker := range brokers {
		membership[index] = broker.ID
	}
	return membership
}

func targetActiveSnapshot(spec controller.TransitionSpec) control.MembershipSnapshot {
	return control.MembershipSnapshot{
		Version: control.SnapshotVersion, Namespace: spec.Namespace, LibraryVersion: spec.LibraryVersion,
		ScalingGroup: spec.Group, Revision: spec.Revision + 4, Epoch: spec.ToEpoch, Phase: control.PhaseActive,
		HashContract: spec.HashContract, Algorithm: spec.Algorithm, CurrentMembership: brokerMembership(spec.Proposed),
		Queue: spec.Queue, Destination: spec.Destination, CurrentResources: slices.Clone(spec.ProposedResources),
	}
}

func GenesisSnapshot(namespace string, group config.ScalingGroup) (control.MembershipSnapshot, error) {
	names, err := control.NewManagedNames(namespace)
	if err != nil {
		return control.MembershipSnapshot{}, err
	}
	resources, err := epochResources(names, group.ID, group.OrderedBrokerIDs, group.EffectiveConsumerSets(), 1)
	if err != nil {
		return control.MembershipSnapshot{}, err
	}
	destination, err := names.EpochIngressTopic(group.ID, 1)
	if err != nil {
		return control.MembershipSnapshot{}, err
	}
	snapshot := control.MembershipSnapshot{
		Version: control.SnapshotVersion, Namespace: namespace, LibraryVersion: group.CustomerLibrary,
		ScalingGroup: group.ID, Revision: 1, Epoch: 1, Phase: control.PhaseActive,
		HashContract: group.HashContract, Algorithm: control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: slices.Clone(group.OrderedBrokerIDs),
		Queue:             control.QueueInfo{Name: group.Queue.NamePrefix, Durable: true},
		Destination:       control.DestinationInfo{Kind: control.DestinationTopic, Name: destination},
		CurrentResources:  resources,
	}
	return snapshot, snapshot.Validate()
}

func validateConfiguredSnapshot(group config.ScalingGroup, snapshot control.MembershipSnapshot) error {
	if snapshot.HashContract != group.HashContract || snapshot.LibraryVersion != group.CustomerLibrary || snapshot.Queue.Name != group.Queue.NamePrefix {
		return fmt.Errorf("runtime: retained membership for %q conflicts with configured contract", group.ID)
	}
	return nil
}

func epochResources(names control.ManagedNames, group string, brokers []string, consumerSets []config.ConsumerSet, epoch uint64) ([]control.EpochResourceIdentity, error) {
	topic, err := names.EpochIngressTopic(group, epoch)
	if err != nil {
		return nil, err
	}
	resources := make([]control.EpochResourceIdentity, 0, len(brokers)*len(consumerSets))
	for _, broker := range brokers {
		for _, consumerSet := range consumerSets {
			queue, err := names.EpochConsumerQueueForBroker(group, broker, consumerSet.ID, epoch)
			if err != nil {
				return nil, err
			}
			resources = append(resources, control.EpochResourceIdentity{Epoch: epoch, BrokerID: broker, ConsumerSet: consumerSet.ID, QueueName: queue, IngressTopic: topic})
		}
	}
	return resources, nil
}

// CommandPublisher publishes the phase snapshot followed by the exact typed
// participant commands whose IDs the controller durably records before Publish.
type CommandPublisher struct {
	Publisher MembershipPublisher
	Now       func() time.Time
	Deadlines map[string]time.Duration
	Catalog   *MembershipCatalog

	mu           sync.RWMutex
	requirements map[string]map[controller.Phase][]controller.ParticipantRequirement
}

func (p *CommandPublisher) Publish(ctx context.Context, update controller.ControlUpdate) error {
	if p == nil || p.Publisher == nil || p.Catalog == nil {
		return errors.New("runtime: command publisher dependencies are required")
	}
	statePhase, wirePhase, err := commandPhases(update.Phase)
	if err != nil {
		return err
	}
	var requirements []controller.ParticipantRequirement
	if statePhase != controller.PhaseCommit {
		requirements = p.requirementsForPhase(update.Group, statePhase)
		if len(requirements) == 0 {
			return fmt.Errorf("runtime: no typed %s command requirements for %q", statePhase, update.Group)
		}
		if !update.Rollback {
			if len(update.IssuedCommandIDs) != len(requirements) {
				return fmt.Errorf("runtime: incomplete durable %s command issuance intent for %q", statePhase, update.Group)
			}
			for _, requirement := range requirements {
				messageID := commandOperationID(update, statePhase, requirement)
				if persisted := update.IssuedCommandIDs[requirement.Participant]; persisted != messageID {
					return fmt.Errorf("runtime: command issuance intent for %q in %s is not durable", requirement.Participant, statePhase)
				}
			}
		}
	}
	if err := p.Publisher.Publish(ctx, update); err != nil {
		return err
	}
	if err := p.Catalog.Put(update.Snapshot); err != nil {
		return err
	}
	if statePhase == controller.PhaseCommit {
		return nil
	}
	now := p.Now
	if now == nil {
		now = time.Now
	}
	issuedAt := now().UTC()
	deadline := issuedAt.Add(p.Deadlines[update.Group])
	if !deadline.After(issuedAt) {
		return fmt.Errorf("runtime: command deadline for %q is not configured", update.Group)
	}
	for _, requirement := range requirements {
		messageID := commandOperationID(update, statePhase, requirement)
		command := control.CommandEnvelope{
			Version: control.ProtocolVersion, MessageID: messageID, Namespace: update.Snapshot.Namespace,
			Group: update.Group, TransitionID: update.TransitionID, Epoch: update.ToEpoch, Phase: wirePhase,
			Participant: requirement.Participant, Role: requirement.Role, IssuedAt: issuedAt, Deadline: deadline,
		}
		if err := p.Publisher.PublishCommand(ctx, command); err != nil {
			return fmt.Errorf("runtime: publish %s command for %q: %w", statePhase, requirement.Participant, err)
		}
	}
	return nil
}

func (p *CommandPublisher) SetRequirements(group string, requirements map[controller.Phase][]controller.ParticipantRequirement) {
	copy := make(map[controller.Phase][]controller.ParticipantRequirement, len(requirements))
	for phase, values := range requirements {
		copy[phase] = slices.Clone(values)
	}
	p.mu.Lock()
	if p.requirements == nil {
		p.requirements = make(map[string]map[controller.Phase][]controller.ParticipantRequirement)
	}
	p.requirements[group] = copy
	p.mu.Unlock()
}

func (p *CommandPublisher) requirementsForPhase(group string, phase controller.Phase) []controller.ParticipantRequirement {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return slices.Clone(p.requirements[group][phase])
}

func (p *CommandPublisher) RestoreRequirements(state controller.PersistentState) {
	for group, transition := range state.Groups {
		if transition != nil {
			p.SetRequirements(group, transition.Spec.RoleRequirements)
		}
	}
}

func commandOperationID(update controller.ControlUpdate, phase controller.Phase, requirement controller.ParticipantRequirement) string {
	return update.Group + "/" + update.TransitionID + "/forward/command/" + string(phase) + "/" + string(requirement.Role) + "/" + requirement.Participant
}

func commandPhases(phase controller.Phase) (controller.Phase, control.Phase, error) {
	switch phase {
	case controller.PhasePrepare:
		return phase, control.PhasePrepare, nil
	case controller.PhasePause:
		return phase, control.PhasePaused, nil
	case controller.PhaseDrain:
		return phase, control.PhaseDrain, nil
	case controller.PhaseCommit:
		return phase, control.PhaseCommitted, nil
	case controller.PhaseActivate:
		return phase, control.PhaseActive, nil
	default:
		return "", "", fmt.Errorf("runtime: unsupported controller phase %q", phase)
	}
}

// ControllerPolicyState maps authoritative retained membership plus durable
// transition state to the pure policy engine input. Its base state is always
// unready; broker readiness is derived only from the telemetry snapshot passed
// for the current evaluation cycle.
type ControllerPolicyState struct {
	Config     config.Config
	Controller *controller.Controller
	Catalog    *MembershipCatalog
	ScaleIn    ScaleInProofProvider
}

func (s *ControllerPolicyState) PolicyState() ([]policy.Broker, []policy.GroupState, error) {
	return s.policyState()
}

func (s *ControllerPolicyState) PolicyStateForTelemetry(snapshot policy.TelemetrySnapshot, evaluatedAt time.Time) ([]policy.Broker, []policy.GroupState, error) {
	if evaluatedAt.IsZero() {
		return nil, nil, errors.New("runtime: policy evaluation time is required")
	}
	if err := snapshot.Validate(); err != nil {
		return nil, nil, err
	}
	if snapshot.CapturedAt.After(evaluatedAt) {
		return nil, nil, errors.New("runtime: policy telemetry capture time is in the future")
	}
	inventory, groups, err := s.policyState()
	if err != nil {
		return nil, nil, err
	}
	samples := make(map[string]policy.BrokerSample, len(snapshot.Brokers))
	for _, sample := range snapshot.Brokers {
		samples[sample.BrokerID] = sample
	}
	for index := range inventory {
		broker := &inventory[index]
		maxAge := s.telemetryMaxAge(*broker)
		sample, ok := samples[broker.ID]
		broker.Ready = maxAge > 0 && ok && sample.ServiceClass == broker.ServiceClass && sample.BrokerVersion == broker.BrokerVersion && brokerTelemetryComplete(sample) && !sample.ObservedAt.After(evaluatedAt) && evaluatedAt.Sub(sample.ObservedAt) <= maxAge
	}
	if s.ScaleIn != nil {
		for index := range groups {
			choice, err := s.ScaleIn.ScaleInChoice(groups[index], inventory, snapshot, evaluatedAt)
			if err != nil {
				return nil, nil, fmt.Errorf("runtime: prove scale-in for group %q: %w", groups[index].ID, err)
			}
			groups[index].ScaleInChoice = choice
		}
	}
	return inventory, groups, nil
}

func brokerTelemetryComplete(sample policy.BrokerSample) bool {
	return sample.Resources.IngressBytesPerSecond.Known && sample.Resources.EgressBytesPerSecond.Known && sample.Resources.SpoolBytes.Known && sample.Resources.Connections.Known
}

func (s *ControllerPolicyState) telemetryMaxAge(broker policy.Broker) time.Duration {
	maxAge := s.Config.Staleness.TelemetryMaxAge.Duration
	for _, groupID := range broker.EligibleGroups {
		configured, ok := configuredGroup(s.Config.Groups, groupID)
		if !ok {
			continue
		}
		groupAge := configured.Handover.TelemetryMaxAge.Duration
		if groupAge > 0 && (maxAge <= 0 || groupAge < maxAge) {
			maxAge = groupAge
		}
	}
	return maxAge
}

func (s *ControllerPolicyState) policyState() ([]policy.Broker, []policy.GroupState, error) {
	if s == nil || s.Controller == nil || s.Catalog == nil {
		return nil, nil, errors.New("runtime: controller policy state is not initialized")
	}
	inventory := s.Config.PolicyBrokers()
	durable := s.Controller.Snapshot()
	groups := make([]policy.GroupState, 0, len(s.Config.Groups))
	for _, configured := range s.Config.Groups {
		retained, ok := s.Catalog.Get(configured.ID)
		if !ok {
			return nil, nil, fmt.Errorf("runtime: group %q has no authoritative membership", configured.ID)
		}
		state := policy.GroupState{ID: configured.ID, Membership: slices.Clone(retained.CurrentMembership), Policy: policy.GroupPolicy{
			MinimumBrokers: configured.Policy.MinimumBrokers, MaximumBrokers: configured.Policy.MaximumBrokers,
			WarmBrokers: configured.Policy.WarmBrokers, HeadroomPercent: configured.Policy.HeadroomPercent,
			PressureWindow: configured.Policy.PressureWindow.Duration, TelemetryMaxAge: configured.Handover.TelemetryMaxAge.Duration,
			Cooldown: configured.Policy.Cooldown.Duration, MaxConcurrentChanges: configured.Policy.MaxConcurrentChanges,
		}}
		if transition := durable.Groups[configured.ID]; transition != nil {
			if !transition.Completed {
				state.ActiveTransitions = 1
			}
			if transition.CompletedAt != nil {
				state.LastTransitionAt = *transition.CompletedAt
			}
		}
		groups = append(groups, state)
	}
	return inventory, groups, nil
}

// RecommendationController validates policy output against the retained
// baseline, prepares the exact fenced target epoch, and only then durably starts
// the typed controller transition.
type RecommendationController struct {
	Controller *controller.Controller
	Catalog    *MembershipCatalog
	Config     config.Config
	Publisher  *CommandPublisher
	Resources  *ManagedEpochResources
	ScaleIn    ScaleInProofProvider
}

func (a *RecommendationController) ApplyRecommendations(ctx context.Context, decision policy.Decision) error {
	if a == nil || a.Controller == nil || a.Catalog == nil || a.Publisher == nil || a.Resources == nil && a.Config.Runtime.Mode == config.RuntimeModeProduction {
		return errors.New("runtime: recommendation controller is not initialized")
	}
	for _, recommendation := range decision.Transitions {
		if recommendation.Action != policy.ActionScaleOut && recommendation.Action != policy.ActionScaleIn {
			return fmt.Errorf("runtime: policy transition for %q has unsupported action %q", recommendation.GroupID, recommendation.Action)
		}
		configured, ok := configuredGroup(a.Config.Groups, recommendation.GroupID)
		if !ok {
			return fmt.Errorf("runtime: policy recommended unknown group %q", recommendation.GroupID)
		}
		baseline, ok := a.Catalog.Get(configured.ID)
		if !ok || baseline.Phase != control.PhaseActive || !baseline.CurrentMembership.Equal(recommendation.CurrentMembership) {
			return fmt.Errorf("runtime: stale policy recommendation for %q", configured.ID)
		}
		if recommendation.Action == policy.ActionScaleIn {
			if a.ScaleIn == nil {
				return errors.New("runtime: scale-in recommendation has no proof provider")
			}
			if err := a.ScaleIn.VerifyScaleIn(recommendation, decision.At); err != nil {
				return fmt.Errorf("runtime: revalidate scale-in for %q: %w", configured.ID, err)
			}
		}
		if _, active := a.Controller.Group(configured.ID); active {
			state, _ := a.Controller.Group(configured.ID)
			if !state.Completed {
				return controller.ErrTransitionInProgress
			}
			if a.Resources != nil {
				if err := a.Resources.cleanupCompletedGroup(ctx, a.Controller.Snapshot(), a.Catalog, configured.ID); err != nil {
					return fmt.Errorf("runtime: finish prior transition cleanup for %q: %w", configured.ID, err)
				}
			}
		}
		spec, err := transitionSpec(a.Config.Namespace, configured, baseline, recommendation, decision.At)
		if err != nil {
			return err
		}
		if a.Resources != nil {
			if err := a.Resources.Prepare(ctx, spec); err != nil {
				return fmt.Errorf("runtime: prepare policy transition resources for %q: %w", configured.ID, err)
			}
		}
		a.Publisher.SetRequirements(configured.ID, spec.RoleRequirements)
		if _, err := a.Controller.Begin(spec); err != nil {
			return fmt.Errorf("runtime: begin policy transition for %q: %w", configured.ID, err)
		}
	}
	return nil
}

func transitionSpec(namespace string, group config.ScalingGroup, baseline control.MembershipSnapshot, recommendation policy.Recommendation, at time.Time) (controller.TransitionSpec, error) {
	if at.IsZero() {
		return controller.TransitionSpec{}, errors.New("runtime: policy decision timestamp is required")
	}
	names, err := control.NewManagedNames(namespace)
	if err != nil {
		return controller.TransitionSpec{}, err
	}
	toEpoch := baseline.Epoch + 1
	resources, err := epochResources(names, group.ID, recommendation.ProposedMembership, group.EffectiveConsumerSets(), toEpoch)
	if err != nil {
		return controller.TransitionSpec{}, err
	}
	brokerMap := func(ids []string) []controller.Broker {
		result := make([]controller.Broker, 0, len(ids))
		for _, id := range ids {
			result = append(result, controller.Broker{ID: id, Destination: baseline.Destination.Name})
		}
		return result
	}
	requirements := typedRequirements(group)
	spec := controller.TransitionSpec{
		ID: fmt.Sprintf("policy-%s-e%d-%d", group.ID, toEpoch, at.UTC().UnixNano()), Namespace: namespace,
		LibraryVersion: group.CustomerLibrary, Group: group.ID, Revision: baseline.Revision + 1,
		FromEpoch: baseline.Epoch, ToEpoch: toEpoch, Current: brokerMap(recommendation.CurrentMembership), Proposed: brokerMap(recommendation.ProposedMembership),
		Queue: baseline.Queue, Destination: baseline.Destination, CurrentResources: slices.Clone(baseline.CurrentResources), ProposedResources: resources,
		HashContract: group.HashContract, Algorithm: control.AlgorithmSHA256BigEndianModulo, DrainGrace: group.Handover.DrainGrace.Duration,
		RoleRequirements: requirements,
	}
	return spec, spec.Validate()
}

func typedRequirements(group config.ScalingGroup) map[controller.Phase][]controller.ParticipantRequirement {
	result := make(map[controller.Phase][]controller.ParticipantRequirement, 4)
	for _, participant := range group.SubscriberIdentities() {
		requirement := controller.ParticipantRequirement{Participant: participant, Role: control.RoleSubscriber}
		result[controller.PhasePrepare] = append(result[controller.PhasePrepare], requirement)
		result[controller.PhaseDrain] = append(result[controller.PhaseDrain], requirement)
		result[controller.PhaseActivate] = append(result[controller.PhaseActivate], requirement)
	}
	for _, participant := range group.RequiredPublishers {
		requirement := controller.ParticipantRequirement{Participant: participant, Role: control.RolePublisher}
		result[controller.PhasePause] = append(result[controller.PhasePause], requirement)
		result[controller.PhaseActivate] = append(result[controller.PhaseActivate], requirement)
	}
	return result
}

func configuredGroup(groups []config.ScalingGroup, id string) (config.ScalingGroup, bool) {
	for _, group := range groups {
		if group.ID == id {
			return group, true
		}
	}
	return config.ScalingGroup{}, false
}

// UnsupportedCapacityCycle fails closed. The current public configuration has
// no exact measured capacity profiles, so inventing limits or relabeling a
// broker version would violate the policy contract.
type UnsupportedCapacityCycle struct{}

func (UnsupportedCapacityCycle) Run(context.Context) error { return ErrCapacityPolicyUnavailable }

// OwnedProcess closes assembly-owned resources after the controller process
// stops, in reverse construction order.
type OwnedProcess struct {
	Process interface{ Run(context.Context) error }
	Close   *CloseGroup
}

func (p *OwnedProcess) Run(ctx context.Context) error {
	if p == nil || p.Process == nil {
		return errors.New("runtime: owned process is not initialized")
	}
	runErr := p.Process.Run(ctx)
	var closeErr error
	if p.Close != nil {
		closeErr = p.Close.Close()
	}
	return errors.Join(runErr, closeErr)
}
