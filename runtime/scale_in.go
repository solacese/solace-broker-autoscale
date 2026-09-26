package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/policy"
)

// ScaleInProofProvider selects and revalidates a proof-bound membership
// contraction. A missing proof is represented by a nil choice and must never be
// interpreted as permission to scale in.
type ScaleInProofProvider interface {
	ScaleInChoice(policy.GroupState, []policy.Broker, policy.TelemetrySnapshot, time.Time) (*policy.ScaleInChoice, error)
	VerifyScaleIn(policy.Recommendation, time.Time) error
}

// ConservativeScaleInProofProvider derives a choice from one exact SEMP cycle,
// retained ACTIVE membership, durable controller state, exact capacity profiles,
// and all configured groups' broker references. It authorizes membership
// contraction only; physical broker deletion remains a separate post-transition
// operation and is not performed by this provider.
type ConservativeScaleInProofProvider struct {
	Config     config.Config
	Controller *controller.Controller
	Catalog    *MembershipCatalog
	Profiles   policy.ProfileCatalog
}

type scaleInEvidence struct {
	Version              int      `json:"version"`
	Group                string   `json:"group"`
	Broker               string   `json:"broker"`
	Epoch                uint64   `json:"epoch"`
	Revision             uint64   `json:"revision"`
	CurrentMembership    []string `json:"current_membership"`
	RemainingMembership  []string `json:"remaining_membership"`
	CapturedAt           string   `json:"captured_at"`
	EvaluatedAt          string   `json:"evaluated_at"`
	MinimumWarmFloor     int      `json:"minimum_warm_floor"`
	HeadroomPercent      float64  `json:"headroom_percent"`
	CurrentBacklogZero   bool     `json:"current_backlog_zero"`
	RemainingCapacityOK  bool     `json:"remaining_capacity_ok"`
	ExclusiveGroupOwner  bool     `json:"exclusive_group_owner"`
	ControllerIdle       bool     `json:"controller_idle"`
	CleanupEvidenceReady bool     `json:"cleanup_evidence_ready"`
	CapacityDisposition  string   `json:"capacity_disposition"`
}

func (p *ConservativeScaleInProofProvider) ScaleInChoice(group policy.GroupState, inventory []policy.Broker, snapshot policy.TelemetrySnapshot, evaluatedAt time.Time) (*policy.ScaleInChoice, error) {
	if p == nil || p.Controller == nil || p.Catalog == nil {
		return nil, errors.New("runtime: scale-in proof provider is not initialized")
	}
	if evaluatedAt.IsZero() || snapshot.CapturedAt.IsZero() || snapshot.CapturedAt.After(evaluatedAt) {
		return nil, nil
	}
	configured, ok := configuredGroup(p.Config.Groups, group.ID)
	if !ok {
		return nil, fmt.Errorf("runtime: scale-in proof references unknown group %q", group.ID)
	}
	floor := configured.Policy.MinimumBrokers + configured.Policy.WarmBrokers
	// Policy telemetry currently has one aggregate row per group/broker. Until the
	// collector aggregates every consumer-set queue, more than one set cannot prove
	// the entire group's backlog is zero and must fail closed.
	if len(configured.EffectiveConsumerSets()) != 1 || group.ActiveTransitions != 0 || len(group.Membership) <= floor {
		return nil, nil
	}
	retained, ok := p.Catalog.Get(group.ID)
	if !ok || retained.Phase != control.PhaseActive || !retained.CurrentMembership.Equal(group.Membership) {
		return nil, nil
	}
	durable := p.Controller.Snapshot()
	cleanupReady, idle := controllerScaleInState(durable, group.ID)
	if !idle || !cleanupReady {
		return nil, nil
	}

	brokerSamples := make(map[string]policy.BrokerSample, len(snapshot.Brokers))
	for _, sample := range snapshot.Brokers {
		brokerSamples[sample.BrokerID] = sample
	}
	groupSamples := make(map[string]policy.GroupSample, len(group.Membership))
	for _, sample := range snapshot.Groups {
		if sample.GroupID == group.ID {
			groupSamples[sample.BrokerID] = sample
		}
	}
	brokers := make(map[string]policy.Broker, len(inventory))
	for _, broker := range inventory {
		brokers[broker.ID] = broker
	}

	// Prefer removing the last eligible member so the authoritative prefix remains
	// unchanged. If it is not provably safe, inspect earlier members without ever
	// reordering the survivors.
	for index := len(group.Membership) - 1; index >= 0; index-- {
		candidate := group.Membership[index]
		remaining := removeOrderedMember(group.Membership, candidate)
		if len(remaining) != len(group.Membership)-1 || len(remaining) < floor {
			continue
		}
		ownedOnlyByGroup, complete := p.exclusiveCurrentOwnership(candidate, group.ID, durable)
		if !complete || !ownedOnlyByGroup {
			continue
		}
		capacityOK, backlogZero, complete, err := p.remainingCapacityProven(configured, group.Membership, remaining, candidate, brokers, brokerSamples, groupSamples, evaluatedAt)
		if err != nil {
			return nil, err
		}
		if !complete || !backlogZero || !capacityOK {
			continue
		}
		evidence := scaleInEvidence{
			Version: 1, Group: group.ID, Broker: candidate, Epoch: retained.Epoch, Revision: retained.Revision,
			CurrentMembership: slices.Clone(group.Membership), RemainingMembership: remaining,
			CapturedAt: snapshot.CapturedAt.UTC().Format(time.RFC3339Nano), EvaluatedAt: evaluatedAt.UTC().Format(time.RFC3339Nano),
			MinimumWarmFloor: floor, HeadroomPercent: configured.Policy.HeadroomPercent,
			CurrentBacklogZero: true, RemainingCapacityOK: true, ExclusiveGroupOwner: true,
			ControllerIdle: true, CleanupEvidenceReady: true, CapacityDisposition: "retain-until-transition-and-cleanup-complete",
		}
		encoded, err := json.Marshal(evidence)
		if err != nil {
			return nil, fmt.Errorf("runtime: encode scale-in evidence: %w", err)
		}
		return &policy.ScaleInChoice{BrokerID: candidate, Safe: true, Evidence: string(encoded)}, nil
	}
	return nil, nil
}

func (p *ConservativeScaleInProofProvider) VerifyScaleIn(recommendation policy.Recommendation, decisionAt time.Time) error {
	if p == nil || p.Controller == nil || p.Catalog == nil {
		return errors.New("runtime: scale-in proof provider is not initialized")
	}
	if recommendation.Evidence == "" || decisionAt.IsZero() {
		return errors.New("runtime: scale-in recommendation requires proof evidence and a decision time")
	}
	var evidence scaleInEvidence
	if err := json.Unmarshal([]byte(recommendation.Evidence), &evidence); err != nil {
		return fmt.Errorf("runtime: decode scale-in evidence: %w", err)
	}
	if evidence.Version != 1 || evidence.Group != recommendation.GroupID || evidence.Broker != recommendation.BrokerID || !evidence.CurrentBacklogZero || !evidence.RemainingCapacityOK || !evidence.ExclusiveGroupOwner || !evidence.ControllerIdle || !evidence.CleanupEvidenceReady || evidence.CapacityDisposition != "retain-until-transition-and-cleanup-complete" {
		return errors.New("runtime: scale-in evidence is incomplete or does not match the recommendation")
	}
	evaluatedAt, err := time.Parse(time.RFC3339Nano, evidence.EvaluatedAt)
	if err != nil || !evaluatedAt.Equal(decisionAt.UTC()) {
		return errors.New("runtime: scale-in evidence does not match the policy decision time")
	}
	configured, ok := configuredGroup(p.Config.Groups, recommendation.GroupID)
	if !ok {
		return fmt.Errorf("runtime: scale-in proof references unknown group %q", recommendation.GroupID)
	}
	floor := configured.Policy.MinimumBrokers + configured.Policy.WarmBrokers
	if evidence.MinimumWarmFloor != floor || evidence.HeadroomPercent != configured.Policy.HeadroomPercent || len(recommendation.ProposedMembership) < floor {
		return errors.New("runtime: scale-in evidence no longer matches configured minimum, warm, or headroom constraints")
	}
	retained, ok := p.Catalog.Get(recommendation.GroupID)
	if !ok || retained.Phase != control.PhaseActive || retained.Epoch != evidence.Epoch || retained.Revision != evidence.Revision || !retained.CurrentMembership.Equal(recommendation.CurrentMembership) || !slices.Equal(evidence.CurrentMembership, recommendation.CurrentMembership) || !slices.Equal(evidence.RemainingMembership, recommendation.ProposedMembership) || !slices.Equal(removeOrderedMember(recommendation.CurrentMembership, recommendation.BrokerID), recommendation.ProposedMembership) {
		return errors.New("runtime: scale-in evidence is stale or does not preserve ordered membership")
	}
	durable := p.Controller.Snapshot()
	cleanupReady, idle := controllerScaleInState(durable, recommendation.GroupID)
	ownedOnlyByGroup, complete := p.exclusiveCurrentOwnership(recommendation.BrokerID, recommendation.GroupID, durable)
	if !cleanupReady || !idle || !complete || !ownedOnlyByGroup {
		return errors.New("runtime: scale-in ownership or controller state changed after proof")
	}
	capturedAt, err := time.Parse(time.RFC3339Nano, evidence.CapturedAt)
	if err != nil || capturedAt.After(decisionAt) || decisionAt.Sub(capturedAt) > configured.Handover.TelemetryMaxAge.Duration {
		return errors.New("runtime: scale-in telemetry proof is stale")
	}
	return nil
}

func controllerScaleInState(state controller.PersistentState, group string) (cleanupReady, idle bool) {
	transition := state.Groups[group]
	if transition == nil {
		return true, true
	}
	if !transition.Completed {
		return false, false
	}
	if transition.RollingBack {
		return true, true
	}
	if transition.CompletedAt == nil || !transition.CleanupRecorded {
		return false, true
	}
	for _, evidence := range state.CleanupEvidence[group] {
		if evidence.TransitionID == transition.Spec.ID && evidence.SourceEpoch == transition.Spec.FromEpoch && evidence.TargetEpoch == transition.Spec.ToEpoch && evidence.OldEpochFenced && !evidence.ZeroSince.IsZero() && !evidence.DrainObservedAt.IsZero() && !evidence.FenceVerifiedAt.IsZero() && !evidence.CommittedAt.IsZero() {
			return true, true
		}
	}
	return false, true
}

func (p *ConservativeScaleInProofProvider) exclusiveCurrentOwnership(brokerID, groupID string, durable controller.PersistentState) (exclusive, complete bool) {
	for _, configured := range p.Config.Groups {
		retained, ok := p.Catalog.Get(configured.ID)
		if !ok {
			return false, false
		}
		if configured.ID != groupID && (slices.Contains(retained.CurrentMembership, brokerID) || slices.Contains(retained.ProposedMembership, brokerID)) {
			return false, true
		}
	}
	for id, transition := range durable.Groups {
		if id == groupID || transition == nil || transition.Completed {
			continue
		}
		if controllerBrokersContain(transition.Spec.Current, brokerID) || controllerBrokersContain(transition.Spec.Proposed, brokerID) {
			return false, true
		}
	}
	return true, true
}

func (p *ConservativeScaleInProofProvider) remainingCapacityProven(group config.ScalingGroup, current, remaining []string, candidate string, inventory map[string]policy.Broker, brokerSamples map[string]policy.BrokerSample, groupSamples map[string]policy.GroupSample, evaluatedAt time.Time) (capacityOK, backlogZero, complete bool, err error) {
	maxAge := group.Handover.TelemetryMaxAge.Duration
	if maxAge <= 0 {
		return false, false, false, nil
	}
	threshold := 1 - group.Policy.HeadroomPercent/100
	if threshold <= 0 {
		return false, false, false, nil
	}
	var demand resourceTotals
	backlogZero = true
	for _, brokerID := range current {
		broker, ok := inventory[brokerID]
		brokerSample, brokerOK := brokerSamples[brokerID]
		groupSample, groupOK := groupSamples[brokerID]
		if !ok || !brokerOK || !groupOK || brokerSample.ServiceClass != broker.ServiceClass || brokerSample.BrokerVersion != broker.BrokerVersion || !freshAt(brokerSample.ObservedAt, evaluatedAt, maxAge) || !freshAt(groupSample.ObservedAt, evaluatedAt, maxAge) || !completeResources(brokerSample.Resources) || !completeResources(groupSample.Resources) || !completeBacklog(groupSample.Backlog) {
			return false, false, false, nil
		}
		if groupSample.Backlog.QueuedMessages.Value != 0 || groupSample.Backlog.UnackedMessages.Value != 0 {
			backlogZero = false
		}
		demand = demand.add(totals(groupSample.Resources))
	}
	if !backlogZero {
		return false, false, true, nil
	}
	candidateLoad := totals(groupSamples[candidate].Resources)
	var remainingLimits resourceTotals
	for _, brokerID := range remaining {
		broker := inventory[brokerID]
		profile, lookupErr := p.Profiles.Lookup(broker.ServiceClass, broker.BrokerVersion)
		if lookupErr != nil {
			return false, false, false, lookupErr
		}
		limits := resourceTotals{profile.Limits.IngressBytesPerSecond, profile.Limits.EgressBytesPerSecond, profile.Limits.SpoolBytes, profile.Limits.Connections}
		remainingLimits = remainingLimits.add(limits)
		projectedWorstCase := totals(brokerSamples[brokerID].Resources).add(candidateLoad)
		if !belowThreshold(projectedWorstCase, limits, threshold) {
			return false, true, true, nil
		}
	}
	return belowThreshold(demand, remainingLimits, threshold), true, true, nil
}

type resourceTotals struct{ ingress, egress, spool, connections float64 }

func (r resourceTotals) add(other resourceTotals) resourceTotals {
	return resourceTotals{r.ingress + other.ingress, r.egress + other.egress, r.spool + other.spool, r.connections + other.connections}
}

func totals(resources policy.Resources) resourceTotals {
	return resourceTotals{resources.IngressBytesPerSecond.Value, resources.EgressBytesPerSecond.Value, resources.SpoolBytes.Value, resources.Connections.Value}
}

func belowThreshold(value, limit resourceTotals, threshold float64) bool {
	return limit.ingress > 0 && limit.egress > 0 && limit.spool > 0 && limit.connections > 0 && value.ingress/limit.ingress < threshold && value.egress/limit.egress < threshold && value.spool/limit.spool < threshold && value.connections/limit.connections < threshold
}

func completeResources(resources policy.Resources) bool {
	return resources.IngressBytesPerSecond.Known && resources.EgressBytesPerSecond.Known && resources.SpoolBytes.Known && resources.Connections.Known
}

func completeBacklog(backlog policy.Backlog) bool {
	return backlog.QueuedMessages.Known && backlog.UnackedMessages.Known
}

func freshAt(observedAt, evaluatedAt time.Time, maxAge time.Duration) bool {
	return !observedAt.IsZero() && !observedAt.After(evaluatedAt) && evaluatedAt.Sub(observedAt) <= maxAge
}

func removeOrderedMember(membership []string, brokerID string) []string {
	result := make([]string, 0, len(membership)-1)
	for _, id := range membership {
		if id != brokerID {
			result = append(result, id)
		}
	}
	return result
}

func controllerBrokersContain(brokers []controller.Broker, id string) bool {
	for _, broker := range brokers {
		if broker.ID == id {
			return true
		}
	}
	return false
}
