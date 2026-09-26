package policy

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"
)

// Action is a capacity-policy recommendation. Recommendations are intents only;
// a controller must still run the coordinated membership transition protocol.
type Action string

const (
	ActionNone     Action = "NONE"
	ActionScaleOut Action = "SCALE_OUT"
	ActionScaleIn  Action = "SCALE_IN"
)

// Recommendation is one deterministic desired membership change. Proposed
// membership preserves current order: scale-out appends; scale-in removes only
// the explicitly selected, proven-safe broker.
type Recommendation struct {
	GroupID            string         `json:"group_id"`
	Action             Action         `json:"action"`
	BrokerID           string         `json:"broker_id,omitempty"`
	CurrentMembership  []string       `json:"current_membership,omitempty"`
	ProposedMembership []string       `json:"proposed_membership,omitempty"`
	Pressure           PressureKind   `json:"pressure"`
	Telemetry          TelemetryState `json:"telemetry"`
	Reason             string         `json:"reason"`
	Evidence           string         `json:"evidence,omitempty"`
}

// Decision contains all per-group outcomes plus the globally admitted
// transitions. All slices are sorted by group ID.
type Decision struct {
	At          time.Time        `json:"at"`
	Aggregation Aggregation      `json:"aggregation"`
	Groups      []Recommendation `json:"groups"`
	Transitions []Recommendation `json:"transitions"`
}

// Engine retains only sustained-pressure state. Feed snapshots in chronological
// order; State and RestoreState allow a runtime to persist the window across
// restarts without coupling policy to storage. Engine is not safe for concurrent
// Evaluate or RestoreState calls; the owning controller serializes policy cycles.
type Engine struct {
	catalog  ProfileCatalog
	options  EngineOptions
	pressure map[string]time.Time
	lastAt   time.Time
}

// EngineOptions bounds the number of concurrent membership changes across all
// groups. Active transitions supplied in GroupState count against this limit.
type EngineOptions struct {
	MaxConcurrentTransitions int `json:"max_concurrent_transitions"`
}

// EngineState is the serializable sustained-pressure state.
type EngineState struct {
	LastEvaluatedAt time.Time            `json:"last_evaluated_at,omitempty"`
	PressureSince   map[string]time.Time `json:"pressure_since,omitempty"`
}

func NewEngine(catalog ProfileCatalog, options EngineOptions) (*Engine, error) {
	if options.MaxConcurrentTransitions < 1 {
		return nil, errors.New("policy: global maximum concurrent transitions must be positive")
	}
	return &Engine{catalog: catalog, options: options, pressure: make(map[string]time.Time)}, nil
}

// State returns an independent snapshot suitable for durable persistence.
func (e *Engine) State() EngineState {
	state := EngineState{LastEvaluatedAt: e.lastAt, PressureSince: make(map[string]time.Time, len(e.pressure))}
	for group, since := range e.pressure {
		state.PressureSince[group] = since
	}
	return state
}

// RestoreState restores sustained windows. Future or zero timestamps are
// rejected because they could prematurely authorize scale-out.
func (e *Engine) RestoreState(state EngineState, now time.Time) error {
	if now.IsZero() {
		return errors.New("policy: restore time is required")
	}
	if !state.LastEvaluatedAt.IsZero() && state.LastEvaluatedAt.After(now) {
		return errors.New("policy: last evaluation is in the future")
	}
	if state.LastEvaluatedAt.IsZero() && len(state.PressureSince) != 0 {
		return errors.New("policy: persisted pressure requires a last evaluation time")
	}
	pressure := make(map[string]time.Time, len(state.PressureSince))
	for group, since := range state.PressureSince {
		if group == "" || since.IsZero() || since.After(state.LastEvaluatedAt) {
			return errors.New("policy: invalid persisted pressure window")
		}
		pressure[group] = since
	}
	e.lastAt = state.LastEvaluatedAt
	e.pressure = pressure
	return nil
}

// Evaluate aggregates one cycle, advances sustained-pressure windows, and
// applies group and global transition bounds. An invalid cycle does not mutate
// engine state. Unknown/stale telemetry resets, rather than pauses, a pressure
// window so the evidence must be continuous.
func (e *Engine) Evaluate(now time.Time, snapshot TelemetrySnapshot, inventory []Broker, groups []GroupState) (Decision, error) {
	if e == nil {
		return Decision{}, errors.New("policy: nil engine")
	}
	if !e.lastAt.IsZero() && !now.After(e.lastAt) {
		return Decision{}, errors.New("policy: evaluation time must increase monotonically")
	}
	aggregation, err := Aggregate(now, snapshot, e.catalog, inventory, groups)
	if err != nil {
		return Decision{}, err
	}

	byGroup := make(map[string]GroupAssessment, len(aggregation.Groups))
	for _, assessment := range aggregation.Groups {
		byGroup[assessment.GroupID] = assessment
	}
	byBroker := make(map[string]BrokerAssessment, len(aggregation.Brokers))
	for _, assessment := range aggregation.Brokers {
		byBroker[assessment.BrokerID] = assessment
	}
	orderedGroups := slices.Clone(groups)
	sort.Slice(orderedGroups, func(i, j int) bool { return orderedGroups[i].ID < orderedGroups[j].ID })

	nextPressure := make(map[string]time.Time, len(e.pressure))
	for group, since := range e.pressure {
		nextPressure[group] = since
	}
	for _, group := range orderedGroups {
		assessment := byGroup[group.ID]
		if assessment.State == TelemetryKnown && assessment.Pressure == PressureBrokerSaturation {
			_, exists := nextPressure[group.ID]
			freshCycle := e.lastAt.IsZero() || hasNewPressureObservation(group, byBroker, e.lastAt)
			switch {
			case !exists && freshCycle:
				nextPressure[group.ID] = now
			case exists && !freshCycle:
				// Replayed or older evidence breaks the window and cannot start another
				// one. A subsequent fresh cycle must establish a new origin.
				delete(nextPressure, group.ID)
			}
		} else {
			delete(nextPressure, group.ID)
		}
	}

	freeGlobal := e.options.MaxConcurrentTransitions
	for _, group := range orderedGroups {
		freeGlobal -= group.ActiveTransitions
	}
	if freeGlobal < 0 {
		freeGlobal = 0
	}

	decision := Decision{At: now, Aggregation: aggregation}
	for _, group := range orderedGroups {
		assessment := byGroup[group.ID]
		assessment.SustainedSince = nextPressure[group.ID]
		updateGroupAssessment(&decision.Aggregation, assessment)
		recommendation, eligible := recommend(now, group, assessment, inventory, nextPressure[group.ID])
		if eligible {
			if freeGlobal > 0 {
				freeGlobal--
				decision.Transitions = append(decision.Transitions, recommendation)
			} else {
				recommendation.Reason = "global transition limit reached"
				recommendation.Action = ActionNone
				recommendation.BrokerID = ""
				recommendation.ProposedMembership = nil
			}
		}
		decision.Groups = append(decision.Groups, recommendation)
	}
	e.pressure = nextPressure
	e.lastAt = now
	return decision, nil
}

func updateGroupAssessment(aggregation *Aggregation, update GroupAssessment) {
	for i := range aggregation.Groups {
		if aggregation.Groups[i].GroupID == update.GroupID {
			aggregation.Groups[i] = update
			return
		}
	}
}

func hasNewPressureObservation(group GroupState, assessments map[string]BrokerAssessment, after time.Time) bool {
	for _, brokerID := range group.Membership {
		if !assessments[brokerID].ObservedAt.After(after) {
			return false
		}
	}
	return true
}

func recommend(now time.Time, group GroupState, assessment GroupAssessment, inventory []Broker, sustainedSince time.Time) (Recommendation, bool) {
	recommendation := Recommendation{
		GroupID: group.ID, Action: ActionNone, Pressure: assessment.Pressure,
		Telemetry: assessment.State, CurrentMembership: slices.Clone(group.Membership),
	}
	if assessment.State != TelemetryKnown {
		switch assessment.State {
		case TelemetryStale:
			recommendation.Reason = fmt.Sprintf("%v: %s", ErrStaleTelemetry, assessment.Reason)
		default:
			recommendation.Reason = fmt.Sprintf("%v: %s", ErrUnknownTelemetry, assessment.Reason)
		}
		return recommendation, false
	}
	if group.ActiveTransitions >= group.Policy.MaxConcurrentChanges {
		recommendation.Reason = "group transition limit reached"
		return recommendation, false
	}
	if group.ActiveTransitions > 0 {
		recommendation.Reason = "group already has a membership transition"
		return recommendation, false
	}
	if !group.LastTransitionAt.IsZero() && now.Sub(group.LastTransitionAt) < group.Policy.Cooldown {
		recommendation.Reason = "group cooldown is active"
		return recommendation, false
	}

	floor := group.Policy.floor()
	if len(group.Membership) < floor {
		return scaleOut(group, assessment, inventory, "membership is below minimum plus warm count")
	}

	switch assessment.Pressure {
	case PressureBrokerSaturation:
		if sustainedSince.IsZero() || now.Sub(sustainedSince) < group.Policy.PressureWindow {
			recommendation.Reason = "broker pressure has not been sustained for the configured window"
			return recommendation, false
		}
		if len(group.Membership) >= group.Policy.MaximumBrokers {
			recommendation.Reason = "broker pressure is sustained but maximum broker count is reached"
			return recommendation, false
		}
		return scaleOut(group, assessment, inventory, "broker pressure is sustained beyond the headroom threshold")
	case PressureDownstreamBacklog:
		recommendation.Reason = "downstream backlog exists without broker saturation"
		return recommendation, false
	}

	if len(group.Membership) <= floor {
		recommendation.Reason = "membership is at minimum plus warm count"
		return recommendation, false
	}
	if group.ScaleInChoice == nil {
		recommendation.Reason = "scale-in requires an explicit safe broker choice"
		return recommendation, false
	}
	if !group.ScaleInChoice.Safe {
		recommendation.Reason = "explicit scale-in choice is not proven safe"
		return recommendation, false
	}
	proposed := removeBroker(group.Membership, group.ScaleInChoice.BrokerID)
	if len(proposed) < floor {
		recommendation.Reason = "scale-in would violate minimum plus warm count"
		return recommendation, false
	}
	recommendation.Action = ActionScaleIn
	recommendation.BrokerID = group.ScaleInChoice.BrokerID
	recommendation.ProposedMembership = proposed
	recommendation.Reason = "explicit broker choice is proven safe and capacity is below the headroom threshold"
	recommendation.Evidence = group.ScaleInChoice.Evidence
	return recommendation, true
}

func scaleOut(group GroupState, assessment GroupAssessment, inventory []Broker, reason string) (Recommendation, bool) {
	recommendation := Recommendation{
		GroupID: group.ID, Action: ActionNone, Pressure: assessment.Pressure,
		Telemetry: assessment.State, CurrentMembership: slices.Clone(group.Membership),
	}
	if len(group.Membership) >= group.Policy.MaximumBrokers {
		recommendation.Reason = "maximum broker count is reached"
		return recommendation, false
	}
	eligible := make([]string, 0)
	for _, broker := range inventory {
		if !slices.Contains(group.Membership, broker.ID) && broker.eligibleFor(group.ID) {
			eligible = append(eligible, broker.ID)
		}
	}
	sort.Strings(eligible)
	if len(eligible) == 0 {
		recommendation.Reason = "no ready broker satisfies the group's feature placement constraints"
		return recommendation, false
	}
	recommendation.Action = ActionScaleOut
	recommendation.BrokerID = eligible[0]
	recommendation.ProposedMembership = append(slices.Clone(group.Membership), eligible[0])
	recommendation.Reason = reason
	return recommendation, true
}

func removeBroker(membership []string, brokerID string) []string {
	result := make([]string, 0, len(membership)-1)
	for _, id := range membership {
		if id != brokerID {
			result = append(result, id)
		}
	}
	return result
}
