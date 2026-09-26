package policy

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
)

// TelemetryState describes whether every measurement needed for an assessment
// is usable. Unknown and stale are distinct terminal states and neither is
// interpreted as an observed zero.
type TelemetryState string

const (
	TelemetryKnown   TelemetryState = "KNOWN"
	TelemetryUnknown TelemetryState = "UNKNOWN"
	TelemetryStale   TelemetryState = "STALE"
)

// Utilization is a dimensionless fraction of a validated capacity profile. A
// value of 1 means the measured profile limit has been reached.
type Utilization struct {
	Ingress     float64 `json:"ingress"`
	Egress      float64 `json:"egress"`
	Spool       float64 `json:"spool"`
	Connections float64 `json:"connections"`
}

// Peak returns the largest resource ratio.
func (u Utilization) Peak() float64 {
	return max(u.Ingress, u.Egress, u.Spool, u.Connections)
}

func (u Utilization) max(other Utilization) Utilization {
	return Utilization{
		Ingress:     max(u.Ingress, other.Ingress),
		Egress:      max(u.Egress, other.Egress),
		Spool:       max(u.Spool, other.Spool),
		Connections: max(u.Connections, other.Connections),
	}
}

// BrokerAssessment combines whole-broker SEMP usage with the sum of all known
// group usage on that broker. Peak utilization uses the larger value in each
// dimension. Consequently one group's assessment observes pressure caused by
// another group sharing the broker without double-counting both sources.
type BrokerAssessment struct {
	BrokerID           string         `json:"broker_id"`
	State              TelemetryState `json:"state"`
	Reason             string         `json:"reason,omitempty"`
	ObservedAt         time.Time      `json:"observed_at,omitempty"`
	BrokerUtilization  Utilization    `json:"broker_utilization"`
	GroupedUtilization Utilization    `json:"grouped_utilization"`
	SharedUtilization  Utilization    `json:"shared_utilization"`
	ContributingGroups []string       `json:"contributing_groups"`
}

// PressureKind separates broker resource exhaustion from a queue backlog on an
// otherwise unsaturated broker. Downstream backlog does not request capacity.
type PressureKind string

const (
	PressureUnknown           PressureKind = "UNKNOWN"
	PressureNone              PressureKind = "NONE"
	PressureBrokerSaturation  PressureKind = "BROKER_SATURATION"
	PressureDownstreamBacklog PressureKind = "DOWNSTREAM_BACKLOG"
)

// GroupAssessment is the current aggregate for one group. WorkloadUtilization
// divides that group's total measured resource demand by its current aggregate
// profile capacity; SharedUtilization is the worst whole/shared broker ratio.
type GroupAssessment struct {
	GroupID             string         `json:"group_id"`
	State               TelemetryState `json:"state"`
	Reason              string         `json:"reason,omitempty"`
	Pressure            PressureKind   `json:"pressure"`
	WorkloadUtilization Utilization    `json:"workload_utilization"`
	SharedUtilization   Utilization    `json:"shared_utilization"`
	QueuedMessages      float64        `json:"queued_messages"`
	UnackedMessages     float64        `json:"unacked_messages"`
	ReadyWarmBrokers    int            `json:"ready_warm_brokers"`
	WarmBrokerShortfall int            `json:"warm_broker_shortfall"`
	SustainedSince      time.Time      `json:"sustained_since,omitempty"`
	Refusal             string         `json:"refusal,omitempty"`
}

// Aggregation is a deterministic view of one telemetry cycle. Broker and group
// assessments are sorted by ID.
type Aggregation struct {
	CapturedAt time.Time          `json:"captured_at"`
	Brokers    []BrokerAssessment `json:"brokers"`
	Groups     []GroupAssessment  `json:"groups"`
}

type resourcesValue struct {
	ingress     float64
	egress      float64
	spool       float64
	connections float64
}

func knownResources(resources Resources) (resourcesValue, bool) {
	if !resources.allKnown() {
		return resourcesValue{}, false
	}
	return resourcesValue{
		ingress:     resources.IngressBytesPerSecond.Value,
		egress:      resources.EgressBytesPerSecond.Value,
		spool:       resources.SpoolBytes.Value,
		connections: resources.Connections.Value,
	}, true
}

func (v resourcesValue) add(other resourcesValue) resourcesValue {
	return resourcesValue{
		ingress:     v.ingress + other.ingress,
		egress:      v.egress + other.egress,
		spool:       v.spool + other.spool,
		connections: v.connections + other.connections,
	}
}

func utilization(value resourcesValue, limit CapacityLimits) Utilization {
	return Utilization{
		Ingress:     value.ingress / limit.IngressBytesPerSecond,
		Egress:      value.egress / limit.EgressBytesPerSecond,
		Spool:       value.spool / limit.SpoolBytes,
		Connections: value.connections / limit.Connections,
	}
}

func utilizationAcross(value resourcesValue, limit resourcesValue) Utilization {
	return Utilization{
		Ingress:     divide(value.ingress, limit.ingress),
		Egress:      divide(value.egress, limit.egress),
		Spool:       divide(value.spool, limit.spool),
		Connections: divide(value.connections, limit.connections),
	}
}

func divide(value, limit float64) float64 {
	if limit == 0 {
		return math.Inf(1)
	}
	return value / limit
}

func limitsValue(limits CapacityLimits) resourcesValue {
	return resourcesValue{limits.IngressBytesPerSecond, limits.EgressBytesPerSecond, limits.SpoolBytes, limits.Connections}
}

type aggregationContext struct {
	now           time.Time
	snapshot      TelemetrySnapshot
	catalog       ProfileCatalog
	brokers       map[string]Broker
	groups        []GroupState
	brokerSamples map[string]BrokerSample
	groupSamples  map[groupBrokerKey]GroupSample
	memberships   map[string][]string
}

// Aggregate validates and combines one SEMP sampling cycle. Missing, unknown,
// or stale rows are represented in the affected assessments rather than
// omitted. Every group known to share a broker is required for that broker's
// grouped aggregation.
func Aggregate(now time.Time, snapshot TelemetrySnapshot, catalog ProfileCatalog, inventory []Broker, groups []GroupState) (Aggregation, error) {
	if now.IsZero() {
		return Aggregation{}, errors.New("policy: evaluation time is required")
	}
	if snapshot.CapturedAt.After(now) {
		return Aggregation{}, errors.New("policy: telemetry capture time is in the future")
	}
	if err := snapshot.Validate(); err != nil {
		return Aggregation{}, err
	}
	brokers, err := validateInventory(inventory, catalog)
	if err != nil {
		return Aggregation{}, err
	}
	seenGroups := make(map[string]struct{}, len(groups))
	memberships := make(map[string][]string, len(brokers))
	for _, group := range groups {
		if _, duplicate := seenGroups[group.ID]; duplicate {
			return Aggregation{}, fmt.Errorf("policy: duplicate group %q", group.ID)
		}
		seenGroups[group.ID] = struct{}{}
		if err := group.validate(now, brokers); err != nil {
			return Aggregation{}, err
		}
		for _, broker := range group.Membership {
			memberships[broker] = append(memberships[broker], group.ID)
		}
	}
	for id := range memberships {
		sort.Strings(memberships[id])
	}

	context := aggregationContext{
		now: now, snapshot: snapshot, catalog: catalog, brokers: brokers,
		groups: slices.Clone(groups), memberships: memberships,
		brokerSamples: make(map[string]BrokerSample, len(snapshot.Brokers)),
		groupSamples:  make(map[groupBrokerKey]GroupSample, len(snapshot.Groups)),
	}
	for _, sample := range snapshot.Brokers {
		broker, ok := brokers[sample.BrokerID]
		if !ok {
			return Aggregation{}, fmt.Errorf("policy: telemetry references unknown broker %q", sample.BrokerID)
		}
		if sample.ServiceClass != broker.ServiceClass || sample.BrokerVersion != broker.BrokerVersion {
			return Aggregation{}, fmt.Errorf("policy: broker %q telemetry profile identity does not match inventory", sample.BrokerID)
		}
		context.brokerSamples[sample.BrokerID] = sample
	}
	for _, sample := range snapshot.Groups {
		if _, ok := seenGroups[sample.GroupID]; !ok {
			return Aggregation{}, fmt.Errorf("policy: telemetry references unknown group %q", sample.GroupID)
		}
		if _, ok := brokers[sample.BrokerID]; !ok {
			return Aggregation{}, fmt.Errorf("policy: telemetry references unknown broker %q", sample.BrokerID)
		}
		context.groupSamples[groupBrokerKey{sample.GroupID, sample.BrokerID}] = sample
	}

	brokerIDs := make([]string, 0, len(brokers))
	for id := range brokers {
		brokerIDs = append(brokerIDs, id)
	}
	sort.Strings(brokerIDs)
	brokerAssessments := make(map[string]BrokerAssessment, len(brokers))
	result := Aggregation{CapturedAt: snapshot.CapturedAt}
	for _, id := range brokerIDs {
		assessment := context.assessBroker(id)
		brokerAssessments[id] = assessment
		result.Brokers = append(result.Brokers, assessment)
	}

	sort.Slice(context.groups, func(i, j int) bool { return context.groups[i].ID < context.groups[j].ID })
	for _, group := range context.groups {
		result.Groups = append(result.Groups, context.assessGroup(group, brokerAssessments))
	}
	return result, nil
}

func (c aggregationContext) assessBroker(id string) BrokerAssessment {
	assessment := BrokerAssessment{BrokerID: id, State: TelemetryKnown, ContributingGroups: slices.Clone(c.memberships[id])}
	sample, ok := c.brokerSamples[id]
	if !ok {
		assessment.State = TelemetryUnknown
		assessment.Reason = "missing broker sample"
		return assessment
	}
	assessment.ObservedAt = sample.ObservedAt
	broker := c.brokers[id]
	profile, _ := c.catalog.Lookup(broker.ServiceClass, broker.BrokerVersion)

	var grouped resourcesValue
	var stale []string
	var unknown []string
	if c.now.Sub(sample.ObservedAt) > maxTelemetryAgeForBroker(id, c.groups) {
		stale = append(stale, "broker sample")
	}
	brokerValues, known := knownResources(sample.Resources)
	if !known {
		unknown = append(unknown, "broker resources")
	}
	for _, groupID := range c.memberships[id] {
		group := findGroup(c.groups, groupID)
		groupSample, exists := c.groupSamples[groupBrokerKey{groupID, id}]
		if !exists {
			unknown = append(unknown, "group "+groupID+" sample")
			continue
		}
		if groupSample.ObservedAt.Before(assessment.ObservedAt) {
			assessment.ObservedAt = groupSample.ObservedAt
		}
		if c.now.Sub(groupSample.ObservedAt) > group.Policy.TelemetryMaxAge {
			stale = append(stale, "group "+groupID+" sample")
		}
		values, allKnown := knownResources(groupSample.Resources)
		if !allKnown {
			unknown = append(unknown, "group "+groupID+" resources")
			continue
		}
		grouped = grouped.add(values)
	}
	if len(stale) != 0 {
		assessment.State = TelemetryStale
		assessment.Reason = strings.Join(stale, ", ")
		return assessment
	}
	if len(unknown) != 0 {
		assessment.State = TelemetryUnknown
		assessment.Reason = strings.Join(unknown, ", ")
		return assessment
	}
	assessment.BrokerUtilization = utilization(brokerValues, profile.Limits)
	assessment.GroupedUtilization = utilization(grouped, profile.Limits)
	assessment.SharedUtilization = assessment.BrokerUtilization.max(assessment.GroupedUtilization)
	return assessment
}

func maxTelemetryAgeForBroker(id string, groups []GroupState) time.Duration {
	var age time.Duration
	for _, group := range groups {
		if slices.Contains(group.Membership, id) && (age == 0 || group.Policy.TelemetryMaxAge < age) {
			age = group.Policy.TelemetryMaxAge
		}
	}
	// Unassigned warm brokers are not used in a decision and have no group policy
	// freshness contract. Their samples, when supplied, are informational.
	if age == 0 {
		return time.Duration(math.MaxInt64)
	}
	return age
}

func findGroup(groups []GroupState, id string) GroupState {
	for _, group := range groups {
		if group.ID == id {
			return group
		}
	}
	panic("validated group missing")
}

func (c aggregationContext) assessGroup(group GroupState, brokerAssessments map[string]BrokerAssessment) GroupAssessment {
	assessment := GroupAssessment{GroupID: group.ID, State: TelemetryKnown, Pressure: PressureNone}
	var demand, capacity resourcesValue
	var stale, unknown []string
	if len(group.Membership) == 0 {
		unknown = append(unknown, "group membership is empty")
	}
	for _, brokerID := range group.Membership {
		brokerAssessment := brokerAssessments[brokerID]
		switch brokerAssessment.State {
		case TelemetryStale:
			stale = append(stale, "broker "+brokerID+": "+brokerAssessment.Reason)
		case TelemetryUnknown:
			unknown = append(unknown, "broker "+brokerID+": "+brokerAssessment.Reason)
		}
		assessment.SharedUtilization = assessment.SharedUtilization.max(brokerAssessment.SharedUtilization)
		broker := c.brokers[brokerID]
		profile, _ := c.catalog.Lookup(broker.ServiceClass, broker.BrokerVersion)
		capacity = capacity.add(limitsValue(profile.Limits))

		sample, ok := c.groupSamples[groupBrokerKey{group.ID, brokerID}]
		if !ok {
			unknown = append(unknown, "missing group sample for broker "+brokerID)
			continue
		}
		if c.now.Sub(sample.ObservedAt) > group.Policy.TelemetryMaxAge {
			stale = append(stale, "stale group sample for broker "+brokerID)
		}
		values, resourcesKnown := knownResources(sample.Resources)
		if !resourcesKnown {
			unknown = append(unknown, "unknown group resources for broker "+brokerID)
		} else {
			demand = demand.add(values)
		}
		if !sample.Backlog.allKnown() {
			unknown = append(unknown, "unknown backlog for broker "+brokerID)
		} else {
			assessment.QueuedMessages += sample.Backlog.QueuedMessages.Value
			assessment.UnackedMessages += sample.Backlog.UnackedMessages.Value
		}
	}
	for _, broker := range c.brokers {
		if !slices.Contains(group.Membership, broker.ID) && broker.eligibleFor(group.ID) {
			assessment.ReadyWarmBrokers++
		}
	}
	assessment.WarmBrokerShortfall = max(0, group.Policy.WarmBrokers-assessment.ReadyWarmBrokers)
	if len(stale) != 0 {
		assessment.State = TelemetryStale
		assessment.Pressure = PressureUnknown
		assessment.Reason = strings.Join(stale, "; ")
		return assessment
	}
	if len(unknown) != 0 {
		assessment.State = TelemetryUnknown
		assessment.Pressure = PressureUnknown
		assessment.Reason = strings.Join(unknown, "; ")
		return assessment
	}
	assessment.WorkloadUtilization = utilizationAcross(demand, capacity)
	if assessment.SharedUtilization.Peak() >= group.Policy.saturationThreshold() {
		assessment.Pressure = PressureBrokerSaturation
	} else if assessment.QueuedMessages > 0 || assessment.UnackedMessages > 0 {
		assessment.Pressure = PressureDownstreamBacklog
	}
	return assessment
}
