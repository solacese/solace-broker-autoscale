// Package policy provides transport-independent telemetry aggregation and
// capacity-policy decisions. A SEMP adapter can populate the sample types in
// this package; the policy itself performs no I/O.
package policy

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"
)

var (
	// ErrProfileNotFound means that no capacity measurement exists for the exact
	// service-class and broker-version pair. Profiles are never inherited or
	// borrowed from another version.
	ErrProfileNotFound = errors.New("policy: exact capacity profile not found")
	// ErrUnknownTelemetry identifies a decision that cannot safely be made
	// because a required measurement is absent or explicitly unknown.
	ErrUnknownTelemetry = errors.New("policy: telemetry is unknown")
	// ErrStaleTelemetry identifies a decision that cannot safely be made because
	// its newest required measurement is too old.
	ErrStaleTelemetry = errors.New("policy: telemetry is stale")
)

// Metric is an explicitly known or unknown non-negative measurement. Unknown
// is not a synonym for zero: callers must set Known for measured zeroes.
type Metric struct {
	Known bool    `json:"known"`
	Value float64 `json:"value"`
}

// KnownMetric constructs a measured value. Validation rejects negative, NaN,
// and infinite values.
func KnownMetric(value float64) Metric { return Metric{Known: true, Value: value} }

// UnknownMetric constructs an unavailable measurement.
func UnknownMetric() Metric { return Metric{} }

func (m Metric) validate(name string) error {
	if !m.Known {
		if m.Value != 0 {
			return fmt.Errorf("policy: unknown %s must not carry a value", name)
		}
		return nil
	}
	if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) || m.Value < 0 {
		return fmt.Errorf("policy: %s must be a finite non-negative value", name)
	}
	return nil
}

// Resources contains the broker resource dimensions used by capacity policy.
// Rates are measured over the collector's sampling interval.
type Resources struct {
	IngressBytesPerSecond Metric `json:"ingress_bytes_per_second"`
	EgressBytesPerSecond  Metric `json:"egress_bytes_per_second"`
	SpoolBytes            Metric `json:"spool_bytes"`
	Connections           Metric `json:"connections"`
}

func (r Resources) validate(prefix string) error {
	checks := []struct {
		name string
		m    Metric
	}{
		{"ingress bytes per second", r.IngressBytesPerSecond},
		{"egress bytes per second", r.EgressBytesPerSecond},
		{"spool bytes", r.SpoolBytes},
		{"connections", r.Connections},
	}
	for _, check := range checks {
		if err := check.m.validate(prefix + " " + check.name); err != nil {
			return err
		}
	}
	return nil
}

func (r Resources) allKnown() bool {
	return r.IngressBytesPerSecond.Known && r.EgressBytesPerSecond.Known &&
		r.SpoolBytes.Known && r.Connections.Known
}

// Backlog contains group-specific delivery state. A measured backlog is kept
// separate from broker resource use so slow application consumption is not
// mislabeled as broker saturation.
type Backlog struct {
	QueuedMessages  Metric `json:"queued_messages"`
	UnackedMessages Metric `json:"unacked_messages"`
}

func (b Backlog) validate(prefix string) error {
	if err := b.QueuedMessages.validate(prefix + " queued messages"); err != nil {
		return err
	}
	return b.UnackedMessages.validate(prefix + " unacknowledged messages")
}

func (b Backlog) allKnown() bool {
	return b.QueuedMessages.Known && b.UnackedMessages.Known
}

// BrokerSample is a point-in-time SEMP-derived sample for one logical broker
// service. An HA service is represented once, not once per node.
type BrokerSample struct {
	BrokerID      string    `json:"broker_id"`
	ServiceClass  string    `json:"service_class"`
	BrokerVersion string    `json:"broker_version"`
	ObservedAt    time.Time `json:"observed_at"`
	Resources     Resources `json:"resources"`
}

// GroupSample is a point-in-time SEMP-derived sample for one scaling group's
// managed destinations on one broker.
type GroupSample struct {
	GroupID    string    `json:"group_id"`
	BrokerID   string    `json:"broker_id"`
	ObservedAt time.Time `json:"observed_at"`
	Resources  Resources `json:"resources"`
	Backlog    Backlog   `json:"backlog"`
}

// TelemetrySnapshot is one collector cycle. Missing expected samples remain
// missing and therefore unknown; callers must not synthesize zero-valued rows.
type TelemetrySnapshot struct {
	CapturedAt time.Time      `json:"captured_at"`
	Brokers    []BrokerSample `json:"brokers"`
	Groups     []GroupSample  `json:"groups"`
}

// Validate checks sample shape without interpreting freshness.
func (s TelemetrySnapshot) Validate() error {
	if s.CapturedAt.IsZero() {
		return errors.New("policy: telemetry capture time is required")
	}
	seenBrokers := make(map[string]struct{}, len(s.Brokers))
	for i, sample := range s.Brokers {
		if sample.BrokerID == "" || sample.ServiceClass == "" || sample.BrokerVersion == "" {
			return fmt.Errorf("policy: broker sample %d requires broker, service class, and version", i)
		}
		if sample.ObservedAt.IsZero() || sample.ObservedAt.After(s.CapturedAt) {
			return fmt.Errorf("policy: broker sample %q has an invalid observation time", sample.BrokerID)
		}
		if _, duplicate := seenBrokers[sample.BrokerID]; duplicate {
			return fmt.Errorf("policy: duplicate broker sample %q", sample.BrokerID)
		}
		seenBrokers[sample.BrokerID] = struct{}{}
		if err := sample.Resources.validate("broker " + sample.BrokerID); err != nil {
			return err
		}
	}
	seenGroups := make(map[groupBrokerKey]struct{}, len(s.Groups))
	for i, sample := range s.Groups {
		if sample.GroupID == "" || sample.BrokerID == "" {
			return fmt.Errorf("policy: group sample %d requires group and broker", i)
		}
		if sample.ObservedAt.IsZero() || sample.ObservedAt.After(s.CapturedAt) {
			return fmt.Errorf("policy: group sample %q/%q has an invalid observation time", sample.GroupID, sample.BrokerID)
		}
		key := groupBrokerKey{group: sample.GroupID, broker: sample.BrokerID}
		if _, duplicate := seenGroups[key]; duplicate {
			return fmt.Errorf("policy: duplicate group sample %q/%q", sample.GroupID, sample.BrokerID)
		}
		seenGroups[key] = struct{}{}
		if err := sample.Resources.validate("group " + sample.GroupID + " broker " + sample.BrokerID); err != nil {
			return err
		}
		if err := sample.Backlog.validate("group " + sample.GroupID + " broker " + sample.BrokerID); err != nil {
			return err
		}
	}
	return nil
}

// CapacityLimits is a measured capacity envelope for one exact broker build.
type CapacityLimits struct {
	IngressBytesPerSecond float64 `json:"ingress_bytes_per_second"`
	EgressBytesPerSecond  float64 `json:"egress_bytes_per_second"`
	SpoolBytes            float64 `json:"spool_bytes"`
	Connections           float64 `json:"connections"`
}

func (l CapacityLimits) validate() error {
	values := []float64{l.IngressBytesPerSecond, l.EgressBytesPerSecond, l.SpoolBytes, l.Connections}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
			return errors.New("policy: all capacity limits must be finite and greater than zero")
		}
	}
	return nil
}

// CapacityProfile is valid only for an exact service-class and broker-version
// pair. There is deliberately no wildcard or nearest-version lookup.
type CapacityProfile struct {
	ServiceClass  string         `json:"service_class"`
	BrokerVersion string         `json:"broker_version"`
	Limits        CapacityLimits `json:"limits"`
}

type profileKey struct {
	serviceClass string
	version      string
}

// ProfileCatalog stores validated exact-match capacity profiles.
type ProfileCatalog struct {
	profiles map[profileKey]CapacityProfile
}

// NewProfileCatalog rejects duplicate or incomplete profiles.
func NewProfileCatalog(profiles []CapacityProfile) (ProfileCatalog, error) {
	catalog := ProfileCatalog{profiles: make(map[profileKey]CapacityProfile, len(profiles))}
	for i, profile := range profiles {
		if profile.ServiceClass == "" || profile.BrokerVersion == "" {
			return ProfileCatalog{}, fmt.Errorf("policy: capacity profile %d requires service class and broker version", i)
		}
		if err := profile.Limits.validate(); err != nil {
			return ProfileCatalog{}, fmt.Errorf("policy: capacity profile %q/%q: %w", profile.ServiceClass, profile.BrokerVersion, err)
		}
		key := profileKey{profile.ServiceClass, profile.BrokerVersion}
		if _, duplicate := catalog.profiles[key]; duplicate {
			return ProfileCatalog{}, fmt.Errorf("policy: duplicate capacity profile %q/%q", profile.ServiceClass, profile.BrokerVersion)
		}
		catalog.profiles[key] = profile
	}
	return catalog, nil
}

// Lookup performs an exact service-class and broker-version match.
func (c ProfileCatalog) Lookup(serviceClass, brokerVersion string) (CapacityProfile, error) {
	profile, ok := c.profiles[profileKey{serviceClass, brokerVersion}]
	if !ok {
		return CapacityProfile{}, fmt.Errorf("%w for service class %q broker version %q", ErrProfileNotFound, serviceClass, brokerVersion)
	}
	return profile, nil
}

// Broker describes an existing data service available to policy. EligibleGroups
// is an explicit feature-placement allowlist. An empty allowlist makes a broker
// ineligible for scale-out, while current memberships remain observable.
type Broker struct {
	ID             string   `json:"id"`
	ServiceClass   string   `json:"service_class"`
	BrokerVersion  string   `json:"broker_version"`
	Ready          bool     `json:"ready"`
	EligibleGroups []string `json:"eligible_groups"`
}

func (b Broker) eligibleFor(group string) bool {
	return b.Ready && slices.Contains(b.EligibleGroups, group)
}

// GroupPolicy controls one independently transitioning scaling group.
// WarmBrokers is active reserve above MinimumBrokers, so the effective floor is
// MinimumBrokers+WarmBrokers and must not exceed MaximumBrokers.
type GroupPolicy struct {
	MinimumBrokers       int           `json:"minimum_brokers"`
	MaximumBrokers       int           `json:"maximum_brokers"`
	WarmBrokers          int           `json:"warm_brokers"`
	HeadroomPercent      float64       `json:"headroom_percent"`
	PressureWindow       time.Duration `json:"pressure_window"`
	TelemetryMaxAge      time.Duration `json:"telemetry_max_age"`
	Cooldown             time.Duration `json:"cooldown"`
	MaxConcurrentChanges int           `json:"max_concurrent_changes"`
}

func (p GroupPolicy) validate(group string) error {
	if p.MinimumBrokers < 1 || p.MaximumBrokers < p.MinimumBrokers || p.WarmBrokers < 0 || p.MinimumBrokers+p.WarmBrokers > p.MaximumBrokers {
		return fmt.Errorf("policy: group %q has invalid minimum, maximum, or warm broker counts", group)
	}
	if math.IsNaN(p.HeadroomPercent) || math.IsInf(p.HeadroomPercent, 0) || p.HeadroomPercent < 0 || p.HeadroomPercent >= 100 {
		return fmt.Errorf("policy: group %q headroom must be in [0,100)", group)
	}
	if p.PressureWindow <= 0 || p.TelemetryMaxAge <= 0 || p.Cooldown < 0 || p.MaxConcurrentChanges < 1 {
		return fmt.Errorf("policy: group %q has invalid pressure window, freshness, cooldown, or concurrency", group)
	}
	return nil
}

func (p GroupPolicy) floor() int { return p.MinimumBrokers + p.WarmBrokers }
func (p GroupPolicy) saturationThreshold() float64 {
	return 1 - p.HeadroomPercent/100
}

// ScaleInChoice is an external, explicit proof-bearing choice of the broker to
// remove. Policy never invents a scale-in victim. Safe must reflect destination
// drain/ownership checks performed by the runtime; Evidence is retained in the
// recommendation for auditability.
type ScaleInChoice struct {
	BrokerID string `json:"broker_id"`
	Safe     bool   `json:"safe"`
	Evidence string `json:"evidence"`
}

// GroupState is the current ordered membership and decision state for one
// scaling group.
type GroupState struct {
	ID                string         `json:"id"`
	Membership        []string       `json:"membership"`
	Policy            GroupPolicy    `json:"policy"`
	ActiveTransitions int            `json:"active_transitions"`
	LastTransitionAt  time.Time      `json:"last_transition_at,omitempty"`
	ScaleInChoice     *ScaleInChoice `json:"scale_in_choice,omitempty"`
}

func (g GroupState) validate(now time.Time, brokers map[string]Broker) error {
	if g.ID == "" {
		return errors.New("policy: group ID is required")
	}
	if err := g.Policy.validate(g.ID); err != nil {
		return err
	}
	if g.ActiveTransitions < 0 {
		return fmt.Errorf("policy: group %q has a negative active transition count", g.ID)
	}
	if !g.LastTransitionAt.IsZero() && g.LastTransitionAt.After(now) {
		return fmt.Errorf("policy: group %q last transition is in the future", g.ID)
	}
	if len(g.Membership) > g.Policy.MaximumBrokers {
		return fmt.Errorf("policy: group %q membership exceeds maximum broker count", g.ID)
	}
	seen := make(map[string]struct{}, len(g.Membership))
	for _, id := range g.Membership {
		if _, ok := brokers[id]; !ok {
			return fmt.Errorf("policy: group %q references unknown broker %q", g.ID, id)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("policy: group %q repeats broker %q", g.ID, id)
		}
		seen[id] = struct{}{}
	}
	if g.ScaleInChoice != nil {
		choice := g.ScaleInChoice
		if choice.BrokerID == "" || !slices.Contains(g.Membership, choice.BrokerID) {
			return fmt.Errorf("policy: group %q scale-in choice must name a current broker", g.ID)
		}
		if choice.Safe && choice.Evidence == "" {
			return fmt.Errorf("policy: group %q safe scale-in choice requires evidence", g.ID)
		}
	}
	return nil
}

func validateInventory(inventory []Broker, catalog ProfileCatalog) (map[string]Broker, error) {
	brokers := make(map[string]Broker, len(inventory))
	for i, broker := range inventory {
		if broker.ID == "" || broker.ServiceClass == "" || broker.BrokerVersion == "" {
			return nil, fmt.Errorf("policy: broker %d requires ID, service class, and version", i)
		}
		if _, duplicate := brokers[broker.ID]; duplicate {
			return nil, fmt.Errorf("policy: duplicate broker %q", broker.ID)
		}
		if _, err := catalog.Lookup(broker.ServiceClass, broker.BrokerVersion); err != nil {
			return nil, err
		}
		groups := slices.Clone(broker.EligibleGroups)
		sort.Strings(groups)
		if duplicate := adjacentDuplicate(groups); duplicate != "" {
			return nil, fmt.Errorf("policy: broker %q repeats eligible group %q", broker.ID, duplicate)
		}
		broker.EligibleGroups = groups
		brokers[broker.ID] = broker
	}
	return brokers, nil
}

func adjacentDuplicate(values []string) string {
	for i := 1; i < len(values); i++ {
		if values[i] == values[i-1] {
			return values[i]
		}
	}
	return ""
}

type groupBrokerKey struct {
	group  string
	broker string
}
