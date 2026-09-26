// Package config loads the workload balancer's non-secret configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/solacese/solace-workload-balancer/policy"
	"gopkg.in/yaml.v3"
)

const (
	RuntimeModeDevelopment = "development"
	RuntimeModeProduction  = "production"

	QueueTypeExclusive   = "exclusive"
	QueueTypePartitioned = "partitioned"

	QueueAccessExclusive    = "exclusive"
	QueueAccessNonExclusive = "non-exclusive"
)

var (
	environmentVariablePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	identifierPattern          = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	managedNamePattern         = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)
	namespaceLabelPattern      = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
)

// Config is the complete controller and shim configuration. Credentials are
// named by environment variable and are never embedded in this document.
type Config struct {
	Namespace        string            `yaml:"namespace,omitempty"`
	Control          ControlBroker     `yaml:"control"`
	DataBrokers      []DataBroker      `yaml:"data_brokers"`
	CapacityProfiles []CapacityProfile `yaml:"capacity_profiles,omitempty"`
	Groups           []ScalingGroup    `yaml:"scaling_groups"`
	Runtime          RuntimeIdentities `yaml:"runtime,omitempty"`
	Publisher        AsyncPublisher    `yaml:"publisher,omitempty"`
	Staleness        StalenessPolicy   `yaml:"staleness,omitempty"`
	Persistence      Persistence       `yaml:"persistence"`
	Cloud            CloudProvisioning `yaml:"cloud"`
}

type ControlBroker struct {
	BrokerID               string                         `yaml:"broker_id"`
	SMFEndpoint            string                         `yaml:"smf_endpoint"`
	SEMPEndpoint           string                         `yaml:"semp_endpoint,omitempty"`
	MessageVPN             string                         `yaml:"message_vpn"`
	Principal              string                         `yaml:"principal,omitempty"`
	UsernameEnv            string                         `yaml:"username_env"`
	PasswordEnv            string                         `yaml:"password_env"`
	SEMPUsernameEnv        string                         `yaml:"semp_username_env,omitempty"`
	SEMPPasswordEnv        string                         `yaml:"semp_password_env,omitempty"`
	ParticipantCredentials []ParticipantControlCredential `yaml:"participant_credentials,omitempty"`
	MembershipLVQPrefix    string                         `yaml:"membership_lvq_prefix"`
	ControlTopicPrefix     string                         `yaml:"control_topic_prefix"`
	Resources              Broker0Resources               `yaml:"resources,omitempty"`
}

// ParticipantControlCredential binds one configured participant identity to a
// distinct Broker 0 principal. Only environment-variable names are persisted;
// the username and password values are resolved inside that participant process.
type ParticipantControlCredential struct {
	Participant string `yaml:"participant"`
	Principal   string `yaml:"principal"`
	UsernameEnv string `yaml:"username_env"`
	PasswordEnv string `yaml:"password_env"`
}

// Broker0Resources names the durable queues and topic namespaces used by the
// control plane. These resources must be provisioned as durable endpoints; the
// configuration intentionally contains no broker credentials or ACL secrets.
type Broker0Resources struct {
	MembershipTopicPrefix string `yaml:"membership_topic_prefix,omitempty"`
	MembershipQueuePrefix string `yaml:"membership_queue_prefix,omitempty"`
	CommandTopicPrefix    string `yaml:"command_topic_prefix,omitempty"`
	CommandQueuePrefix    string `yaml:"command_queue_prefix,omitempty"`
	RegistrationTopic     string `yaml:"registration_topic,omitempty"`
	RegistrationQueue     string `yaml:"registration_queue,omitempty"`
	ReadinessTopic        string `yaml:"readiness_topic,omitempty"`
	ReadinessQueue        string `yaml:"readiness_queue,omitempty"`
	TelemetryTopic        string `yaml:"telemetry_topic,omitempty"`
	TelemetryQueue        string `yaml:"telemetry_queue,omitempty"`
}

type DataBroker struct {
	ID              string            `yaml:"id"`
	SMFEndpoint     string            `yaml:"smf_endpoint"`
	SEMPEndpoint    string            `yaml:"semp_endpoint,omitempty"`
	MessageVPN      string            `yaml:"message_vpn"`
	UsernameEnv     string            `yaml:"username_env"`
	PasswordEnv     string            `yaml:"password_env"`
	SEMPUsernameEnv string            `yaml:"semp_username_env,omitempty"`
	SEMPPasswordEnv string            `yaml:"semp_password_env,omitempty"`
	ServiceClass    string            `yaml:"service_class,omitempty"`
	BrokerVersion   string            `yaml:"broker_version,omitempty"`
	EligibleGroups  []string          `yaml:"eligible_groups,omitempty"`
	Labels          map[string]string `yaml:"labels,omitempty"`
}

// CapacityProfile is an operator-supplied measured capacity envelope for one
// exact service-class and broker-version pair. Throughput is bytes/second,
// spool is bytes, and connections is a count. Values are measurements, not
// vendor-advertised limits, and are never inherited by another broker version.
type CapacityProfile struct {
	ServiceClass  string         `yaml:"service_class"`
	BrokerVersion string         `yaml:"broker_version"`
	Limits        CapacityLimits `yaml:"limits"`
}

type CapacityLimits struct {
	IngressBytesPerSecond float64 `yaml:"ingress_bytes_per_second"`
	EgressBytesPerSecond  float64 `yaml:"egress_bytes_per_second"`
	SpoolBytes            float64 `yaml:"spool_bytes"`
	Connections           float64 `yaml:"connections"`
}

// PolicyCapacityProfiles converts configuration without weakening policy's
// exact-match and numeric validation. Callers should build a ProfileCatalog with
// policy.NewProfileCatalog before starting capacity automation.
func (c Config) PolicyCapacityProfiles() []policy.CapacityProfile {
	profiles := make([]policy.CapacityProfile, len(c.CapacityProfiles))
	for index, profile := range c.CapacityProfiles {
		profiles[index] = policy.CapacityProfile{
			ServiceClass:  profile.ServiceClass,
			BrokerVersion: profile.BrokerVersion,
			Limits: policy.CapacityLimits{
				IngressBytesPerSecond: profile.Limits.IngressBytesPerSecond,
				EgressBytesPerSecond:  profile.Limits.EgressBytesPerSecond,
				SpoolBytes:            profile.Limits.SpoolBytes,
				Connections:           profile.Limits.Connections,
			},
		}
	}
	return profiles
}

// PolicyProfileCatalog returns the validated exact-match capacity catalog.
func (c Config) PolicyProfileCatalog() (policy.ProfileCatalog, error) {
	return policy.NewProfileCatalog(c.PolicyCapacityProfiles())
}

// PolicyBrokers converts configured data services into the policy inventory.
// Readiness is deliberately false because configuration cannot prove current
// broker health; runtime observations must set it before scale-out is eligible.
func (c Config) PolicyBrokers() []policy.Broker {
	brokers := make([]policy.Broker, len(c.DataBrokers))
	for index, broker := range c.DataBrokers {
		brokers[index] = policy.Broker{
			ID:             broker.ID,
			ServiceClass:   broker.ServiceClass,
			BrokerVersion:  broker.BrokerVersion,
			EligibleGroups: slices.Clone(broker.EligibleGroups),
		}
	}
	return brokers
}

const LegacyConsumerSetID = "default"

type ScalingGroup struct {
	ID                 string         `yaml:"id"`
	HashContract       string         `yaml:"hash_contract"`
	CustomerLibrary    string         `yaml:"customer_library_version"`
	OrderedBrokerIDs   []string       `yaml:"ordered_broker_ids"`
	Queue              Queue          `yaml:"queue"`
	Policy             ScalingPolicy  `yaml:"policy"`
	Handover           HandoverPolicy `yaml:"handover"`
	RequiredPublishers []string       `yaml:"required_publishers"`
	ConsumerSets       []ConsumerSet  `yaml:"consumer_sets,omitempty"`
	// RequiredSubscribers is the legacy single-consumer-set form. It is mutually
	// exclusive with ConsumerSets and maps unambiguously to the "default" set.
	RequiredSubscribers []string `yaml:"required_subscribers,omitempty"`
}

// ConsumerSet is one independent pub/sub subscription. Every broker/epoch gets
// one queue for each set; subscribers within a set attach to that shared queue
// and therefore compete, while different sets receive independent copies.
type ConsumerSet struct {
	ID          string   `yaml:"id"`
	Subscribers []string `yaml:"subscribers"`
}

// EffectiveConsumerSets returns explicit sets, or the unambiguous legacy
// single-set projection when required_subscribers is used.
func (g ScalingGroup) EffectiveConsumerSets() []ConsumerSet {
	if len(g.ConsumerSets) != 0 {
		result := make([]ConsumerSet, len(g.ConsumerSets))
		for index, set := range g.ConsumerSets {
			result[index] = ConsumerSet{ID: set.ID, Subscribers: slices.Clone(set.Subscribers)}
		}
		return result
	}
	if len(g.RequiredSubscribers) == 0 {
		return nil
	}
	return []ConsumerSet{{ID: LegacyConsumerSetID, Subscribers: slices.Clone(g.RequiredSubscribers)}}
}

func (g ScalingGroup) SubscriberIdentities() []string {
	var result []string
	for _, set := range g.EffectiveConsumerSets() {
		result = append(result, set.Subscribers...)
	}
	return result
}

func (g ScalingGroup) ConsumerSetForSubscriber(participant string) (string, bool) {
	for _, set := range g.EffectiveConsumerSets() {
		if slices.Contains(set.Subscribers, participant) {
			return set.ID, true
		}
	}
	return "", false
}

type Queue struct {
	NamePrefix       string `yaml:"name_prefix"`
	Type             string `yaml:"type"`
	Access           string `yaml:"access"`
	Partitions       int    `yaml:"partitions,omitempty"`
	MaxRedeliveries  int    `yaml:"max_redeliveries"`
	DeadMessageQueue string `yaml:"dead_message_queue"`
}

type ScalingPolicy struct {
	MinimumBrokers  int      `yaml:"minimum_brokers"`
	MaximumBrokers  int      `yaml:"maximum_brokers"`
	WarmBrokers     int      `yaml:"warm_brokers"`
	HeadroomPercent float64  `yaml:"headroom_percent"`
	PressureWindow  Duration `yaml:"pressure_window"`
	Cooldown        Duration `yaml:"cooldown"`
	// MaxConcurrentChanges must be 1. Independent groups may transition
	// concurrently, bounded by the number of configured groups.
	MaxConcurrentChanges int `yaml:"max_concurrent_changes"`
}

type HandoverPolicy struct {
	ReadinessTimeout  Duration `yaml:"readiness_timeout"`
	TelemetryMaxAge   Duration `yaml:"telemetry_max_age"`
	DrainGrace        Duration `yaml:"drain_grace"`
	TransitionTimeout Duration `yaml:"transition_timeout"`
}

// RuntimeIdentities declares every process identity that is allowed to take
// part in a handover. Production mode requires this inventory and verifies that
// every group participant is declared exactly once.
type RuntimeIdentities struct {
	Mode        string   `yaml:"mode,omitempty"`
	Controller  string   `yaml:"controller,omitempty"`
	Publishers  []string `yaml:"publishers,omitempty"`
	Subscribers []string `yaml:"subscribers,omitempty"`
	Brokers     []string `yaml:"brokers,omitempty"`
	Observers   []string `yaml:"observers,omitempty"`
}

// ParticipantControlCredential returns the non-secret Broker 0 principal and
// credential environment names configured for one participant.
func (c Config) ParticipantControlCredential(participant string) (ParticipantControlCredential, bool) {
	for _, credential := range c.Control.ParticipantCredentials {
		if credential.Participant == participant {
			return credential, true
		}
	}
	return ParticipantControlCredential{}, false
}

// AsyncPublisher bounds concurrent broker work and shutdown latency. BatchWait
// is the maximum time a partially filled dispatch batch may wait.
type AsyncPublisher struct {
	MaxConcurrency  int      `yaml:"max_concurrency,omitempty"`
	BatchSize       int      `yaml:"batch_size,omitempty"`
	BatchWait       Duration `yaml:"batch_wait,omitempty"`
	PublishTimeout  Duration `yaml:"publish_timeout,omitempty"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout,omitempty"`
}

// StalenessPolicy bounds how long cached control-plane observations remain
// usable. Missing or older observations must be treated as unknown.
type StalenessPolicy struct {
	MembershipMaxAge  Duration `yaml:"membership_max_age,omitempty"`
	ParticipantMaxAge Duration `yaml:"participant_max_age,omitempty"`
	TelemetryMaxAge   Duration `yaml:"telemetry_max_age,omitempty"`
}

type Persistence struct {
	ControllerState   string `yaml:"controller_state"`
	PublisherOutbox   string `yaml:"publisher_outbox"`
	OutboxMaxMessages int    `yaml:"outbox_max_messages"`
	OutboxMaxBytes    int64  `yaml:"outbox_max_bytes"`
}

// CloudProvisioning contains only placement, cost, and lifecycle controls.
// CredentialEnv names an environment variable; its secret value never belongs
// in this file. DeleteAfterTest applies only to resources whose exact provider
// identities were recorded as created by this deployment.
type CloudProvisioning struct {
	Enabled             bool     `yaml:"enabled"`
	Provider            string   `yaml:"provider,omitempty"`
	APIBaseURL          string   `yaml:"api_base_url,omitempty"`
	CredentialEnv       string   `yaml:"credential_env,omitempty"`
	Region              string   `yaml:"region,omitempty"`
	DatacenterID        string   `yaml:"datacenter_id,omitempty"`
	BrokerVersion       string   `yaml:"broker_version,omitempty"`
	ControlServiceClass string   `yaml:"control_service_class,omitempty"`
	DataServiceClass    string   `yaml:"data_service_class,omitempty"`
	DesiredServices     int      `yaml:"desired_services,omitempty"`
	MaxPaidServices     int      `yaml:"max_paid_services"`
	MaxRuntime          Duration `yaml:"max_runtime,omitempty"`
	CleanupTimeout      Duration `yaml:"cleanup_timeout,omitempty"`
	DeleteAfterTest     bool     `yaml:"delete_after_test,omitempty"`
}

// Duration is a YAML duration such as "30s" or "5m".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}
	d.Duration = parsed
	return nil
}

func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func Load(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode configuration: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	strict, err := c.validateRuntime()
	if err != nil {
		return err
	}
	if err := validateNamespace(c.Namespace, strict); err != nil {
		return err
	}
	if err := c.validateControl(strict); err != nil {
		return err
	}
	if len(c.Groups) == 0 {
		return errors.New("at least one scaling group is required")
	}
	groupIDs := make(map[string]struct{}, len(c.Groups))
	for i, group := range c.Groups {
		if err := validateIdentifier(fmt.Sprintf("scaling_groups[%d].id", i), group.ID); err != nil {
			return err
		}
		if _, duplicate := groupIDs[group.ID]; duplicate {
			return fmt.Errorf("duplicate scaling group id %q", group.ID)
		}
		groupIDs[group.ID] = struct{}{}
	}

	catalog, err := c.validateCapacityProfiles(strict)
	if err != nil {
		return err
	}
	capacityConfigured := strict || len(c.CapacityProfiles) != 0
	for _, broker := range c.DataBrokers {
		capacityConfigured = capacityConfigured || broker.ServiceClass != "" || broker.BrokerVersion != "" || len(broker.EligibleGroups) != 0
	}

	brokerIDs := make(map[string]struct{}, len(c.DataBrokers))
	for i, broker := range c.DataBrokers {
		path := fmt.Sprintf("data_brokers[%d]", i)
		if err := validateIdentifier(path+".id", broker.ID); err != nil {
			return err
		}
		if strings.TrimSpace(broker.MessageVPN) == "" {
			return fmt.Errorf("%s: message VPN is required", path)
		}
		if err := validateEndpoint(path+".smf_endpoint", broker.SMFEndpoint, strict, "tcp", "tcps", "ws", "wss"); err != nil {
			return err
		}
		if err := validateCredentialPair(path, broker.UsernameEnv, broker.PasswordEnv); err != nil {
			return err
		}
		if err := validateSEMPCredentials(path, broker.SEMPEndpoint, broker.SEMPUsernameEnv, broker.SEMPPasswordEnv, strict); err != nil {
			return err
		}
		if capacityConfigured {
			if strings.TrimSpace(broker.ServiceClass) == "" || strings.TrimSpace(broker.BrokerVersion) == "" {
				return fmt.Errorf("%s: service_class and broker_version are required for capacity policy", path)
			}
			if broker.ServiceClass != strings.TrimSpace(broker.ServiceClass) || broker.BrokerVersion != strings.TrimSpace(broker.BrokerVersion) {
				return fmt.Errorf("%s: service_class and broker_version cannot contain surrounding whitespace", path)
			}
			if broker.EligibleGroups == nil {
				return fmt.Errorf("%s.eligible_groups is required; use [] to explicitly disable placement", path)
			}
			if _, lookupErr := catalog.Lookup(broker.ServiceClass, broker.BrokerVersion); lookupErr != nil {
				return fmt.Errorf("%s: %w", path, lookupErr)
			}
			seenEligible := make(map[string]struct{}, len(broker.EligibleGroups))
			for _, group := range broker.EligibleGroups {
				if _, exists := groupIDs[group]; !exists {
					return fmt.Errorf("%s.eligible_groups references unknown scaling group %q", path, group)
				}
				if _, duplicate := seenEligible[group]; duplicate {
					return fmt.Errorf("%s.eligible_groups repeats scaling group %q", path, group)
				}
				seenEligible[group] = struct{}{}
			}
		}
		if _, duplicate := brokerIDs[broker.ID]; duplicate {
			return fmt.Errorf("duplicate data broker id %q", broker.ID)
		}
		brokerIDs[broker.ID] = struct{}{}
		for key, value := range broker.Labels {
			if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
				return fmt.Errorf("%s.labels: keys and values cannot be empty", path)
			}
		}
	}

	for i, group := range c.Groups {
		if err := c.validateGroup(i, group, brokerIDs, strict); err != nil {
			return err
		}
	}

	if c.Persistence.ControllerState == "" || c.Persistence.PublisherOutbox == "" || c.Persistence.OutboxMaxMessages < 1 || c.Persistence.OutboxMaxBytes < 1 {
		return errors.New("persistence paths and positive outbox limits are required")
	}
	if err := c.validateOperationalControls(strict); err != nil {
		return err
	}
	return c.validateCloud()
}

func (c Config) validateCapacityProfiles(required bool) (policy.ProfileCatalog, error) {
	if required && len(c.CapacityProfiles) == 0 {
		return policy.ProfileCatalog{}, errors.New("capacity_profiles requires at least one measured exact service-class and broker-version profile")
	}
	for index, profile := range c.CapacityProfiles {
		if profile.ServiceClass != strings.TrimSpace(profile.ServiceClass) || profile.BrokerVersion != strings.TrimSpace(profile.BrokerVersion) {
			return policy.ProfileCatalog{}, fmt.Errorf("capacity_profiles[%d]: service_class and broker_version cannot contain surrounding whitespace", index)
		}
	}
	catalog, err := c.PolicyProfileCatalog()
	if err != nil {
		return policy.ProfileCatalog{}, fmt.Errorf("capacity_profiles: %w", err)
	}
	return catalog, nil
}

func findDataBroker(brokers []DataBroker, id string) (DataBroker, bool) {
	for _, broker := range brokers {
		if broker.ID == id {
			return broker, true
		}
	}
	return DataBroker{}, false
}

func (c Config) validateRuntime() (bool, error) {
	mode := c.Runtime.Mode
	if mode == "" {
		mode = RuntimeModeDevelopment
	}
	if mode != RuntimeModeDevelopment && mode != RuntimeModeProduction {
		return false, fmt.Errorf("runtime.mode must be %q or %q", RuntimeModeDevelopment, RuntimeModeProduction)
	}

	seen := make(map[string]string)
	add := func(kind, identity string) error {
		if err := validateIdentifier("runtime "+kind+" identity", identity); err != nil {
			return err
		}
		if prior, exists := seen[identity]; exists {
			return fmt.Errorf("runtime identity %q is declared as both %s and %s", identity, prior, kind)
		}
		seen[identity] = kind
		return nil
	}
	if c.Runtime.Controller != "" {
		if err := add("controller", c.Runtime.Controller); err != nil {
			return false, err
		}
	}
	for _, identity := range c.Runtime.Publishers {
		if err := add("publisher", identity); err != nil {
			return false, err
		}
	}
	for _, identity := range c.Runtime.Subscribers {
		if err := add("subscriber", identity); err != nil {
			return false, err
		}
	}
	for _, identity := range c.Runtime.Brokers {
		if err := add("broker", identity); err != nil {
			return false, err
		}
	}
	for _, identity := range c.Runtime.Observers {
		if err := add("observer", identity); err != nil {
			return false, err
		}
	}
	strict := mode == RuntimeModeProduction || c.Cloud.Enabled
	if strict && (c.Runtime.Controller == "" || len(c.Runtime.Publishers) == 0 || len(c.Runtime.Subscribers) == 0) {
		return false, errors.New("production or cloud-enabled runtime requires controller, publisher, and subscriber identities")
	}
	if strict && len(c.Runtime.Brokers)+len(c.Runtime.Observers) == 0 {
		return false, errors.New("production or cloud-enabled runtime requires at least one broker or observer telemetry identity")
	}
	return strict, nil
}

func (c Config) validateControl(strict bool) error {
	if err := validateIdentifier("control broker id", c.Control.BrokerID); err != nil {
		return err
	}
	if strings.TrimSpace(c.Control.MessageVPN) == "" {
		return errors.New("control broker message VPN is required")
	}
	if err := validateEndpoint("control.smf_endpoint", c.Control.SMFEndpoint, strict, "tcp", "tcps", "ws", "wss"); err != nil {
		return err
	}
	if err := validateCredentialPair("control", c.Control.UsernameEnv, c.Control.PasswordEnv); err != nil {
		return err
	}
	if c.Control.Principal != "" {
		if err := validateIdentifier("control.principal", c.Control.Principal); err != nil {
			return err
		}
	}
	if err := c.validateParticipantControlCredentials(strict); err != nil {
		return err
	}
	if err := validateSEMPCredentials("control", c.Control.SEMPEndpoint, c.Control.SEMPUsernameEnv, c.Control.SEMPPasswordEnv, strict); err != nil {
		return err
	}
	if err := validateManagedName("control.membership_lvq_prefix", c.Control.MembershipLVQPrefix, true); err != nil {
		return err
	}
	if err := validateManagedName("control.control_topic_prefix", c.Control.ControlTopicPrefix, true); err != nil {
		return err
	}
	if strict {
		if err := validateNamespaceOwnership(c.Namespace, "control.membership_lvq_prefix", c.Control.MembershipLVQPrefix); err != nil {
			return err
		}
		if err := validateNamespaceOwnership(c.Namespace, "control.control_topic_prefix", c.Control.ControlTopicPrefix); err != nil {
			return err
		}
	}
	return c.Control.Resources.validate(c.Namespace, strict)
}

func (c Config) validateParticipantControlCredentials(strict bool) error {
	configured := make(map[string]ParticipantControlCredential, len(c.Control.ParticipantCredentials))
	principals := make(map[string]string, len(c.Control.ParticipantCredentials))
	usernameEnvs := make(map[string]string, len(c.Control.ParticipantCredentials))
	passwordEnvs := make(map[string]string, len(c.Control.ParticipantCredentials))
	for index, credential := range c.Control.ParticipantCredentials {
		path := fmt.Sprintf("control.participant_credentials[%d]", index)
		if err := validateIdentifier(path+".participant", credential.Participant); err != nil {
			return err
		}
		if err := validateIdentifier(path+".principal", credential.Principal); err != nil {
			return err
		}
		if err := validateCredentialPair(path, credential.UsernameEnv, credential.PasswordEnv); err != nil {
			return err
		}
		if credential.Principal != credential.Participant {
			return fmt.Errorf("control principal %q must equal participant identity %q", credential.Principal, credential.Participant)
		}
		if _, duplicate := configured[credential.Participant]; duplicate {
			return fmt.Errorf("duplicate control credential for participant %q", credential.Participant)
		}
		configured[credential.Participant] = credential
		if strict && !slices.Contains(c.configuredParticipants(), credential.Participant) {
			return fmt.Errorf("control credential references undeclared participant %q", credential.Participant)
		}
		for value, label := range map[string]string{
			credential.Principal:   "principal",
			credential.UsernameEnv: "username environment variable",
			credential.PasswordEnv: "password environment variable",
		} {
			var owners map[string]string
			switch label {
			case "principal":
				owners = principals
			case "username environment variable":
				owners = usernameEnvs
			default:
				owners = passwordEnvs
			}
			if prior, reused := owners[value]; reused {
				return fmt.Errorf("control participant %s %q is shared by %q and %q", label, value, prior, credential.Participant)
			}
			owners[value] = credential.Participant
		}
		if strict && (credential.Principal == c.Control.Principal || credential.UsernameEnv == c.Control.UsernameEnv || credential.PasswordEnv == c.Control.PasswordEnv) {
			return fmt.Errorf("control credential for participant %q must not share the controller principal or credential environment variables", credential.Participant)
		}
	}
	if !strict {
		return nil
	}
	if c.Control.Principal == "" {
		return errors.New("production or cloud-enabled runtime requires control.principal")
	}
	if c.Control.Principal != c.Runtime.Controller {
		return errors.New("control.principal must equal the configured runtime controller identity")
	}
	for _, participant := range c.configuredParticipants() {
		if _, ok := configured[participant]; !ok {
			return fmt.Errorf("production or cloud-enabled runtime requires a distinct control credential for participant %q", participant)
		}
	}
	return nil
}

func (c Config) configuredParticipants() []string {
	participants := make([]string, 0, len(c.Runtime.Publishers)+len(c.Runtime.Subscribers)+len(c.Runtime.Brokers)+len(c.Runtime.Observers))
	participants = append(participants, c.Runtime.Publishers...)
	participants = append(participants, c.Runtime.Subscribers...)
	participants = append(participants, c.Runtime.Brokers...)
	participants = append(participants, c.Runtime.Observers...)
	return participants
}

func (r Broker0Resources) validate(namespace string, strict bool) error {
	values := []struct {
		name  string
		value string
	}{
		{"membership_topic_prefix", r.MembershipTopicPrefix},
		{"membership_queue_prefix", r.MembershipQueuePrefix},
		{"command_topic_prefix", r.CommandTopicPrefix},
		{"command_queue_prefix", r.CommandQueuePrefix},
		{"registration_topic", r.RegistrationTopic},
		{"registration_queue", r.RegistrationQueue},
		{"readiness_topic", r.ReadinessTopic},
		{"readiness_queue", r.ReadinessQueue},
		{"telemetry_topic", r.TelemetryTopic},
		{"telemetry_queue", r.TelemetryQueue},
	}
	for _, entry := range values {
		path := "control.resources." + entry.name
		if entry.value == "" && !strict {
			continue
		}
		if err := validateManagedName(path, entry.value, true); err != nil {
			return err
		}
		if strict {
			if err := validateNamespaceOwnership(namespace, path, entry.value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c Config) validateGroup(index int, group ScalingGroup, brokerIDs map[string]struct{}, strict bool) error {
	path := fmt.Sprintf("scaling_groups[%d]", index)
	if err := validateIdentifier(path+".id", group.ID); err != nil {
		return err
	}
	if strings.TrimSpace(group.HashContract) == "" || strings.TrimSpace(group.CustomerLibrary) == "" {
		return fmt.Errorf("%s: contract versions are required", path)
	}

	seenBrokers := make(map[string]struct{}, len(group.OrderedBrokerIDs))
	for _, id := range group.OrderedBrokerIDs {
		if _, exists := brokerIDs[id]; !exists {
			return fmt.Errorf("scaling group %q references unknown broker %q", group.ID, id)
		}
		if _, duplicate := seenBrokers[id]; duplicate {
			return fmt.Errorf("scaling group %q repeats broker %q", group.ID, id)
		}
		seenBrokers[id] = struct{}{}
	}

	policy := group.Policy
	if policy.MinimumBrokers < 1 || policy.MaximumBrokers < policy.MinimumBrokers {
		return fmt.Errorf("scaling group %q has invalid broker bounds", group.ID)
	}
	if len(group.OrderedBrokerIDs) < policy.MinimumBrokers || len(group.OrderedBrokerIDs) > policy.MaximumBrokers {
		return fmt.Errorf("scaling group %q initial membership is outside broker bounds", group.ID)
	}
	if policy.WarmBrokers < 0 || policy.WarmBrokers > policy.MaximumBrokers-policy.MinimumBrokers {
		return fmt.Errorf("scaling group %q warm_brokers must fit between minimum and maximum broker bounds", group.ID)
	}
	if math.IsNaN(policy.HeadroomPercent) || math.IsInf(policy.HeadroomPercent, 0) || policy.HeadroomPercent < 0 || policy.HeadroomPercent >= 100 {
		return fmt.Errorf("scaling group %q headroom_percent must be at least zero and less than 100", group.ID)
	}
	if policy.PressureWindow.Duration <= 0 || policy.Cooldown.Duration < 0 {
		return fmt.Errorf("scaling group %q pressure_window must be positive and cooldown cannot be negative", group.ID)
	}
	if policy.MaxConcurrentChanges != 1 {
		return fmt.Errorf("scaling group %q max_concurrent_changes must equal 1; the controller supports one active transition per group", group.ID)
	}

	if err := validateQueue(group.ID, group.Queue, c.Namespace, strict); err != nil {
		return err
	}
	if group.Handover.ReadinessTimeout.Duration <= 0 || group.Handover.TelemetryMaxAge.Duration <= 0 || group.Handover.DrainGrace.Duration <= 0 || group.Handover.TransitionTimeout.Duration <= 0 {
		return fmt.Errorf("scaling group %q handover timings must be positive", group.ID)
	}
	if len(group.ConsumerSets) != 0 && len(group.RequiredSubscribers) != 0 {
		return fmt.Errorf("scaling group %q cannot configure both consumer_sets and required_subscribers", group.ID)
	}
	consumerSets := group.EffectiveConsumerSets()
	if len(consumerSets) == 0 {
		return fmt.Errorf("scaling group %q requires at least one consumer set", group.ID)
	}
	participants := make(map[string]string, len(group.RequiredPublishers)+len(group.SubscriberIdentities()))
	validateParticipants := func(kind string, identities []string) error {
		for _, identity := range identities {
			if err := validateIdentifier("scaling group "+group.ID+" "+kind+" identity", identity); err != nil {
				return err
			}
			if prior, duplicate := participants[identity]; duplicate {
				return fmt.Errorf("scaling group %q participant %q is duplicated across %s and %s", group.ID, identity, prior, kind)
			}
			participants[identity] = kind
		}
		return nil
	}
	if err := validateParticipants("publishers", group.RequiredPublishers); err != nil {
		return err
	}
	seenConsumerSets := make(map[string]struct{}, len(consumerSets))
	for setIndex, set := range consumerSets {
		if err := validateIdentifier(fmt.Sprintf("%s.consumer_sets[%d].id", path, setIndex), set.ID); err != nil {
			return err
		}
		if _, duplicate := seenConsumerSets[set.ID]; duplicate {
			return fmt.Errorf("scaling group %q repeats consumer set %q", group.ID, set.ID)
		}
		seenConsumerSets[set.ID] = struct{}{}
		if len(set.Subscribers) == 0 {
			return fmt.Errorf("scaling group %q consumer set %q requires at least one subscriber", group.ID, set.ID)
		}
		if err := validateParticipants("consumer set "+set.ID+" subscribers", set.Subscribers); err != nil {
			return err
		}
	}
	if len(participants) == 0 {
		return fmt.Errorf("scaling group %q requires at least one participant", group.ID)
	}
	if strict {
		declaredPublishers := sliceSet(c.Runtime.Publishers)
		declaredSubscribers := sliceSet(c.Runtime.Subscribers)
		for _, identity := range group.RequiredPublishers {
			if _, ok := declaredPublishers[identity]; !ok {
				return fmt.Errorf("scaling group %q requires undeclared runtime publisher %q", group.ID, identity)
			}
		}
		for _, identity := range group.SubscriberIdentities() {
			if _, ok := declaredSubscribers[identity]; !ok {
				return fmt.Errorf("scaling group %q requires undeclared runtime subscriber %q", group.ID, identity)
			}
		}
	}
	return nil
}

func validateQueue(group string, queue Queue, namespace string, strict bool) error {
	if err := validateManagedName("scaling group "+group+" queue name_prefix", queue.NamePrefix, false); err != nil {
		return err
	}
	if strict {
		if err := validateNamespaceOwnership(namespace, "scaling group "+group+" queue name_prefix", queue.NamePrefix); err != nil {
			return err
		}
	}
	if queue.MaxRedeliveries < 1 {
		return fmt.Errorf("scaling group %q max_redeliveries must be positive", group)
	}
	if err := validateManagedName("scaling group "+group+" dead_message_queue", queue.DeadMessageQueue, false); err != nil {
		return err
	}
	if queue.DeadMessageQueue == queue.NamePrefix {
		return fmt.Errorf("scaling group %q dead_message_queue must differ from name_prefix", group)
	}
	if strict {
		if err := validateNamespaceOwnership(namespace, "scaling group "+group+" dead_message_queue", queue.DeadMessageQueue); err != nil {
			return err
		}
	}

	switch queue.Type {
	case QueueTypeExclusive:
		if queue.Access != QueueAccessExclusive {
			return fmt.Errorf("scaling group %q exclusive queue requires exclusive access", group)
		}
		if queue.Partitions != 0 {
			return fmt.Errorf("scaling group %q exclusive queue cannot set partitions", group)
		}
	case QueueTypePartitioned:
		if queue.Access != QueueAccessNonExclusive {
			return fmt.Errorf("scaling group %q partitioned queue requires non-exclusive access", group)
		}
		if queue.Partitions < 1 {
			return fmt.Errorf("scaling group %q partitioned queue requires positive partitions", group)
		}
	default:
		return fmt.Errorf("scaling group %q queue type must be exclusive or partitioned", group)
	}
	return nil
}

func (c Config) validateOperationalControls(production bool) error {
	publisherConfigured := c.Publisher.MaxConcurrency != 0 || c.Publisher.BatchSize != 0 || c.Publisher.BatchWait.Duration != 0 || c.Publisher.PublishTimeout.Duration != 0 || c.Publisher.ShutdownTimeout.Duration != 0
	if production || publisherConfigured {
		if c.Publisher.MaxConcurrency < 1 || c.Publisher.BatchSize < 1 || c.Publisher.BatchWait.Duration <= 0 || c.Publisher.PublishTimeout.Duration <= 0 || c.Publisher.ShutdownTimeout.Duration <= 0 {
			return errors.New("publisher concurrency, batch size, batch wait, publish timeout, and shutdown timeout must be positive")
		}
	}

	stalenessConfigured := c.Staleness.MembershipMaxAge.Duration != 0 || c.Staleness.ParticipantMaxAge.Duration != 0 || c.Staleness.TelemetryMaxAge.Duration != 0
	if production || stalenessConfigured {
		if c.Staleness.MembershipMaxAge.Duration <= 0 || c.Staleness.ParticipantMaxAge.Duration <= 0 || c.Staleness.TelemetryMaxAge.Duration <= 0 {
			return errors.New("membership, participant, and telemetry staleness limits must be positive")
		}
	}
	return nil
}

func (c Config) validateCloud() error {
	cloud := c.Cloud
	if cloud.CredentialEnv != "" {
		if err := validateEnvironmentVariable("cloud.credential_env", cloud.CredentialEnv); err != nil {
			return err
		}
	}
	if !cloud.Enabled {
		if cloud.DesiredServices != 0 || cloud.MaxPaidServices != 0 {
			return errors.New("desired_services and max_paid_services must be zero while cloud provisioning is disabled")
		}
		if cloud.MaxRuntime.Duration < 0 || cloud.CleanupTimeout.Duration < 0 {
			return errors.New("cloud runtime and cleanup durations cannot be negative")
		}
		if cloud.DeleteAfterTest {
			return errors.New("delete_after_test cannot be enabled while cloud provisioning is disabled")
		}
		return nil
	}

	if strings.TrimSpace(cloud.Provider) == "" || cloud.APIBaseURL == "" || cloud.CredentialEnv == "" || strings.TrimSpace(cloud.Region) == "" || strings.TrimSpace(cloud.DatacenterID) == "" || strings.TrimSpace(cloud.BrokerVersion) == "" || strings.TrimSpace(cloud.ControlServiceClass) == "" || strings.TrimSpace(cloud.DataServiceClass) == "" {
		return errors.New("enabled cloud provisioning requires provider, API base URL, credential env, region, datacenter ID, broker version, and control/data service classes")
	}
	endpoint, err := url.ParseRequestURI(cloud.APIBaseURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil {
		return errors.New("cloud api_base_url must be an absolute HTTPS URL without embedded credentials")
	}
	if cloud.DesiredServices < 1 || cloud.MaxPaidServices < 1 {
		return errors.New("enabled cloud provisioning requires positive desired_services and max_paid_services")
	}
	if cloud.DesiredServices > cloud.MaxPaidServices {
		return errors.New("cloud desired_services cannot exceed max_paid_services")
	}
	if cloud.MaxRuntime.Duration <= 0 {
		return errors.New("enabled cloud provisioning requires a positive max_runtime")
	}
	if cloud.DeleteAfterTest && cloud.CleanupTimeout.Duration <= 0 {
		return errors.New("delete_after_test requires a positive cleanup_timeout")
	}
	if cloud.CleanupTimeout.Duration < 0 {
		return errors.New("cloud cleanup_timeout cannot be negative")
	}
	return nil
}

func validateNamespace(namespace string, required bool) error {
	if namespace == "" {
		if required {
			return errors.New("namespace is required in production or when cloud provisioning is enabled")
		}
		return nil
	}
	if len(namespace) > 253 {
		return errors.New("namespace must not exceed 253 characters")
	}
	for _, label := range strings.Split(namespace, ".") {
		if !namespaceLabelPattern.MatchString(label) {
			return fmt.Errorf("namespace %q must contain lowercase DNS labels", namespace)
		}
	}
	return nil
}

func validateIdentifier(field, value string) error {
	if !identifierPattern.MatchString(value) {
		return fmt.Errorf("%s %q must start with an alphanumeric character and contain only letters, digits, dot, underscore, or hyphen", field, value)
	}
	return nil
}

func validateManagedName(field, value string, allowSlash bool) error {
	if value == "" || !managedNamePattern.MatchString(value) || strings.HasSuffix(value, "/") || strings.Contains(value, "//") || strings.ContainsAny(value, "*>") {
		return fmt.Errorf("%s %q is not a valid managed resource name or prefix", field, value)
	}
	if !allowSlash && strings.Contains(value, "/") {
		return fmt.Errorf("%s %q cannot contain a topic separator", field, value)
	}
	return nil
}

func validateNamespaceOwnership(namespace, field, value string) error {
	if value == namespace || strings.HasPrefix(value, namespace+".") || strings.HasPrefix(value, namespace+"/") {
		return nil
	}
	return fmt.Errorf("%s %q must be inside namespace %q", field, value, namespace)
}

func validateCredentialPair(path, usernameEnv, passwordEnv string) error {
	if err := validateEnvironmentVariable(path+".username_env", usernameEnv); err != nil {
		return err
	}
	return validateEnvironmentVariable(path+".password_env", passwordEnv)
}

func validateSEMPCredentials(path, endpoint, usernameEnv, passwordEnv string, required bool) error {
	configured := endpoint != "" || usernameEnv != "" || passwordEnv != ""
	if !configured && !required {
		return nil
	}
	if err := validateEndpoint(path+".semp_endpoint", endpoint, required, "http", "https"); err != nil {
		return err
	}
	if err := validateEnvironmentVariable(path+".semp_username_env", usernameEnv); err != nil {
		return err
	}
	return validateEnvironmentVariable(path+".semp_password_env", passwordEnv)
}

func validateEndpoint(field, value string, secureOnly bool, schemes ...string) error {
	endpoint, err := url.ParseRequestURI(value)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || endpoint.User != nil {
		return fmt.Errorf("%s must be an absolute URL without embedded credentials", field)
	}
	allowed := false
	for _, scheme := range schemes {
		if endpoint.Scheme == scheme {
			allowed = true
			break
		}
	}
	if !allowed {
		return fmt.Errorf("%s uses unsupported scheme %q", field, endpoint.Scheme)
	}
	if secureOnly && endpoint.Scheme != "https" && endpoint.Scheme != "tcps" && endpoint.Scheme != "wss" {
		return fmt.Errorf("%s must use a secure transport in production or cloud mode", field)
	}
	return nil
}

func validateEnvironmentVariable(field, value string) error {
	if !environmentVariablePattern.MatchString(value) {
		return fmt.Errorf("%s %q is not a valid environment-variable name", field, value)
	}
	return nil
}

func sliceSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}
