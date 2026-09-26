package config

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadExample(t *testing.T) {
	path := filepath.Join("..", "config.example.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Skip("example configuration not written yet")
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(cfg.Groups))
	}
	if cfg.Runtime.Mode != RuntimeModeProduction {
		t.Fatalf("runtime mode = %q, want production", cfg.Runtime.Mode)
	}
	if cfg.Control.SEMPEndpoint == "" || cfg.DataBrokers[0].SEMPUsernameEnv == "" {
		t.Fatal("example must configure non-secret SEMP connectivity")
	}
	if cfg.Control.Resources.CommandQueuePrefix == "" || cfg.Control.Resources.TelemetryQueue == "" {
		t.Fatal("example must name durable Broker 0 queues")
	}
	if cfg.Publisher.MaxConcurrency < 1 || cfg.Staleness.MembershipMaxAge.Duration <= 0 {
		t.Fatal("example must bound publisher work and stale observations")
	}
	if len(cfg.CapacityProfiles) != 1 || cfg.DataBrokers[0].ServiceClass == "" || len(cfg.DataBrokers[0].EligibleGroups) != 2 {
		t.Fatal("example must configure exact measured capacity and explicit feature placement")
	}
	if _, err := cfg.PolicyProfileCatalog(); err != nil {
		t.Fatalf("example policy profiles: %v", err)
	}
	if cfg.Cloud.Enabled || cfg.Cloud.DesiredServices != 0 || cfg.Cloud.MaxPaidServices != 0 || cfg.Cloud.DeleteAfterTest {
		t.Fatal("example must not enable or request paid cloud provisioning")
	}
}

func TestValidateAcceptsLegacyDevelopmentConfiguration(t *testing.T) {
	cfg := validConfig()
	cfg.Namespace = ""
	cfg.Runtime = RuntimeIdentities{}
	cfg.Publisher = AsyncPublisher{}
	cfg.Staleness = StalenessPolicy{}
	cfg.Control.SEMPEndpoint = ""
	cfg.Control.SEMPUsernameEnv = ""
	cfg.Control.SEMPPasswordEnv = ""
	cfg.Control.Resources = Broker0Resources{}
	cfg.DataBrokers[0].SEMPEndpoint = ""
	cfg.DataBrokers[0].SEMPUsernameEnv = ""
	cfg.DataBrokers[0].SEMPPasswordEnv = ""
	cfg.DataBrokers[0].ServiceClass = ""
	cfg.DataBrokers[0].BrokerVersion = ""
	cfg.DataBrokers[0].EligibleGroups = nil
	cfg.CapacityProfiles = nil

	if err := cfg.Validate(); err != nil {
		t.Fatalf("legacy development configuration: %v", err)
	}
}

func TestValidateRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		want string
		edit func(*Config)
	}{
		{"runtime mode", "runtime.mode", func(c *Config) { c.Runtime.Mode = "staging" }},
		{"runtime controller missing", "requires controller", func(c *Config) { c.Runtime.Controller = "" }},
		{"runtime identity duplicate", "declared as both", func(c *Config) { c.Runtime.Subscribers[0] = c.Runtime.Publishers[0] }},
		{"runtime identity invalid", "runtime publisher identity", func(c *Config) { c.Runtime.Publishers[0] = "publisher/one" }},
		{"telemetry identity missing", "telemetry identity", func(c *Config) {
			c.Runtime.Observers = nil
			c.Control.ParticipantCredentials = c.Control.ParticipantCredentials[:2]
		}},
		{"controller principal mismatch", "must equal", func(c *Config) { c.Control.Principal = "other-controller" }},
		{"participant credential missing", "distinct control credential", func(c *Config) { c.Control.ParticipantCredentials = c.Control.ParticipantCredentials[1:] }},
		{"participant principal mismatch", "must equal participant", func(c *Config) { c.Control.ParticipantCredentials[0].Principal = "other" }},
		{"participant username env shared", "is shared", func(c *Config) {
			c.Control.ParticipantCredentials[1].UsernameEnv = c.Control.ParticipantCredentials[0].UsernameEnv
		}},
		{"participant controller env shared", "must not share", func(c *Config) { c.Control.ParticipantCredentials[0].UsernameEnv = c.Control.UsernameEnv }},
		{"namespace missing", "namespace is required", func(c *Config) { c.Namespace = "" }},
		{"namespace uppercase", "lowercase DNS labels", func(c *Config) { c.Namespace = "SWLB" }},
		{"namespace empty label", "lowercase DNS labels", func(c *Config) { c.Namespace = "swlb..prod" }},
		{"namespace long label", "lowercase DNS labels", func(c *Config) { c.Namespace = strings.Repeat("a", 64) }},
		{"control ID invalid", "control broker id", func(c *Config) { c.Control.BrokerID = "broker/0" }},
		{"control SMF insecure", "secure transport", func(c *Config) { c.Control.SMFEndpoint = "tcp://broker-0.example:55555" }},
		{"control SMF credentials inline", "without embedded credentials", func(c *Config) { c.Control.SMFEndpoint = "tcps://user:secret@broker-0.example:55443" }},
		{"control SEMP missing", "semp_endpoint", func(c *Config) { c.Control.SEMPEndpoint = "" }},
		{"control SEMP insecure", "secure transport", func(c *Config) { c.Control.SEMPEndpoint = "http://broker-0.example:943" }},
		{"control SEMP username invalid", "semp_username_env", func(c *Config) { c.Control.SEMPUsernameEnv = "literal-user" }},
		{"control password missing", "password_env", func(c *Config) { c.Control.PasswordEnv = "" }},
		{"membership prefix wildcard", "membership_lvq_prefix", func(c *Config) { c.Control.MembershipLVQPrefix = "swlb.*" }},
		{"control prefix outside namespace", "inside namespace", func(c *Config) { c.Control.ControlTopicPrefix = "other/control" }},
		{"Broker 0 resource missing", "registration_queue", func(c *Config) { c.Control.Resources.RegistrationQueue = "" }},
		{"Broker 0 resource outside namespace", "inside namespace", func(c *Config) { c.Control.Resources.TelemetryQueue = "other.telemetry" }},
		{"data ID invalid", "data_brokers[0].id", func(c *Config) { c.DataBrokers[0].ID = "bad id" }},
		{"data SMF insecure", "secure transport", func(c *Config) { c.DataBrokers[0].SMFEndpoint = "ws://broker-a.example" }},
		{"data username missing", "username_env", func(c *Config) { c.DataBrokers[0].UsernameEnv = "" }},
		{"data password invalid", "password_env", func(c *Config) { c.DataBrokers[0].PasswordEnv = "plain-text-password" }},
		{"data SEMP password missing", "semp_password_env", func(c *Config) { c.DataBrokers[0].SEMPPasswordEnv = "" }},
		{"data service class missing", "service_class and broker_version", func(c *Config) { c.DataBrokers[0].ServiceClass = "" }},
		{"data broker version missing", "service_class and broker_version", func(c *Config) { c.DataBrokers[0].BrokerVersion = "" }},
		{"data profile mismatch", "exact capacity profile not found", func(c *Config) { c.DataBrokers[0].BrokerVersion = "10.4.2" }},
		{"eligible group unknown", "references unknown scaling group", func(c *Config) { c.DataBrokers[0].EligibleGroups = []string{"unknown"} }},
		{"eligible groups omitted", "eligible_groups is required", func(c *Config) { c.DataBrokers[0].EligibleGroups = nil }},
		{"eligible group duplicate", "repeats scaling group", func(c *Config) { c.DataBrokers[0].EligibleGroups = []string{"orders", "orders"} }},
		{"capacity profiles missing", "capacity_profiles requires", func(c *Config) { c.CapacityProfiles = nil }},
		{"capacity service class missing", "requires service class and broker version", func(c *Config) { c.CapacityProfiles[0].ServiceClass = "" }},
		{"capacity broker version missing", "requires service class and broker version", func(c *Config) { c.CapacityProfiles[0].BrokerVersion = "" }},
		{"capacity service class whitespace", "surrounding whitespace", func(c *Config) { c.CapacityProfiles[0].ServiceClass = " class-a" }},
		{"data broker version whitespace", "surrounding whitespace", func(c *Config) { c.DataBrokers[0].BrokerVersion = "10.4.1 " }},
		{"capacity ingress zero", "finite and greater than zero", func(c *Config) { c.CapacityProfiles[0].Limits.IngressBytesPerSecond = 0 }},
		{"capacity egress negative", "finite and greater than zero", func(c *Config) { c.CapacityProfiles[0].Limits.EgressBytesPerSecond = -1 }},
		{"capacity spool NaN", "finite and greater than zero", func(c *Config) { c.CapacityProfiles[0].Limits.SpoolBytes = math.NaN() }},
		{"capacity connections infinite", "finite and greater than zero", func(c *Config) { c.CapacityProfiles[0].Limits.Connections = math.Inf(1) }},
		{"capacity duplicate exact pair", "duplicate capacity profile", func(c *Config) { c.CapacityProfiles = append(c.CapacityProfiles, c.CapacityProfiles[0]) }},
		{"data label empty", "labels", func(c *Config) { c.DataBrokers[0].Labels["region"] = "" }},
		{"queue prefix missing", "name_prefix", func(c *Config) { c.Groups[0].Queue.NamePrefix = "" }},
		{"queue prefix outside namespace", "inside namespace", func(c *Config) { c.Groups[0].Queue.NamePrefix = "other.orders" }},
		{"exclusive access mismatch", "requires exclusive access", func(c *Config) { c.Groups[0].Queue.Access = QueueAccessNonExclusive }},
		{"exclusive partitions", "cannot set partitions", func(c *Config) { c.Groups[0].Queue.Partitions = 2 }},
		{"partitioned access mismatch", "requires non-exclusive access", func(c *Config) { c.Groups[0].Queue.Type = QueueTypePartitioned; c.Groups[0].Queue.Partitions = 4 }},
		{"partitioned count missing", "requires positive partitions", func(c *Config) {
			c.Groups[0].Queue.Type = QueueTypePartitioned
			c.Groups[0].Queue.Access = QueueAccessNonExclusive
		}},
		{"queue type invalid", "queue type", func(c *Config) { c.Groups[0].Queue.Type = "ordinary" }},
		{"redeliveries zero", "max_redeliveries", func(c *Config) { c.Groups[0].Queue.MaxRedeliveries = 0 }},
		{"DMQ missing", "dead_message_queue", func(c *Config) { c.Groups[0].Queue.DeadMessageQueue = "" }},
		{"DMQ same as queue", "must differ", func(c *Config) { c.Groups[0].Queue.DeadMessageQueue = c.Groups[0].Queue.NamePrefix }},
		{"DMQ outside namespace", "inside namespace", func(c *Config) { c.Groups[0].Queue.DeadMessageQueue = "other.orders.dmq" }},
		{"warm brokers negative", "warm_brokers", func(c *Config) { c.Groups[0].Policy.WarmBrokers = -1 }},
		{"warm brokers exceed range", "warm_brokers", func(c *Config) { c.Groups[0].Policy.WarmBrokers = 1 }},
		{"headroom negative", "headroom_percent", func(c *Config) { c.Groups[0].Policy.HeadroomPercent = -1 }},
		{"headroom too high", "headroom_percent", func(c *Config) { c.Groups[0].Policy.HeadroomPercent = 100 }},
		{"headroom NaN", "headroom_percent", func(c *Config) { c.Groups[0].Policy.HeadroomPercent = math.NaN() }},
		{"pressure window zero", "pressure_window", func(c *Config) { c.Groups[0].Policy.PressureWindow = duration(0) }},
		{"cooldown negative", "cooldown", func(c *Config) { c.Groups[0].Policy.Cooldown = duration(-time.Second) }},
		{"concurrent changes zero", "max_concurrent_changes must equal 1", func(c *Config) { c.Groups[0].Policy.MaxConcurrentChanges = 0 }},
		{"concurrent changes above one", "max_concurrent_changes must equal 1", func(c *Config) { c.Groups[0].Policy.MaxConcurrentChanges = 2 }},
		{"readiness timeout zero", "handover timings", func(c *Config) { c.Groups[0].Handover.ReadinessTimeout = duration(0) }},
		{"telemetry age zero", "handover timings", func(c *Config) { c.Groups[0].Handover.TelemetryMaxAge = duration(0) }},
		{"drain grace zero", "handover timings", func(c *Config) { c.Groups[0].Handover.DrainGrace = duration(0) }},
		{"transition timeout zero", "handover timings", func(c *Config) { c.Groups[0].Handover.TransitionTimeout = duration(0) }},
		{"empty participants", "requires at least one participant", func(c *Config) { c.Groups[0].RequiredPublishers = nil; c.Groups[0].RequiredSubscribers = nil }},
		{"publisher duplicate", "duplicated", func(c *Config) { c.Groups[0].RequiredPublishers = []string{"publisher-1", "publisher-1"} }},
		{"cross-role participant duplicate", "duplicated", func(c *Config) { c.Groups[0].RequiredSubscribers[0] = c.Groups[0].RequiredPublishers[0] }},
		{"publisher undeclared", "undeclared runtime publisher", func(c *Config) { c.Groups[0].RequiredPublishers[0] = "publisher-2" }},
		{"subscriber undeclared", "undeclared runtime subscriber", func(c *Config) { c.Groups[0].RequiredSubscribers[0] = "subscriber-2" }},
		{"publisher concurrency zero", "publisher concurrency", func(c *Config) { c.Publisher.MaxConcurrency = 0 }},
		{"publisher batch zero", "batch size", func(c *Config) { c.Publisher.BatchSize = 0 }},
		{"publisher batch wait zero", "batch wait", func(c *Config) { c.Publisher.BatchWait = duration(0) }},
		{"publisher publish timeout zero", "publish timeout", func(c *Config) { c.Publisher.PublishTimeout = duration(0) }},
		{"publisher shutdown timeout zero", "shutdown timeout", func(c *Config) { c.Publisher.ShutdownTimeout = duration(0) }},
		{"membership age zero", "staleness limits", func(c *Config) { c.Staleness.MembershipMaxAge = duration(0) }},
		{"participant age zero", "staleness limits", func(c *Config) { c.Staleness.ParticipantMaxAge = duration(0) }},
		{"telemetry staleness zero", "staleness limits", func(c *Config) { c.Staleness.TelemetryMaxAge = duration(0) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.edit(&cfg)
			assertValidationError(t, cfg, test.want)
		})
	}
}

func TestValidateAllowsExplicitlyEmptyPlacementForWarmBroker(t *testing.T) {
	cfg := validConfig()
	cfg.DataBrokers = append(cfg.DataBrokers, DataBroker{
		ID: "broker-warm", SMFEndpoint: "tcps://broker-warm.example:55443", SEMPEndpoint: "https://broker-warm.example:943",
		MessageVPN: "data", UsernameEnv: "DATA_USERNAME", PasswordEnv: "DATA_PASSWORD",
		SEMPUsernameEnv: "DATA_SEMP_USERNAME", SEMPPasswordEnv: "DATA_SEMP_PASSWORD",
		ServiceClass: "class-a", BrokerVersion: "10.4.1", EligibleGroups: []string{},
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("explicit empty placement: %v", err)
	}
}

func TestPolicyConversionsAreExactAndDefensive(t *testing.T) {
	cfg := validConfig()
	profiles := cfg.PolicyCapacityProfiles()
	if len(profiles) != 1 || profiles[0].ServiceClass != "class-a" || profiles[0].BrokerVersion != "10.4.1" || profiles[0].Limits.SpoolBytes != 100 {
		t.Fatalf("unexpected policy profiles: %#v", profiles)
	}
	catalog, err := cfg.PolicyProfileCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Lookup("class-a", "10.4.1"); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Lookup("class-a", "10.4"); err == nil {
		t.Fatal("capacity profile lookup borrowed from another version")
	}

	brokers := cfg.PolicyBrokers()
	if len(brokers) != 1 || brokers[0].ID != "broker-a" || brokers[0].Ready || len(brokers[0].EligibleGroups) != 1 || brokers[0].EligibleGroups[0] != "orders" {
		t.Fatalf("unexpected policy brokers: %#v", brokers)
	}
	brokers[0].EligibleGroups[0] = "mutated"
	if cfg.DataBrokers[0].EligibleGroups[0] != "orders" {
		t.Fatal("policy conversion aliased configuration placement")
	}
}

func TestValidateDevelopmentCapacityConfigurationIsAllOrNothing(t *testing.T) {
	cfg := validConfig()
	cfg.Namespace = ""
	cfg.Runtime = RuntimeIdentities{}
	cfg.Publisher = AsyncPublisher{}
	cfg.Staleness = StalenessPolicy{}
	cfg.Control.SEMPEndpoint = ""
	cfg.Control.SEMPUsernameEnv = ""
	cfg.Control.SEMPPasswordEnv = ""
	cfg.Control.Resources = Broker0Resources{}
	cfg.DataBrokers[0].SEMPEndpoint = ""
	cfg.DataBrokers[0].SEMPUsernameEnv = ""
	cfg.DataBrokers[0].SEMPPasswordEnv = ""

	cfg.CapacityProfiles = nil
	assertValidationError(t, cfg, "exact capacity profile not found")
}

func TestValidateCloudControls(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.Cloud = validCloud()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})

	tests := []struct {
		name string
		want string
		edit func(*CloudProvisioning)
	}{
		{"credential env", "credential_env", func(c *CloudProvisioning) { c.CredentialEnv = "token-value" }},
		{"API URL missing", "requires provider", func(c *CloudProvisioning) { c.APIBaseURL = "" }},
		{"API URL insecure", "absolute HTTPS", func(c *CloudProvisioning) { c.APIBaseURL = "http://api.example" }},
		{"API URL credentials", "absolute HTTPS", func(c *CloudProvisioning) { c.APIBaseURL = "https://user:secret@api.example" }},
		{"region missing", "requires provider", func(c *CloudProvisioning) { c.Region = "" }},
		{"datacenter missing", "requires provider", func(c *CloudProvisioning) { c.DatacenterID = "" }},
		{"broker version missing", "requires provider", func(c *CloudProvisioning) { c.BrokerVersion = "" }},
		{"control class missing", "requires provider", func(c *CloudProvisioning) { c.ControlServiceClass = "" }},
		{"data class missing", "requires provider", func(c *CloudProvisioning) { c.DataServiceClass = "" }},
		{"desired zero", "positive desired_services", func(c *CloudProvisioning) { c.DesiredServices = 0 }},
		{"limit zero", "positive desired_services", func(c *CloudProvisioning) { c.MaxPaidServices = 0 }},
		{"desired over limit", "cannot exceed", func(c *CloudProvisioning) { c.DesiredServices = 4 }},
		{"runtime zero", "positive max_runtime", func(c *CloudProvisioning) { c.MaxRuntime = duration(0) }},
		{"cleanup zero", "positive cleanup_timeout", func(c *CloudProvisioning) { c.CleanupTimeout = duration(0) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Cloud = validCloud()
			test.edit(&cfg.Cloud)
			assertValidationError(t, cfg, test.want)
		})
	}

	t.Run("cloud mode is strict", func(t *testing.T) {
		cfg := validConfig()
		cfg.Runtime.Mode = RuntimeModeDevelopment
		cfg.Runtime.Controller = ""
		cfg.Cloud = validCloud()
		assertValidationError(t, cfg, "cloud-enabled runtime requires")
	})

	t.Run("disabled limits remain zero", func(t *testing.T) {
		cfg := validConfig()
		cfg.Cloud.MaxPaidServices = 1
		assertValidationError(t, cfg, "must be zero")
	})

	t.Run("disabled deletion forbidden", func(t *testing.T) {
		cfg := validConfig()
		cfg.Cloud.DeleteAfterTest = true
		assertValidationError(t, cfg, "cannot be enabled")
	})
}

func TestLoadRejectsUnknownFieldsAndInvalidDurations(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("..", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		old  string
		new  string
		want string
	}{
		{"unknown", "namespace: swlb", "namespace: swlb\nunknown_setting: true", "field unknown_setting not found"},
		{"duration", "batch_wait: 10ms", "batch_wait: immediately", "invalid duration"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(strings.Replace(string(contents), test.old, test.new, 1)), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func validConfig() Config {
	return Config{
		Namespace: "swlb",
		Control: ControlBroker{
			BrokerID:        "broker-0",
			SMFEndpoint:     "tcps://broker-0.example:55443",
			SEMPEndpoint:    "https://broker-0.example:943",
			MessageVPN:      "control",
			Principal:       "controller-1",
			UsernameEnv:     "CONTROL_USERNAME",
			PasswordEnv:     "CONTROL_PASSWORD",
			SEMPUsernameEnv: "CONTROL_SEMP_USERNAME",
			SEMPPasswordEnv: "CONTROL_SEMP_PASSWORD",
			ParticipantCredentials: []ParticipantControlCredential{
				{Participant: "publisher-1", Principal: "publisher-1", UsernameEnv: "PUBLISHER_CONTROL_USERNAME", PasswordEnv: "PUBLISHER_CONTROL_PASSWORD"},
				{Participant: "subscriber-1", Principal: "subscriber-1", UsernameEnv: "SUBSCRIBER_CONTROL_USERNAME", PasswordEnv: "SUBSCRIBER_CONTROL_PASSWORD"},
				{Participant: "observer-1", Principal: "observer-1", UsernameEnv: "OBSERVER_CONTROL_USERNAME", PasswordEnv: "OBSERVER_CONTROL_PASSWORD"},
			},
			MembershipLVQPrefix: "swlb.membership",
			ControlTopicPrefix:  "swlb/control/v1",
			Resources: Broker0Resources{
				MembershipTopicPrefix: "swlb/control/v1/membership",
				MembershipQueuePrefix: "swlb.membership",
				CommandTopicPrefix:    "swlb/control/v1/command",
				CommandQueuePrefix:    "swlb.command",
				RegistrationTopic:     "swlb/control/v1/registration",
				RegistrationQueue:     "swlb.registration",
				ReadinessTopic:        "swlb/control/v1/readiness",
				ReadinessQueue:        "swlb.readiness",
				TelemetryTopic:        "swlb/control/v1/telemetry",
				TelemetryQueue:        "swlb.telemetry",
			},
		},
		DataBrokers: []DataBroker{{
			ID:              "broker-a",
			SMFEndpoint:     "tcps://broker-a.example:55443",
			SEMPEndpoint:    "https://broker-a.example:943",
			MessageVPN:      "data",
			UsernameEnv:     "DATA_USERNAME",
			PasswordEnv:     "DATA_PASSWORD",
			SEMPUsernameEnv: "DATA_SEMP_USERNAME",
			SEMPPasswordEnv: "DATA_SEMP_PASSWORD",
			ServiceClass:    "class-a",
			BrokerVersion:   "10.4.1",
			EligibleGroups:  []string{"orders"},
			Labels:          map[string]string{"region": "us-central"},
		}},
		CapacityProfiles: []CapacityProfile{{
			ServiceClass: "class-a", BrokerVersion: "10.4.1",
			Limits: CapacityLimits{IngressBytesPerSecond: 100, EgressBytesPerSecond: 100, SpoolBytes: 100, Connections: 100},
		}},
		Groups: []ScalingGroup{{
			ID:                  "orders",
			HashContract:        "orders-v1",
			CustomerLibrary:     "routing-v1",
			OrderedBrokerIDs:    []string{"broker-a"},
			Queue:               Queue{NamePrefix: "swlb.orders", Type: QueueTypeExclusive, Access: QueueAccessExclusive, MaxRedeliveries: 5, DeadMessageQueue: "swlb.orders.dmq"},
			Policy:              ScalingPolicy{MinimumBrokers: 1, MaximumBrokers: 1, HeadroomPercent: 25, PressureWindow: duration(time.Minute), Cooldown: duration(5 * time.Minute), MaxConcurrentChanges: 1},
			Handover:            HandoverPolicy{ReadinessTimeout: duration(time.Minute), TelemetryMaxAge: duration(15 * time.Second), DrainGrace: duration(30 * time.Second), TransitionTimeout: duration(10 * time.Minute)},
			RequiredPublishers:  []string{"publisher-1"},
			RequiredSubscribers: []string{"subscriber-1"},
		}},
		Runtime:     RuntimeIdentities{Mode: RuntimeModeProduction, Controller: "controller-1", Publishers: []string{"publisher-1"}, Subscribers: []string{"subscriber-1"}, Observers: []string{"observer-1"}},
		Publisher:   AsyncPublisher{MaxConcurrency: 4, BatchSize: 16, BatchWait: duration(10 * time.Millisecond), PublishTimeout: duration(10 * time.Second), ShutdownTimeout: duration(30 * time.Second)},
		Staleness:   StalenessPolicy{MembershipMaxAge: duration(5 * time.Minute), ParticipantMaxAge: duration(30 * time.Second), TelemetryMaxAge: duration(15 * time.Second)},
		Persistence: Persistence{ControllerState: "state.json", PublisherOutbox: "outbox", OutboxMaxMessages: 100, OutboxMaxBytes: 1024},
		Cloud:       CloudProvisioning{Enabled: false, MaxRuntime: duration(8 * time.Hour), CleanupTimeout: duration(30 * time.Minute)},
	}
}

func validCloud() CloudProvisioning {
	return CloudProvisioning{
		Enabled:             true,
		Provider:            "solace-cloud",
		APIBaseURL:          "https://api.solace.cloud",
		CredentialEnv:       "SOLACE_CLOUD_TOKEN",
		Region:              "us-central",
		DatacenterID:        "dc-1",
		BrokerVersion:       "10.25",
		ControlServiceClass: "developer",
		DataServiceClass:    "enterprise",
		DesiredServices:     2,
		MaxPaidServices:     3,
		MaxRuntime:          duration(8 * time.Hour),
		CleanupTimeout:      duration(30 * time.Minute),
		DeleteAfterTest:     true,
	}
}

func duration(value time.Duration) Duration { return Duration{Duration: value} }

func assertValidationError(t *testing.T, cfg Config, want string) {
	t.Helper()
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Validate error = %v, want containing %q", err, want)
	}
}
