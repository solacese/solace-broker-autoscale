// Package qualification provides an opt-in real-broker qualification workload.
// Constructing a Runner is side-effect free; Run is the only operation that
// provisions or connects to broker resources.
package qualification

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/cloud"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/integration"
	"github.com/solacese/solace-workload-balancer/semp"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

const (
	FlightGroup  = "flight-operations"
	BaggageGroup = "baggage-tracking"

	ScenarioStatic            = "static-3-broker-data-flow"
	ScenarioFence             = "fence-nack-semantics"
	ScenarioPartitioned       = "partitioned-queue-capability"
	ScenarioDrain             = "drain-telemetry"
	ScenarioBroker0Browse     = "broker0-nondestructive-browse"
	ScenarioBroker0CommandAck = "broker0-command-ack-routing"
	ScenarioTransition        = "broker0-transition"

	HeaderRunID       = "qualification.run_id"
	HeaderSequence    = "qualification.sequence"
	HeaderSentNanos   = "qualification.sent_unix_nano"
	HeaderPayloadKind = "qualification.payload_kind"
)

// Bundles are the four already-created logical services. Control is Broker 0;
// Data is Broker A/B/C in authoritative routing order.
type Bundles struct {
	Control cloud.ConnectionBundle
	Data    [3]cloud.ConnectionBundle
}

// RunBundles is a positional convenience for qualification hooks that already
// receive exactly four connection bundles.
func RunBundles(ctx context.Context, broker0Bundle, brokerA, brokerB, brokerC cloud.ConnectionBundle, namespace string, options Options) (Result, error) {
	return Run(ctx, Bundles{Control: broker0Bundle, Data: [3]cloud.ConnectionBundle{brokerA, brokerB, brokerC}}, namespace, options)
}

// WorkloadHook is directly usable by cloud orchestration commands after they
// resolve their four services to connection bundles.
type WorkloadHook func(context.Context, cloud.ConnectionBundle, cloud.ConnectionBundle, cloud.ConnectionBundle, cloud.ConnectionBundle, string) (Result, error)

// Hook returns a concrete qualification hook bound to options.
func Hook(options Options) WorkloadHook {
	return func(ctx context.Context, b0, a, b, c cloud.ConnectionBundle, namespace string) (Result, error) {
		return RunBundles(ctx, b0, a, b, c, namespace, options)
	}
}

// BundleSource lets an existing cloud command translate its plan/resource
// records into connection bundles without coupling qualification to command-local
// types. CommandHook has the same three-argument shape as cmd/qualify-cloud's
// qualification hook and is ready to assign directly.
type BundleSource[Plan any, Resources any] func(context.Context, Plan, Resources) (Bundles, string, error)

type CommandHook[Plan any, Resources any] func(context.Context, Plan, Resources) error

func HookFor[Plan any, Resources any](source BundleSource[Plan, Resources], options Options, consume func(Result) error) CommandHook[Plan, Resources] {
	return func(ctx context.Context, plan Plan, resources Resources) error {
		if source == nil {
			return errors.New("qualification: connection bundle source is required")
		}
		bundles, namespace, err := source(ctx, plan, resources)
		if err != nil {
			return err
		}
		result, err := Run(ctx, bundles, namespace, options)
		if consume != nil {
			err = errors.Join(err, consume(result))
		}
		return err
	}
}

// QueueAPI is the SEMP boundary. *semp.Client implements it.
type QueueAPI interface {
	CreateQueue(context.Context, semp.QueueSpec) (bool, error)
	EnsureQueue(context.Context, semp.QueueSpec) error
	CreateSubscription(context.Context, string, string, string) error
	FenceQueue(context.Context, string, string) error
	UnfenceQueue(context.Context, string, string) error
	MonitorQueue(context.Context, string, string) (semp.QueueStatus, error)
	MonitorDrain(context.Context, string, string) (semp.QueueStatus, error)
	DeleteQueue(context.Context, string, string) error
	QueueExists(context.Context, string, string) (bool, error)
}

// QueueAPIFactory creates one management client per service.
type QueueAPIFactory interface {
	New(cloud.ConnectionBundle) (QueueAPI, error)
}

type QueueAPIFactoryFunc func(cloud.ConnectionBundle) (QueueAPI, error)

func (f QueueAPIFactoryFunc) New(bundle cloud.ConnectionBundle) (QueueAPI, error) { return f(bundle) }

// DataPlane creates the existing native publisher and subscriber adapters. Tests
// can inject a deterministic fake; the default connects using package integration.
type DataPlane interface {
	Open(context.Context, Bundles, string) (Session, error)
}

type DataPlaneFunc func(context.Context, Bundles, string) (Session, error)

func (f DataPlaneFunc) Open(ctx context.Context, bundles Bundles, namespace string) (Session, error) {
	return f(ctx, bundles, namespace)
}

// Session carries publisher/subscriber adapters used by the staged workload.
// NativeSession uses package integration; fake sessions can provide compatible
// publisher and consumer boundaries without importing SDK implementation types.
type Session interface {
	BrokerPublisher() shimPublisher.BrokerPublisher
	AsyncBrokerPublisher() shimPublisher.AsyncBrokerPublisher
	ConsumerFactory() shimSubscriber.ConsumerFactory
	Close() error
}

// NativeSession exposes native adapters for callers that need direct access.
type NativeSession interface {
	Session
	Publishers() map[string]*integration.GuaranteedPublisher
	AsyncPublishers() map[string]*integration.AsyncPersistentPublisher
	NativeConsumerFactory() integration.ConsumerFactory
}

// Broker0Hooks expose capabilities intentionally absent from the public Solace
// Go API. A sidecar can supply a non-destructive browser and transition driver.
type Broker0Hooks struct {
	SnapshotPublisher  SnapshotPublisher
	Browser            broker0.Browser
	IndependentBrowser broker0.Browser
	CommandAck         CommandAcknowledgementProbe
	Transition         TransitionHook
	Close              func() error
}

// SnapshotPublisher publishes one authoritative snapshot to its managed LVQ and
// update topic. This is a sidecar seam because the native Go adapter currently
// has no public non-destructive browser counterpart.
type SnapshotPublisher interface {
	PublishSnapshot(context.Context, control.MembershipSnapshot) error
}

type SnapshotPublisherFunc func(context.Context, control.MembershipSnapshot) error

func (f SnapshotPublisherFunc) PublishSnapshot(ctx context.Context, snapshot control.MembershipSnapshot) error {
	return f(ctx, snapshot)
}

// TransitionHook drives a sidecar-supported real Broker 0 transition. The hook
// receives the provisioned plan and may use the supplied observer to record
// received deliveries and drain samples.
type TransitionHook interface {
	RunTransition(context.Context, ProvisionedPlan, Observer) error
}

// CommandAcknowledgementProbe verifies the real durable command and
// acknowledgement route independently of the intentionally in-process
// transition coordinator used by qualification.
type CommandAcknowledgementProbe interface {
	ProveCommandAcknowledgement(context.Context, ProvisionedPlan) error
}

type CommandAcknowledgementProbeFunc func(context.Context, ProvisionedPlan) error

func (f CommandAcknowledgementProbeFunc) ProveCommandAcknowledgement(ctx context.Context, plan ProvisionedPlan) error {
	return f(ctx, plan)
}

// Broker0PlanConfigurer lets concrete hooks bind to the exact immutable resource
// plan before any snapshot publication or browse is attempted.
type Broker0PlanConfigurer interface {
	Configure(context.Context, ProvisionedPlan) error
}

type TransitionHookFunc func(context.Context, ProvisionedPlan, Observer) error

func (f TransitionHookFunc) RunTransition(ctx context.Context, plan ProvisionedPlan, observer Observer) error {
	return f(ctx, plan, observer)
}

// Options bounds a run. Defaults keep unit fakes quick; real runs should increase
// Keys and EventsPerKey to obtain meaningful performance evidence.
type Options struct {
	RunID                    string
	Keys                     int
	EventsPerKey             int
	PublishTimeout           time.Duration
	ReceiveTimeout           time.Duration
	DrainPollInterval        time.Duration
	DrainGrace               time.Duration
	CleanupTimeout           time.Duration
	TargetDuration           time.Duration
	OutboxMaxMessages        int
	OutboxMaxBytes           int64
	AcceptBatchSize          int
	QueueMaxSpoolMB          uint64
	QueueMaxRedeliveries     uint64
	PartitionCount           uint32
	MaxInFlight              int
	MaxInFlightPerBroker     int
	DispatchIdlePollInterval time.Duration
	AllowSkipped             bool
	QueueFactory             QueueAPIFactory
	DataPlane                DataPlane
	Broker0                  Broker0Hooks
	JavaExecutable           string
	JarPath                  string
	BrowserTimeout           time.Duration
	Progress                 func(string)
	Now                      func() time.Time
}

func (o Options) withDefaults() Options {
	if o.Keys == 0 {
		o.Keys = 24
	}
	if o.EventsPerKey == 0 {
		o.EventsPerKey = 8
	}
	if o.PublishTimeout == 0 {
		o.PublishTimeout = 10 * time.Second
	}
	if o.ReceiveTimeout == 0 {
		o.ReceiveTimeout = 30 * time.Second
	}
	if o.DrainPollInterval == 0 {
		o.DrainPollInterval = 250 * time.Millisecond
	}
	if o.DrainGrace == 0 {
		o.DrainGrace = time.Second
	}
	if o.CleanupTimeout == 0 {
		o.CleanupTimeout = 30 * time.Second
	}
	if o.TargetDuration == 0 && !o.AllowSkipped {
		o.TargetDuration = time.Minute
	}
	if o.OutboxMaxMessages == 0 {
		o.OutboxMaxMessages = 250_000
	}
	if o.OutboxMaxBytes == 0 {
		o.OutboxMaxBytes = 64 << 20
	}
	if o.AcceptBatchSize == 0 {
		o.AcceptBatchSize = 1000
	}
	if o.QueueMaxSpoolMB == 0 {
		o.QueueMaxSpoolMB = 256
	}
	if o.QueueMaxRedeliveries == 0 {
		o.QueueMaxRedeliveries = 5
	}
	if o.PartitionCount == 0 {
		o.PartitionCount = 4
	}
	if o.MaxInFlight == 0 {
		o.MaxInFlight = 4096
	}
	if o.MaxInFlightPerBroker == 0 {
		o.MaxInFlightPerBroker = o.MaxInFlight
	}
	if o.DispatchIdlePollInterval == 0 {
		o.DispatchIdlePollInterval = time.Millisecond
	}
	if o.JavaExecutable == "" {
		o.JavaExecutable = "java"
	}
	if o.JarPath == "" {
		o.JarPath = "tools/lvq-browser/target/lvq-browser-1.0.0-SNAPSHOT-all.jar"
	}
	if o.BrowserTimeout == 0 {
		o.BrowserTimeout = 10 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

func (o Options) report(stage string) {
	if o.Progress != nil {
		o.Progress(stage)
	}
}

func (o Options) validate() error {
	if o.Keys < 1 || o.EventsPerKey < 1 || o.PublishTimeout <= 0 || o.ReceiveTimeout <= 0 || o.DrainPollInterval <= 0 || o.DrainGrace <= 0 || o.CleanupTimeout <= 0 || o.BrowserTimeout <= 0 || o.TargetDuration < 0 {
		return errors.New("qualification: positive workload counts and durations are required")
	}
	if strings.TrimSpace(o.JavaExecutable) == "" || strings.TrimSpace(o.JarPath) == "" {
		return errors.New("qualification: Java executable and LVQ browser JAR path are required")
	}
	if o.OutboxMaxMessages < o.Keys*o.EventsPerKey*2 || o.OutboxMaxBytes < 1 || o.AcceptBatchSize < 1 || o.QueueMaxSpoolMB < 1 || o.QueueMaxRedeliveries < 1 || o.PartitionCount < 1 || o.MaxInFlight < 1 || o.MaxInFlightPerBroker < 1 || o.MaxInFlightPerBroker > o.MaxInFlight || o.DispatchIdlePollInterval <= 0 {
		return errors.New("qualification: capacity limits cannot contain the configured workload")
	}
	return nil
}

// ResourceRef is one exact queue or subscription created by this run. It never
// contains endpoint URLs or credentials.
type ResourceRef struct {
	Role       string `json:"role"`
	Kind       string `json:"kind"`
	MessageVPN string `json:"message_vpn"`
	Queue      string `json:"queue"`
	Topic      string `json:"topic,omitempty"`
}

// Quantiles describe observed durations using nearest-rank p50/p95/p99.
type Quantiles struct {
	P50 time.Duration `json:"p50"`
	P95 time.Duration `json:"p95"`
	P99 time.Duration `json:"p99"`
	Max time.Duration `json:"max"`
}

// ValidationResult retains aggregate counters and timings rather than message
// contents. DurableAcceptanceLatency is batch-weighted: one measured atomic
// AcceptBatch duration is repeated once per successfully accepted message.
type ValidationResult struct {
	Expected                 int            `json:"expected"`
	Deliveries               int            `json:"deliveries"`
	UniqueEventIDs           int            `json:"unique_event_ids"`
	Missing                  int            `json:"missing"`
	Duplicates               int            `json:"duplicates"`
	OutOfOrder               int            `json:"out_of_order"`
	UnexpectedBroker         int            `json:"unexpected_broker"`
	BrokerDistribution       map[string]int `json:"broker_distribution"`
	Latency                  Quantiles      `json:"latency"` // Deprecated compatibility alias for EndToEndConsumedLatency.
	DurableAcceptanceLatency Quantiles      `json:"durable_acceptance_latency"`
	BrokerPositiveACKLatency Quantiles      `json:"broker_positive_ack_latency"`
	EndToEndConsumedLatency  Quantiles      `json:"end_to_end_consumed_latency"`
	Accepted                 uint64         `json:"accepted"`
	PositiveACKs             uint64         `json:"positive_acks"`
	OutboxHighWaterMessages  int            `json:"outbox_high_water_messages"`
	OutboxHighWaterBytes     int64          `json:"outbox_high_water_bytes"`
	ThroughputPerSecond      float64        `json:"throughput_per_second"`
}

// ScenarioResult reports PASS, FAIL, or SKIP without secret-bearing details.
type ScenarioResult struct {
	Name       string           `json:"name"`
	Status     string           `json:"status"`
	Reason     string           `json:"reason,omitempty"`
	Duration   time.Duration    `json:"duration"`
	Validation ValidationResult `json:"validation,omitempty"`
}

// Result is safe to serialize: service IDs, URLs, usernames, passwords, tokens,
// payloads, event IDs, and raw errors are deliberately absent.
type Result struct {
	NamespaceHash string           `json:"namespace_hash"`
	StartedAt     time.Time        `json:"started_at"`
	Duration      time.Duration    `json:"duration"`
	Scenarios     []ScenarioResult `json:"scenarios"`
	Cleanup       string           `json:"cleanup"`
	allowSkipped  bool
}

func (r Result) Passed() bool {
	for _, scenario := range r.Scenarios {
		if scenario.Status == "FAIL" || (scenario.Status == "SKIP" && !r.allowSkipped) {
			return false
		}
	}
	return r.Cleanup == "complete"
}

// ProvisionedPlan is safe to hand to a sidecar. Bundles remain available for
// connections, but Result never serializes this type.
type ProvisionedPlan struct {
	Namespace        string
	RunID            string
	Bundles          Bundles
	Resources        []ResourceRef
	Groups           []GroupPlan
	MembershipQueues map[string]string
	Participants     QualificationParticipants
	Broker0Queues    []broker0.ParticipantQueue
}

type QualificationParticipants struct {
	Publisher  string
	Subscriber string
	Observer   string
}

type GroupPlan struct {
	Group       string
	Contract    string
	Partitioned bool
	Epoch       uint64
	Destination string
	BrokerIDs   []string
	Queues      map[string]string
	Epochs      []EpochPlan
}

const QualificationConsumerSet = "qualification"

// EpochPlan names every epoch-specific ingress topic and exact broker queue used
// by a qualification membership state. Ingress is initially enabled only for the
// group's first epoch; transition targets are provisioned fenced.
type EpochPlan struct {
	Epoch        uint64
	Destination  string
	BrokerIDs    []string
	Queues       map[string]string
	MessageVPNs  map[string]string
	IngressTopic string
}

// DeliveryObservation is the transport-neutral evidence recorded by tests,
// native consumers, or a sidecar transition driver.
type DeliveryObservation struct {
	EventID      string
	Group        string
	BusinessHash string
	BrokerID     string
	Sequence     uint64
	SentAt       time.Time
	ReceivedAt   time.Time
}

// Observer accepts received-delivery evidence. It is safe for concurrent use.
type Observer interface {
	Observe(DeliveryObservation) error
}

type ObserverFunc func(DeliveryObservation) error

func (f ObserverFunc) Observe(observation DeliveryObservation) error { return f(observation) }

func joinErrors(errs []error) error { return errors.Join(errs...) }

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func validateNamespace(namespace string) error {
	if strings.TrimSpace(namespace) == "" || len(namespace) > 80 {
		return errors.New("qualification: namespace must be 1-80 characters")
	}
	for index, r := range namespace {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || (r == '-' && index > 0) || (r == '.' && index > 0)) {
			return fmt.Errorf("qualification: namespace must contain lowercase DNS-style characters")
		}
	}
	if strings.HasSuffix(namespace, "-") || strings.HasSuffix(namespace, ".") || strings.Contains(namespace, "..") {
		return errors.New("qualification: namespace has an invalid suffix or label")
	}
	return nil
}
