package qualification

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	swlbruntime "github.com/solacese/solace-workload-balancer/runtime"
)

// CheckDefaultCapabilities verifies local executables needed by the concrete
// real-broker hooks. It has no broker or cloud side effects.
func CheckDefaultCapabilities(options Options) error {
	options = options.withDefaults()
	if _, err := os.Stat(options.JarPath); err != nil {
		return fmt.Errorf("qualification: JCSMP browser JAR is unavailable: %w", err)
	}
	java, err := exec.LookPath(options.JavaExecutable)
	if err != nil {
		return fmt.Errorf("qualification: Java executable is unavailable: %w", err)
	}
	command := exec.Command(java, "-version")
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("qualification: Java runtime check failed")
	}
	if !supportedJavaVersion(string(output)) {
		return errors.New("qualification: Java 8 or newer is required for the JCSMP browser")
	}
	return nil
}

func supportedJavaVersion(output string) bool {
	marker := `version "`
	start := strings.Index(output, marker)
	if start < 0 {
		return false
	}
	version := output[start+len(marker):]
	if end := strings.IndexByte(version, '"'); end >= 0 {
		version = version[:end]
	}
	parts := strings.Split(version, ".")
	if len(parts) == 0 {
		return false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}
	if major == 1 && len(parts) > 1 {
		major, err = strconv.Atoi(parts[1])
		if err != nil {
			return false
		}
	}
	return major >= 8
}

// ConfigureDefaultHooks installs the concrete Broker 0 publisher, two
// independent JCSMP browsers, and direct controller transition driver. Resources
// are opened lazily from the exact ProvisionedPlan produced by Run.
func ConfigureDefaultHooks(options *Options) error {
	if options == nil {
		return errors.New("qualification: options are required")
	}
	if err := CheckDefaultCapabilities(*options); err != nil {
		return err
	}
	setup := &defaultBroker0Setup{options: options.withDefaults()}
	options.Broker0 = Broker0Hooks{
		SnapshotPublisher:  setup,
		Browser:            lazyBrowser{setup: setup, independent: false},
		IndependentBrowser: lazyBrowser{setup: setup, independent: true},
		CommandAck:         setup,
		Transition:         setup,
		Close:              setup.Close,
	}
	return nil
}

type defaultBroker0Setup struct {
	mu         sync.Mutex
	options    Options
	plan       *ProvisionedPlan
	connection *swlbruntime.SMFConnection
	native     *swlbruntime.NativePublisher
	publisher  *broker0.PersistentControlPublisher
	browsers   [2]*broker0.JCSMPBrowser
	directory  string
}

func (s *defaultBroker0Setup) initialize(ctx context.Context, plan ProvisionedPlan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.plan != nil {
		if s.plan.Namespace != plan.Namespace || s.plan.RunID != plan.RunID {
			return errors.New("qualification: default Broker 0 hooks already bound to another plan")
		}
		return nil
	}
	bundle := plan.Bundles.Control
	connection, err := swlbruntime.ConnectBroker0WithTrustStore(ctx, bundle.SMFHosts[0], bundle.MessageVPN, bundle.ServiceCredential.Username, bundle.ServiceCredential.Password, plan.Namespace+"-qualification-control", trustStorePath())
	if err != nil {
		return err
	}
	native, err := swlbruntime.NewNativePublisher(connection.Service, s.options.PublishTimeout)
	if err != nil {
		_ = connection.Close()
		return err
	}
	directory, err := os.MkdirTemp("", "swlb-qualification-broker0-")
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		return err
	}
	operations, err := broker0.OpenJSONOperationStore(filepath.Join(directory, "operations.json"))
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		_ = os.RemoveAll(directory)
		return err
	}
	names, err := control.NewManagedNames(plan.Namespace)
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		_ = os.RemoveAll(directory)
		return err
	}
	controlNative := snapshotFanoutPublisher{native: native, names: names}
	publisher, err := broker0.NewPersistentControlPublisherWithTargets(controlNative, operations, broker0.DestinationResolverFunc(func(kind broker0.Kind, group string) (string, error) {
		if kind == broker0.KindMembershipSnapshot {
			return names.SnapshotTopic(group)
		}
		return "", errors.New("qualification: unsupported shared Broker 0 publication")
	}), broker0.TargetDestinationResolverFunc(func(kind broker0.Kind, group string, role control.ParticipantRole, participant string) (string, error) {
		switch kind {
		case broker0.KindCommand:
			return names.CommandTopic(group, role, participant)
		case broker0.KindRegistration:
			return names.RegistrationTopicFor(group, role, participant)
		case broker0.KindAcknowledgement:
			return names.AcknowledgementTopicFor(group, role, participant)
		case broker0.KindTelemetry:
			return names.TelemetryTopicFor(group, role, participant)
		default:
			return "", errors.New("qualification: unsupported participant Broker 0 publication")
		}
	}))
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		_ = os.RemoveAll(directory)
		return err
	}
	resolver := broker0.MembershipQueueResolverFunc(func(_ context.Context, group string) (string, error) {
		queue := plan.MembershipQueues[group]
		if queue == "" {
			return "", errors.New("qualification: membership queue is not in resource plan")
		}
		return queue, nil
	})
	browserOptions := broker0.JCSMPBrowserOptions{JavaExecutable: s.options.JavaExecutable, JarPath: s.options.JarPath, Namespace: plan.Namespace, Host: bundle.SMFHosts[0], VPN: bundle.MessageVPN, Username: bundle.ServiceCredential.Username, Password: bundle.ServiceCredential.Password, Timeout: s.options.BrowserTimeout}
	if trustStore := os.Getenv("SWLB_JAVA_TRUST_STORE"); trustStore != "" {
		info, err := os.Stat(trustStore)
		if err != nil || info.IsDir() {
			return errors.New("qualification: SWLB_JAVA_TRUST_STORE must name a readable Java trust-store file")
		}
		browserOptions.TrustStorePath = trustStore
	}
	first, err := broker0.NewJCSMPBrowser(resolver, browserOptions)
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		_ = os.RemoveAll(directory)
		return err
	}
	second, err := broker0.NewJCSMPBrowser(resolver, browserOptions)
	if err != nil {
		_ = first.Close()
		_ = native.Close()
		_ = connection.Close()
		_ = os.RemoveAll(directory)
		return err
	}
	s.plan = &plan
	s.connection, s.native, s.publisher, s.browsers, s.directory = connection, native, publisher, [2]*broker0.JCSMPBrowser{first, second}, directory
	return nil
}

func (s *defaultBroker0Setup) Configure(ctx context.Context, plan ProvisionedPlan) error {
	return s.initialize(ctx, plan)
}

func (s *defaultBroker0Setup) PublishSnapshot(ctx context.Context, snapshot control.MembershipSnapshot) error {
	s.mu.Lock()
	publisher := s.publisher
	s.mu.Unlock()
	if publisher == nil {
		return errors.New("qualification: default Broker 0 hooks are not initialized")
	}
	return publisher.PublishSnapshot(ctx, snapshot)
}

func (s *defaultBroker0Setup) ProveCommandAcknowledgement(ctx context.Context, plan ProvisionedPlan) (runErr error) {
	if err := s.initialize(ctx, plan); err != nil {
		return err
	}
	s.mu.Lock()
	connection, publisher, directory := s.connection, s.publisher, s.directory
	s.mu.Unlock()
	if connection == nil || publisher == nil || directory == "" {
		return errors.New("qualification: default Broker 0 command/ack probe is not initialized")
	}
	commandBinding, err := broker0Binding(plan, FlightGroup, plan.Participants.Subscriber, broker0.KindCommand)
	if err != nil {
		return err
	}
	ackBinding, err := broker0Binding(plan, FlightGroup, plan.Participants.Subscriber, broker0.KindAcknowledgement)
	if err != nil {
		return err
	}
	ackReceiver, err := swlbruntime.BindControllerInbox(connection.Service, ackBinding, 100*time.Millisecond)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, ackReceiver.Close()) }()

	commandOperations, err := broker0.OpenJSONOperationStore(filepath.Join(directory, "command-probe.json"))
	if err != nil {
		return err
	}
	ackOperations, err := broker0.OpenJSONOperationStore(filepath.Join(directory, "ack-probe.json"))
	if err != nil {
		return err
	}
	now := s.options.Now().UTC()
	command := control.CommandEnvelope{
		Version: control.ProtocolVersion, MessageID: "probe-command-" + plan.RunID,
		Namespace: plan.Namespace, Group: FlightGroup, TransitionID: "probe-transition-" + plan.RunID,
		Epoch: 2, Phase: control.PhasePrepare, Participant: plan.Participants.Subscriber,
		Role: control.RoleSubscriber, IssuedAt: now, Deadline: now.Add(s.options.ReceiveTimeout),
	}
	executor := commandExecutorFunc(func(_ context.Context, got control.CommandEnvelope) error {
		if got != command {
			return errors.New("qualification: command probe scope changed in durable routing")
		}
		return nil
	})
	handler, err := broker0.NewCommandHandler(command.Participant, command.Role, command.Namespace, []string{command.Group}, commandOperations, executor, publisher)
	if err != nil {
		return err
	}
	if err := publisher.PublishCommand(ctx, command); err != nil {
		return err
	}
	commandReceiver, err := (swlbruntime.NativeDurableReceiverFactory{Service: connection.Service, Poll: 100 * time.Millisecond}).BindDurable(ctx, commandBinding)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, commandReceiver.Close()) }()
	commandCtx, cancelCommand := context.WithTimeout(ctx, s.options.ReceiveTimeout)
	commandDelivery, err := commandReceiver.Receive(commandCtx)
	cancelCommand()
	if err != nil {
		return fmt.Errorf("qualification: receive durable command probe: %w", err)
	}
	if err := handler.Handle(ctx, commandDelivery); err != nil {
		return err
	}

	proof := &commandAckProof{expected: command}
	inbox, err := broker0.NewControllerInbox(ackReceiver, proof, ackOperations)
	if err != nil {
		return err
	}
	ackCtx, cancelAck := context.WithTimeout(ctx, s.options.ReceiveTimeout)
	ackDelivery, err := ackReceiver.Receive(ackCtx)
	cancelAck()
	if err != nil {
		return fmt.Errorf("qualification: receive durable acknowledgement probe: %w", err)
	}
	if err := inbox.Handle(ctx, ackDelivery); err != nil {
		return err
	}
	if !proof.accepted {
		return errors.New("qualification: command acknowledgement probe was not accepted")
	}
	return nil
}

type commandExecutorFunc func(context.Context, control.CommandEnvelope) error

func (f commandExecutorFunc) Execute(ctx context.Context, command control.CommandEnvelope) error {
	return f(ctx, command)
}

type commandAckProof struct {
	expected control.CommandEnvelope
	accepted bool
}

func (p *commandAckProof) Acknowledge(ack controller.Acknowledgement) error {
	if ack.CommandID != p.expected.MessageID || ack.Namespace != p.expected.Namespace || ack.Group != p.expected.Group ||
		ack.TransitionID != p.expected.TransitionID || ack.Epoch != p.expected.Epoch || ack.Participant != p.expected.Participant ||
		ack.AuthenticatedParticipant != p.expected.Participant || ack.Role != p.expected.Role {
		return errors.New("qualification: acknowledgement probe scope changed in durable routing")
	}
	p.accepted = true
	return nil
}

func (*commandAckProof) Observe(controller.Telemetry) error {
	return errors.New("qualification: command acknowledgement probe received telemetry")
}

func broker0Binding(plan ProvisionedPlan, group, participant string, kind broker0.Kind) (broker0.ParticipantQueue, error) {
	for _, binding := range plan.Broker0Queues {
		if binding.Group == group && binding.Participant == participant && binding.Kind == kind {
			return binding, nil
		}
	}
	return broker0.ParticipantQueue{}, fmt.Errorf("qualification: Broker 0 %s binding is not in resource plan", kind)
}

func (s *defaultBroker0Setup) RunTransition(ctx context.Context, plan ProvisionedPlan, observer Observer) error {
	if err := s.initialize(ctx, plan); err != nil {
		return err
	}
	s.mu.Lock()
	publisher, connection := s.publisher, s.connection
	s.mu.Unlock()
	return TransportTransitionDriver{
		Publisher: publisher, ControlConnection: connection,
		Browser: lazyBrowser{setup: s}, IndependentBrowser: lazyBrowser{setup: s, independent: true},
		Queues: sempFactory{}, DataPlane: nativeDataPlane{timeout: s.options.PublishTimeout, maxInFlightPerBroker: s.options.MaxInFlightPerBroker, partitionCount: s.options.PartitionCount}, Options: s.options,
	}.RunTransition(ctx, plan, observer)
}

type snapshotFanoutPublisher struct {
	native *swlbruntime.NativePublisher
	names  control.ManagedNames
}

func (p snapshotFanoutPublisher) PublishPersistent(ctx context.Context, publication broker0.Publication) error {
	if err := p.native.PublishPersistent(ctx, publication); err != nil {
		return err
	}
	if publication.Kind != broker0.KindMembershipSnapshot {
		return nil
	}
	var group string
	for _, candidate := range []string{FlightGroup, BaggageGroup} {
		topic, err := p.names.SnapshotTopic(candidate)
		if err == nil && topic == publication.Destination {
			group = candidate
			break
		}
	}
	if group == "" {
		return errors.New("qualification: snapshot destination is outside managed groups")
	}
	updateTopic, err := p.names.UpdateTopic(group)
	if err != nil {
		return err
	}
	copy := publication
	copy.Destination = updateTopic
	copy.OperationID += "/update"
	return p.native.PublishPersistent(ctx, copy)
}

type lazyBrowser struct {
	setup       *defaultBroker0Setup
	independent bool
}

func (b lazyBrowser) Browse(ctx context.Context, group string) ([]broker0.BrowsedMessage, error) {
	b.setup.mu.Lock()
	index := 0
	if b.independent {
		index = 1
	}
	browser := b.setup.browsers[index]
	b.setup.mu.Unlock()
	if browser == nil {
		return nil, errors.New("qualification: default Broker 0 hooks are not initialized")
	}
	return browser.Browse(ctx, group)
}

func (s *defaultBroker0Setup) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for index := len(s.browsers) - 1; index >= 0; index-- {
		if s.browsers[index] != nil {
			errs = append(errs, s.browsers[index].Close())
		}
	}
	if s.native != nil {
		errs = append(errs, s.native.Close())
	}
	if s.connection != nil {
		errs = append(errs, s.connection.Close())
	}
	if s.directory != "" {
		errs = append(errs, os.RemoveAll(s.directory))
	}
	return errors.Join(errs...)
}
