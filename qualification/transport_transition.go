package qualification

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
	"github.com/solacese/solace-workload-balancer/routing"
	swlbruntime "github.com/solacese/solace-workload-balancer/runtime"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

// TransportTransitionDriver exercises the controller and independently running
// runtime participant components through the provisioned Broker 0 queues. The
// Cloud qualification service credential is deliberately shared by the
// components; queue bindings, rather than distinct principals, authenticate the
// qualification identities.
type TransportTransitionDriver struct {
	Publisher          *broker0.PersistentControlPublisher
	ControlConnection  *swlbruntime.SMFConnection
	Browser            broker0.Browser
	IndependentBrowser broker0.Browser
	Queues             QueueAPIFactory
	DataPlane          DataPlane
	Options            Options
}

func (d TransportTransitionDriver) RunTransition(ctx context.Context, plan ProvisionedPlan, observer Observer) (runErr error) {
	flight, baggage, err := transitionGroups(plan)
	if err != nil {
		return err
	}
	if d.Publisher == nil || d.ControlConnection == nil || d.Browser == nil || d.IndependentBrowser == nil || d.Queues == nil || d.DataPlane == nil {
		return errors.New("qualification: transported transition driver dependencies are required")
	}
	options := d.Options.withDefaults()
	clients, err := transitionQueueClients(plan.Bundles, d.Queues)
	if err != nil {
		return err
	}
	stateDirectory, err := os.MkdirTemp("", "swlb-qualification-transport-transition-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stateDirectory)

	catalog := swlbruntime.NewMembershipCatalog()
	commandPublisher := &swlbruntime.CommandPublisher{
		Publisher: d.Publisher,
		Catalog:   catalog,
		Deadlines: map[string]time.Duration{FlightGroup: options.ReceiveTimeout, BaggageGroup: options.ReceiveTimeout},
		Now:       options.Now,
	}
	fence := transitionFence{groups: map[string]GroupPlan{flight.Group: flight}, clients: clients, now: options.Now}
	store := controller.JSONStore{Path: filepath.Join(stateDirectory, "controller.json")}
	controllerOptions := controller.Options{
		TelemetryFreshness: time.Minute, ReadinessFreshness: time.Minute,
		FenceFreshness: time.Minute, ZeroGrace: options.DrainGrace,
		PhaseTimeout: options.ReceiveTimeout, Now: options.Now,
	}
	opened, err := controller.Open(store, fence, commandPublisher, controllerOptions)
	if err != nil {
		return err
	}
	target := &transportControllerTarget{controller: opened}
	coordinator := &restartableController{
		Controller: opened, store: store, fence: fence, publisher: commandPublisher,
		options: controllerOptions, restarted: make(map[string]bool), target: target,
	}

	inboxCtx, cancelInboxes := context.WithCancel(ctx)
	inboxErrs := make(chan error, 3)
	var inboxes []*broker0.ControllerInbox
	for _, binding := range []broker0.ParticipantQueue{
		mustBroker0Binding(plan, FlightGroup, plan.Participants.Publisher, broker0.KindAcknowledgement),
		mustBroker0Binding(plan, FlightGroup, plan.Participants.Subscriber, broker0.KindAcknowledgement),
		mustBroker0Binding(plan, FlightGroup, plan.Participants.Observer, broker0.KindTelemetry),
	} {
		receiver, bindErr := swlbruntime.BindControllerInbox(d.ControlConnection.Service, binding, 100*time.Millisecond)
		if bindErr != nil {
			cancelInboxes()
			return bindErr
		}
		operations, openErr := broker0.OpenJSONOperationStore(filepath.Join(stateDirectory, "controller-inbox-"+string(binding.Kind)+"-"+string(binding.Role)+".json"))
		if openErr != nil {
			_ = receiver.Close()
			cancelInboxes()
			return openErr
		}
		inbox, inboxErr := broker0.NewControllerInbox(receiver, target, operations)
		if inboxErr != nil {
			_ = receiver.Close()
			cancelInboxes()
			return inboxErr
		}
		inboxes = append(inboxes, inbox)
		go func() { inboxErrs <- inbox.Run(inboxCtx) }()
	}
	defer func() {
		cancelInboxes()
		for _, inbox := range inboxes {
			runErr = errors.Join(runErr, inbox.Close())
		}
	}()

	probeLedger := newObservationLedger()
	combinedObserver := ObserverFunc(func(observation DeliveryObservation) error {
		if err := probeLedger.Observe(observation); err != nil {
			return err
		}
		if observer != nil {
			return observer.Observe(observation)
		}
		return nil
	})
	participants, err := newTransportParticipants(ctx, plan, d.DataPlane, d.Publisher, combinedObserver, stateDirectory, options)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, participants.Close()) }()

	initialFlight := activeSnapshotForEpoch(plan.Namespace, flight, flight.Epochs[0], 1)
	initialBaggage := activeSnapshotForEpoch(plan.Namespace, baggage, baggage.Epochs[0], 1)
	for _, snapshot := range []control.MembershipSnapshot{initialFlight, initialBaggage} {
		if err := d.Publisher.PublishSnapshot(ctx, snapshot); err != nil {
			return err
		}
		if err := catalog.Put(snapshot); err != nil {
			return err
		}
	}
	if err := participants.WaitSnapshots(ctx, initialFlight, initialBaggage); err != nil {
		return err
	}
	// A qualification restart closes all native connections and reconstructs both
	// participants from their durable operation stores and publisher outbox.
	if err := participants.Restart(ctx); err != nil {
		return fmt.Errorf("qualification: restart transported participants: %w", err)
	}
	if err := participants.WaitSnapshots(ctx, initialFlight, initialBaggage); err != nil {
		return err
	}

	baggageBefore, err := epochIngressStates(ctx, clients, baggage.Epochs[0])
	if err != nil {
		return err
	}
	for index := 1; index < len(flight.Epochs); index++ {
		source, targetEpoch := flight.Epochs[index-1], flight.Epochs[index]
		if err := driveTransportTransition(ctx, coordinator, commandPublisher, participants, probeLedger, d.Publisher, plan, flight, baggage.Epochs[0], flight.Epochs[:index], source, targetEpoch, index, uint64(2+(index-1)*5), clients, options, inboxErrs); err != nil {
			return err
		}
	}
	final := activeSnapshotForEpoch(plan.Namespace, flight, flight.Epochs[len(flight.Epochs)-1], uint64(1+(len(flight.Epochs)-1)*5))
	if err := assertRetainedActive(ctx, d.Browser, d.IndependentBrowser, final, options.DrainPollInterval); err != nil {
		return err
	}
	baggageAfter, err := epochIngressStates(ctx, clients, baggage.Epochs[0])
	if err != nil {
		return err
	}
	if !mapsEqual(baggageBefore, baggageAfter) {
		return errors.New("qualification: baggage ingress changed during transported flight transition")
	}
	return nil
}

func driveTransportTransition(ctx context.Context, coordinator *restartableController, commandPublisher *swlbruntime.CommandPublisher, participants *transportParticipants, ledger *observationLedger, controlPublisher *broker0.PersistentControlPublisher, plan ProvisionedPlan, group GroupPlan, baggage EpochPlan, oldSources []EpochPlan, source, target EpochPlan, sequence int, revision uint64, clients map[string]QueueAPI, options Options, inboxErrs <-chan error) error {
	transitionID := fmt.Sprintf("%s-flight-%d-%d", plan.RunID, source.Epoch, target.Epoch)
	spec := controller.TransitionSpec{
		ID: transitionID, Namespace: plan.Namespace, LibraryVersion: qualificationLibraryVersion,
		Group: group.Group, Revision: revision, FromEpoch: source.Epoch, ToEpoch: target.Epoch,
		Current: transitionBrokers(source), Proposed: transitionBrokers(target),
		Queue: logicalQueue(group.Group), Destination: logicalDestination(group.Group),
		CurrentResources: epochResources(source), ProposedResources: epochResources(target),
		HashContract: group.Contract, Algorithm: control.AlgorithmSHA256BigEndianModulo,
		DrainGrace: options.DrainGrace,
		RoleRequirements: map[controller.Phase][]controller.ParticipantRequirement{
			controller.PhasePrepare:  {{Participant: plan.Participants.Subscriber, Role: control.RoleSubscriber}},
			controller.PhasePause:    {{Participant: plan.Participants.Publisher, Role: control.RolePublisher}},
			controller.PhaseDrain:    {{Participant: plan.Participants.Subscriber, Role: control.RoleSubscriber}},
			controller.PhaseActivate: {{Participant: plan.Participants.Publisher, Role: control.RolePublisher}, {Participant: plan.Participants.Subscriber, Role: control.RoleSubscriber}},
		},
	}
	commandPublisher.SetRequirements(group.Group, spec.RoleRequirements)
	if _, err := coordinator.Begin(spec); err != nil {
		return err
	}
	var buffered *transitionProbe
	baggageSent := false
	activeVerified := false
	for attempts := 0; attempts < 512; attempts++ {
		select {
		case err := <-inboxErrs:
			if err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("qualification: Broker 0 controller inbox stopped: %w", err)
			}
		default:
		}
		state, _ := coordinator.Group(group.Group)
		if state.Completed {
			return coordinator.requireRestarts(transitionID)
		}
		if err := coordinator.restartAtBoundary(state); err != nil {
			return err
		}
		state, _ = coordinator.Group(group.Group)
		if state.Phase == controller.PhasePause && state.PausePublished && buffered == nil {
			status := participants.Publisher.Dispatcher.Status(group.Group)
			if status.Paused && status.TransitionID == transitionID && status.Quiescent {
				probe, err := acceptTransitionProbe(participants.Publisher.Publisher, ledger, plan.RunID, target, sequence, "flight", options)
				if err != nil {
					return err
				}
				if probe.receipt.State != outbox.StateUnassigned {
					return errors.New("qualification: Flight probe was routed while transported publisher was paused")
				}
				buffered = &probe
			}
		}
		if buffered != nil && !baggageSent {
			if err := publishTransitionProbe(ctx, participants.Publisher.Publisher, participants.Publisher.Dispatcher, ledger, plan.RunID, baggage, sequence, "baggage", options); err != nil {
				return err
			}
			baggageSent = true
		}
		if state.Phase == controller.PhaseDrain && state.DrainPublished {
			statuses, _ := monitorEpochSources(ctx, clients, source)
			for _, broker := range source.BrokerIDs {
				if err := publishDrainTelemetry(ctx, controlPublisher, sourceTelemetry(plan.Namespace, group.Group, transitionID, plan.Participants.Observer, source, broker, statuses[broker], options.Now())); err != nil {
					return err
				}
			}
		}
		if state.Phase == controller.PhaseActivate && state.ActivatePublished && !activeVerified {
			status := participants.Publisher.Dispatcher.Status(group.Group)
			if status.Phase == control.PhaseActive && status.Epoch == target.Epoch {
				if err := assertOldSourcesFenced(ctx, clients, oldSources); err != nil {
					return err
				}
				if err := assertStalePublishesRejected(ctx, participants.StalePublisher(), oldSources, options.PublishTimeout); err != nil {
					return err
				}
				if buffered == nil {
					return errors.New("qualification: transported transition reached ACTIVE without paused Flight probe")
				}
				assigned, err := participants.Publisher.Publisher.Lookup(buffered.message.EventID)
				if err != nil {
					return err
				}
				if assigned.State != outbox.StateReady || assigned.Epoch != target.Epoch || !slices.Contains(target.BrokerIDs, assigned.Broker) {
					return errors.New("qualification: paused Flight probe was not assigned to transported active target epoch")
				}
				buffered.receipt.Epoch, buffered.receipt.Broker = assigned.Epoch, assigned.Broker
				if err := dispatchTransitionProbe(ctx, participants.Publisher.Dispatcher, ledger, *buffered, options.ReceiveTimeout); err != nil {
					return err
				}
				buffered = nil
				activeVerified = true
			}
		}
		if _, err := coordinator.Reconcile(ctx, group.Group); err != nil && !errors.Is(err, controller.ErrReadinessStale) {
			return err
		}
		if err := waitTransitionPoll(ctx, options.DrainPollInterval); err != nil {
			return err
		}
	}
	return errors.New("qualification: transported transition did not converge")
}

func publishDrainTelemetry(ctx context.Context, publisher *broker0.PersistentControlPublisher, sample controller.Telemetry) error {
	metric := func(value controller.Count) control.MetricCount {
		return control.MetricCount{Known: value.Known, Value: value.Value}
	}
	return publisher.PublishTelemetry(ctx, control.TelemetryEnvelope{
		Version: control.ProtocolVersion, MessageID: sample.MessageID, Namespace: sample.Namespace,
		Group: sample.Group, TransitionID: sample.TransitionID, Epoch: sample.Epoch, Phase: control.PhaseDrain,
		Participant: sample.Participant, Role: sample.Role, SourceBroker: sample.SourceBroker, SourceQueue: sample.SourceQueue,
		ObservedAt: sample.ObservedAt, Queued: metric(sample.Queued), Stored: metric(sample.Stored), Unacked: metric(sample.Unacked),
	})
}

// transportControllerTarget lets durable inbox goroutines follow a reconstructed
// controller without bypassing their Broker 0 delivery boundary.
type transportControllerTarget struct {
	mu         sync.RWMutex
	controller *controller.Controller
}

func (t *transportControllerTarget) set(value *controller.Controller) {
	t.mu.Lock()
	t.controller = value
	t.mu.Unlock()
}
func (t *transportControllerTarget) Acknowledge(ack controller.Acknowledgement) error {
	t.mu.RLock()
	value := t.controller
	t.mu.RUnlock()
	return value.Acknowledge(ack)
}
func (t *transportControllerTarget) Observe(sample controller.Telemetry) error {
	t.mu.RLock()
	value := t.controller
	t.mu.RUnlock()
	return value.Observe(sample)
}

type transportParticipants struct {
	plan             ProvisionedPlan
	plane            DataPlane
	publisher        *broker0.PersistentControlPublisher
	observer         Observer
	directory        string
	options          Options
	Publisher        *swlbruntime.PublisherParticipant
	publisherR       *transportParticipantRuntime
	subscriber       *transportParticipantRuntime
	publisherStatus  *transportSnapshotStatus
	subscriberStatus *transportSnapshotStatus
}

func newTransportParticipants(ctx context.Context, plan ProvisionedPlan, plane DataPlane, publisher *broker0.PersistentControlPublisher, observer Observer, directory string, options Options) (*transportParticipants, error) {
	p := &transportParticipants{plan: plan, plane: plane, publisher: publisher, observer: observer, directory: directory, options: options}
	if err := p.start(ctx); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func (p *transportParticipants) start(ctx context.Context) error {
	publisherStatus := newTransportSnapshotStatus()
	subscriberStatus := newTransportSnapshotStatus()
	publisher, publisherRuntime, err := p.buildPublisher(ctx, publisherStatus)
	if err != nil {
		return err
	}
	subscriberRuntime, err := p.buildSubscriber(ctx, subscriberStatus)
	if err != nil {
		_ = publisherRuntime.Close()
		return err
	}
	p.Publisher, p.publisherR, p.subscriber = publisher, publisherRuntime, subscriberRuntime
	p.publisherStatus, p.subscriberStatus = publisherStatus, subscriberStatus
	publisherRuntime.start(func(runCtx context.Context) error { return publisher.Run(runCtx) })
	subscriberRuntime.start(func(runCtx context.Context) error { return subscriberRuntime.process.Run(runCtx) })
	return nil
}

func (p *transportParticipants) Restart(ctx context.Context) error {
	if err := p.stop(); err != nil {
		return err
	}
	return p.start(ctx)
}

func (p *transportParticipants) Close() error { return p.stop() }

func (p *transportParticipants) stop() error {
	var err error
	if p.publisherR != nil {
		err = errors.Join(err, p.publisherR.Close())
	}
	if p.subscriber != nil {
		err = errors.Join(err, p.subscriber.Close())
	}
	p.publisherR, p.subscriber, p.Publisher = nil, nil, nil
	return err
}

func (p *transportParticipants) WaitSnapshots(ctx context.Context, snapshots ...control.MembershipSnapshot) error {
	for _, snapshot := range snapshots {
		for name, status := range map[string]*transportSnapshotStatus{"publisher": p.publisherStatus, "subscriber": p.subscriberStatus} {
			if err := status.wait(ctx, snapshot.ScalingGroup, snapshot.Revision, snapshot.Phase); err != nil {
				return fmt.Errorf("qualification: wait for %s snapshot: %w", name, err)
			}
		}
	}
	return nil
}

func (p *transportParticipants) StalePublisher() shimPublisher.AsyncBrokerPublisher {
	return p.publisherR.session.AsyncBrokerPublisher()
}

func (p *transportParticipants) buildPublisher(ctx context.Context, status *transportSnapshotStatus) (*swlbruntime.PublisherParticipant, *transportParticipantRuntime, error) {
	participant := p.plan.Participants.Publisher
	runtimeState, err := p.controlRuntime(ctx, participant, control.RolePublisher)
	if err != nil {
		return nil, nil, err
	}
	session, err := p.plane.Open(ctx, p.plan.Bundles, p.plan.Namespace+"-"+participant)
	if err != nil {
		_ = runtimeState.Close()
		return nil, nil, err
	}
	runtimeState.session = session
	store, err := outbox.Open(filepath.Join(p.directory, "publisher-outbox.db"), outbox.Limits{MaxMessages: p.options.OutboxMaxMessages, MaxBytes: p.options.OutboxMaxBytes})
	if err != nil {
		_ = runtimeState.Close()
		return nil, nil, err
	}
	runtimeState.closers = append(runtimeState.closers, store)
	shim, err := shimPublisher.New(shimPublisher.Config{Outbox: store, CustomerLibrary: customerLibrary(), Broker: session.BrokerPublisher(), Contracts: map[string]shimPublisher.Contract{
		FlightGroup: {HashContract: routing.FlightOperationsDomain, LibraryVersion: qualificationLibraryVersion}, BaggageGroup: {HashContract: routing.BaggageDomain, LibraryVersion: qualificationLibraryVersion},
	}})
	if err != nil {
		_ = runtimeState.Close()
		return nil, nil, err
	}
	dispatcher, err := shimPublisher.NewAsyncDispatcher(shim, session.AsyncBrokerPublisher(), shimPublisher.AsyncDispatcherConfig{MaxInFlight: p.options.MaxInFlight, MaxInFlightPerBroker: p.options.MaxInFlightPerBroker, AckTimeout: p.options.PublishTimeout, CompletionBatchSize: 1024, CompletionFlushPeriod: 10 * time.Millisecond})
	if err != nil {
		_ = runtimeState.Close()
		return nil, nil, err
	}
	runtimeState.contextClosers = append(runtimeState.contextClosers, dispatcher.Close)
	phases := swlbruntime.NewParticipantPhaseState()
	applier := &transportPublisherApplier{PublisherSnapshotApplier: swlbruntime.PublisherSnapshotApplier{Publisher: shim, Registration: &swlbruntime.ParticipantRegistration{Participant: participant, Role: control.RolePublisher, Publisher: runtimeState.publisher, Now: p.options.Now}, Phases: phases}, status: status}
	executor := swlbruntime.PublisherCommandExecutor{Dispatcher: dispatcher, Phases: phases}
	process, err := runtimeState.finish(ctx, applier, executor)
	if err != nil {
		_ = runtimeState.Close()
		return nil, nil, err
	}
	runtimeState.process = process
	return &swlbruntime.PublisherParticipant{Process: process, Publisher: shim, Dispatcher: dispatcher, BatchSize: 128, BatchWait: time.Hour}, runtimeState, nil
}

func (p *transportParticipants) buildSubscriber(ctx context.Context, status *transportSnapshotStatus) (*transportParticipantRuntime, error) {
	participant := p.plan.Participants.Subscriber
	runtimeState, err := p.controlRuntime(ctx, participant, control.RoleSubscriber)
	if err != nil {
		return nil, err
	}
	session, err := p.plane.Open(ctx, p.plan.Bundles, p.plan.Namespace+"-"+participant)
	if err != nil {
		_ = runtimeState.Close()
		return nil, err
	}
	runtimeState.session = session
	readiness := swlbruntime.NewSubscriberReadiness()
	factory := attributedFactory{inner: session.ConsumerFactory(), observer: p.observer, now: p.options.Now}
	shim, err := shimSubscriber.New(shimSubscriber.Config{Participant: participant, Library: customerLibrary(), Handler: shimSubscriber.HandlerFunc(func(context.Context, customer.MessageView) error { return nil }), Factory: factory, Reporter: readiness, Contracts: map[string]shimSubscriber.Contract{
		FlightGroup: {HashContract: routing.FlightOperationsDomain, LibraryVersion: qualificationLibraryVersion}, BaggageGroup: {HashContract: routing.BaggageDomain, LibraryVersion: qualificationLibraryVersion},
	}})
	if err != nil {
		_ = runtimeState.Close()
		return nil, err
	}
	runtimeState.contextClosers = append(runtimeState.contextClosers, shim.Close)
	phases := swlbruntime.NewParticipantPhaseState()
	base := &swlbruntime.SubscriberSnapshotApplier{Subscriber: shim, Registration: &swlbruntime.ParticipantRegistration{Participant: participant, Role: control.RoleSubscriber, ConsumerSet: QualificationConsumerSet, Publisher: runtimeState.publisher, Now: p.options.Now}, Phases: phases, Timeout: p.options.PublishTimeout}
	applier := &transportSubscriberApplier{SubscriberSnapshotApplier: base, status: status}
	executor := swlbruntime.SubscriberCommandExecutor{Readiness: readiness, Applier: base, Phases: phases}
	process, err := runtimeState.finish(ctx, applier, executor)
	if err != nil {
		_ = runtimeState.Close()
		return nil, err
	}
	runtimeState.process = process
	return runtimeState, nil
}

func (p *transportParticipants) controlRuntime(ctx context.Context, participant string, role control.ParticipantRole) (*transportParticipantRuntime, error) {
	bundle := p.plan.Bundles.Control
	connection, err := swlbruntime.ConnectBroker0WithTrustStore(ctx, bundle.SMFHosts[0], bundle.MessageVPN, bundle.ServiceCredential.Username, bundle.ServiceCredential.Password, participant, trustStorePath())
	if err != nil {
		return nil, err
	}
	native, err := swlbruntime.NewNativePublisher(connection.Service, p.options.PublishTimeout)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	dir := filepath.Join(p.directory, participant)
	publishOps, err := broker0.OpenJSONOperationStore(filepath.Join(dir, "publish.json"))
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		return nil, err
	}
	operations, err := broker0.OpenJSONOperationStore(filepath.Join(dir, "updates.json"))
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		return nil, err
	}
	commandOps, err := broker0.OpenJSONOperationStore(filepath.Join(dir, "commands.json"))
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		return nil, err
	}
	publisher, err := broker0.NewPersistentControlPublisherWithTargets(native, publishOps, broker0.DestinationResolverFunc(func(kind broker0.Kind, group string) (string, error) {
		return "", fmt.Errorf("qualification: participant cannot publish shared %s for %s", kind, group)
	}), broker0.TargetDestinationResolverFunc(func(kind broker0.Kind, group string, messageRole control.ParticipantRole, requested string) (string, error) {
		if requested != participant || messageRole != role {
			return "", errors.New("qualification: participant publication scope mismatch")
		}
		binding, err := broker0Binding(p.plan, group, participant, kind)
		if err != nil {
			return "", err
		}
		return binding.Topic, nil
	}))
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		return nil, err
	}
	browser, err := newTransportBrowser(p.plan, participant, p.options)
	if err != nil {
		_ = native.Close()
		_ = connection.Close()
		return nil, err
	}
	return &transportParticipantRuntime{plan: p.plan, participant: participant, role: role, connection: connection, native: native, publisher: publisher, browser: browser, operations: operations, commandOperations: commandOps, timeout: p.options.PublishTimeout}, nil
}

type transportParticipantRuntime struct {
	plan              ProvisionedPlan
	participant       string
	role              control.ParticipantRole
	connection        *swlbruntime.SMFConnection
	native            *swlbruntime.NativePublisher
	publisher         *broker0.PersistentControlPublisher
	browser           *broker0.JCSMPBrowser
	operations        broker0.OperationStore
	commandOperations broker0.OperationStore
	process           *swlbruntime.ParticipantProcess
	session           Session
	timeout           time.Duration
	cancel            context.CancelFunc
	done              chan error
	closers           []interface{ Close() error }
	contextClosers    []func(context.Context) error
}

func (r *transportParticipantRuntime) finish(ctx context.Context, applier broker0.SnapshotApplier, executor broker0.CommandExecutor) (*swlbruntime.ParticipantProcess, error) {
	groups := []config.ScalingGroup{{ID: FlightGroup}, {ID: BaggageGroup}}
	updates := broker0.DurableUpdateSubscriber{Factory: swlbruntime.NativeDurableReceiverFactory{Service: r.connection.Service, Poll: 100 * time.Millisecond}, Resolver: broker0.UpdateQueueResolverFunc(func(_ context.Context, group, participant string) (string, error) {
		binding, err := broker0Binding(r.plan, group, participant, broker0.KindMembershipSnapshot)
		return binding.Queue, err
	})}
	membership, err := swlbruntime.NewParticipant(updates, r.browser, applier, r.operations, groups, r.participant, 100*time.Millisecond, time.Minute, r.timeout)
	if err != nil {
		return nil, err
	}
	handler, err := broker0.NewCommandHandler(r.participant, r.role, r.plan.Namespace, []string{FlightGroup, BaggageGroup}, r.commandOperations, executor, r.publisher)
	if err != nil {
		return nil, err
	}
	components := map[string]swlbruntime.Component{"membership": participantProcessComponent{process: membership}}
	for _, group := range []string{FlightGroup, BaggageGroup} {
		binding, err := broker0Binding(r.plan, group, r.participant, broker0.KindCommand)
		if err != nil {
			return nil, err
		}
		receiver, err := (swlbruntime.NativeDurableReceiverFactory{Service: r.connection.Service, Poll: 100 * time.Millisecond}).BindDurable(ctx, binding)
		if err != nil {
			return nil, err
		}
		inbox, err := broker0.NewCommandInbox(receiver, handler)
		if err != nil {
			_ = receiver.Close()
			return nil, err
		}
		components["command:"+group] = inbox
	}
	return swlbruntime.NewParticipantProcess(components, r.timeout)
}

func (r *transportParticipantRuntime) start(run func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel, r.done = cancel, make(chan error, 1)
	go func() { r.done <- run(ctx) }()
}

func (r *transportParticipantRuntime) Close() error {
	if r == nil {
		return nil
	}
	if r.cancel != nil {
		r.cancel()
	}
	var result error
	if r.done != nil {
		select {
		case err := <-r.done:
			result = errors.Join(result, err)
		case <-time.After(r.timeout):
			result = errors.Join(result, errors.New("qualification: participant shutdown timed out"))
		}
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	for index := len(r.contextClosers) - 1; index >= 0; index-- {
		result = errors.Join(result, r.contextClosers[index](closeCtx))
	}
	for index := len(r.closers) - 1; index >= 0; index-- {
		result = errors.Join(result, r.closers[index].Close())
	}
	if r.session != nil {
		result = errors.Join(result, r.session.Close())
	}
	if r.browser != nil {
		result = errors.Join(result, r.browser.Close())
	}
	if r.native != nil {
		result = errors.Join(result, r.native.Close())
	}
	if r.connection != nil {
		result = errors.Join(result, r.connection.Close())
	}
	return result
}

type participantProcessComponent struct {
	process *swlbruntime.ParticipantProcess
}

func (c participantProcessComponent) Run(ctx context.Context) error { return c.process.Run(ctx) }
func (participantProcessComponent) Close() error                    { return nil }

func newTransportBrowser(plan ProvisionedPlan, participant string, options Options) (*broker0.JCSMPBrowser, error) {
	bundle := plan.Bundles.Control
	resolver := broker0.MembershipQueueResolverFunc(func(_ context.Context, group string) (string, error) {
		queue := plan.MembershipQueues[group]
		if queue == "" {
			return "", errors.New("qualification: membership queue is not in run-scoped resource plan")
		}
		return queue, nil
	})
	browserOptions := broker0.JCSMPBrowserOptions{JavaExecutable: options.JavaExecutable, JarPath: options.JarPath, Namespace: plan.Namespace, Host: bundle.SMFHosts[0], VPN: bundle.MessageVPN, Username: bundle.ServiceCredential.Username, Password: bundle.ServiceCredential.Password, Timeout: options.BrowserTimeout}
	if trustStore := os.Getenv("SWLB_JAVA_TRUST_STORE"); trustStore != "" {
		browserOptions.TrustStorePath = trustStore
	}
	_ = participant
	return broker0.NewJCSMPBrowser(resolver, browserOptions)
}

func mustBroker0Binding(plan ProvisionedPlan, group, participant string, kind broker0.Kind) broker0.ParticipantQueue {
	binding, err := broker0Binding(plan, group, participant, kind)
	if err != nil {
		panic(err)
	}
	return binding
}

func mapsEqual(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

type transportSnapshotStatus struct {
	mu      sync.Mutex
	values  map[string]control.MembershipSnapshot
	changed chan struct{}
}

func newTransportSnapshotStatus() *transportSnapshotStatus {
	return &transportSnapshotStatus{values: make(map[string]control.MembershipSnapshot), changed: make(chan struct{})}
}
func (s *transportSnapshotStatus) record(snapshot control.MembershipSnapshot) {
	s.mu.Lock()
	s.values[snapshot.ScalingGroup] = snapshot.Clone()
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}
func (s *transportSnapshotStatus) wait(ctx context.Context, group string, revision uint64, phase control.Phase) error {
	for {
		s.mu.Lock()
		snapshot := s.values[group]
		changed := s.changed
		s.mu.Unlock()
		if snapshot.Revision >= revision && snapshot.Phase == phase {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

type transportPublisherApplier struct {
	swlbruntime.PublisherSnapshotApplier
	status *transportSnapshotStatus
}

func (a *transportPublisherApplier) Apply(ctx context.Context, message broker0.Message) error {
	if err := a.PublisherSnapshotApplier.Apply(ctx, message); err != nil {
		return err
	}
	a.status.record(*message.Snapshot)
	return nil
}

type transportSubscriberApplier struct {
	*swlbruntime.SubscriberSnapshotApplier
	status *transportSnapshotStatus
}

func (a *transportSubscriberApplier) Apply(ctx context.Context, message broker0.Message) error {
	if err := a.SubscriberSnapshotApplier.Apply(ctx, message); err != nil {
		return err
	}
	a.status.record(*message.Snapshot)
	return nil
}
