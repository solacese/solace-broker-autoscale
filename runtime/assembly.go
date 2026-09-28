package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/integration"
	"github.com/solacese/solace-workload-balancer/policy"
	"github.com/solacese/solace-workload-balancer/semp"
)

// ControllerAssembler exposes construction seams for focused tests. Production
// uses AMQP 1.0 for Broker 0 control and SEMP for managed data resources.
type NativeControlPublisher interface {
	broker0.NativePersistentPublisher
	Close() error
}

type ControllerAssembler struct {
	Connect              func(context.Context, config.Config, Credentials) (*AMQPControlConnection, error)
	NewPublisher         func(*AMQPControlConnection, time.Duration) (NativeControlPublisher, error)
	NewSEMPClient        func(config.DataBroker, BrokerCredentials) (SEMPQueueClient, error)
	BindInbox            func(*AMQPControlConnection, broker0.ParticipantQueue, time.Duration) (broker0.DurableReceiver, error)
	BindSnapshotRequests func(context.Context, *AMQPControlConnection, broker0.ParticipantQueue) (broker0.SnapshotRequestReceiver, error)
	NewPolicy            func(config.Config, *controller.Controller, *MembershipCatalog, map[string]SEMPQueueClient, *CommandPublisher) (PolicyCycle, error)
}

func DefaultControllerAssembler() ControllerAssembler {
	return ControllerAssembler{
		Connect: func(ctx context.Context, cfg config.Config, credentials Credentials) (*AMQPControlConnection, error) {
			return ConnectAMQPBroker0(ctx, cfg.Control.AMQPEndpoint, credentials.Control.SMFUsername, credentials.Control.SMFPassword, cfg.Runtime.Controller)
		},
		NewPublisher: func(connection *AMQPControlConnection, _ time.Duration) (NativeControlPublisher, error) {
			return NewAMQPNativePublisher(connection)
		},
		NewSEMPClient: func(broker config.DataBroker, credentials BrokerCredentials) (SEMPQueueClient, error) {
			return semp.NewClient(semp.ClientOptions{BaseURL: broker.SEMPEndpoint, Username: credentials.SEMPUsername, Password: credentials.SEMPPassword})
		},
		BindInbox: func(connection *AMQPControlConnection, binding broker0.ParticipantQueue, _ time.Duration) (broker0.DurableReceiver, error) {
			return (AMQPDurableReceiverFactory{Session: connection.Session}).BindDurable(context.Background(), binding)
		},
		BindSnapshotRequests: func(ctx context.Context, connection *AMQPControlConnection, binding broker0.ParticipantQueue) (broker0.SnapshotRequestReceiver, error) {
			return BindAMQPSnapshotRequests(ctx, connection.Session, binding)
		},
		NewPolicy: newProductionPolicy,
	}
}

// AssembleController creates no broker or cloud resources. Every queue/topic it
// opens must already exist and be protected by operator-managed ACLs.
func AssembleController(ctx context.Context, cfg config.Config, credentials Credentials, logger *slog.Logger, assembler ControllerAssembler) (process *OwnedProcess, err error) {
	if cfg.Cloud.Enabled {
		return nil, errors.New("runtime: cloud provisioning is disabled in the controller process; provision resources explicitly")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if assembler.Connect == nil || assembler.NewPublisher == nil || assembler.NewSEMPClient == nil || assembler.BindInbox == nil || assembler.BindSnapshotRequests == nil || assembler.NewPolicy == nil {
		return nil, errors.New("runtime: controller assembler dependencies are required")
	}

	connection, err := assembler.Connect(ctx, cfg, credentials)
	if err != nil {
		return nil, err
	}
	owners := &CloseGroup{Closers: []interface{ Close() error }{connection}}
	defer func() {
		if err != nil {
			err = errors.Join(err, owners.Close())
		}
	}()

	nativePublisher, err := assembler.NewPublisher(connection, cfg.Publisher.PublishTimeout.Duration)
	if err != nil {
		return nil, err
	}
	owners.Closers = append(owners.Closers, nativePublisher)

	operationDirectory := cfg.Persistence.ControllerState + ".operations"
	publishOperations, err := broker0.OpenJSONOperationStore(filepath.Join(operationDirectory, "publish.json"))
	if err != nil {
		return nil, err
	}
	inboxOperations, err := broker0.OpenJSONOperationStore(filepath.Join(operationDirectory, "inbox.json"))
	if err != nil {
		return nil, err
	}

	names, err := control.NewManagedNames(cfg.Namespace)
	if err != nil {
		return nil, err
	}
	publisher, err := broker0.NewPersistentControlPublisherWithTargets(nativePublisher, publishOperations,
		broker0.DestinationResolverFunc(func(kind broker0.Kind, group string) (string, error) {
			switch kind {
			case broker0.KindMembershipSnapshot:
				return names.SnapshotTopic(group)
			case broker0.KindRegistration:
				return names.RegistrationTopic(group)
			case broker0.KindAcknowledgement:
				return names.AcknowledgementTopic(group)
			case broker0.KindTelemetry:
				return names.TelemetryTopic(group)
			default:
				return "", fmt.Errorf("runtime: unsupported shared Broker 0 kind %q", kind)
			}
		}), broker0.TargetDestinationResolverFunc(func(kind broker0.Kind, group string, role control.ParticipantRole, participant string) (string, error) {
			switch kind {
			case broker0.KindCommand:
				return names.CommandTopic(group, role, participant)
			case broker0.KindRegistration:
				return names.RegistrationTopic(group)
			default:
				return "", fmt.Errorf("runtime: unsupported participant Broker 0 kind %q", kind)
			}
		}))
	if err != nil {
		return nil, err
	}
	fanout := &SnapshotFanoutPublisher{Publisher: publisher, Names: names}
	catalog := NewMembershipCatalog()
	deadlines := make(map[string]time.Duration, len(cfg.Groups))
	for _, group := range cfg.Groups {
		deadlines[group.ID] = group.Handover.ReadinessTimeout.Duration
	}
	commandPublisher := &CommandPublisher{Publisher: fanout, Catalog: catalog, Deadlines: deadlines}

	fenceTargets := make(map[string]BrokerTarget, len(cfg.DataBrokers))
	messageVPNs := make(map[string]string, len(cfg.DataBrokers))
	for _, broker := range cfg.DataBrokers {
		client, clientErr := assembler.NewSEMPClient(broker, credentials.DataBrokers[broker.ID])
		if clientErr != nil {
			return nil, fmt.Errorf("runtime: create SEMP client for %q: %w", broker.ID, clientErr)
		}
		fenceTargets[broker.ID] = BrokerTarget{Client: client}
		messageVPNs[broker.ID] = broker.MessageVPN
	}
	resourceClients := queueClients(fenceTargets)
	managedResources := &ManagedEpochResources{Config: cfg, Clients: resourceClients}

	store := controller.JSONStore{Path: cfg.Persistence.ControllerState}
	var coordinator *controller.Controller
	fence := SEMPBrokerFence{Brokers: fenceTargets, Queues: FenceQueueResolverFunc(func(_ context.Context, request controller.FenceRequest, broker controller.Broker) ([]ManagedQueue, error) {
		return resolveManagedFenceQueues(coordinator, catalog, messageVPNs, request, broker)
	})}
	coordinator, err = controller.Open(store, fence, commandPublisher, controller.Options{
		TelemetryFreshness:        cfg.Staleness.TelemetryMaxAge.Duration,
		ReadinessFreshness:        cfg.Staleness.ParticipantMaxAge.Duration,
		FenceFreshness:            cfg.Staleness.TelemetryMaxAge.Duration,
		DrainGraceByGroup:         drainGraceByGroup(cfg.Groups),
		TelemetryFreshnessByGroup: telemetryFreshnessByGroup(cfg.Groups),
		PhaseTimeoutByGroup:       phaseTimeoutByGroup(cfg.Groups),
	})
	if err != nil {
		return nil, err
	}
	commandPublisher.RestoreRequirements(coordinator.Snapshot())

	if err := BootstrapMembership(ctx, cfg, coordinator.Snapshot(), fanout, catalog, managedResources, coordinator); err != nil {
		return nil, err
	}

	bindings, err := controllerInboxBindings(cfg, names)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(bindings)+len(cfg.Groups)*4)
	for _, binding := range bindings {
		receiver, bindErr := assembler.BindInbox(connection, binding, 500*time.Millisecond)
		if bindErr != nil {
			return nil, fmt.Errorf("runtime: bind controller inbox %q: %w", binding.Queue, bindErr)
		}
		inbox, inboxErr := broker0.NewControllerInbox(receiver, &registrationAwareController{Controller: coordinator, Config: cfg, Catalog: catalog}, inboxOperations)
		if inboxErr != nil {
			_ = receiver.Close()
			return nil, inboxErr
		}
		owners.Closers = append(owners.Closers, inbox)
		components = append(components, inbox)
	}

	for _, group := range cfg.Groups {
		for _, scoped := range []struct {
			role        control.ParticipantRole
			participant string
		}{
			{control.RolePublisher, first(group.RequiredPublishers)},
			{control.RoleSubscriber, first(group.SubscriberIdentities())},
		} {
			if scoped.participant == "" {
				continue
			}
			queue, nameErr := names.SnapshotRequestQueue(group.ID, scoped.role, scoped.participant)
			if nameErr != nil {
				return nil, nameErr
			}
			topic, nameErr := names.SnapshotRequestTopic(group.ID, scoped.role, scoped.participant)
			if nameErr != nil {
				return nil, nameErr
			}
			receiver, bindErr := assembler.BindSnapshotRequests(ctx, connection, broker0.ParticipantQueue{Participant: scoped.participant, Principal: scoped.participant, Role: scoped.role, Group: group.ID, Kind: broker0.KindSnapshotRequest, Queue: queue, Topic: topic})
			if bindErr != nil {
				return nil, bindErr
			}
			server := &broker0.SnapshotRequestServer{Receiver: receiver, Publisher: integration.AMQPSnapshotResponsePublisher{Session: connection.Session}, Source: coordinator,
				Authorize: func(request control.SnapshotRequest, principal string) error {
					if request.Namespace != cfg.Namespace || request.Participant != principal {
						return errors.New("runtime: unauthorized snapshot request")
					}
					return nil
				},
				Replies: broker0.SnapshotReplyResolverFunc(func(request control.SnapshotRequest) (string, error) {
					return names.SnapshotReplyQueue(request.Group, request.Role, request.Participant)
				}),
			}
			owners.Closers = append(owners.Closers, server)
			components = append(components, server)
		}
	}

	policyCycle, err := assembler.NewPolicy(cfg, coordinator, catalog, resourceClients, commandPublisher)
	if err != nil {
		return nil, err
	}
	if policyCycle == nil {
		return nil, errors.New("runtime: policy assembler returned nil")
	}
	components = append(components, policyComponent{cycle: policyCycle})
	triggers := make(chan Trigger, max(1, len(cfg.Groups)))
	components = append(components, &SEMPDrainMonitor{
		Controller: coordinator, Brokers: fenceTargets, MessageVPNs: messageVPNs,
		Triggers: triggers, Interval: boundedReconcileInterval(cfg.Groups),
		Participant: cfg.Runtime.Controller, Logger: logger,
	})
	controllerProcess, err := NewControllerProcess(coordinator, components, triggers, ControllerOptions{
		Groups:                  configuredGroupIDs(cfg.Groups),
		ReconcileInterval:       boundedReconcileInterval(cfg.Groups),
		ActionTimeout:           cfg.Staleness.TelemetryMaxAge.Duration,
		ShutdownTimeout:         cfg.Publisher.ShutdownTimeout.Duration,
		Logger:                  logger,
		Cleaner:                 managedEpochCleaner{Resources: managedResources, Catalog: catalog},
		Catalog:                 catalog,
		RollbackFenceReversible: true,
	})
	if err != nil {
		return nil, err
	}
	// ControllerProcess now owns its components. Keep only the browser, publisher,
	// and connection in the outer resource group so receivers close exactly once.
	owners.Closers = owners.Closers[:2]
	return &OwnedProcess{Process: controllerProcess, Close: owners}, nil
}

func resolveManagedFenceQueues(coordinator *controller.Controller, catalog *MembershipCatalog, messageVPNs map[string]string, request controller.FenceRequest, broker controller.Broker) ([]ManagedQueue, error) {
	if coordinator != nil {
		if state, ok := coordinator.Group(request.Group); ok && !state.Completed {
			if request.TransitionID != state.Spec.ID {
				return nil, fmt.Errorf("runtime: fence request transition %q conflicts with durable transition %q for group %q", request.TransitionID, state.Spec.ID, request.Group)
			}
			resources := state.Spec.CurrentResources
			if request.Epoch == state.Spec.ToEpoch {
				resources = state.Spec.ProposedResources
			} else if request.Epoch != state.Spec.FromEpoch {
				return nil, fmt.Errorf("runtime: fence request epoch %d conflicts with durable transition for group %q", request.Epoch, request.Group)
			}
			return exactManagedFenceQueues(messageVPNs, request, broker, resources)
		}
	}
	snapshot, ok := catalog.Get(request.Group)
	if !ok {
		return nil, ErrNoRetainedMembership
	}
	resources := snapshot.CurrentResources
	if request.Epoch != snapshot.Epoch {
		resources = snapshot.ProposedResources
	}
	return exactManagedFenceQueues(messageVPNs, request, broker, resources)
}

func exactManagedFenceQueues(messageVPNs map[string]string, request controller.FenceRequest, broker controller.Broker, resources []control.EpochResourceIdentity) ([]ManagedQueue, error) {
	vpn := messageVPNs[broker.ID]
	if vpn == "" {
		return nil, fmt.Errorf("runtime: no message VPN for broker %q", broker.ID)
	}
	var queues []ManagedQueue
	for _, resource := range resources {
		if resource.Epoch == request.Epoch && resource.BrokerID == broker.ID {
			queues = append(queues, ManagedQueue{MessageVPN: vpn, Name: resource.QueueName})
		}
	}
	if len(queues) == 0 {
		return nil, fmt.Errorf("runtime: no managed epoch resource for group %q broker %q epoch %d", request.Group, broker.ID, request.Epoch)
	}
	return queues, nil
}

func controllerInboxBindings(cfg config.Config, names control.ManagedNames) ([]broker0.ParticipantQueue, error) {
	var bindings []broker0.ParticipantQueue
	add := func(group string, role control.ParticipantRole, participant string, kind broker0.Kind) error {
		credential, ok := cfg.ParticipantControlCredential(participant)
		if !ok {
			return fmt.Errorf("runtime: no Broker 0 credential for controller inbox participant %q", participant)
		}
		var queue, topic string
		var err error
		switch kind {
		case broker0.KindRegistration:
			queue, err = names.RegistrationQueueFor(group, role, participant)
			if err == nil {
				topic, err = names.RegistrationTopicFor(group, role, participant)
			}
		case broker0.KindAcknowledgement:
			queue, err = names.AcknowledgementQueueFor(group, role, participant)
			if err == nil {
				topic, err = names.AcknowledgementTopicFor(group, role, participant)
			}
		case broker0.KindTelemetry:
			queue, err = names.TelemetryQueueFor(group, role, participant)
			if err == nil {
				topic, err = names.TelemetryTopicFor(group, role, participant)
			}
		default:
			return fmt.Errorf("runtime: unsupported controller inbox kind %q", kind)
		}
		if err != nil {
			return err
		}
		bindings = append(bindings, broker0.ParticipantQueue{
			Participant: participant, Principal: credential.Principal, Role: role,
			Group: group, Kind: kind, Queue: queue, Topic: topic,
		})
		return nil
	}
	for _, group := range cfg.Groups {
		for _, participant := range group.RequiredPublishers {
			if err := add(group.ID, control.RolePublisher, participant, broker0.KindRegistration); err != nil {
				return nil, err
			}
			if err := add(group.ID, control.RolePublisher, participant, broker0.KindAcknowledgement); err != nil {
				return nil, err
			}
		}
		for _, participant := range group.SubscriberIdentities() {
			if err := add(group.ID, control.RoleSubscriber, participant, broker0.KindRegistration); err != nil {
				return nil, err
			}
			if err := add(group.ID, control.RoleSubscriber, participant, broker0.KindAcknowledgement); err != nil {
				return nil, err
			}
		}
		for _, participant := range cfg.Runtime.Brokers {
			if err := add(group.ID, control.RoleBroker, participant, broker0.KindTelemetry); err != nil {
				return nil, err
			}
		}
		for _, participant := range cfg.Runtime.Observers {
			if err := add(group.ID, control.RoleObserver, participant, broker0.KindTelemetry); err != nil {
				return nil, err
			}
		}
	}
	return bindings, nil
}

func newProductionPolicy(cfg config.Config, coordinator *controller.Controller, membership *MembershipCatalog, clients map[string]SEMPQueueClient, publisher *CommandPublisher) (PolicyCycle, error) {
	if len(cfg.CapacityProfiles) == 0 {
		return nil, ErrCapacityPolicyUnavailable
	}
	catalog, err := cfg.PolicyProfileCatalog()
	if err != nil {
		return nil, fmt.Errorf("runtime: build capacity profile catalog: %w", err)
	}
	engine, err := policy.NewEngine(catalog, policy.EngineOptions{MaxConcurrentTransitions: maximumConcurrentTransitions(cfg.Groups)})
	if err != nil {
		return nil, err
	}
	policyStore := JSONPolicyEngineStateStore{Path: policyEngineStatePath(cfg.Persistence.ControllerState)}
	persisted, err := policyStore.Load()
	if err != nil {
		return nil, fmt.Errorf("runtime: restore policy state: %w", err)
	}
	restoreAt := time.Now().UTC()
	if err := validateConfiguredPolicyState(persisted, cfg.Groups); err != nil {
		return nil, fmt.Errorf("runtime: restore policy state: %w", err)
	}
	if err := engine.RestoreState(persisted, restoreAt); err != nil {
		return nil, fmt.Errorf("runtime: restore policy state: %w", err)
	}
	scaleIn := &ConservativeScaleInProofProvider{Config: cfg, Controller: coordinator, Catalog: membership, Profiles: catalog}
	state := &ControllerPolicyState{Config: cfg, Controller: coordinator, Catalog: membership, ScaleIn: scaleIn}
	targets := semp.PolicyTargetProviderFunc(func() ([]semp.BrokerTarget, []semp.GroupTarget, error) {
		inventory, groups, stateErr := state.PolicyState()
		if stateErr != nil {
			return nil, nil, stateErr
		}
		brokers := make([]semp.BrokerTarget, 0, len(inventory))
		for _, broker := range inventory {
			configured, ok := configuredBroker(cfg.DataBrokers, broker.ID)
			if !ok {
				return nil, nil, fmt.Errorf("runtime: missing data broker %q", broker.ID)
			}
			client, ok := clients[broker.ID].(semp.MonitorClient)
			if !ok {
				return nil, nil, fmt.Errorf("runtime: SEMP client for %q cannot collect capacity telemetry", broker.ID)
			}
			brokers = append(brokers, semp.BrokerTarget{ID: broker.ID, MessageVPN: configured.MessageVPN, ServiceClass: broker.ServiceClass, BrokerVersion: broker.BrokerVersion, Client: client})
		}
		groupTargets := make([]semp.GroupTarget, 0)
		for _, group := range groups {
			snapshot, ok := membership.Get(group.ID)
			if !ok {
				return nil, nil, fmt.Errorf("runtime: group %q has no authoritative membership", group.ID)
			}
			for _, brokerID := range group.Membership {
				found := false
				for _, resource := range snapshot.CurrentResources {
					if resource.BrokerID == brokerID && resource.Epoch == snapshot.Epoch {
						groupTargets = append(groupTargets, semp.GroupTarget{GroupID: group.ID, BrokerID: brokerID, Queue: resource.QueueName})
						found = true
					}
				}
				if !found {
					return nil, nil, fmt.Errorf("runtime: group %q has no current resources for broker %q", group.ID, brokerID)
				}
			}
		}
		return brokers, groupTargets, nil
	})
	return &PolicyRuntime{
		Collector: semp.PolicyCollector{Targets: targets}, State: state, Engine: engine, Store: policyStore,
		Applier:  &RecommendationController{Controller: coordinator, Catalog: membership, Config: cfg, Publisher: publisher, Resources: &ManagedEpochResources{Config: cfg, Clients: clients}, ScaleIn: scaleIn},
		Interval: boundedPolicyInterval(cfg.Groups),
	}, nil
}

func validateConfiguredPolicyState(state policy.EngineState, groups []config.ScalingGroup) error {
	configured := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		configured[group.ID] = struct{}{}
	}
	for group := range state.PressureSince {
		if _, ok := configured[group]; !ok {
			return fmt.Errorf("persisted pressure references unknown group %q", group)
		}
	}
	return nil
}

func configuredBroker(brokers []config.DataBroker, id string) (config.DataBroker, bool) {
	for _, broker := range brokers {
		if broker.ID == id {
			return broker, true
		}
	}
	return config.DataBroker{}, false
}

func maximumConcurrentTransitions(groups []config.ScalingGroup) int {
	// Each validated group permits one active transition. Using the group count as
	// the global bound preserves independent-group concurrency without implying
	// that a controller can coordinate multiple transitions within one group.
	return max(1, len(groups))
}

func boundedPolicyInterval(groups []config.ScalingGroup) time.Duration {
	interval := 15 * time.Second
	for _, group := range groups {
		if value := group.Handover.TelemetryMaxAge.Duration / 2; value > 0 && value < interval {
			interval = value
		}
	}
	return interval
}

func queueClients(targets map[string]BrokerTarget) map[string]SEMPQueueClient {
	result := make(map[string]SEMPQueueClient, len(targets))
	for id, target := range targets {
		result[id] = target.Client
	}
	return result
}

func boundedReconcileInterval(groups []config.ScalingGroup) time.Duration {
	interval := 5 * time.Second
	for _, group := range groups {
		if timeout := group.Handover.TelemetryMaxAge.Duration / 2; timeout > 0 && timeout < interval {
			interval = timeout
		}
	}
	return interval
}

// Registration is useful for liveness diagnostics, but transition membership is
// always the explicit configured set. Unknown identities and role/group drift are
// rejected before their durable transport delivery is acknowledged.
type registrationAwareController struct {
	Controller *controller.Controller
	Config     config.Config
	Catalog    *MembershipCatalog
}

func (h *registrationAwareController) Acknowledge(ack controller.Acknowledgement) error {
	return h.Controller.Acknowledge(ack)
}
func (h *registrationAwareController) Observe(sample controller.Telemetry) error {
	return h.Controller.Observe(sample)
}
func (h *registrationAwareController) Register(registration control.RegistrationEnvelope) error {
	group, ok := configuredGroup(h.Config.Groups, registration.Group)
	if !ok || registration.Namespace != h.Config.Namespace || registration.LibraryVersion != group.CustomerLibrary {
		return controller.ErrStaleMessage
	}
	snapshot, ok := h.Catalog.Get(registration.Group)
	if !ok || registration.Epoch != snapshot.Epoch || registration.Phase != snapshot.Phase || (snapshot.Transition == nil) != (registration.TransitionID == "") || snapshot.Transition != nil && registration.TransitionID != snapshot.Transition.ID {
		return controller.ErrStaleMessage
	}
	var allowed bool
	switch registration.Role {
	case control.RolePublisher:
		allowed = slices.Contains(group.RequiredPublishers, registration.Participant)
	case control.RoleSubscriber:
		consumerSet, configured := group.ConsumerSetForSubscriber(registration.Participant)
		allowed = configured && registration.ConsumerSet == consumerSet
	}
	if !allowed {
		return controller.ErrUnknownParticipant
	}
	return nil
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
