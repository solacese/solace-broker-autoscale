package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/integration"
	"github.com/solacese/solace-workload-balancer/outbox"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

// ParticipantAssembler exposes native construction seams for command tests.
type ParticipantAssembler struct {
	ConnectControl func(context.Context, config.Config, Credentials, string) (*AMQPControlConnection, error)
	ConnectData    func(context.Context, config.DataBroker, BrokerCredentials, string) (*integration.AMQPConnection, error)
}

func DefaultParticipantAssembler() ParticipantAssembler {
	return ParticipantAssembler{
		ConnectControl: func(ctx context.Context, cfg config.Config, credentials Credentials, participant string) (*AMQPControlConnection, error) {
			if credentials.Control.Principal == "" {
				return nil, errors.New("runtime: participant Broker 0 principal is required")
			}
			return ConnectAMQPBroker0(ctx, cfg.Control.AMQPEndpoint, credentials.Control.SMFUsername, credentials.Control.SMFPassword, participant)
		},
		ConnectData: func(ctx context.Context, broker config.DataBroker, credentials BrokerCredentials, participant string) (*integration.AMQPConnection, error) {
			return integration.ConnectAMQP(ctx, broker.AMQPEndpoint, credentials.SMFUsername, credentials.SMFPassword, participant+"."+broker.ID, nil)
		},
	}
}

func AssemblePublisherParticipant(ctx context.Context, cfg config.Config, credentials Credentials, participant string, library customer.CustomerLibrary, assembler ParticipantAssembler) (result *PublisherParticipant, err error) {
	groups, err := participantAssemblyInputs(cfg, participant, control.RolePublisher, library, assembler)
	if err != nil {
		return nil, err
	}
	owners := &CloseGroup{}
	defer func() {
		if err != nil {
			err = errors.Join(err, owners.Close())
		}
	}()
	controlConnection, nativeControlPublisher, publisher, requester, updateSubscriber, updateOperations, commandOperations, err := assembleParticipantControl(ctx, cfg, credentials, participant, control.RolePublisher, groups, assembler, owners)
	if err != nil {
		return nil, err
	}
	_ = controlConnection

	pool, err := participantDataPool(cfg, groups, credentials, participant, assembler)
	if err != nil {
		return nil, err
	}
	owners.Closers = append(owners.Closers, pool)
	store, err := outbox.Open(filepath.Join(cfg.Persistence.PublisherOutbox, participant+".db"), outbox.Limits{MaxMessages: cfg.Persistence.OutboxMaxMessages, MaxBytes: cfg.Persistence.OutboxMaxBytes})
	if err != nil {
		return nil, err
	}
	owners.Closers = append(owners.Closers, store)

	publisherContracts, _ := participantContracts(groups)
	shim, err := shimPublisher.New(shimPublisher.Config{
		Outbox: store, CustomerLibrary: library,
		Broker:    unavailableSyncPublisher{}, // synchronous path is unused by AsyncDispatcher
		Contracts: publisherContracts,
	})
	if err != nil {
		return nil, err
	}
	dispatcher, err := shimPublisher.NewAsyncDispatcher(shim, &integration.AMQPAsyncPublisher{Pool: pool}, shimPublisher.AsyncDispatcherConfig{
		MaxInEventsA: cfg.Publisher.MaxConcurrency, AckTimeout: cfg.Publisher.PublishTimeout.Duration,
		CompletionBatchSize: cfg.Publisher.BatchSize, CompletionFlushPeriod: cfg.Publisher.BatchWait.Duration,
	})
	if err != nil {
		return nil, err
	}
	owners.Closers = append(owners.Closers, contextCloser{timeout: cfg.Publisher.ShutdownTimeout.Duration, close: dispatcher.Close})
	phases := NewParticipantPhaseState()
	applier := &PublisherSnapshotApplier{Publisher: shim, Registration: &ParticipantRegistration{Participant: participant, Role: control.RolePublisher, Publisher: publisher}, Phases: phases}
	groupIDs := configuredGroupIDs(groups)
	handler, err := broker0.NewCommandHandler(participant, control.RolePublisher, cfg.Namespace, groupIDs, commandOperations, PublisherCommandExecutor{Dispatcher: dispatcher, Phases: phases}, publisher)
	if err != nil {
		return nil, err
	}
	components, err := commandComponents(ctx, cfg, controlConnection.Session, participant, control.RolePublisher, groupIDs, handler)
	if err != nil {
		return nil, err
	}
	participantProcess, err := newParticipantWithCommands(updateSubscriber, requester, applier, updateOperations, groups, participant, control.RolePublisher, cfg, components)
	if err != nil {
		return nil, err
	}
	_ = nativeControlPublisher
	return &PublisherParticipant{Process: participantProcess, Publisher: shim, Dispatcher: dispatcher, BatchSize: cfg.Publisher.BatchSize, BatchWait: cfg.Publisher.BatchWait.Duration, close: owners}, nil
}

func AssembleSubscriberParticipant(ctx context.Context, cfg config.Config, credentials Credentials, participant string, library customer.CustomerLibrary, handler shimSubscriber.Handler, assembler ParticipantAssembler) (result *SubscriberParticipant, err error) {
	groups, err := participantAssemblyInputs(cfg, participant, control.RoleSubscriber, library, assembler)
	if err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("runtime: subscriber application handler is required")
	}
	owners := &CloseGroup{}
	defer func() {
		if err != nil {
			err = errors.Join(err, owners.Close())
		}
	}()
	controlConnection, _, publisher, requester, updateSubscriber, updateOperations, commandOperations, err := assembleParticipantControl(ctx, cfg, credentials, participant, control.RoleSubscriber, groups, assembler, owners)
	if err != nil {
		return nil, err
	}
	pool, err := participantDataPool(cfg, groups, credentials, participant, assembler)
	if err != nil {
		return nil, err
	}
	owners.Closers = append(owners.Closers, pool)
	_, subscriberContracts := participantContracts(groups)
	readiness := NewSubscriberReadiness()

	var shim *shimSubscriber.Shim
	factory := subscriberParticipantFactory{
		Delegate:   integration.AMQPConsumerFactory{Pool: pool},
		Subscriber: func() subscriberSettlementController { return shim },
	}
	shim, err = shimSubscriber.New(shimSubscriber.Config{
		Participant: participant, Library: library, Handler: handler,
		Factory: factory, Reporter: readiness, Contracts: subscriberContracts,
	})
	if err != nil {
		return nil, err
	}
	owners.Closers = append(owners.Closers, contextCloser{timeout: cfg.Publisher.ShutdownTimeout.Duration, close: shim.Close})
	phases := NewParticipantPhaseState()
	consumerSets, err := participantConsumerSets(groups, participant)
	if err != nil {
		return nil, err
	}
	applier := &SubscriberSnapshotApplier{Subscriber: shim, Registration: &ParticipantRegistration{Participant: participant, Role: control.RoleSubscriber, ConsumerSets: consumerSets, Publisher: publisher}, Phases: phases, Timeout: cfg.Publisher.ShutdownTimeout.Duration}
	groupIDs := configuredGroupIDs(groups)
	commandHandler, err := broker0.NewCommandHandler(participant, control.RoleSubscriber, cfg.Namespace, groupIDs, commandOperations, SubscriberCommandExecutor{Readiness: readiness, Applier: applier, Phases: phases}, publisher)
	if err != nil {
		return nil, err
	}
	components, err := commandComponents(ctx, cfg, controlConnection.Session, participant, control.RoleSubscriber, groupIDs, commandHandler)
	if err != nil {
		return nil, err
	}
	participantProcess, err := newParticipantWithCommands(updateSubscriber, requester, applier, updateOperations, groups, participant, control.RoleSubscriber, cfg, components)
	if err != nil {
		return nil, err
	}
	return &SubscriberParticipant{Process: participantProcess, Subscriber: shim, close: owners}, nil
}

func participantConsumerSets(groups []config.ScalingGroup, participant string) (map[string]string, error) {
	result := make(map[string]string, len(groups))
	for _, group := range groups {
		consumerSet, ok := group.ConsumerSetForSubscriber(participant)
		if !ok {
			return nil, fmt.Errorf("runtime: subscriber %q has no consumer set in group %q", participant, group.ID)
		}
		result[group.ID] = consumerSet
	}
	return result, nil
}

func participantAssemblyInputs(cfg config.Config, participant string, role control.ParticipantRole, library customer.CustomerLibrary, assembler ParticipantAssembler) ([]config.ScalingGroup, error) {
	if cfg.Cloud.Enabled {
		return nil, errors.New("runtime: participant commands do not provision cloud resources")
	}
	if library == nil {
		return nil, errors.New("runtime: customer library is required")
	}
	if assembler.ConnectControl == nil || assembler.ConnectData == nil {
		return nil, errors.New("runtime: participant assembler dependencies are required")
	}
	return ParticipantGroups(cfg, participant, role)
}

func assembleParticipantControl(ctx context.Context, cfg config.Config, credentials Credentials, participant string, role control.ParticipantRole, groups []config.ScalingGroup, assembler ParticipantAssembler, owners *CloseGroup) (*AMQPControlConnection, *AMQPNativePublisher, *broker0.PersistentControlPublisher, broker0.SnapshotRequester, broker0.UpdateSubscriber, broker0.OperationStore, broker0.OperationStore, error) {
	connection, err := assembler.ConnectControl(ctx, cfg, credentials, participant)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	owners.Closers = append(owners.Closers, connection)
	nativePublisher, err := NewAMQPNativePublisher(connection)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	owners.Closers = append(owners.Closers, nativePublisher)
	stateDir := ParticipantOperationDirectory(cfg, participant)
	publishOperations, err := broker0.OpenJSONOperationStore(filepath.Join(stateDir, "publish.json"))
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	updateOperations, err := broker0.OpenJSONOperationStore(filepath.Join(stateDir, "updates.json"))
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	commandOperations, err := broker0.OpenJSONOperationStore(filepath.Join(stateDir, "commands.json"))
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	names, err := control.NewManagedNames(cfg.Namespace)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	controlPublisher, err := broker0.NewPersistentControlPublisherWithTargets(nativePublisher, publishOperations,
		broker0.DestinationResolverFunc(func(kind broker0.Kind, group string) (string, error) {
			return "", fmt.Errorf("runtime: participant publication kind %q must be identity-scoped", kind)
		}),
		broker0.TargetDestinationResolverFunc(func(kind broker0.Kind, group string, messageRole control.ParticipantRole, requested string) (string, error) {
			if requested != participant || messageRole != role {
				return "", errors.New("runtime: participant publication scope mismatch")
			}
			switch kind {
			case broker0.KindAcknowledgement:
				return names.AcknowledgementTopicFor(group, messageRole, requested)
			case broker0.KindRegistration:
				return names.RegistrationTopicFor(group, messageRole, requested)
			case broker0.KindTelemetry:
				return names.TelemetryTopicFor(group, messageRole, requested)
			default:
				return "", fmt.Errorf("runtime: unsupported participant publication kind %q", kind)
			}
		}))
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	factory := AMQPDurableReceiverFactory{Session: connection.Session}
	updates := broker0.DurableUpdateSubscriber{Factory: factory, Resolver: broker0.UpdateQueueResolverFunc(func(_ context.Context, group, requested string) (string, error) {
		if requested != participant || !hasConfiguredGroup(groups, group) {
			return "", errors.New("runtime: update queue scope does not match participant")
		}
		return names.UpdateQueue(group, participant)
	})}
	requester := &groupSnapshotRequester{Session: connection.Session, Names: names, Participant: participant, Role: role}
	return connection, nativePublisher, controlPublisher, requester, updates, updateOperations, commandOperations, nil
}

type groupSnapshotRequester struct {
	Session     *amqp.Session
	Names       control.ManagedNames
	Participant string
	Role        control.ParticipantRole
}

func (r *groupSnapshotRequester) RequestSnapshot(ctx context.Context, request control.SnapshotRequest) (control.SnapshotResponse, error) {
	if request.Participant != r.Participant || request.Role != r.Role {
		return control.SnapshotResponse{}, errors.New("runtime: snapshot request scope mismatch")
	}
	requestAddress, err := r.Names.SnapshotRequestTopic(request.Group, request.Role, request.Participant)
	if err != nil {
		return control.SnapshotResponse{}, err
	}
	replyAddress, err := r.Names.SnapshotReplyQueue(request.Group, request.Role, request.Participant)
	if err != nil {
		return control.SnapshotResponse{}, err
	}
	return (integration.AMQPRequestClient{Session: r.Session, RequestAddress: requestAddress, ReplyAddress: replyAddress}).RequestSnapshot(ctx, request)
}
func commandComponents(ctx context.Context, cfg config.Config, session *amqp.Session, participant string, role control.ParticipantRole, groups []string, handler *broker0.CommandHandler) (map[string]Component, error) {
	factory := AMQPDurableReceiverFactory{Session: session}
	names, err := control.NewManagedNames(cfg.Namespace)
	if err != nil {
		return nil, err
	}
	resolver := broker0.CommandQueueResolverFunc(func(_ context.Context, group string, requestedRole control.ParticipantRole, requestedParticipant string) (string, error) {
		if requestedRole != role || requestedParticipant != participant {
			return "", errors.New("runtime: command queue scope does not match participant")
		}
		return names.CommandQueue(group, requestedRole, requestedParticipant)
	})
	inboxes, err := broker0.BindCommandInboxes(ctx, factory, resolver, participant, role, groups, handler)
	if err != nil {
		return nil, err
	}
	components := make(map[string]Component, len(inboxes))
	for group, inbox := range inboxes {
		components["command:"+group] = inbox
	}
	return components, nil
}

func newParticipantWithCommands(subscriber broker0.UpdateSubscriber, requester broker0.SnapshotRequester, applier broker0.SnapshotApplier, operations broker0.OperationStore, groups []config.ScalingGroup, participant string, role control.ParticipantRole, cfg config.Config, commands map[string]Component) (*ParticipantProcess, error) {
	membership, err := NewParticipant(subscriber, requester, applier, operations, cfg.Namespace, groups, participant, role, cfg.Staleness.MembershipMaxAge.Duration/2, cfg.Staleness.MembershipMaxAge.Duration, cfg.Publisher.ShutdownTimeout.Duration)
	if err != nil {
		return nil, err
	}
	components := make(map[string]Component, len(commands)+1)
	components["membership"] = participantProcessComponent{process: membership}
	for name, command := range commands {
		components[name] = command
	}
	return NewParticipantProcess(components, cfg.Publisher.ShutdownTimeout.Duration)
}

type participantProcessComponent struct{ process *ParticipantProcess }

func (c participantProcessComponent) Run(ctx context.Context) error { return c.process.Run(ctx) }
func (participantProcessComponent) Close() error                    { return nil }

type subscriberSettlementDirective interface {
	SubscriberSettlement() string
}

type subscriberSettlementController interface {
	Retry(context.Context, string, customer.BusinessHash) error
	Release(context.Context, string, customer.BusinessHash) error
}

type subscriberParticipantFactory struct {
	Delegate   shimSubscriber.ConsumerFactory
	Subscriber func() subscriberSettlementController
}

func (f subscriberParticipantFactory) Prepare(ctx context.Context, binding shimSubscriber.Binding, deliver func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
	if f.Delegate == nil || f.Subscriber == nil {
		return nil, errors.New("runtime: subscriber settlement adapter is not initialized")
	}
	return f.Delegate.Prepare(ctx, binding, func(deliveryCtx context.Context, delivery shimSubscriber.Delivery) error {
		err := deliver(deliveryCtx, delivery)
		var directive subscriberSettlementDirective
		if !errors.As(err, &directive) {
			return err
		}
		routed, ok := delivery.(shimSubscriber.RoutedDelivery)
		if !ok {
			return err
		}
		metadata, metadataErr := routed.RoutingMetadata()
		if metadataErr != nil || metadata.Group != binding.Group {
			return err
		}
		subscriber := f.Subscriber()
		if subscriber == nil {
			return err
		}
		for {
			switch directive.SubscriberSettlement() {
			case "retry":
				err = subscriber.Retry(deliveryCtx, metadata.Group, metadata.BusinessHash)
			case "reject", "release":
				err = subscriber.Release(deliveryCtx, metadata.Group, metadata.BusinessHash)
				if err != nil {
					return &shimSubscriber.DeliveryError{Err: err, Retained: true}
				}
				return nil
			default:
				return err
			}
			if err == nil {
				return nil
			}
			if !errors.As(err, &directive) {
				return &shimSubscriber.DeliveryError{Err: err, Retained: true}
			}
		}
	})
}

func participantDataPool(cfg config.Config, groups []config.ScalingGroup, credentials Credentials, participant string, assembler ParticipantAssembler) (*integration.AMQPConnectionPool, error) {
	brokers := make(map[string]config.DataBroker)
	for _, broker := range cfg.DataBrokers {
		for _, group := range broker.EligibleGroups {
			if hasConfiguredGroup(groups, group) {
				brokers[broker.ID] = broker
				break
			}
		}
	}
	maxConnections := max(1, min(cfg.Publisher.MaxConcurrency, len(brokers)))
	return integration.NewAMQPConnectionPool(maxConnections, 5*time.Minute, func(ctx context.Context, brokerID, endpoint string) (*integration.AMQPConnection, error) {
		broker, ok := brokers[brokerID]
		if !ok {
			return nil, fmt.Errorf("runtime: broker %q is not eligible for participant", brokerID)
		}
		broker.AMQPEndpoint = endpoint
		return assembler.ConnectData(ctx, broker, credentials.DataBrokers[brokerID], participant)
	})
}

func connectDataServices(ctx context.Context, cfg config.Config, groups []config.ScalingGroup, credentials Credentials, participant string, assembler ParticipantAssembler, owners *CloseGroup) (map[string]*integration.AMQPConnection, error) {
	required := make(map[string]struct{})
	for _, broker := range cfg.DataBrokers {
		for _, groupID := range broker.EligibleGroups {
			if hasConfiguredGroup(groups, groupID) {
				required[broker.ID] = struct{}{}
				break
			}
		}
	}
	services := make(map[string]*integration.AMQPConnection, len(required))
	for _, broker := range cfg.DataBrokers {
		if _, needed := required[broker.ID]; !needed {
			continue
		}
		service, err := assembler.ConnectData(ctx, broker, credentials.DataBrokers[broker.ID], participant)
		if err != nil {
			return nil, fmt.Errorf("runtime: connect data broker %q: %w", broker.ID, err)
		}
		services[broker.ID] = service
		owners.Closers = append(owners.Closers, service)
	}
	return services, nil
}

func configuredGroupIDs(groups []config.ScalingGroup) []string {
	ids := make([]string, len(groups))
	for index, group := range groups {
		ids[index] = group.ID
	}
	return ids
}

func hasConfiguredGroup(groups []config.ScalingGroup, id string) bool {
	for _, group := range groups {
		if group.ID == id {
			return true
		}
	}
	return false
}

type contextCloser struct {
	timeout time.Duration
	close   func(context.Context) error
}

func (c contextCloser) Close() error {
	timeout := c.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.close(ctx)
}
