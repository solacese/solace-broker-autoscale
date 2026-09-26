package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/integration"
	"github.com/solacese/solace-workload-balancer/outbox"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
	"solace.dev/go/messaging/pkg/solace"
)

// ParticipantAssembler exposes native construction seams for command tests.
type ParticipantAssembler struct {
	ConnectControl func(context.Context, config.Config, Credentials, string) (*SMFConnection, error)
	ConnectData    func(context.Context, config.DataBroker, BrokerCredentials, string) (solace.MessagingService, error)
	NewBrowser     func(config.Config, Credentials, broker0.MembershipQueueResolver) (broker0.Browser, error)
}

func DefaultParticipantAssembler() ParticipantAssembler {
	return ParticipantAssembler{
		ConnectControl: func(ctx context.Context, cfg config.Config, credentials Credentials, participant string) (*SMFConnection, error) {
			if credentials.Control.Principal == "" {
				return nil, errors.New("runtime: participant Broker 0 principal is required")
			}
			return ConnectBroker0(ctx, cfg.Control.SMFEndpoint, cfg.Control.MessageVPN, credentials.Control.SMFUsername, credentials.Control.SMFPassword, participant)
		},
		ConnectData: func(ctx context.Context, broker config.DataBroker, credentials BrokerCredentials, participant string) (solace.MessagingService, error) {
			return integration.ConnectContext(ctx, integration.Connection{
				Host: broker.SMFEndpoint, MessageVPN: broker.MessageVPN, Username: credentials.SMFUsername,
				Password: credentials.SMFPassword, ApplicationID: participant + "." + broker.ID,
			})
		},
		NewBrowser: newProductionBrowser,
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
	controlConnection, nativeControlPublisher, publisher, browser, updateSubscriber, updateOperations, commandOperations, err := assembleParticipantControl(ctx, cfg, credentials, participant, control.RolePublisher, groups, assembler, owners)
	if err != nil {
		return nil, err
	}
	_ = controlConnection

	services, err := connectDataServices(ctx, cfg, groups, credentials, participant, assembler, owners)
	if err != nil {
		return nil, err
	}
	store, err := outbox.Open(filepath.Join(cfg.Persistence.PublisherOutbox, participant+".db"), outbox.Limits{MaxMessages: cfg.Persistence.OutboxMaxMessages, MaxBytes: cfg.Persistence.OutboxMaxBytes})
	if err != nil {
		return nil, err
	}
	owners.Closers = append(owners.Closers, store)
	asyncPublishers := make(map[string]*integration.AsyncPersistentPublisher, len(services))
	for brokerID, service := range services {
		native, buildErr := integration.NewAsyncPersistentPublisher(service, uint(cfg.Publisher.MaxConcurrency))
		if buildErr != nil {
			return nil, fmt.Errorf("runtime: create data publisher for %q: %w", brokerID, buildErr)
		}
		asyncPublishers[brokerID] = native
		owners.Closers = append(owners.Closers, native)
	}
	publisherContracts, _ := participantContracts(groups)
	shim, err := shimPublisher.New(shimPublisher.Config{
		Outbox: store, CustomerLibrary: library,
		Broker:    integration.PublisherAdapter{}, // synchronous path is unused by AsyncDispatcher
		Contracts: publisherContracts,
	})
	if err != nil {
		return nil, err
	}
	dispatcher, err := shimPublisher.NewAsyncDispatcher(shim, integration.AsyncPublisherAdapter{Publishers: asyncPublishers, PartitionPolicy: participantPartitionPolicy(groups)}, shimPublisher.AsyncDispatcherConfig{
		MaxInFlight: cfg.Publisher.MaxConcurrency, AckTimeout: cfg.Publisher.PublishTimeout.Duration,
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
	components, err := commandComponents(ctx, cfg, controlConnection.Service, participant, control.RolePublisher, groupIDs, handler)
	if err != nil {
		return nil, err
	}
	participantProcess, err := newParticipantWithCommands(updateSubscriber, browser, applier, updateOperations, groups, participant, cfg, components)
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
	controlConnection, _, publisher, browser, updateSubscriber, updateOperations, commandOperations, err := assembleParticipantControl(ctx, cfg, credentials, participant, control.RoleSubscriber, groups, assembler, owners)
	if err != nil {
		return nil, err
	}
	services, err := connectDataServices(ctx, cfg, groups, credentials, participant, assembler, owners)
	if err != nil {
		return nil, err
	}
	_, subscriberContracts := participantContracts(groups)
	readiness := NewSubscriberReadiness()
	exclusiveGroups := make(map[string]bool, len(groups))
	consumersPerBinding := make(map[string]int, len(groups))
	for _, group := range groups {
		exclusive := group.Queue.Type == "exclusive" || group.Queue.Access == "exclusive"
		exclusiveGroups[group.ID] = exclusive
		if exclusive {
			consumersPerBinding[group.ID] = 1
		} else {
			consumersPerBinding[group.ID] = max(1, group.Queue.Partitions)
		}
	}
	var shim *shimSubscriber.Shim
	factory := subscriberParticipantFactory{
		Delegate:   integration.ConsumerFactory{Services: services, ExclusiveGroups: exclusiveGroups, ConsumersPerBinding: consumersPerBinding},
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
	components, err := commandComponents(ctx, cfg, controlConnection.Service, participant, control.RoleSubscriber, groupIDs, commandHandler)
	if err != nil {
		return nil, err
	}
	participantProcess, err := newParticipantWithCommands(updateSubscriber, browser, applier, updateOperations, groups, participant, cfg, components)
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
	if assembler.ConnectControl == nil || assembler.ConnectData == nil || assembler.NewBrowser == nil {
		return nil, errors.New("runtime: participant assembler dependencies are required")
	}
	return ParticipantGroups(cfg, participant, role)
}

func assembleParticipantControl(ctx context.Context, cfg config.Config, credentials Credentials, participant string, role control.ParticipantRole, groups []config.ScalingGroup, assembler ParticipantAssembler, owners *CloseGroup) (*SMFConnection, *NativePublisher, *broker0.PersistentControlPublisher, broker0.Browser, broker0.UpdateSubscriber, broker0.OperationStore, broker0.OperationStore, error) {
	connection, err := assembler.ConnectControl(ctx, cfg, credentials, participant)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	owners.Closers = append(owners.Closers, connection)
	nativePublisher, err := NewNativePublisher(connection.Service, cfg.Publisher.PublishTimeout.Duration)
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
	controlPublisher, err := broker0.NewPersistentControlPublisherWithTargets(nativePublisher, publishOperations, broker0.DestinationResolverFunc(func(kind broker0.Kind, group string) (string, error) {
		return "", fmt.Errorf("runtime: participant publication kind %q must be identity-scoped", kind)
	}), broker0.TargetDestinationResolverFunc(func(kind broker0.Kind, group string, messageRole control.ParticipantRole, requestedParticipant string) (string, error) {
		if requestedParticipant != participant || messageRole != role {
			return "", errors.New("runtime: participant publication scope mismatch")
		}
		switch kind {
		case broker0.KindAcknowledgement:
			return names.AcknowledgementTopicFor(group, messageRole, requestedParticipant)
		case broker0.KindRegistration:
			return names.RegistrationTopicFor(group, messageRole, requestedParticipant)
		case broker0.KindTelemetry:
			return names.TelemetryTopicFor(group, messageRole, requestedParticipant)
		default:
			return "", fmt.Errorf("runtime: unsupported participant publication kind %q", kind)
		}
	}))
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	membershipResolver := broker0.MembershipQueueResolverFunc(func(_ context.Context, group string) (string, error) {
		if !hasConfiguredGroup(groups, group) {
			return "", fmt.Errorf("runtime: participant is not assigned to group %q", group)
		}
		return names.MembershipQueue(group)
	})
	browser, err := assembler.NewBrowser(cfg, credentials, membershipResolver)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	if closer, ok := browser.(interface{ Close() error }); ok {
		owners.Closers = append(owners.Closers, closer)
	}
	factory := NativeDurableReceiverFactory{Service: connection.Service, Poll: participantReceivePoll}
	updates := broker0.DurableUpdateSubscriber{Factory: factory, Resolver: broker0.UpdateQueueResolverFunc(func(_ context.Context, group, requestedParticipant string) (string, error) {
		if requestedParticipant != participant || !hasConfiguredGroup(groups, group) {
			return "", errors.New("runtime: update queue scope does not match participant")
		}
		return names.UpdateQueue(group, participant)
	})}
	_ = role
	return connection, nativePublisher, controlPublisher, browser, updates, updateOperations, commandOperations, nil
}

func commandComponents(ctx context.Context, cfg config.Config, service solace.MessagingService, participant string, role control.ParticipantRole, groups []string, handler *broker0.CommandHandler) (map[string]Component, error) {
	factory := NativeDurableReceiverFactory{Service: service, Poll: participantReceivePoll}
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

func newParticipantWithCommands(subscriber broker0.UpdateSubscriber, browser broker0.Browser, applier broker0.SnapshotApplier, operations broker0.OperationStore, groups []config.ScalingGroup, participant string, cfg config.Config, commands map[string]Component) (*ParticipantProcess, error) {
	membership, err := NewParticipant(subscriber, browser, applier, operations, groups, participant, cfg.Staleness.MembershipMaxAge.Duration/2, cfg.Staleness.MembershipMaxAge.Duration, cfg.Publisher.ShutdownTimeout.Duration)
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

func connectDataServices(ctx context.Context, cfg config.Config, groups []config.ScalingGroup, credentials Credentials, participant string, assembler ParticipantAssembler, owners *CloseGroup) (map[string]solace.MessagingService, error) {
	required := make(map[string]struct{})
	for _, broker := range cfg.DataBrokers {
		for _, groupID := range broker.EligibleGroups {
			if hasConfiguredGroup(groups, groupID) {
				required[broker.ID] = struct{}{}
				break
			}
		}
	}
	services := make(map[string]solace.MessagingService, len(required))
	for _, broker := range cfg.DataBrokers {
		if _, needed := required[broker.ID]; !needed {
			continue
		}
		service, err := assembler.ConnectData(ctx, broker, credentials.DataBrokers[broker.ID], participant)
		if err != nil {
			return nil, fmt.Errorf("runtime: connect data broker %q: %w", broker.ID, err)
		}
		services[broker.ID] = service
		owners.Closers = append(owners.Closers, serviceCloser{service: service})
	}
	return services, nil
}

func connectPublisherData(ctx context.Context, cfg config.Config, groups []config.ScalingGroup, credentials Credentials, participant string, assembler ParticipantAssembler, owners *CloseGroup) (map[string]solace.MessagingService, map[string]*integration.AsyncPersistentPublisher, error) {
	services, err := connectDataServices(ctx, cfg, groups, credentials, participant, assembler, owners)
	if err != nil {
		return nil, nil, err
	}
	publishers := make(map[string]*integration.AsyncPersistentPublisher, len(services))
	for brokerID, service := range services {
		publisher, err := integration.NewAsyncPersistentPublisher(service, uint(cfg.Publisher.MaxConcurrency))
		if err != nil {
			return nil, nil, fmt.Errorf("runtime: create data publisher for %q: %w", brokerID, err)
		}
		publishers[brokerID] = publisher
		owners.Closers = append(owners.Closers, publisher)
	}
	return services, publishers, nil
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

type serviceCloser struct{ service solace.MessagingService }

func (c serviceCloser) Close() error {
	if c.service == nil || !c.service.IsConnected() {
		return nil
	}
	return c.service.Disconnect()
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
