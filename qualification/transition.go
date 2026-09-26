package qualification

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/integration"
	"github.com/solacese/solace-workload-balancer/outbox"
	"github.com/solacese/solace-workload-balancer/routing"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

// DirectTransitionDriver is the smallest real-broker controller driver used by
// qualification. It drives only the provisioned Flight epoch chain and leaves
// Baggage untouched, proving that scaling groups remain isolated.
type DirectTransitionDriver struct {
	Publisher          SnapshotPublisher
	Browser            broker0.Browser
	IndependentBrowser broker0.Browser
	Queues             QueueAPIFactory
	DataPlane          DataPlane
	Options            Options
}

func (d DirectTransitionDriver) RunTransition(ctx context.Context, plan ProvisionedPlan, observer Observer) error {
	flight, baggage, err := transitionGroups(plan)
	if err != nil {
		return err
	}
	if d.Publisher == nil || d.Browser == nil || d.IndependentBrowser == nil || d.Queues == nil || d.DataPlane == nil {
		return errors.New("qualification: transition driver dependencies are required")
	}
	options := d.Options.withDefaults()
	connectCtx, cancelConnect := context.WithTimeout(ctx, options.PublishTimeout)
	session, err := d.DataPlane.Open(connectCtx, plan.Bundles, plan.Namespace+"-transition")
	cancelConnect()
	if err != nil {
		return fmt.Errorf("qualification: open transition data plane: %w", err)
	}
	defer session.Close()

	clients, err := transitionQueueClients(plan.Bundles, d.Queues)
	if err != nil {
		return err
	}
	fence := transitionFence{groups: map[string]GroupPlan{flight.Group: flight}, clients: clients, now: options.Now}
	reconciler := control.NewReconciler()
	if err := reconciler.BeginSubscribe(); err != nil {
		return err
	}
	if err := reconciler.ApplyBrowse(activeSnapshotForEpoch(plan.Namespace, flight, flight.Epochs[0], 1)); err != nil {
		return err
	}
	if _, err := reconciler.FinishBrowse(); err != nil {
		return err
	}
	controlPublisher := &transitionControlPublisher{publisher: d.Publisher, reconciler: reconciler}
	stateDirectory, err := os.MkdirTemp("", "swlb-qualification-controller-")
	if err != nil {
		return fmt.Errorf("qualification: create controller state directory: %w", err)
	}
	defer os.RemoveAll(stateDirectory)
	store := controller.JSONStore{Path: filepath.Join(stateDirectory, "state.json")}
	controllerOptions := controller.Options{
		TelemetryFreshness: time.Minute, ReadinessFreshness: time.Minute,
		FenceFreshness: time.Minute, ZeroGrace: options.DrainGrace,
		PhaseTimeout: options.ReceiveTimeout, Now: options.Now,
	}
	opened, err := controller.Open(store, fence, controlPublisher, controllerOptions)
	if err != nil {
		return err
	}
	coordinator := &restartableController{
		Controller: opened, store: store, fence: fence, publisher: controlPublisher,
		options: controllerOptions, restarted: make(map[string]bool),
	}
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
	consumer, err := newTransitionConsumer(ctx, session, []GroupPlan{flight, baggage}, plan.Participants.Subscriber, combinedObserver, options.Now)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), options.PublishTimeout)
		defer cancelClose()
		_ = consumer.Close(closeCtx)
	}()
	publisher, dispatcher, closePublisher, err := newTransitionPublisher(plan, session, flight, baggage, options)
	if err != nil {
		return err
	}
	defer closePublisher()
	if err := publishActiveSnapshot(ctx, d.Publisher, plan.Namespace, baggage, baggage.Epochs[0], 1); err != nil {
		return err
	}
	baggageBefore, err := epochIngressStates(ctx, clients, baggage.Epochs[0])
	if err != nil {
		return err
	}

	for index := 1; index < len(flight.Epochs); index++ {
		source, target := flight.Epochs[index-1], flight.Epochs[index]
		// Revision 1 is the initial ACTIVE snapshot. Each controller transition
		// consumes five monotonically increasing revisions.
		if err := driveTransition(ctx, coordinator, consumer, publisher, dispatcher, probeLedger, plan, flight, baggage.Epochs[0], flight.Epochs[:index], source, target, index, uint64(2+(index-1)*5), clients, session.AsyncBrokerPublisher(), options); err != nil {
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
	if !maps.Equal(baggageBefore, baggageAfter) {
		return errors.New("qualification: baggage ingress changed during flight transition")
	}
	return nil
}

func newTransitionPublisher(plan ProvisionedPlan, session Session, flight, baggage GroupPlan, options Options) (*shimPublisher.Publisher, *shimPublisher.AsyncDispatcher, func(), error) {
	directory, err := os.MkdirTemp("", "swlb-qualification-transition-outbox-")
	if err != nil {
		return nil, nil, nil, err
	}
	store, err := outbox.Open(filepath.Join(directory, "outbox.db"), outbox.Limits{MaxMessages: options.OutboxMaxMessages, MaxBytes: options.OutboxMaxBytes})
	if err != nil {
		_ = os.RemoveAll(directory)
		return nil, nil, nil, err
	}
	publisher, err := shimPublisher.New(shimPublisher.Config{
		Outbox: store, CustomerLibrary: customerLibrary(), Broker: session.BrokerPublisher(),
		Contracts: map[string]shimPublisher.Contract{
			FlightGroup:  {HashContract: routing.FlightOperationsDomain, LibraryVersion: qualificationLibraryVersion},
			BaggageGroup: {HashContract: routing.BaggageDomain, LibraryVersion: qualificationLibraryVersion},
		},
	})
	if err != nil {
		_ = store.Close()
		_ = os.RemoveAll(directory)
		return nil, nil, nil, err
	}
	for _, group := range []GroupPlan{flight, baggage} {
		snapshot := activeSnapshot(group)
		snapshot.Namespace = plan.Namespace
		if err := publisher.ApplyMembership(snapshot); err != nil {
			_ = store.Close()
			_ = os.RemoveAll(directory)
			return nil, nil, nil, err
		}
	}
	dispatcher, err := shimPublisher.NewAsyncDispatcher(publisher, session.AsyncBrokerPublisher(), shimPublisher.AsyncDispatcherConfig{
		MaxInFlight: options.MaxInFlight, MaxInFlightPerBroker: options.MaxInFlightPerBroker,
		AckTimeout: options.PublishTimeout, CompletionBatchSize: 1024, CompletionFlushPeriod: 10 * time.Millisecond,
	})
	if err != nil {
		_ = store.Close()
		_ = os.RemoveAll(directory)
		return nil, nil, nil, err
	}
	cleanup := func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), options.PublishTimeout)
		defer cancel()
		_ = dispatcher.Close(closeCtx)
		_ = store.Close()
		_ = os.RemoveAll(directory)
	}
	return publisher, dispatcher, cleanup, nil
}

func driveTransition(ctx context.Context, coordinator *restartableController, consumer transitionConsumer, publisher *shimPublisher.Publisher, dispatcher *shimPublisher.AsyncDispatcher, ledger *observationLedger, plan ProvisionedPlan, group GroupPlan, baggage EpochPlan, oldSources []EpochPlan, source, target EpochPlan, sequence int, revision uint64, clients map[string]QueueAPI, stalePublisher shimPublisher.AsyncBrokerPublisher, options Options) error {
	transitionID := fmt.Sprintf("%s-flight-%d-%d", plan.RunID, source.Epoch, target.Epoch)
	spec := controller.TransitionSpec{
		ID: transitionID, Namespace: plan.Namespace, LibraryVersion: qualificationLibraryVersion,
		Group: group.Group, Revision: revision, FromEpoch: source.Epoch, ToEpoch: target.Epoch,
		Current: transitionBrokers(source), Proposed: transitionBrokers(target),
		Queue:            logicalQueue(group.Group),
		Destination:      logicalDestination(group.Group),
		CurrentResources: epochResources(source), ProposedResources: epochResources(target),
		HashContract: group.Contract, Algorithm: control.AlgorithmSHA256BigEndianModulo,
		DrainGrace: options.DrainGrace,
		RoleRequirements: map[controller.Phase][]controller.ParticipantRequirement{
			controller.PhasePrepare:  {{Participant: plan.Participants.Subscriber, Role: control.RoleSubscriber}},
			controller.PhasePause:    {{Participant: plan.Participants.Publisher, Role: control.RolePublisher}},
			controller.PhaseDrain:    {{Participant: plan.Participants.Subscriber, Role: control.RoleSubscriber}},
			controller.PhaseActivate: {{Participant: plan.Participants.Subscriber, Role: control.RoleSubscriber}},
		},
	}
	if _, err := coordinator.Begin(spec); err != nil {
		return err
	}
	var buffered *transitionProbe
	activeVerified := false
	for attempts := 0; attempts < 256; attempts++ {
		state, _ := coordinator.Group(group.Group)
		if state.Completed {
			return coordinator.requireRestarts(transitionID)
		}
		if err := coordinator.restartAtBoundary(state); err != nil {
			return err
		}
		state, _ = coordinator.Group(group.Group)
		polledDrain := false
		switch state.Phase {
		case controller.PhasePrepare:
			if state.PreparePublished {
				if err := publisher.ApplyMembership(transitionSnapshot(state.Spec, controller.PhasePrepare)); err != nil {
					return err
				}
				if err := consumer.Prepare(ctx, source, target, transitionID); err != nil {
					return err
				}
				if err := acknowledge(coordinator.Controller, state, plan.Participants.Subscriber, options.Now()); err != nil {
					return err
				}
			}
		case controller.PhasePause:
			if state.PausePublished {
				if err := publisher.ApplyMembership(transitionSnapshot(state.Spec, controller.PhasePause)); err != nil {
					return err
				}
				if _, err := dispatcher.WaitQuiescent(ctx, group.Group); err != nil {
					return err
				}
				if buffered == nil {
					probe, err := acceptTransitionProbe(publisher, ledger, plan.RunID, target, sequence, "flight", options)
					if err != nil {
						return err
					}
					if probe.receipt.State != outbox.StateUnassigned {
						return errors.New("qualification: Flight probe was routed while publisher was paused")
					}
					buffered = &probe
					if err := publishTransitionProbe(ctx, publisher, dispatcher, ledger, plan.RunID, baggage, sequence, "baggage", options); err != nil {
						return err
					}
				}
				if err := acknowledge(coordinator.Controller, state, plan.Participants.Publisher, options.Now()); err != nil {
					return err
				}
			}
		case controller.PhaseDrain:
			if state.DrainPublished {
				polledDrain = true
				statuses, pollErr := monitorEpochSources(ctx, clients, source)
				for _, broker := range source.BrokerIDs {
					if err := coordinator.Observe(sourceTelemetry(plan.Namespace, group.Group, transitionID, plan.Participants.Observer, source, broker, statuses[broker], options.Now())); err != nil {
						return err
					}
				}
				if pollErr == nil {
					if err := acknowledge(coordinator.Controller, state, plan.Participants.Subscriber, options.Now()); err != nil {
						return err
					}
				}
				// Failed SEMP reads were recorded above as unknown samples, which reset
				// the affected source's zero interval. Reconcile still runs so the phase
				// deadline remains enforced while later polls re-establish fresh evidence.
			}
		case controller.PhaseActivate:
			if state.ActivatePublished && !activeVerified {
				if err := publisher.ApplyMembership(activeSnapshotForEpoch(plan.Namespace, group, target, state.Spec.Revision+4)); err != nil {
					return err
				}
				if err := consumer.Activate(ctx, group.Group, transitionID); err != nil {
					return err
				}
				if err := assertOldSourcesFenced(ctx, clients, oldSources); err != nil {
					return err
				}
				if err := assertStalePublishesRejected(ctx, stalePublisher, oldSources, options.PublishTimeout); err != nil {
					return err
				}
				if buffered == nil {
					return errors.New("qualification: transition reached ACTIVE without a paused Flight probe")
				}
				assigned, err := publisher.Lookup(buffered.message.EventID)
				if err != nil {
					return err
				}
				if assigned.State != outbox.StateReady || assigned.Epoch != target.Epoch || !slices.Contains(target.BrokerIDs, assigned.Broker) {
					return errors.New("qualification: paused Flight probe was not assigned to the active target epoch")
				}
				buffered.receipt.Epoch, buffered.receipt.Broker = assigned.Epoch, assigned.Broker
				if err := dispatchTransitionProbe(ctx, dispatcher, ledger, *buffered, options.ReceiveTimeout); err != nil {
					return err
				}
				if err := acknowledge(coordinator.Controller, state, plan.Participants.Subscriber, options.Now()); err != nil {
					return err
				}
				activeVerified = true
			}
		}
		if _, err := coordinator.Reconcile(ctx, group.Group); err != nil {
			return err
		}
		if polledDrain {
			state, _ := coordinator.Group(group.Group)
			if state.Phase == controller.PhaseDrain {
				if err := waitTransitionPoll(ctx, options.DrainPollInterval); err != nil {
					return err
				}
			}
		}
	}
	state, _ := coordinator.Group(group.Group)
	return fmt.Errorf("qualification: transition did not converge from phase %s (prepare=%t pause=%t drain=%t commit=%t activate=%t completed=%t)", state.Phase, state.PreparePublished, state.PausePublished, state.DrainPublished, state.CommitPublished, state.ActivatePublished, state.Completed)
}

type transitionProbe struct {
	message customer.MessageView
	receipt shimPublisher.Receipt
}

func acceptTransitionProbe(publisher *shimPublisher.Publisher, ledger *observationLedger, runID string, epoch EpochPlan, sequence int, kind string, options Options) (transitionProbe, error) {
	payload := trafficPayload{Kind: kind, Carrier: "QA", Sequence: uint64(sequence)}
	if kind == "flight" {
		payload.Number = "T001"
		payload.Date = "2026-09-25"
		payload.Leg = "TRANSITION"
	} else {
		payload.Journey = "TRANSITION-BAG"
	}
	message := message(runID, kind, sequence, sequence, options.Now().UTC(), payload)
	receipt, err := publisher.Accept(message)
	if err != nil {
		return transitionProbe{}, fmt.Errorf("qualification: accept transition probe: %w", err)
	}
	return transitionProbe{message: message, receipt: receipt}, nil
}

func dispatchTransitionProbe(ctx context.Context, dispatcher *shimPublisher.AsyncDispatcher, ledger *observationLedger, probe transitionProbe, receiveTimeout time.Duration) error {
	ledger.expect(probe.message.EventID, expectedEvent{
		group: probe.receipt.Group, hash: probe.receipt.Hash, broker: probe.receipt.Broker,
		sequence: mustSequence(probe.message), sentAt: time.Unix(0, mustInt64(probe.message.Headers[HeaderSentNanos])),
	})
	for {
		before := dispatcher.Status(probe.receipt.Group).Acknowledged
		report, dispatchErr := dispatcher.Dispatch(ctx, 1)
		if dispatchErr != nil && !errors.Is(dispatchErr, integration.ErrPublisherBackpressure) {
			return dispatchErr
		}
		if report.Attempted == 0 || dispatchErr != nil {
			if err := waitForDispatchCapacity(ctx, dispatcher); err != nil {
				return err
			}
			continue
		}
		for {
			status := dispatcher.Status(probe.receipt.Group)
			if status.LastError != nil {
				return status.LastError
			}
			if status.AckUncertain != 0 || status.Uncertain != 0 {
				return errors.New("qualification: transition probe acknowledgement became uncertain")
			}
			if status.Acknowledged > before {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
		break
	}
	receiveCtx, cancelReceive := context.WithTimeout(ctx, receiveTimeout)
	defer cancelReceive()
	if err := ledger.waitEvent(receiveCtx, probe.message.EventID); err != nil {
		return err
	}
	validation := ledger.result(time.Time{}, time.Time{})
	if validation.Missing != 0 || validation.Duplicates != 0 || validation.OutOfOrder != 0 || validation.UnexpectedBroker != 0 {
		return errors.New("qualification: transition traffic invariants failed")
	}
	return nil
}

func publishTransitionProbe(ctx context.Context, publisher *shimPublisher.Publisher, dispatcher *shimPublisher.AsyncDispatcher, ledger *observationLedger, runID string, epoch EpochPlan, sequence int, kind string, options Options) error {
	probe, err := acceptTransitionProbe(publisher, ledger, runID, epoch, sequence, kind, options)
	if err != nil {
		return err
	}
	if probe.receipt.Epoch != epoch.Epoch || !slices.Contains(epoch.BrokerIDs, probe.receipt.Broker) {
		return errors.New("qualification: transition probe routed outside active epoch membership")
	}
	return dispatchTransitionProbe(ctx, dispatcher, ledger, probe, options.ReceiveTimeout)
}

func acknowledge(c *controller.Controller, state controller.GroupState, participant string, now time.Time) error {
	var role control.ParticipantRole
	for _, requirement := range state.Spec.RoleRequirements[state.Phase] {
		if requirement.Participant == participant {
			role = requirement.Role
			break
		}
	}
	if role == "" {
		return fmt.Errorf("qualification: participant %q has no role requirement in %s", participant, state.Phase)
	}
	commandID := state.IssuedCommandIDs[state.Phase][participant]
	if commandID == "" {
		return fmt.Errorf("qualification: participant %q has no issued command in %s", participant, state.Phase)
	}
	return c.Acknowledge(controller.Acknowledgement{
		CommandID: commandID, Namespace: state.Spec.Namespace,
		Group: state.Spec.Group, TransitionID: state.Spec.ID, Epoch: state.Spec.ToEpoch,
		Phase: state.Phase, Participant: participant, AuthenticatedParticipant: participant,
		Role: role, ObservedAt: now,
	})
}

func transitionSnapshot(spec controller.TransitionSpec, phase controller.Phase) control.MembershipSnapshot {
	snapshot := control.MembershipSnapshot{
		Version: control.SnapshotVersion, Namespace: spec.Namespace, LibraryVersion: spec.LibraryVersion,
		ScalingGroup: spec.Group, Revision: spec.Revision, Epoch: spec.FromEpoch,
		HashContract: spec.HashContract, Algorithm: spec.Algorithm,
		CurrentMembership:  membershipFromBrokers(spec.Current),
		ProposedMembership: membershipFromBrokers(spec.Proposed),
		Transition:         &control.Transition{ID: spec.ID, FromEpoch: spec.FromEpoch, ToEpoch: spec.ToEpoch},
		Queue:              spec.Queue,
		Destination:        spec.Destination,
		CurrentResources:   append([]control.EpochResourceIdentity(nil), spec.CurrentResources...),
		ProposedResources:  append([]control.EpochResourceIdentity(nil), spec.ProposedResources...),
	}
	switch phase {
	case controller.PhasePrepare:
		snapshot.Phase = control.PhasePrepare
	case controller.PhasePause:
		snapshot.Revision++
		snapshot.Phase = control.PhasePaused
	}
	return snapshot
}

func membershipFromBrokers(brokers []controller.Broker) control.Membership {
	membership := make(control.Membership, len(brokers))
	for index, broker := range brokers {
		membership[index] = broker.ID
	}
	return membership
}

type transitionConsumer interface {
	Prepare(context.Context, EpochPlan, EpochPlan, string) error
	Activate(context.Context, string, string) error
	Close(context.Context) error
}

func newTransitionConsumer(ctx context.Context, session Session, groups []GroupPlan, participant string, observer Observer, now func() time.Time) (transitionConsumer, error) {
	factory := attributedFactory{inner: session.ConsumerFactory(), observer: observer, now: now}
	shim, err := shimSubscriber.New(shimSubscriber.Config{
		Participant: participant, Library: customerLibrary(),
		Handler: shimSubscriber.HandlerFunc(func(context.Context, customer.MessageView) error { return nil }),
		Factory: factory, Reporter: readinessSink{}, Contracts: subscriberContracts(groups),
	})
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		for _, broker := range group.Epochs[0].BrokerIDs {
			if err := shim.AddActive(ctx, subscriberBinding(group.Group, broker, group.Epochs[0])); err != nil {
				return nil, err
			}
		}
	}
	return subscriberTransition{shim: shim}, nil
}

type subscriberTransition struct{ shim *shimSubscriber.Shim }

func (s subscriberTransition) Prepare(ctx context.Context, source, target EpochPlan, transitionID string) error {
	transition := shimSubscriber.Transition{ID: transitionID, Group: FlightGroup, SourceEpoch: source.Epoch, ProposedEpoch: target.Epoch}
	for _, broker := range source.BrokerIDs {
		transition.Source = append(transition.Source, subscriberBinding(FlightGroup, broker, source))
	}
	for _, broker := range target.BrokerIDs {
		transition.Proposed = append(transition.Proposed, subscriberBinding(FlightGroup, broker, target))
	}
	return s.shim.Prepare(ctx, transition)
}

func (s subscriberTransition) Activate(ctx context.Context, group, transitionID string) error {
	return s.shim.Activate(ctx, group, transitionID)
}

func (s subscriberTransition) Close(ctx context.Context) error {
	return s.shim.Close(ctx)
}

func subscriberBinding(group, broker string, epoch EpochPlan) shimSubscriber.Binding {
	return shimSubscriber.Binding{Group: group, BrokerID: broker, Epoch: epoch.Epoch, Destination: epoch.Queues[broker]}
}

func activeSnapshotForEpoch(namespace string, group GroupPlan, epoch EpochPlan, revision uint64) control.MembershipSnapshot {
	return control.MembershipSnapshot{
		Version: control.SnapshotVersion, Namespace: namespace, LibraryVersion: qualificationLibraryVersion,
		ScalingGroup: group.Group, Revision: revision, Epoch: epoch.Epoch, Phase: control.PhaseActive,
		HashContract: group.Contract, Algorithm: control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: control.Membership(append([]string(nil), epoch.BrokerIDs...)),
		Queue:             logicalQueue(group.Group),
		Destination:       logicalDestination(group.Group),
		CurrentResources:  epochResources(epoch),
	}
}

func publishActiveSnapshot(ctx context.Context, publisher SnapshotPublisher, namespace string, group GroupPlan, epoch EpochPlan, revision uint64) error {
	return publisher.PublishSnapshot(ctx, activeSnapshotForEpoch(namespace, group, epoch, revision))
}

func sameActiveSnapshot(left, right control.MembershipSnapshot) bool {
	return left.Version == right.Version && left.Namespace == right.Namespace && left.LibraryVersion == right.LibraryVersion &&
		left.ScalingGroup == right.ScalingGroup && left.Revision == right.Revision && left.Epoch == right.Epoch &&
		left.Phase == control.PhaseActive && right.Phase == control.PhaseActive &&
		left.HashContract == right.HashContract && left.Algorithm == right.Algorithm &&
		left.CurrentMembership.Equal(right.CurrentMembership) && left.ProposedMembership.Equal(right.ProposedMembership) &&
		left.Transition == nil && right.Transition == nil && left.Queue == right.Queue && left.Destination == right.Destination &&
		slices.Equal(left.CurrentResources, right.CurrentResources) && slices.Equal(left.ProposedResources, right.ProposedResources)
}

func assertRetainedActive(ctx context.Context, first, second broker0.Browser, expected control.MembershipSnapshot, laterDelay time.Duration) error {
	for _, candidate := range []struct {
		name    string
		browser broker0.Browser
	}{{name: "primary", browser: first}, {name: "independent", browser: second}} {
		observed, err := browseLatestSnapshot(ctx, candidate.browser, expected.ScalingGroup)
		if err != nil {
			return fmt.Errorf("qualification: %s final Broker 0 browse: %w", candidate.name, err)
		}
		if !sameActiveSnapshot(observed, expected) {
			return fmt.Errorf("qualification: %s final Broker 0 browse did not retain expected ACTIVE revision %d epoch %d membership %v", candidate.name, expected.Revision, expected.Epoch, expected.CurrentMembership)
		}
	}
	if err := waitTransitionPoll(ctx, laterDelay); err != nil {
		return err
	}
	later, err := browseLatestSnapshot(ctx, first, expected.ScalingGroup)
	if err != nil {
		return fmt.Errorf("qualification: later Broker 0 browse: %w", err)
	}
	if !sameActiveSnapshot(later, expected) {
		return errors.New("qualification: later Broker 0 browse did not retain the expected final ACTIVE snapshot")
	}
	return nil
}

func browseLatestSnapshot(ctx context.Context, browser broker0.Browser, group string) (control.MembershipSnapshot, error) {
	messages, err := browser.Browse(ctx, group)
	if err != nil {
		return control.MembershipSnapshot{}, err
	}
	reconciler := control.NewReconciler()
	if err := reconciler.BeginSubscribe(); err != nil {
		return control.MembershipSnapshot{}, err
	}
	found := false
	for _, message := range messages {
		if message.Kind != broker0.KindMembershipSnapshot {
			continue
		}
		snapshot, err := control.ParseMembershipSnapshot(message.Payload)
		if err != nil {
			return control.MembershipSnapshot{}, fmt.Errorf("parse retained membership snapshot: %w", err)
		}
		if snapshot.ScalingGroup != group {
			return control.MembershipSnapshot{}, fmt.Errorf("retained snapshot group %q does not match requested group %q", snapshot.ScalingGroup, group)
		}
		if err := reconciler.ApplyBrowse(snapshot); err != nil {
			return control.MembershipSnapshot{}, err
		}
		found = true
	}
	if !found {
		return control.MembershipSnapshot{}, errors.New("browse returned no retained membership snapshot")
	}
	return reconciler.FinishBrowse()
}

func assertOldSourcesFenced(ctx context.Context, clients map[string]QueueAPI, sources []EpochPlan) error {
	for _, source := range sources {
		for _, broker := range source.BrokerIDs {
			client := clients[broker]
			if client == nil {
				return fmt.Errorf("qualification: no SEMP client for old source broker %q", broker)
			}
			status, err := client.MonitorQueue(ctx, source.MessageVPNs[broker], source.Queues[broker])
			if err != nil {
				return fmt.Errorf("qualification: verify old source epoch %d broker %s fence: %w", source.Epoch, broker, err)
			}
			if status.IngressEnabled {
				return fmt.Errorf("qualification: old source epoch %d broker %s queue is not fenced after ACTIVE", source.Epoch, broker)
			}
		}
	}
	return nil
}

func assertStalePublishesRejected(ctx context.Context, publisher shimPublisher.AsyncBrokerPublisher, sources []EpochPlan, timeout time.Duration) error {
	for _, source := range sources {
		probeDestination := strings.TrimSuffix(source.IngressTopic, ">") + "stale-probe"
		for index, broker := range source.BrokerIDs {
			attempt := shimPublisher.PublishAttempt{EventID: fmt.Sprintf("stale-%d-%d", source.Epoch, index), Number: 1, Epoch: source.Epoch}
			future, err := publisher.PublishAsync(ctx, broker, shimPublisher.BrokerMessage{EventID: attempt.EventID, Payload: []byte("probe"), Topic: "qualification/stale", Properties: map[string]string{shimPublisher.PropertyScalingGroup: FlightGroup, shimPublisher.PropertyBusinessHash: "d343ff7173bbd7145ebd3be6658818295249dd84a84ed4f137b24d73de2279b8", shimPublisher.PropertyHashContract: "flight-operations-v1", shimPublisher.PropertyLibraryVersion: qualificationLibraryVersion}, Destination: probeDestination, Epoch: source.Epoch}, attempt)
			if err != nil {
				return fmt.Errorf("qualification: stale epoch %d publish to %s failed before broker receipt: %w", source.Epoch, broker, err)
			}
			if future == nil {
				return errors.New("qualification: stale publish returned no receipt future")
			}
			publishCtx, cancelPublish := context.WithTimeout(ctx, timeout)
			result, awaitErr := future.Await(publishCtx)
			cancelPublish()
			if awaitErr != nil || result.Attempt != attempt || result.Outcome != shimPublisher.OutcomeRejected || result.Err == nil {
				return fmt.Errorf("qualification: stale epoch %d publish to %s did not receive a definitive negative acknowledgement", source.Epoch, broker)
			}
		}
	}
	return nil
}

func waitTransitionPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func transitionGroups(plan ProvisionedPlan) (GroupPlan, GroupPlan, error) {
	var flight, baggage GroupPlan
	for _, group := range plan.Groups {
		if group.Group == FlightGroup {
			flight = group
		}
		if group.Group == BaggageGroup {
			baggage = group
		}
	}
	if len(flight.Epochs) != 4 || len(baggage.Epochs) != 1 {
		return GroupPlan{}, GroupPlan{}, errors.New("qualification: transition resource plan is incomplete")
	}
	want := [][]string{{"broker-a"}, {"broker-a", "broker-b"}, {"broker-a", "broker-b", "broker-c"}, {"broker-a", "broker-b"}}
	for i := range want {
		if !slices.Equal(flight.Epochs[i].BrokerIDs, want[i]) {
			return GroupPlan{}, GroupPlan{}, errors.New("qualification: unexpected flight membership chain")
		}
	}
	return flight, baggage, nil
}

func transitionQueueClients(bundles Bundles, factory QueueAPIFactory) (map[string]QueueAPI, error) {
	clients := make(map[string]QueueAPI, 3)
	for index, role := range []string{"broker-a", "broker-b", "broker-c"} {
		client, err := factory.New(bundles.Data[index])
		if err != nil {
			return nil, err
		}
		clients[role] = client
	}
	return clients, nil
}

type transitionFence struct {
	groups  map[string]GroupPlan
	clients map[string]QueueAPI
	now     func() time.Time
}

func (f transitionFence) Fence(ctx context.Context, request controller.FenceRequest) error {
	return f.change(ctx, request, false)
}
func (f transitionFence) Unfence(ctx context.Context, request controller.FenceRequest) error {
	return f.change(ctx, request, true)
}
func (f transitionFence) EnableIngress(ctx context.Context, request controller.FenceRequest) error {
	return f.change(ctx, request, true)
}
func (f transitionFence) change(ctx context.Context, request controller.FenceRequest, enabled bool) error {
	epoch, err := findEpoch(f.groups[request.Group], request.Epoch)
	if err != nil {
		return err
	}
	for _, broker := range request.Brokers {
		client := f.clients[broker.ID]
		queue := epoch.Queues[broker.ID]
		if client == nil || queue == "" {
			return errors.New("qualification: transition queue not provisioned")
		}
		if enabled {
			err = client.UnfenceQueue(ctx, epoch.MessageVPNs[broker.ID], queue)
		} else {
			err = client.FenceQueue(ctx, epoch.MessageVPNs[broker.ID], queue)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (f transitionFence) VerifyFence(ctx context.Context, request controller.FenceRequest) (controller.FenceStatus, error) {
	enabled, at, err := f.verify(ctx, request, false)
	return controller.FenceStatus{Fenced: enabled, ObservedAt: at}, err
}
func (f transitionFence) VerifyIngress(ctx context.Context, request controller.FenceRequest) (controller.IngressStatus, error) {
	enabled, at, err := f.verify(ctx, request, true)
	return controller.IngressStatus{Enabled: enabled, ObservedAt: at}, err
}
func (f transitionFence) verify(ctx context.Context, request controller.FenceRequest, want bool) (bool, time.Time, error) {
	epoch, err := findEpoch(f.groups[request.Group], request.Epoch)
	if err != nil {
		return false, time.Time{}, err
	}
	for _, broker := range request.Brokers {
		status, err := f.clients[broker.ID].MonitorQueue(ctx, epoch.MessageVPNs[broker.ID], epoch.Queues[broker.ID])
		if err != nil {
			return false, time.Time{}, err
		}
		if status.IngressEnabled != want {
			return false, f.observedAt(), nil
		}
	}
	return true, f.observedAt(), nil
}

func (f transitionFence) observedAt() time.Time {
	// Controller samples its clock immediately before invoking the adapter and
	// rejects observations in its future. Keep the adapter timestamp safely behind
	// that boundary while remaining well inside the configured freshness window.
	return f.now().Add(-time.Second)
}

func findEpoch(group GroupPlan, epoch uint64) (EpochPlan, error) {
	for _, candidate := range group.Epochs {
		if candidate.Epoch == epoch {
			return candidate, nil
		}
	}
	return EpochPlan{}, errors.New("qualification: epoch not provisioned")
}
func transitionBrokers(epoch EpochPlan) []controller.Broker {
	result := make([]controller.Broker, 0, len(epoch.BrokerIDs))
	for _, id := range epoch.BrokerIDs {
		result = append(result, controller.Broker{ID: id, Destination: epoch.Destination})
	}
	return result
}
func epochResources(epoch EpochPlan) []control.EpochResourceIdentity {
	result := make([]control.EpochResourceIdentity, 0, len(epoch.BrokerIDs))
	for _, id := range epoch.BrokerIDs {
		result = append(result, control.EpochResourceIdentity{Epoch: epoch.Epoch, BrokerID: id, ConsumerSet: QualificationConsumerSet, QueueName: epoch.Queues[id], IngressTopic: epoch.IngressTopic})
	}
	return result
}
func epochIngressStates(ctx context.Context, clients map[string]QueueAPI, epoch EpochPlan) (map[string]bool, error) {
	states := make(map[string]bool, len(epoch.BrokerIDs))
	for _, broker := range epoch.BrokerIDs {
		status, err := clients[broker].MonitorQueue(ctx, epoch.MessageVPNs[broker], epoch.Queues[broker])
		if err != nil {
			return nil, err
		}
		states[broker] = status.IngressEnabled
	}
	return states, nil
}

func monitorEpochSources(ctx context.Context, clients map[string]QueueAPI, epoch EpochPlan) (map[string]resultQueueStatus, error) {
	result := make(map[string]resultQueueStatus, len(epoch.BrokerIDs))
	var errs []error
	for _, broker := range epoch.BrokerIDs {
		status, err := clients[broker].MonitorDrain(ctx, epoch.MessageVPNs[broker], epoch.Queues[broker])
		if err != nil {
			result[broker] = resultQueueStatus{Known: false}
			errs = append(errs, fmt.Errorf("monitor drain on %s: %w", broker, err))
			continue
		}
		result[broker] = resultQueueStatus{
			Known: true, SpooledMessages: status.SpooledMessages, SpoolUsageBytes: status.SpoolUsageBytes,
			UnackedMessages: status.UnackedMessages, InProgressAckMessages: status.InProgressAckMessages,
		}
	}
	return result, errors.Join(errs...)
}

func sourceTelemetry(namespace, group, transitionID, participant string, epoch EpochPlan, broker string, status resultQueueStatus, observedAt time.Time) controller.Telemetry {
	return controller.Telemetry{
		MessageID: fmt.Sprintf("%s/%s/%s/%d", transitionID, broker, epoch.Queues[broker], observedAt.UnixNano()),
		Namespace: namespace, Group: group, TransitionID: transitionID, Epoch: epoch.Epoch,
		Participant: participant, Role: control.RoleObserver,
		SourceBroker: broker, SourceQueue: epoch.Queues[broker], ObservedAt: observedAt,
		Queued: controller.Count{Known: status.Known, Value: status.SpooledMessages},
		// SEMP spool usage is an allocation diagnostic and may remain nonzero after
		// the final message drains. The message count is the authoritative stored
		// predicate, matching runtime.SEMPDrainMonitor.
		Stored:  controller.Count{Known: status.Known, Value: status.SpooledMessages},
		Unacked: controller.Count{Known: status.Known, Value: status.UnackedMessages + status.InProgressAckMessages},
	}
}

type resultQueueStatus struct {
	Known                                                                    bool
	SpooledMessages, SpoolUsageBytes, UnackedMessages, InProgressAckMessages uint64
}

type restartableController struct {
	*controller.Controller
	store     controller.Store
	fence     controller.BrokerFence
	publisher controller.ControlPublisher
	options   controller.Options
	restarted map[string]bool
	target    interface{ set(*controller.Controller) }
}

func (c *restartableController) restartOnce(boundary string) error {
	if c.restarted[boundary] {
		return nil
	}
	before := c.Controller.Snapshot()
	reopened, err := controller.Open(c.store, c.fence, c.publisher, c.options)
	if err != nil {
		return fmt.Errorf("qualification: restart controller at %s: %w", boundary, err)
	}
	for group, previous := range before.Groups {
		restored, ok := reopened.Group(group)
		if !ok || restored.Phase != previous.Phase || restored.Completed != previous.Completed || !restored.PhaseDeadline.Equal(previous.PhaseDeadline) || !phaseHistoryEqual(restored.PhaseHistory, previous.PhaseHistory) {
			return fmt.Errorf("qualification: controller durable phase state changed across restart at %s", boundary)
		}
	}
	c.Controller = reopened
	if c.target != nil {
		c.target.set(reopened)
	}
	c.restarted[boundary] = true
	return nil
}

func phaseHistoryEqual(left, right []controller.PhaseEvent) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Phase != right[index].Phase || !left[index].EnteredAt.Equal(right[index].EnteredAt) {
			return false
		}
		if left[index].ExitedAt == nil || right[index].ExitedAt == nil {
			if left[index].ExitedAt != nil || right[index].ExitedAt != nil {
				return false
			}
			continue
		}
		if !left[index].ExitedAt.Equal(*right[index].ExitedAt) {
			return false
		}
	}
	return true
}

func (c *restartableController) requireRestarts(string) error {
	for _, boundary := range []string{"prepare-entry", "pause-entry", "drain-entry", "drain-published", "commit-entry", "activate-entry", "active-published"} {
		if !c.restarted[boundary] {
			return fmt.Errorf("qualification: controller restart boundary %s was not exercised", boundary)
		}
	}
	return nil
}

func (c *restartableController) Acknowledge(ack controller.Acknowledgement) error {
	return c.Controller.Acknowledge(ack)
}

func (c *restartableController) Observe(sample controller.Telemetry) error {
	return c.Controller.Observe(sample)
}

func (c *restartableController) restartAtBoundary(state controller.GroupState) error {
	var boundary string
	switch {
	case state.Phase == controller.PhasePrepare && !state.PreparePublished:
		boundary = "prepare-entry"
	case state.Phase == controller.PhasePause && !state.PausePublished && !state.FenceAttempted:
		boundary = "pause-entry"
	case state.Phase == controller.PhaseDrain && !state.DrainPublished && state.Fenced:
		boundary = "drain-entry"
	case state.Phase == controller.PhaseDrain && state.DrainPublished:
		boundary = "drain-published"
	case state.Phase == controller.PhaseCommit && !state.CommitPublished && state.FenceVerifiedAt != nil:
		boundary = "commit-entry"
	case state.Phase == controller.PhaseActivate && !state.TargetIngressEnabled && !state.ActivatePublished:
		boundary = "activate-entry"
	case state.Phase == controller.PhaseActivate && state.ActivatePublished:
		boundary = "active-published"
	default:
		return nil
	}
	return c.restartOnce(boundary)
}

type transitionControlPublisher struct {
	publisher  SnapshotPublisher
	reconciler *control.Reconciler
}

func (p *transitionControlPublisher) Publish(ctx context.Context, update controller.ControlUpdate) error {
	if p.reconciler == nil {
		return errors.New("qualification: transition reconciler is required")
	}
	if _, err := p.reconciler.ApplyUpdate(update.Snapshot); err != nil {
		return fmt.Errorf("qualification: reconcile transition snapshot: %w", err)
	}
	return p.publisher.PublishSnapshot(ctx, update.Snapshot)
}
