package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/integration"
	"github.com/solacese/solace-workload-balancer/routing"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
	"solace.dev/go/messaging/pkg/solace"
)

const participantReceivePoll = 500 * time.Millisecond

// ParticipantGroups returns only groups which explicitly require this identity
// in the requested role. An identity never implicitly joins every group.
func ParticipantGroups(cfg config.Config, participant string, role control.ParticipantRole) ([]config.ScalingGroup, error) {
	if participant == "" {
		return nil, errors.New("runtime: participant identity is required")
	}
	var identities []string
	switch role {
	case control.RolePublisher:
		identities = cfg.Runtime.Publishers
	case control.RoleSubscriber:
		identities = cfg.Runtime.Subscribers
	default:
		return nil, fmt.Errorf("runtime: unsupported participant role %q", role)
	}
	if !slices.Contains(identities, participant) {
		return nil, fmt.Errorf("runtime: participant %q is not declared as %s", participant, role)
	}
	groups := make([]config.ScalingGroup, 0)
	for _, group := range cfg.Groups {
		required := group.SubscriberIdentities()
		if role == control.RolePublisher {
			required = group.RequiredPublishers
		}
		if slices.Contains(required, participant) {
			groups = append(groups, group)
		}
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("runtime: participant %q is not required by any scaling group", participant)
	}
	return groups, nil
}

func ParticipantOperationDirectory(cfg config.Config, participant string) string {
	return filepath.Join(cfg.Persistence.PublisherOutbox, "participants", participant)
}

// AirlineCustomerLibrary is the explicit application boundary supported by the
// two repository contracts. Unknown groups fail rather than guessing a hash.
type AirlineCustomerLibrary struct{}

func (AirlineCustomerLibrary) GetScalingGroup(message customer.MessageView) (string, error) {
	group := message.Headers["scaling-group"]
	if group == "" {
		return "", errors.New("runtime: scaling-group header is required")
	}
	return group, nil
}

func (AirlineCustomerLibrary) GetBusinessHash(message customer.MessageView) (customer.BusinessHash, error) {
	switch message.Headers["scaling-group"] {
	case "flight-operations":
		return flightHash(message)
	case "baggage-tracking":
		return baggageHash(message)
	default:
		return customer.BusinessHash{}, fmt.Errorf("runtime: unsupported scaling group %q", message.Headers["scaling-group"])
	}
}

// Kept behind narrow functions so executable tests can exercise the library
// without coupling their protocol to routing internals.
var flightHash = func(message customer.MessageView) (customer.BusinessHash, error) {
	return routing.FlightOperationsHash(message.Headers["carrier"], message.Headers["flight-number"], message.Headers["departure-date"], message.Headers["leg-id"])
}
var baggageHash = func(message customer.MessageView) (customer.BusinessHash, error) {
	return routing.BaggageHash(message.Headers["carrier"], message.Headers["bag-journey-id"])
}

// PublisherParticipant owns the publisher shim, durable outbox, native data
// publishers, Broker 0 membership/command processes, and bounded dispatch loop.
type PublisherParticipant struct {
	Process    *ParticipantProcess
	Publisher  *shimPublisher.Publisher
	Dispatcher *shimPublisher.AsyncDispatcher
	BatchSize  int
	BatchWait  time.Duration
	close      *CloseGroup
	closeOnce  sync.Once
	closeErr   error
}

func (p *PublisherParticipant) Accept(message customer.MessageView) (shimPublisher.Receipt, error) {
	return p.Publisher.Accept(message)
}

func (p *PublisherParticipant) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 2)
	go func() { result <- p.Process.Run(ctx) }()
	go func() { result <- p.runDispatch(ctx) }()
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-result:
	}
	cancel()
	return errors.Join(runErr, p.Close())
}

func (p *PublisherParticipant) runDispatch(ctx context.Context) error {
	ticker := time.NewTicker(p.BatchWait)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := p.Dispatcher.Dispatch(ctx, p.BatchSize); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, shimPublisher.ErrDispatcherClosed) && !errors.Is(err, shimPublisher.ErrPublishRetryable) {
				return fmt.Errorf("runtime: dispatch publisher outbox: %w", err)
			}
		}
	}
}

func (p *PublisherParticipant) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		if p.close != nil {
			p.closeErr = p.close.Close()
		}
	})
	return p.closeErr
}

// ParticipantPhaseState tracks only snapshots successfully applied by the
// local shim. Command executors wait here so a command delivered before its
// matching update remains unsettled until the authoritative phase arrives.
type ParticipantPhaseState struct {
	mu        sync.RWMutex
	snapshots map[string]control.MembershipSnapshot
	changed   chan struct{}
}

func NewParticipantPhaseState() *ParticipantPhaseState {
	return &ParticipantPhaseState{snapshots: make(map[string]control.MembershipSnapshot), changed: make(chan struct{})}
}

func (s *ParticipantPhaseState) Record(snapshot control.MembershipSnapshot) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.snapshots[snapshot.ScalingGroup] = snapshot.Clone()
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}

func (s *ParticipantPhaseState) Clear(group string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.snapshots, group)
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}

func (s *ParticipantPhaseState) Wait(ctx context.Context, command control.CommandEnvelope) error {
	if s == nil {
		return errors.New("runtime: participant phase state is not configured")
	}
	for {
		s.mu.RLock()
		snapshot, ok := s.snapshots[command.Group]
		changed := s.changed
		s.mu.RUnlock()
		if ok && snapshotMatchesCommand(snapshot, command) {
			return nil
		}
		remaining := time.Until(command.Deadline)
		if remaining <= 0 {
			return fmt.Errorf("runtime: command %q deadline expired while waiting for authoritative phase", command.MessageID)
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-changed:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			return fmt.Errorf("runtime: command %q deadline expired while waiting for authoritative phase", command.MessageID)
		}
	}
}

func snapshotMatchesCommand(snapshot control.MembershipSnapshot, command control.CommandEnvelope) bool {
	if snapshot.ScalingGroup != command.Group || snapshot.Phase != command.Phase {
		return false
	}
	if command.Phase == control.PhaseActive {
		return snapshot.Epoch == command.Epoch
	}
	return snapshot.Transition != nil && snapshot.Transition.ID == command.TransitionID && snapshot.Transition.ToEpoch == command.Epoch
}

// PublisherSnapshotApplier is strict per-group and starts closed until an
// authoritative ACTIVE snapshot has been applied.
type RegistrationPublisher interface {
	PublishRegistration(context.Context, control.RegistrationEnvelope) error
}

type PublisherSnapshotApplier struct {
	Publisher    *shimPublisher.Publisher
	Registration *ParticipantRegistration
	Phases       *ParticipantPhaseState
}

func (a *PublisherSnapshotApplier) Apply(ctx context.Context, message broker0.Message) error {
	if a == nil || a.Publisher == nil || message.Kind != broker0.KindMembershipSnapshot || message.Snapshot == nil {
		return errors.New("runtime: publisher membership applier received an invalid message")
	}
	if err := a.Publisher.ApplyMembership(*message.Snapshot); err != nil {
		return err
	}
	if a.Registration == nil || a.Phases == nil {
		return errors.New("runtime: publisher registration and phase state are not configured")
	}
	if err := a.Registration.Publish(ctx, *message.Snapshot); err != nil {
		return err
	}
	a.Phases.Record(*message.Snapshot)
	return nil
}

func (a *PublisherSnapshotApplier) FailClosed(_ context.Context, group string, _ error) error {
	if a == nil || a.Publisher == nil {
		return errors.New("runtime: publisher membership applier is not initialized")
	}
	if a.Phases != nil {
		a.Phases.Clear(group)
	}
	return a.Publisher.FailClosed(group)
}

// PublisherCommandExecutor waits for exact, authoritative local phase evidence.
type PublisherCommandExecutor struct {
	Dispatcher *shimPublisher.AsyncDispatcher
	Phases     *ParticipantPhaseState
}

func (e PublisherCommandExecutor) Execute(ctx context.Context, command control.CommandEnvelope) error {
	if e.Dispatcher == nil || e.Phases == nil {
		return errors.New("runtime: publisher command executor is not initialized")
	}
	if err := e.Phases.Wait(ctx, command); err != nil {
		return err
	}
	status := e.Dispatcher.Status(command.Group)
	switch command.Phase {
	case control.PhasePaused:
		if !status.Paused || status.TransitionID != command.TransitionID {
			return errors.New("runtime: publisher has not applied the requested PAUSED transition")
		}
		status, err := e.Dispatcher.WaitQuiescent(ctx, command.Group)
		if err != nil {
			return err
		}
		if !status.Quiescent {
			return errors.New("runtime: publisher did not become quiescent")
		}
		return nil
	case control.PhaseActive:
		if status.Phase != control.PhaseActive || status.Epoch != command.Epoch {
			return errors.New("runtime: publisher has not applied the requested ACTIVE epoch")
		}
		return nil
	default:
		return fmt.Errorf("runtime: publisher cannot execute %s command", command.Phase)
	}
}

// SubscriberParticipant owns one strict subscriber shim and the shared
// participant control process.
type SubscriberParticipant struct {
	Process    *ParticipantProcess
	Subscriber *shimSubscriber.Shim
	close      *CloseGroup
	closeOnce  sync.Once
	closeErr   error
}

func (p *SubscriberParticipant) Run(ctx context.Context) error {
	err := p.Process.Run(ctx)
	return errors.Join(err, p.Close())
}

func (p *SubscriberParticipant) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		if p.close != nil {
			p.closeErr = p.close.Close()
		}
	})
	return p.closeErr
}

// SubscriberReadiness retains exact prepare evidence until a matching durable
// command arrives; it never publishes an uncorrelated readiness message.
type SubscriberReadiness struct {
	mu      sync.RWMutex
	reports map[string]shimSubscriber.Readiness
}

func NewSubscriberReadiness() *SubscriberReadiness {
	return &SubscriberReadiness{reports: make(map[string]shimSubscriber.Readiness)}
}

func (r *SubscriberReadiness) ReportReadiness(_ context.Context, readiness shimSubscriber.Readiness) error {
	if readiness.Group == "" || readiness.TransitionID == "" || readiness.Epoch == 0 {
		return errors.New("runtime: incomplete subscriber readiness")
	}
	key := readinessKey(readiness.Group, readiness.TransitionID, readiness.Epoch)
	r.mu.Lock()
	r.reports[key] = readiness
	r.mu.Unlock()
	if !readiness.Ready {
		return fmt.Errorf("runtime: subscriber not ready: %s", readiness.Reason)
	}
	return nil
}

func (r *SubscriberReadiness) ready(group, transition string, epoch uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reports[readinessKey(group, transition, epoch)].Ready
}

func readinessKey(group, transition string, epoch uint64) string {
	return fmt.Sprintf("%s\x00%s\x00%d", group, transition, epoch)
}

// ParticipantRegistration publishes idempotent liveness after the local shim
// successfully applies an authoritative snapshot. It never runs before that
// point, so registration cannot be confused with readiness.
type ParticipantRegistration struct {
	Participant  string
	Role         control.ParticipantRole
	ConsumerSet  string // legacy single-set construction
	ConsumerSets map[string]string
	Publisher    RegistrationPublisher
	Now          func() time.Time
}

func (r *ParticipantRegistration) Publish(ctx context.Context, snapshot control.MembershipSnapshot) error {
	if r == nil {
		return nil
	}
	if r.Publisher == nil || r.Participant == "" || snapshot.LibraryVersion == "" {
		return errors.New("runtime: participant registration is not initialized")
	}
	now := r.Now
	if now == nil {
		now = time.Now
	}
	transitionID := ""
	if snapshot.Transition != nil {
		transitionID = snapshot.Transition.ID
	}
	consumerSet := r.ConsumerSet
	if r.Role == control.RoleSubscriber && len(r.ConsumerSets) != 0 {
		var ok bool
		consumerSet, ok = r.ConsumerSets[snapshot.ScalingGroup]
		if !ok || consumerSet == "" {
			return fmt.Errorf("runtime: subscriber %q has no consumer set for group %q", r.Participant, snapshot.ScalingGroup)
		}
	}
	registration := control.RegistrationEnvelope{
		Version:   control.ProtocolVersion,
		MessageID: fmt.Sprintf("%s/registration/%s/%s/%d/%s", snapshot.ScalingGroup, r.Role, r.Participant, snapshot.Epoch, snapshot.Phase),
		Namespace: snapshot.Namespace, Group: snapshot.ScalingGroup, TransitionID: transitionID,
		Epoch: snapshot.Epoch, Phase: snapshot.Phase, Participant: r.Participant, Role: r.Role,
		ConsumerSet: consumerSet, LibraryVersion: snapshot.LibraryVersion, ObservedAt: now().UTC(),
	}
	return r.Publisher.PublishRegistration(ctx, registration)
}

// SubscriberSnapshotApplier delegates exact contracts and resource selection to
// the shim. FailClosed closes all native consumers synchronously.
type SubscriberSnapshotApplier struct {
	Subscriber   *shimSubscriber.Shim
	Registration *ParticipantRegistration
	Phases       *ParticipantPhaseState
	Timeout      time.Duration
}

func (a *SubscriberSnapshotApplier) Apply(ctx context.Context, message broker0.Message) error {
	if a == nil || a.Subscriber == nil || message.Kind != broker0.KindMembershipSnapshot || message.Snapshot == nil {
		return errors.New("runtime: subscriber membership applier received an invalid message")
	}
	if a.Registration == nil {
		return errors.New("runtime: subscriber registration is not configured")
	}
	consumerSet := a.Registration.ConsumerSet
	if len(a.Registration.ConsumerSets) != 0 {
		consumerSet = a.Registration.ConsumerSets[message.Snapshot.ScalingGroup]
	}
	if consumerSet == "" {
		return fmt.Errorf("runtime: subscriber has no consumer set for group %q", message.Snapshot.ScalingGroup)
	}
	if err := a.Subscriber.ApplySnapshot(ctx, *message.Snapshot, participantResourceDestination(message.Snapshot, consumerSet)); err != nil {
		return err
	}
	if a.Registration == nil || a.Phases == nil {
		return errors.New("runtime: subscriber registration and phase state are not configured")
	}
	if err := a.Registration.Publish(ctx, *message.Snapshot); err != nil {
		return err
	}
	a.Phases.Record(*message.Snapshot)
	return nil
}

func (a *SubscriberSnapshotApplier) FailClosed(ctx context.Context, group string, _ error) error {
	if a == nil || a.Subscriber == nil {
		return errors.New("runtime: subscriber membership applier is not initialized")
	}
	if a.Phases != nil {
		a.Phases.Clear(group)
	}
	timeout := a.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	return a.Subscriber.FailClosed(closeCtx, group)
}

// SubscriberCommandExecutor validates that the snapshot side effects matching
// the durable command have completed.
type SubscriberCommandExecutor struct {
	Readiness *SubscriberReadiness
	Applier   *SubscriberSnapshotApplier
	Phases    *ParticipantPhaseState
}

func (e SubscriberCommandExecutor) Execute(ctx context.Context, command control.CommandEnvelope) error {
	if e.Readiness == nil || e.Applier == nil || e.Applier.Subscriber == nil || e.Phases == nil {
		return errors.New("runtime: subscriber command executor is not initialized")
	}
	if err := e.Phases.Wait(ctx, command); err != nil {
		return err
	}
	switch command.Phase {
	case control.PhasePrepare, control.PhaseDrain:
		if !e.Readiness.ready(command.Group, command.TransitionID, command.Epoch) {
			return errors.New("runtime: subscriber destination readiness has not been established")
		}
		return nil
	case control.PhaseActive:
		// ACTIVE has already been applied by the membership orchestrator before a
		// command can be acknowledged. The strict command scope and shim's idempotent
		// activation make replay safe.
		return nil
	default:
		return fmt.Errorf("runtime: subscriber cannot execute %s command", command.Phase)
	}
}

func participantResourceDestination(snapshot *control.MembershipSnapshot, consumerSet string) func(string, uint64, control.DestinationInfo) (string, error) {
	return func(brokerID string, epoch uint64, _ control.DestinationInfo) (string, error) {
		resources := snapshot.CurrentResources
		if snapshot.Transition != nil && epoch == snapshot.Transition.ToEpoch {
			resources = snapshot.ProposedResources
		}
		match := ""
		for _, resource := range resources {
			if resource.Epoch == epoch && resource.BrokerID == brokerID && resource.ConsumerSet == consumerSet {
				if match != "" {
					return "", fmt.Errorf("runtime: duplicate resource for broker %q consumer set %q epoch %d", brokerID, consumerSet, epoch)
				}
				match = resource.QueueName
			}
		}
		if match == "" {
			return "", fmt.Errorf("runtime: no resource for broker %q epoch %d", brokerID, epoch)
		}
		return match, nil
	}
}

func closeServices(services map[string]solace.MessagingService) error {
	var result error
	for _, service := range services {
		if service != nil && service.IsConnected() {
			result = errors.Join(result, service.Disconnect())
		}
	}
	return result
}

func participantContracts(groups []config.ScalingGroup) (map[string]shimPublisher.Contract, map[string]shimSubscriber.Contract) {
	publisherContracts := make(map[string]shimPublisher.Contract, len(groups))
	subscriberContracts := make(map[string]shimSubscriber.Contract, len(groups))
	for _, group := range groups {
		publisherContracts[group.ID] = shimPublisher.Contract{HashContract: group.HashContract, LibraryVersion: group.CustomerLibrary}
		subscriberContracts[group.ID] = shimSubscriber.Contract{HashContract: group.HashContract, LibraryVersion: group.CustomerLibrary}
	}
	return publisherContracts, subscriberContracts
}

func participantPartitionPolicy(groups []config.ScalingGroup) integration.PartitionPolicy {
	policy := make(integration.PartitionPolicy, len(groups))
	for _, group := range groups {
		policy[group.ID] = group.Queue.Type == "partitioned"
	}
	return policy
}

// Use an interface assertion here so accidental command assembly regressions
// are caught at compile time.
var _ broker0.SnapshotApplier = (*PublisherSnapshotApplier)(nil)
var _ broker0.SnapshotApplier = (*SubscriberSnapshotApplier)(nil)
var _ broker0.CommandExecutor = PublisherCommandExecutor{}
var _ broker0.CommandExecutor = SubscriberCommandExecutor{}
var _ shimSubscriber.ReadinessReporter = (*SubscriberReadiness)(nil)
