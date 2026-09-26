// Package subscriber provides the transport-neutral core of a transition-aware
// subscriber shim.
package subscriber

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
)

var (
	ErrTransitionInProgress = errors.New("subscriber: transition already in progress")
	ErrTransitionNotFound   = errors.New("subscriber: transition not found")
	ErrStaleTransition      = errors.New("subscriber: stale transition")
	ErrKeyBlocked           = errors.New("subscriber: business key is blocked by a failed delivery")
	ErrNoBlockedDelivery    = errors.New("subscriber: business key has no blocked delivery")
	ErrSourceCoverage       = errors.New("subscriber: active consumers do not cover transition source")
	ErrWrongScalingGroup    = errors.New("subscriber: message belongs to a different scaling group")
	ErrUnexpectedContract   = errors.New("subscriber: routing contract does not match configured expectation")
	ErrRoutingMetadata      = errors.New("subscriber: authoritative routing metadata is required")
)

// Binding identifies one relevant data broker and its epoch-scoped destination.
type Binding struct {
	Group       string `json:"group"`
	BrokerID    string `json:"broker_id"`
	Epoch       uint64 `json:"epoch"`
	Destination string `json:"destination"`
}

func (b Binding) Validate() error {
	if b.Group == "" || b.BrokerID == "" || b.Destination == "" || b.Epoch == 0 {
		return errors.New("subscriber: binding group, broker, epoch and destination are required")
	}
	return nil
}

func (b Binding) key() string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%s", b.Group, b.BrokerID, b.Epoch, b.Destination)
}

// Transition describes only the bindings relevant to this subscriber. Source
// bindings remain running while Proposed consumers are prepared.
type Transition struct {
	ID            string    `json:"id"`
	Group         string    `json:"group"`
	SourceEpoch   uint64    `json:"source_epoch"`
	ProposedEpoch uint64    `json:"proposed_epoch"`
	Source        []Binding `json:"source"`
	Proposed      []Binding `json:"proposed"`
}

// TransitionFromSnapshot adapts the shared control-plane contract to the
// transport bindings used by this shim. destination resolves a broker identity
// and epoch to its concrete data-plane destination; no endpoint or credentials
// are carried in the control snapshot.
func TransitionFromSnapshot(snapshot control.MembershipSnapshot, destination func(brokerID string, epoch uint64, logical control.DestinationInfo) (string, error)) (Transition, error) {
	if err := snapshot.Validate(); err != nil {
		return Transition{}, err
	}
	if snapshot.Phase != control.PhasePrepare && snapshot.Phase != control.PhasePaused && snapshot.Phase != control.PhaseDrain {
		return Transition{}, errors.New("subscriber: transition snapshot must be PREPARE, PAUSED, or DRAIN")
	}
	if destination == nil || snapshot.Transition == nil {
		return Transition{}, errors.New("subscriber: transition and destination resolver are required")
	}
	transition := Transition{
		ID:            snapshot.Transition.ID,
		Group:         snapshot.ScalingGroup,
		SourceEpoch:   snapshot.Transition.FromEpoch,
		ProposedEpoch: snapshot.Transition.ToEpoch,
	}
	for _, broker := range snapshot.CurrentMembership {
		name, err := destination(broker, transition.SourceEpoch, snapshot.Destination)
		if err != nil {
			return Transition{}, fmt.Errorf("subscriber: resolve source broker %q: %w", broker, err)
		}
		transition.Source = append(transition.Source, Binding{Group: transition.Group, BrokerID: broker, Epoch: transition.SourceEpoch, Destination: name})
	}
	for _, broker := range snapshot.ProposedMembership {
		name, err := destination(broker, transition.ProposedEpoch, snapshot.Destination)
		if err != nil {
			return Transition{}, fmt.Errorf("subscriber: resolve proposed broker %q: %w", broker, err)
		}
		transition.Proposed = append(transition.Proposed, Binding{Group: transition.Group, BrokerID: broker, Epoch: transition.ProposedEpoch, Destination: name})
	}
	return transition, transition.Validate()
}

func (t Transition) Validate() error {
	if t.ID == "" || t.Group == "" || t.SourceEpoch == 0 || t.ProposedEpoch <= t.SourceEpoch {
		return errors.New("subscriber: valid transition id, group and increasing epochs are required")
	}
	if len(t.Proposed) == 0 {
		return errors.New("subscriber: at least one proposed binding is required")
	}
	for _, set := range [][]Binding{t.Source, t.Proposed} {
		seen := make(map[string]struct{}, len(set))
		for _, binding := range set {
			if err := binding.Validate(); err != nil {
				return err
			}
			if binding.Group != t.Group {
				return fmt.Errorf("subscriber: binding group %q does not match transition group %q", binding.Group, t.Group)
			}
			if _, exists := seen[binding.key()]; exists {
				return fmt.Errorf("subscriber: duplicate binding for broker %q", binding.BrokerID)
			}
			seen[binding.key()] = struct{}{}
		}
	}
	for _, binding := range t.Source {
		if binding.Epoch != t.SourceEpoch {
			return fmt.Errorf("subscriber: source binding epoch %d does not match %d", binding.Epoch, t.SourceEpoch)
		}
	}
	for _, binding := range t.Proposed {
		if binding.Epoch != t.ProposedEpoch {
			return fmt.Errorf("subscriber: proposed binding epoch %d does not match %d", binding.Epoch, t.ProposedEpoch)
		}
	}
	return nil
}

// Delivery remains owned by the transport. It must remain valid until Handle
// returns and, on retained failure, until Retry or Release is called for its
// business key.
type Delivery interface {
	Message() customer.MessageView
	Ack(context.Context) error
}

// RejectableDelivery supports explicit terminal poison settlement. Release
// requires this capability and leaves the key blocked when it is unavailable.
type RejectableDelivery interface {
	Delivery
	Reject(context.Context) error
}

// DeliveryError tells the transport whether the shim retained delivery ownership
// for a later Retry or Release. Callback errors without this marker are terminal
// to the callback invocation and the transport must settle or dispose the message.
type DeliveryError struct {
	Err      error
	Retained bool
}

func (e *DeliveryError) Error() string { return e.Err.Error() }
func (e *DeliveryError) Unwrap() error { return e.Err }

func RetainedDelivery(err error) bool {
	var deliveryErr *DeliveryError
	return errors.As(err, &deliveryErr) && deliveryErr.Retained
}

func retainedDeliveryError(err error) error {
	return &DeliveryError{Err: err, Retained: true}
}

// RoutingMetadata is the publisher-authenticated routing identity carried by a
// native delivery. The subscriber validates it against the binding and the
// installed customer library before choosing an ordering lane.
type RoutingMetadata struct {
	Group          string
	BusinessHash   customer.BusinessHash
	HashContract   string
	LibraryVersion string
	Epoch          uint64
	OriginalTopic  string
}

// RoutedDelivery is implemented by transports that expose authoritative routing
// metadata. Handle rejects deliveries that do not implement this interface because
// their hash contract and customer-library version cannot be verified safely.
type RoutedDelivery interface {
	Delivery
	RoutingMetadata() (RoutingMetadata, error)
}

// Handler is the application callback. A nil error means the application has
// durably accepted the message and it is safe for the shim to ACK it.
type Handler interface {
	Handle(context.Context, customer.MessageView) error
}

type HandlerFunc func(context.Context, customer.MessageView) error

func (f HandlerFunc) Handle(ctx context.Context, message customer.MessageView) error {
	return f(ctx, message)
}

// Consumer is prepared in a non-delivering state. Activate and Close must be
// idempotent because transition reconciliation can replay them.
type Consumer interface {
	Activate(context.Context) error
	Pause(context.Context) error
	Close(context.Context) error
}

// ConsumerFactory establishes a consumer without starting delivery. deliver is
// called only after Activate succeeds.
type ConsumerFactory interface {
	Prepare(context.Context, Binding, func(context.Context, Delivery) error) (Consumer, error)
}

// Readiness is scoped to one transition and proposed epoch.
type Readiness struct {
	Participant  string
	Group        string
	TransitionID string
	Epoch        uint64
	Bindings     []Binding
	Ready        bool
	Reason       string
}

type ReadinessReporter interface {
	ReportReadiness(context.Context, Readiness) error
}

// Contract identifies the routing metadata this subscriber accepts for one
// scaling group. Both values must match the control snapshot and every delivery.
type Contract struct {
	HashContract   string
	LibraryVersion string
}

// Config defines immutable subscriber dependencies and per-group routing
// expectations.
type Config struct {
	Participant string
	Library     customer.CustomerLibrary
	Handler     Handler
	Factory     ConsumerFactory
	Reporter    ReadinessReporter
	Contracts   map[string]Contract
}

// Shim coordinates consumer bindings and serializes application processing by
// scaling group and business key.
type Shim struct {
	lifecycleMu sync.Mutex
	mu          sync.Mutex
	participant string
	library     customer.CustomerLibrary
	handler     Handler
	factory     ConsumerFactory
	reporter    ReadinessReporter
	contracts   map[string]Contract
	groups      map[string]*groupState
	cleanup     map[string]Consumer
	lanesMu     sync.Mutex
	lanes       map[laneKey]*lane
}

type groupState struct {
	active     map[string]boundConsumer
	transition *preparedTransition
}

type boundConsumer struct {
	binding  Binding
	consumer Consumer
}

type preparedTransition struct {
	spec           Transition
	prepared       map[string]boundConsumer
	activated      map[string]bool
	closedSource   map[string]bool
	closedPrepared map[string]bool
	resumedSource  map[string]bool
	paused         bool
}

type laneKey struct {
	group string
	hash  customer.BusinessHash
}

type lane struct {
	mu      sync.Mutex
	waiters map[uint64]chan struct{}
	next    uint64
	serving uint64
	users   int
	blocked *blockedDelivery
}

type blockedDelivery struct {
	delivery     Delivery
	appSucceeded bool
}

func New(config Config) (*Shim, error) {
	if config.Participant == "" || config.Library == nil || config.Handler == nil || config.Factory == nil || config.Reporter == nil || len(config.Contracts) == 0 {
		return nil, errors.New("subscriber: participant, customer library, handler, factory, reporter and contracts are required")
	}
	contracts := maps.Clone(config.Contracts)
	for group, contract := range contracts {
		if group == "" || contract.HashContract == "" || contract.LibraryVersion == "" {
			return nil, errors.New("subscriber: contract map cannot contain an empty group, hash contract, or library version")
		}
	}
	return &Shim{
		participant: config.Participant,
		library:     config.Library,
		handler:     config.Handler,
		factory:     config.Factory,
		reporter:    config.Reporter,
		contracts:   contracts,
		groups:      make(map[string]*groupState),
		cleanup:     make(map[string]Consumer),
		lanes:       make(map[laneKey]*lane),
	}, nil
}

// AddActive prepares and activates an initial source binding. It is intended for
// bootstrap; membership transitions should use Prepare and Activate.
func (s *Shim) AddActive(ctx context.Context, binding Binding) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.addActive(ctx, binding)
}

func (s *Shim) addActive(ctx context.Context, binding Binding) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	group := s.ensureGroup(binding.Group)
	if _, exists := group.active[binding.key()]; exists {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	consumer, err := s.factory.Prepare(ctx, binding, s.deliveryHandler(binding))
	if err != nil {
		return fmt.Errorf("subscriber: prepare active binding: %w", err)
	}
	if err := consumer.Activate(ctx); err != nil {
		activateErr := fmt.Errorf("subscriber: activate binding: %w", err)
		if closeErr := consumer.Close(ctx); closeErr != nil {
			s.trackCleanup(consumer)
			return errors.Join(activateErr, fmt.Errorf("subscriber: close failed active binding: %w", closeErr))
		}
		return activateErr
	}
	s.mu.Lock()
	group = s.ensureGroup(binding.Group)
	if _, exists := group.active[binding.key()]; exists {
		s.mu.Unlock()
		if err := consumer.Close(ctx); err != nil {
			s.trackCleanup(consumer)
			return fmt.Errorf("subscriber: close duplicate active binding: %w", err)
		}
		return nil
	}
	group.active[binding.key()] = boundConsumer{binding: binding, consumer: consumer}
	s.mu.Unlock()
	return nil
}

// ApplySnapshot applies the shared wire phases: PREPARE creates non-delivering
// consumers, PAUSED fences local source delivery, and ACTIVE activates a matching
// prepared epoch. An ACTIVE snapshot with no pending transition bootstraps the
// listed current bindings.
func (s *Shim) ApplySnapshot(ctx context.Context, snapshot control.MembershipSnapshot, destination func(string, uint64, control.DestinationInfo) (string, error)) (err error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if err := snapshot.Validate(); err != nil {
		return err
	}
	if destination == nil {
		return errors.New("subscriber: destination resolver is required")
	}
	if err := s.validateContract(snapshot.ScalingGroup, snapshot.HashContract, snapshot.LibraryVersion); err != nil {
		return fmt.Errorf("subscriber: apply snapshot: %w", err)
	}
	if snapshot.Phase == control.PhaseCommitted {
		// Membership is durably committed but new-epoch ingress and consumers are
		// not active yet. Keep both prepared and source bindings unchanged.
		return nil
	}
	if snapshot.Phase == control.PhaseActive {
		s.mu.Lock()
		state := s.groups[snapshot.ScalingGroup]
		var pending *preparedTransition
		if state != nil {
			pending = state.transition
		}
		s.mu.Unlock()
		if pending != nil {
			var target []Binding
			if pending.spec.ProposedEpoch == snapshot.Epoch && membershipMatches(pending.spec.Proposed, snapshot.CurrentMembership) {
				if err := s.activate(ctx, snapshot.ScalingGroup, pending.spec.ID); err != nil {
					return err
				}
				target = pending.spec.Proposed
			} else if pending.spec.SourceEpoch == snapshot.Epoch && membershipMatches(pending.spec.Source, snapshot.CurrentMembership) {
				if err := s.rollback(ctx, snapshot.ScalingGroup, pending.spec.ID); err != nil {
					return err
				}
				target = pending.spec.Source
			} else {
				return ErrStaleTransition
			}
			current := make(map[string]struct{}, len(target))
			for _, binding := range target {
				current[binding.key()] = struct{}{}
			}
			return s.closeObsoleteActive(ctx, snapshot.ScalingGroup, current)
		}
		current := make(map[string]struct{}, len(snapshot.CurrentMembership))
		for _, broker := range snapshot.CurrentMembership {
			name, err := destination(broker, snapshot.Epoch, snapshot.Destination)
			if err != nil {
				return fmt.Errorf("subscriber: resolve active broker %q: %w", broker, err)
			}
			binding := Binding{Group: snapshot.ScalingGroup, BrokerID: broker, Epoch: snapshot.Epoch, Destination: name}
			current[binding.key()] = struct{}{}
			if err := s.addActive(ctx, binding); err != nil {
				return err
			}
		}
		return s.closeObsoleteActive(ctx, snapshot.ScalingGroup, current)
	}

	transition, err := TransitionFromSnapshot(snapshot, destination)
	if err != nil {
		return err
	}
	for _, source := range transition.Source {
		if err := s.addActive(ctx, source); err != nil {
			return fmt.Errorf("subscriber: establish source coverage: %w", err)
		}
	}
	if err := s.prepare(ctx, transition); err != nil {
		return err
	}
	// PAUSED and DRAIN fence publishers, not subscribers. Source consumers must
	// keep running until the controller proves the old queues are drained.
	return nil
}

// Prepare establishes every proposed consumer in a paused state while retaining
// all source consumers. Readiness is reported only after all prepares succeed.
func (s *Shim) Prepare(ctx context.Context, transition Transition) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.prepare(ctx, transition)
}

func (s *Shim) prepare(ctx context.Context, transition Transition) error {
	if err := transition.Validate(); err != nil {
		return err
	}
	transition.Source = slices.Clone(transition.Source)
	transition.Proposed = slices.Clone(transition.Proposed)

	s.mu.Lock()
	group := s.ensureGroup(transition.Group)
	if group.transition != nil {
		if equalTransition(group.transition.spec, transition) {
			prepared := bindings(group.transition.prepared)
			s.mu.Unlock()
			return s.reporter.ReportReadiness(ctx, s.readiness(transition, prepared, true, ""))
		}
		s.mu.Unlock()
		return ErrTransitionInProgress
	}
	active := maps.Clone(group.active)
	s.mu.Unlock()

	if missing := missingBindings(transition.Source, active); len(missing) != 0 {
		reason := fmt.Sprintf("source coverage incomplete: missing %v", missing)
		coverageErr := fmt.Errorf("%w: %v", ErrSourceCoverage, missing)
		if reportErr := s.reporter.ReportReadiness(ctx, s.readiness(transition, nil, false, reason)); reportErr != nil {
			return errors.Join(coverageErr, fmt.Errorf("subscriber: report not ready: %w", reportErr))
		}
		return coverageErr
	}

	prepared := make(map[string]boundConsumer, len(transition.Proposed))
	for _, binding := range transition.Proposed {
		if existing, ok := active[binding.key()]; ok {
			prepared[binding.key()] = existing
			continue
		}
		consumer, err := s.factory.Prepare(ctx, binding, s.deliveryHandler(binding))
		if err != nil {
			prepareErr := fmt.Errorf("subscriber: prepare proposed binding: %w", err)
			closeErr := s.closePrepared(ctx, prepared, active)
			reportErr := s.reporter.ReportReadiness(ctx, s.readiness(transition, bindings(prepared), false, err.Error()))
			if reportErr != nil {
				reportErr = fmt.Errorf("subscriber: report not ready: %w", reportErr)
			}
			return errors.Join(prepareErr, closeErr, reportErr)
		}
		prepared[binding.key()] = boundConsumer{binding: binding, consumer: consumer}
	}

	s.mu.Lock()
	group = s.ensureGroup(transition.Group)
	if group.transition != nil {
		s.mu.Unlock()
		return errors.Join(ErrTransitionInProgress, s.closePrepared(ctx, prepared, active))
	}
	group.transition = &preparedTransition{
		spec: transition, prepared: prepared,
		activated: make(map[string]bool), closedSource: make(map[string]bool),
		closedPrepared: make(map[string]bool), resumedSource: make(map[string]bool),
	}
	s.mu.Unlock()

	if err := s.reporter.ReportReadiness(ctx, s.readiness(transition, transition.Proposed, true, "")); err != nil {
		return fmt.Errorf("subscriber: report readiness: %w", err)
	}
	return nil
}

// PauseSource pauses current-epoch consumers without activating proposed ones.
func (s *Shim) PauseSource(ctx context.Context, group, transitionID string) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.pauseSource(ctx, group, transitionID)
}

func (s *Shim) pauseSource(ctx context.Context, group, transitionID string) error {
	s.mu.Lock()
	state, ok := s.groups[group]
	if !ok || state.transition == nil {
		s.mu.Unlock()
		return ErrTransitionNotFound
	}
	if state.transition.spec.ID != transitionID {
		s.mu.Unlock()
		return ErrStaleTransition
	}
	if state.transition.paused {
		s.mu.Unlock()
		return nil
	}
	active := maps.Clone(state.active)
	sourceEpoch := state.transition.spec.SourceEpoch
	s.mu.Unlock()

	for _, source := range active {
		if source.binding.Epoch == sourceEpoch {
			if err := source.consumer.Pause(ctx); err != nil {
				return fmt.Errorf("subscriber: pause source broker %q: %w", source.binding.BrokerID, err)
			}
		}
	}
	s.mu.Lock()
	if current := s.groups[group].transition; current != nil && current.spec.ID == transitionID {
		current.paused = true
	}
	s.mu.Unlock()
	return nil
}

// Activate starts all proposed consumers, then closes source-epoch consumers.
// It rejects stale transition IDs and leaves unrelated groups untouched.
func (s *Shim) Activate(ctx context.Context, group, transitionID string) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.activate(ctx, group, transitionID)
}

func (s *Shim) activate(ctx context.Context, group, transitionID string) error {
	s.mu.Lock()
	state, ok := s.groups[group]
	if !ok || state.transition == nil {
		s.mu.Unlock()
		return ErrTransitionNotFound
	}
	transition := state.transition
	if transition.spec.ID != transitionID {
		s.mu.Unlock()
		return ErrStaleTransition
	}
	prepared := maps.Clone(transition.prepared)
	active := maps.Clone(state.active)
	s.mu.Unlock()

	for key, proposed := range prepared {
		if transition.activated[key] {
			continue
		}
		if err := proposed.consumer.Activate(ctx); err != nil {
			return fmt.Errorf("subscriber: activate proposed broker %q: %w", proposed.binding.BrokerID, err)
		}
		transition.activated[key] = true
	}
	for key, source := range active {
		if source.binding.Epoch == transition.spec.SourceEpoch {
			if !transition.closedSource[key] {
				if err := source.consumer.Close(ctx); err != nil {
					return fmt.Errorf("subscriber: close source broker %q: %w", source.binding.BrokerID, err)
				}
				transition.closedSource[key] = true
			}
			delete(active, key)
		}
	}
	for key, proposed := range prepared {
		active[key] = proposed
	}

	s.mu.Lock()
	state = s.ensureGroup(group)
	if state.transition == nil || state.transition.spec.ID != transitionID {
		s.mu.Unlock()
		return ErrStaleTransition
	}
	state.active = active
	state.transition = nil
	s.mu.Unlock()
	return nil
}

// Rollback closes only proposed consumers and resumes source consumers that the
// shim paused. Source consumers are retained throughout preparation.
func (s *Shim) Rollback(ctx context.Context, group, transitionID string) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.rollback(ctx, group, transitionID)
}

func (s *Shim) rollback(ctx context.Context, group, transitionID string) error {
	s.mu.Lock()
	state, ok := s.groups[group]
	if !ok || state.transition == nil {
		s.mu.Unlock()
		return ErrTransitionNotFound
	}
	transition := state.transition
	if transition.spec.ID != transitionID {
		s.mu.Unlock()
		return ErrStaleTransition
	}
	prepared := maps.Clone(transition.prepared)
	active := maps.Clone(state.active)
	paused := transition.paused
	s.mu.Unlock()

	for key, proposed := range prepared {
		if _, retained := active[key]; retained || transition.closedPrepared[key] {
			continue
		}
		if err := proposed.consumer.Close(ctx); err != nil {
			return fmt.Errorf("subscriber: close prepared broker %q: %w", proposed.binding.BrokerID, err)
		}
		transition.closedPrepared[key] = true
	}
	if paused {
		for key, source := range active {
			if source.binding.Epoch == transition.spec.SourceEpoch && !transition.resumedSource[key] {
				if err := source.consumer.Activate(ctx); err != nil {
					return fmt.Errorf("subscriber: resume source broker %q: %w", source.binding.BrokerID, err)
				}
				transition.resumedSource[key] = true
			}
		}
	}
	s.mu.Lock()
	if current := s.groups[group].transition; current != nil && current.spec.ID == transitionID {
		s.groups[group].transition = nil
	}
	s.mu.Unlock()
	return nil
}

// Handle processes a delivery on the lane identified by its computed scaling
// group and full business hash. Lanes are independent across groups and keys.
func (s *Shim) Handle(ctx context.Context, binding Binding, delivery Delivery) error {
	if ctx == nil {
		ctx = context.Background()
	}
	message := delivery.Message()
	group, err := s.library.GetScalingGroup(message)
	if err != nil {
		return fmt.Errorf("subscriber: compute scaling group: %w", err)
	}
	if group != binding.Group {
		return fmt.Errorf("%w: got %q, binding is %q", ErrWrongScalingGroup, group, binding.Group)
	}
	hash, err := s.library.GetBusinessHash(message)
	if err != nil {
		return fmt.Errorf("subscriber: compute business hash: %w", err)
	}
	routed, ok := delivery.(RoutedDelivery)
	if !ok {
		return ErrRoutingMetadata
	}
	metadata, metadataErr := routed.RoutingMetadata()
	if metadataErr != nil {
		return fmt.Errorf("subscriber: validate routing metadata: %w", metadataErr)
	}
	if metadata.Group != group || metadata.BusinessHash != hash || metadata.Epoch != binding.Epoch {
		return fmt.Errorf("subscriber: routing metadata does not match customer library and binding")
	}
	if metadata.OriginalTopic == "" || metadata.HashContract == "" || metadata.LibraryVersion == "" {
		return fmt.Errorf("subscriber: routing metadata is incomplete")
	}
	if err := s.validateContract(group, metadata.HashContract, metadata.LibraryVersion); err != nil {
		return err
	}
	if message.Topic != "" && metadata.OriginalTopic != message.Topic {
		return fmt.Errorf("subscriber: original topic metadata does not match delivered topic")
	}
	hash = metadata.BusinessHash
	key := laneKey{group: group, hash: hash}
	lane, ticket := s.acquireLane(key)
	if err := waitForTurn(ctx, lane, ticket); err != nil {
		s.releaseLane(key, lane)
		return err
	}
	defer func() {
		finishTurn(lane, ticket)
		s.releaseLane(key, lane)
	}()
	if err := s.handler.Handle(ctx, message); err != nil {
		lane.mu.Lock()
		lane.blocked = &blockedDelivery{delivery: delivery}
		lane.mu.Unlock()
		return retainedDeliveryError(fmt.Errorf("subscriber: application handling failed: %w", err))
	}
	if err := delivery.Ack(ctx); err != nil {
		lane.mu.Lock()
		lane.blocked = &blockedDelivery{delivery: delivery, appSucceeded: true}
		lane.mu.Unlock()
		return retainedDeliveryError(fmt.Errorf("subscriber: acknowledge delivery: %w", err))
	}
	return nil
}

// Retry retries the blocked delivery. If application processing previously
// succeeded and only ACK failed, Retry retries only the ACK.
func (s *Shim) Retry(ctx context.Context, group string, hash customer.BusinessHash) error {
	key := laneKey{group: group, hash: hash}
	lane := s.acquireExistingLane(key)
	if lane == nil {
		return ErrNoBlockedDelivery
	}
	defer s.releaseLane(key, lane)
	lane.mu.Lock()
	defer lane.mu.Unlock()
	blocked := lane.blocked
	if blocked == nil {
		return ErrNoBlockedDelivery
	}
	if !blocked.appSucceeded {
		if err := s.handler.Handle(ctx, blocked.delivery.Message()); err != nil {
			return fmt.Errorf("subscriber: application retry failed: %w", err)
		}
		blocked.appSucceeded = true
	}
	if err := blocked.delivery.Ack(ctx); err != nil {
		return fmt.Errorf("subscriber: acknowledge retried delivery: %w", err)
	}
	lane.blocked = nil
	notifyServing(lane)
	return nil
}

// FailClosed terminates all active and prepared consumers for one group. Other
// groups remain available and must reconcile independently.
func (s *Shim) FailClosed(ctx context.Context, groupName string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if groupName == "" {
		return errors.New("subscriber: fail-closed group is required")
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	group := s.groups[groupName]
	if group == nil {
		s.mu.Unlock()
		return nil
	}
	seen := make(map[string]Consumer)
	for _, active := range group.active {
		seen[consumerIdentity(active.consumer)] = active.consumer
	}
	if group.transition != nil {
		for _, prepared := range group.transition.prepared {
			seen[consumerIdentity(prepared.consumer)] = prepared.consumer
		}
	}
	s.mu.Unlock()
	var errs []error
	for _, consumer := range seen {
		if err := consumer.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("subscriber: close consumer for group %q: %w", groupName, err))
		}
	}
	if len(errs) == 0 {
		s.mu.Lock()
		for identity := range seen {
			delete(s.cleanup, identity)
		}
		delete(s.groups, groupName)
		s.mu.Unlock()
	}
	return errors.Join(errs...)
}

// Close terminates every active and prepared consumer owned by the shim. A
// consumer reused by a prepared transition is closed only once.
func (s *Shim) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	type ownedConsumer struct {
		group    string
		key      string
		consumer Consumer
	}
	seen := make(map[string]struct{})
	owned := make([]ownedConsumer, 0)
	for identity, consumer := range s.cleanup {
		seen[identity] = struct{}{}
		owned = append(owned, ownedConsumer{consumer: consumer})
	}
	for groupName, group := range s.groups {
		for key, active := range group.active {
			identity := consumerIdentity(active.consumer)
			if _, ok := seen[identity]; !ok {
				seen[identity] = struct{}{}
				owned = append(owned, ownedConsumer{group: groupName, key: key, consumer: active.consumer})
			}
		}
		if group.transition != nil {
			for key, prepared := range group.transition.prepared {
				identity := consumerIdentity(prepared.consumer)
				if _, ok := seen[identity]; !ok {
					seen[identity] = struct{}{}
					owned = append(owned, ownedConsumer{group: groupName, key: key, consumer: prepared.consumer})
				}
			}
		}
	}
	s.mu.Unlock()
	var errs []error
	for _, item := range owned {
		if err := item.consumer.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("subscriber: close consumer: %w", err))
			continue
		}
		s.mu.Lock()
		delete(s.cleanup, consumerIdentity(item.consumer))
		if group := s.groups[item.group]; group != nil {
			if active, ok := group.active[item.key]; ok && sameConsumer(active.consumer, item.consumer) {
				delete(group.active, item.key)
			}
			if group.transition != nil {
				if prepared, ok := group.transition.prepared[item.key]; ok && sameConsumer(prepared.consumer, item.consumer) {
					delete(group.transition.prepared, item.key)
				}
			}
		}
		s.mu.Unlock()
	}
	if len(errs) == 0 {
		s.mu.Lock()
		s.groups = make(map[string]*groupState)
		s.cleanup = make(map[string]Consumer)
		s.mu.Unlock()
	}
	return errors.Join(errs...)
}

// Release terminally rejects the poison delivery before unblocking its key.
// If settlement fails, ownership and the block are retained so callers can retry.
func (s *Shim) Release(ctx context.Context, group string, hash customer.BusinessHash) error {
	key := laneKey{group: group, hash: hash}
	lane := s.acquireExistingLane(key)
	if lane == nil {
		return ErrNoBlockedDelivery
	}
	defer s.releaseLane(key, lane)
	lane.mu.Lock()
	defer lane.mu.Unlock()
	if lane.blocked == nil {
		return ErrNoBlockedDelivery
	}
	rejectable, ok := lane.blocked.delivery.(RejectableDelivery)
	if !ok {
		return errors.New("subscriber: delivery does not support explicit rejection")
	}
	if err := rejectable.Reject(ctx); err != nil {
		return fmt.Errorf("subscriber: reject released delivery: %w", err)
	}
	lane.blocked = nil
	notifyServing(lane)
	return nil
}

// BlockedError identifies a poisoned business-key lane without exposing message
// payload data.
type BlockedError struct {
	Group        string
	BusinessHash customer.BusinessHash
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("%s: group %q hash %s", ErrKeyBlocked, e.Group, hex.EncodeToString(e.BusinessHash[:]))
}

func (e *BlockedError) Unwrap() error { return ErrKeyBlocked }

func (s *Shim) deliveryHandler(binding Binding) func(context.Context, Delivery) error {
	return func(ctx context.Context, delivery Delivery) error { return s.Handle(ctx, binding, delivery) }
}

func (s *Shim) validateContract(group, hashContract, libraryVersion string) error {
	expected, ok := s.contracts[group]
	if !ok {
		return fmt.Errorf("%w: group %q is not configured", ErrUnexpectedContract, group)
	}
	if hashContract != expected.HashContract || libraryVersion != expected.LibraryVersion {
		return fmt.Errorf("%w for group %q: got hash contract %q and library version %q, want %q and %q", ErrUnexpectedContract, group, hashContract, libraryVersion, expected.HashContract, expected.LibraryVersion)
	}
	return nil
}

func (s *Shim) trackCleanup(consumer Consumer) {
	s.mu.Lock()
	s.cleanup[consumerIdentity(consumer)] = consumer
	s.mu.Unlock()
}

func consumerIdentity(consumer Consumer) string {
	return fmt.Sprintf("%T:%p", consumer, consumer)
}

func sameConsumer(left, right Consumer) bool {
	return consumerIdentity(left) == consumerIdentity(right)
}

func (s *Shim) acquireLane(key laneKey) (*lane, uint64) {
	s.lanesMu.Lock()
	defer s.lanesMu.Unlock()
	current := s.lanes[key]
	if current == nil {
		current = &lane{waiters: make(map[uint64]chan struct{})}
		s.lanes[key] = current
	}
	current.mu.Lock()
	ticket := current.next
	current.next++
	current.users++
	current.waiters[ticket] = make(chan struct{}, 1)
	current.mu.Unlock()
	return current, ticket
}

func (s *Shim) acquireExistingLane(key laneKey) *lane {
	s.lanesMu.Lock()
	defer s.lanesMu.Unlock()
	current := s.lanes[key]
	if current == nil {
		return nil
	}
	current.mu.Lock()
	current.users++
	current.mu.Unlock()
	return current
}

func (s *Shim) releaseLane(key laneKey, current *lane) {
	s.lanesMu.Lock()
	defer s.lanesMu.Unlock()
	current.mu.Lock()
	current.users--
	idle := current.users == 0 && current.blocked == nil
	current.mu.Unlock()
	if idle && s.lanes[key] == current {
		delete(s.lanes, key)
	}
}

func waitForTurn(ctx context.Context, current *lane, ticket uint64) error {
	current.mu.Lock()
	for {
		if err := ctx.Err(); err != nil {
			delete(current.waiters, ticket)
			if ticket == current.serving && current.blocked == nil {
				current.serving++
				notifyServing(current)
			}
			current.mu.Unlock()
			return err
		}
		if ticket == current.serving && current.blocked == nil {
			delete(current.waiters, ticket)
			current.mu.Unlock()
			return nil
		}
		wait := current.waiters[ticket]
		current.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
		}
		current.mu.Lock()
	}
}

func finishTurn(current *lane, ticket uint64) {
	current.mu.Lock()
	if ticket == current.serving {
		current.serving++
	}
	notifyServing(current)
	current.mu.Unlock()
}

func notifyServing(current *lane) {
	if current.blocked != nil {
		return
	}
	for current.serving < current.next {
		wait, ok := current.waiters[current.serving]
		if !ok {
			current.serving++
			continue
		}
		delete(current.waiters, current.serving)
		wait <- struct{}{}
		return
	}
}

func (s *Shim) ensureGroup(group string) *groupState {
	state := s.groups[group]
	if state == nil {
		state = &groupState{active: make(map[string]boundConsumer)}
		s.groups[group] = state
	}
	return state
}

func (s *Shim) readiness(transition Transition, prepared []Binding, ready bool, reason string) Readiness {
	return Readiness{
		Participant:  s.participant,
		Group:        transition.Group,
		TransitionID: transition.ID,
		Epoch:        transition.ProposedEpoch,
		Bindings:     slices.Clone(prepared),
		Ready:        ready,
		Reason:       reason,
	}
}

func (s *Shim) closePrepared(ctx context.Context, prepared, active map[string]boundConsumer) error {
	var errs []error
	for key, consumer := range prepared {
		if _, retained := active[key]; retained {
			continue
		}
		if err := consumer.consumer.Close(ctx); err != nil {
			s.trackCleanup(consumer.consumer)
			errs = append(errs, fmt.Errorf("subscriber: close prepared broker %q: %w", consumer.binding.BrokerID, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Shim) closeObsoleteActive(ctx context.Context, group string, current map[string]struct{}) error {
	s.mu.Lock()
	state := s.groups[group]
	if state == nil {
		s.mu.Unlock()
		return nil
	}
	obsolete := make(map[string]boundConsumer)
	for key, active := range state.active {
		if _, keep := current[key]; !keep {
			obsolete[key] = active
		}
	}
	s.mu.Unlock()

	for key, active := range obsolete {
		if err := active.consumer.Close(ctx); err != nil {
			return fmt.Errorf("subscriber: close obsolete active broker %q: %w", active.binding.BrokerID, err)
		}
		s.mu.Lock()
		if state := s.groups[group]; state != nil {
			if owned, ok := state.active[key]; ok && owned.consumer == active.consumer {
				delete(state.active, key)
			}
		}
		s.mu.Unlock()
	}
	return nil
}

func missingBindings(required []Binding, active map[string]boundConsumer) []string {
	missing := make([]string, 0)
	for _, binding := range required {
		if _, ok := active[binding.key()]; !ok {
			missing = append(missing, binding.BrokerID)
		}
	}
	return missing
}

func bindings(consumers map[string]boundConsumer) []Binding {
	result := make([]Binding, 0, len(consumers))
	for _, consumer := range consumers {
		result = append(result, consumer.binding)
	}
	slices.SortFunc(result, func(a, b Binding) int {
		if a.BrokerID < b.BrokerID {
			return -1
		}
		if a.BrokerID > b.BrokerID {
			return 1
		}
		return 0
	})
	return result
}

func equalTransition(a, b Transition) bool {
	return a.ID == b.ID && a.Group == b.Group && a.SourceEpoch == b.SourceEpoch && a.ProposedEpoch == b.ProposedEpoch && slices.Equal(a.Source, b.Source) && slices.Equal(a.Proposed, b.Proposed)
}

func membershipMatches(bindings []Binding, membership control.Membership) bool {
	if len(bindings) != len(membership) {
		return false
	}
	for index := range bindings {
		if bindings[index].BrokerID != membership[index] {
			return false
		}
	}
	return true
}
