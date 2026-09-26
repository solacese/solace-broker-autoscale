package qualification

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/integration"
	"github.com/solacese/solace-workload-balancer/outbox"
	"github.com/solacese/solace-workload-balancer/routing"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

type trafficPayload struct {
	Kind     string `json:"kind"`
	Carrier  string `json:"carrier"`
	Number   string `json:"number,omitempty"`
	Date     string `json:"date,omitempty"`
	Leg      string `json:"leg,omitempty"`
	Journey  string `json:"journey,omitempty"`
	Sequence uint64 `json:"sequence"`
}

const qualificationLibraryVersion = "qualification-v1"

func subscriberContracts(groups []GroupPlan) map[string]shimSubscriber.Contract {
	contracts := make(map[string]shimSubscriber.Contract, len(groups))
	for _, group := range groups {
		contracts[group.Group] = shimSubscriber.Contract{HashContract: group.Contract, LibraryVersion: qualificationLibraryVersion}
	}
	return contracts
}

func customerLibrary() customer.CustomerLibrary {
	decode := func(message customer.MessageView) (trafficPayload, error) {
		switch payload := message.Payload.(type) {
		case trafficPayload:
			return payload, nil
		case *trafficPayload:
			if payload == nil {
				return trafficPayload{}, errors.New("nil qualification payload")
			}
			return *payload, nil
		case []byte:
			var value trafficPayload
			if err := json.Unmarshal(payload, &value); err != nil {
				return value, err
			}
			return value, nil
		default:
			return trafficPayload{}, fmt.Errorf("unsupported qualification payload %T", message.Payload)
		}
	}
	return customer.CustomerLibraryFuncs{
		ScalingGroup: func(message customer.MessageView) (string, error) {
			payload, err := decode(message)
			if err != nil {
				return "", err
			}
			switch payload.Kind {
			case "flight":
				return FlightGroup, nil
			case "baggage":
				return BaggageGroup, nil
			default:
				return "", fmt.Errorf("unknown qualification payload kind %q", payload.Kind)
			}
		},
		BusinessHash: func(message customer.MessageView) (customer.BusinessHash, error) {
			payload, err := decode(message)
			if err != nil {
				return customer.BusinessHash{}, err
			}
			if payload.Kind == "flight" {
				return routing.FlightOperationsHash(payload.Carrier, payload.Number, payload.Date, payload.Leg)
			}
			if payload.Kind == "baggage" {
				return routing.BaggageHash(payload.Carrier, payload.Journey)
			}
			return customer.BusinessHash{}, fmt.Errorf("unknown qualification payload kind %q", payload.Kind)
		},
	}
}

type expectedEvent struct {
	group    string
	hash     string
	broker   string
	sequence uint64
	sentAt   time.Time
}

type observationLedger struct {
	mu                         sync.Mutex
	expected                   map[string]expectedEvent
	received                   []DeliveryObservation
	durableAcceptanceLatencies []time.Duration
	brokerPositiveACKLatencies []time.Duration
	accepted                   uint64
	positiveACKs               uint64
	outboxHighWaterMessages    int
	outboxHighWaterBytes       int64
	notify                     chan struct{}
}

func newObservationLedger() *observationLedger {
	return &observationLedger{expected: make(map[string]expectedEvent), notify: make(chan struct{}, 1)}
}

func (l *observationLedger) observeAcceptance(duration time.Duration, messages int) {
	if messages <= 0 {
		return
	}
	l.mu.Lock()
	l.accepted += uint64(messages)
	for range messages {
		l.durableAcceptanceLatencies = append(l.durableAcceptanceLatencies, duration)
	}
	l.mu.Unlock()
}

func (l *observationLedger) ObserveTerminalAttempt(observation shimPublisher.TerminalAttemptObservation) {
	if observation.Outcome != shimPublisher.OutcomeAcknowledged {
		return
	}
	l.mu.Lock()
	l.positiveACKs++
	l.brokerPositiveACKLatencies = append(l.brokerPositiveACKLatencies, observation.BrokerLatency)
	l.mu.Unlock()
	select {
	case l.notify <- struct{}{}:
	default:
	}
}

func (l *observationLedger) observeOutbox(stats outbox.Stats) {
	l.mu.Lock()
	if stats.HighWaterMessages > l.outboxHighWaterMessages {
		l.outboxHighWaterMessages = stats.HighWaterMessages
	}
	if stats.HighWaterBytes > l.outboxHighWaterBytes {
		l.outboxHighWaterBytes = stats.HighWaterBytes
	}
	l.mu.Unlock()
}

func (l *observationLedger) expect(id string, event expectedEvent) {
	l.mu.Lock()
	l.expected[id] = event
	l.mu.Unlock()
}

func (l *observationLedger) Observe(observation DeliveryObservation) error {
	if observation.EventID == "" || observation.Group == "" || observation.BusinessHash == "" || observation.BrokerID == "" || observation.Sequence == 0 || observation.SentAt.IsZero() || observation.ReceivedAt.IsZero() {
		return errors.New("qualification: incomplete delivery observation")
	}
	l.mu.Lock()
	l.received = append(l.received, observation)
	l.mu.Unlock()
	select {
	case l.notify <- struct{}{}:
	default:
	}
	return nil
}

func (l *observationLedger) wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		uniqueExpected := make(map[string]struct{}, len(l.received))
		for _, observation := range l.received {
			if _, expected := l.expected[observation.EventID]; expected {
				uniqueExpected[observation.EventID] = struct{}{}
			}
		}
		done := len(uniqueExpected) >= len(l.expected)
		l.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("qualification: receive deadline elapsed: %w", ctx.Err())
		case <-l.notify:
		}
	}
}

func (l *observationLedger) waitPositiveACKs(ctx context.Context, expected uint64) error {
	for {
		l.mu.Lock()
		done := l.positiveACKs >= expected
		l.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("qualification: positive ACK deadline elapsed: %w", ctx.Err())
		case <-l.notify:
		}
	}
}

func (l *observationLedger) waitEvent(ctx context.Context, eventID string) error {
	for {
		l.mu.Lock()
		observed := false
		for _, observation := range l.received {
			if observation.EventID == eventID {
				observed = true
				break
			}
		}
		l.mu.Unlock()
		if observed {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("qualification: receive deadline elapsed: %w", ctx.Err())
		case <-l.notify:
		}
	}
}

func (l *observationLedger) result(started, ended time.Time) ValidationResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := ValidationResult{Expected: len(l.expected), Deliveries: len(l.received), BrokerDistribution: make(map[string]int)}
	seen := make(map[string]int, len(l.received))
	last := make(map[string]uint64)
	var latencies []time.Duration
	for _, observation := range l.received {
		seen[observation.EventID]++
		expected, ok := l.expected[observation.EventID]
		if !ok {
			result.UnexpectedBroker++
			continue
		}
		result.BrokerDistribution[observation.BrokerID]++
		if expected.group != observation.Group || expected.hash != observation.BusinessHash || expected.broker != observation.BrokerID || expected.sequence != observation.Sequence || !expected.sentAt.Equal(observation.SentAt) {
			result.UnexpectedBroker++
		}
		key := observation.Group + "\x00" + observation.BusinessHash
		if previous := last[key]; observation.Sequence != previous+1 {
			result.OutOfOrder++
		}
		if observation.Sequence > last[key] {
			last[key] = observation.Sequence
		}
		if !expected.sentAt.IsZero() && !observation.ReceivedAt.Before(expected.sentAt) {
			latencies = append(latencies, observation.ReceivedAt.Sub(expected.sentAt))
		}
	}
	for _, count := range seen {
		if count > 1 {
			result.Duplicates += count - 1
		}
	}
	for id := range l.expected {
		if seen[id] != 0 {
			result.UniqueEventIDs++
		}
		if seen[id] == 0 {
			result.Missing++
		}
	}
	result.EndToEndConsumedLatency = quantiles(latencies)
	result.Latency = result.EndToEndConsumedLatency
	result.DurableAcceptanceLatency = quantiles(l.durableAcceptanceLatencies)
	result.BrokerPositiveACKLatency = quantiles(l.brokerPositiveACKLatencies)
	result.Accepted = l.accepted
	result.PositiveACKs = l.positiveACKs
	result.OutboxHighWaterMessages = l.outboxHighWaterMessages
	result.OutboxHighWaterBytes = l.outboxHighWaterBytes
	if duration := ended.Sub(started); duration > 0 {
		result.ThroughputPerSecond = float64(result.UniqueEventIDs) / duration.Seconds()
	} else if result.UniqueEventIDs > 0 {
		result.ThroughputPerSecond = float64(result.UniqueEventIDs)
	}
	return result
}

func quantiles(values []time.Duration) Quantiles {
	if len(values) == 0 {
		return Quantiles{}
	}
	values = append([]time.Duration(nil), values...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	pick := func(percent int) time.Duration {
		index := (percent*len(values)+99)/100 - 1
		if index < 0 {
			index = 0
		}
		return values[index]
	}
	return Quantiles{P50: pick(50), P95: pick(95), P99: pick(99), Max: values[len(values)-1]}
}

type attributedFactory struct {
	inner    shimSubscriber.ConsumerFactory
	observer Observer
	now      func() time.Time
}

func (f attributedFactory) Prepare(ctx context.Context, binding shimSubscriber.Binding, deliver func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
	return f.inner.Prepare(ctx, binding, func(ctx context.Context, delivery shimSubscriber.Delivery) error {
		return deliver(ctx, &observedDelivery{Delivery: delivery, brokerID: binding.BrokerID, observer: f.observer, now: f.now})
	})
}

type observedDelivery struct {
	shimSubscriber.Delivery
	brokerID string
	observer Observer
	now      func() time.Time
	message  *customer.MessageView
}

func (d *observedDelivery) Message() customer.MessageView {
	if d.message == nil {
		message := d.Delivery.Message()
		d.message = &message
	}
	return *d.message
}

func (d *observedDelivery) RoutingMetadata() (shimSubscriber.RoutingMetadata, error) {
	routed, ok := d.Delivery.(shimSubscriber.RoutedDelivery)
	if !ok {
		return shimSubscriber.RoutingMetadata{}, errors.New("qualification: native delivery has no routing metadata")
	}
	return routed.RoutingMetadata()
}

func (d *observedDelivery) Ack(ctx context.Context) error {
	message := d.Message()
	metadata, err := d.RoutingMetadata()
	if err != nil {
		return err
	}
	sequence, err := strconv.ParseUint(message.Headers[HeaderSequence], 10, 64)
	if err != nil || sequence == 0 {
		return errors.New("qualification: invalid sequence header")
	}
	sentNanos, err := strconv.ParseInt(message.Headers[HeaderSentNanos], 10, 64)
	if err != nil || sentNanos <= 0 {
		return errors.New("qualification: invalid sent timestamp header")
	}
	if err := d.Delivery.Ack(ctx); err != nil {
		return err
	}
	return d.observer.Observe(DeliveryObservation{
		EventID: message.EventID, Group: metadata.Group, BusinessHash: hex.EncodeToString(metadata.BusinessHash[:]),
		BrokerID: d.brokerID, Sequence: sequence, SentAt: time.Unix(0, sentNanos), ReceivedAt: d.now(),
	})
}

type readinessSink struct{}

func (readinessSink) ReportReadiness(context.Context, shimSubscriber.Readiness) error { return nil }

func observationMetadata(message customer.MessageView) (string, string, uint64, time.Time, error) {
	library := customerLibrary()
	group, err := library.GetScalingGroup(message)
	if err != nil {
		return "", "", 0, time.Time{}, err
	}
	hash, err := library.GetBusinessHash(message)
	if err != nil {
		return "", "", 0, time.Time{}, err
	}
	sequence, err := strconv.ParseUint(message.Headers[HeaderSequence], 10, 64)
	if err != nil || sequence == 0 {
		return "", "", 0, time.Time{}, errors.New("qualification: invalid sequence header")
	}
	sentNanos, err := strconv.ParseInt(message.Headers[HeaderSentNanos], 10, 64)
	if err != nil || sentNanos <= 0 {
		return "", "", 0, time.Time{}, errors.New("qualification: invalid sent timestamp header")
	}
	return group, hex.EncodeToString(hash[:]), sequence, time.Unix(0, sentNanos), nil
}

func activeSnapshot(plan GroupPlan) control.MembershipSnapshot {
	epoch := plan.Epochs[0]
	return control.MembershipSnapshot{
		Version: control.SnapshotVersion, LibraryVersion: qualificationLibraryVersion, ScalingGroup: plan.Group,
		Revision: 1, Epoch: plan.Epoch, Phase: control.PhaseActive,
		HashContract: plan.Contract, Algorithm: control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: control.Membership(append([]string(nil), plan.BrokerIDs...)),
		Queue:             logicalQueue(plan.Group),
		Destination:       logicalDestination(plan.Group),
		CurrentResources:  epochResources(epoch),
	}
}

func enableStaticEpoch(ctx context.Context, plan ProvisionedPlan, epochs map[string]EpochPlan, factory QueueAPIFactory) error {
	clients, err := transitionQueueClients(plan.Bundles, factory)
	if err != nil {
		return err
	}
	for _, epoch := range epochs {
		for _, broker := range epoch.BrokerIDs {
			if err := clients[broker].UnfenceQueue(ctx, epoch.MessageVPNs[broker], epoch.Queues[broker]); err != nil {
				return fmt.Errorf("qualification: enable static epoch %d queue on %s: %w", epoch.Epoch, broker, err)
			}
		}
	}
	return nil
}

func restoreStaticEpochs(ctx context.Context, plan ProvisionedPlan, factory QueueAPIFactory) error {
	clients, err := transitionQueueClients(plan.Bundles, factory)
	if err != nil {
		return err
	}
	var errs []error
	for _, group := range plan.Groups {
		for _, epoch := range group.Epochs[1:] {
			for _, broker := range epoch.BrokerIDs {
				if err := clients[broker].FenceQueue(ctx, epoch.MessageVPNs[broker], epoch.Queues[broker]); err != nil {
					errs = append(errs, fmt.Errorf("qualification: restore epoch %d queue fence on %s: %w", epoch.Epoch, broker, err))
				}
			}
		}
	}
	return errors.Join(errs...)
}

func widestEpoch(group GroupPlan) (EpochPlan, error) {
	if len(group.Epochs) == 0 {
		return EpochPlan{}, fmt.Errorf("qualification: group %q has no provisioned epochs", group.Group)
	}
	widest := group.Epochs[0]
	for _, epoch := range group.Epochs[1:] {
		if len(epoch.BrokerIDs) > len(widest.BrokerIDs) {
			widest = epoch
		}
	}
	return widest, nil
}

func logicalQueue(group string) control.QueueInfo {
	return control.QueueInfo{Name: group + ".managed", Durable: true}
}

func logicalDestination(group string) control.DestinationInfo {
	return control.DestinationInfo{Kind: control.DestinationTopic, Name: group + "/managed"}
}

func messages(runID string, keys, eventsPerKey int, now func() time.Time) []customer.MessageView {
	result := make([]customer.MessageView, 0, keys*eventsPerKey*2)
	// Interleave business keys by sequence. Per-key FIFO remains unchanged, while
	// every bounded outbox window exposes independent lane heads to the dispatcher.
	for sequence := 1; sequence <= eventsPerKey; sequence++ {
		for key := 0; key < keys; key++ {
			sent := now().UTC()
			result = append(result,
				message(runID, "flight", key, sequence, sent, trafficPayload{Kind: "flight", Carrier: "QA", Number: fmt.Sprintf("%04d", key), Date: "2026-09-25", Leg: fmt.Sprintf("LEG-%04d", key), Sequence: uint64(sequence)}),
				message(runID, "baggage", key, sequence, sent, trafficPayload{Kind: "baggage", Carrier: "QA", Journey: fmt.Sprintf("BAG-%08d", key), Sequence: uint64(sequence)}),
			)
		}
	}
	return result
}

func message(runID, kind string, key, sequence int, sent time.Time, payload trafficPayload) customer.MessageView {
	return customer.MessageView{Topic: "qualification/" + kind, EventID: fmt.Sprintf("%s-%s-%06d-%06d", runID, kind, key, sequence), Payload: payload, Headers: map[string]string{HeaderRunID: runID, HeaderSequence: strconv.Itoa(sequence), HeaderSentNanos: strconv.FormatInt(sent.UnixNano(), 10), HeaderPayloadKind: kind}}
}

func runStaticTraffic(ctx context.Context, plan ProvisionedPlan, options Options, session Session, queues QueueAPIFactory) (ValidationResult, error) {
	ledger := newObservationLedger()
	library := customerLibrary()
	factory := attributedFactory{inner: session.ConsumerFactory(), observer: ledger, now: options.Now}
	shim, err := shimSubscriber.New(shimSubscriber.Config{
		Participant: plan.Participants.Subscriber, Library: library,
		Handler: shimSubscriber.HandlerFunc(func(context.Context, customer.MessageView) error { return nil }),
		Factory: factory, Reporter: readinessSink{}, Contracts: subscriberContracts(plan.Groups),
	})
	if err != nil {
		return ValidationResult{}, err
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), options.PublishTimeout)
		defer cancelClose()
		_ = shim.Close(closeCtx)
	}()
	staticEpochs := make(map[string]EpochPlan, len(plan.Groups))
	for _, group := range plan.Groups {
		epoch, err := widestEpoch(group)
		if err != nil {
			return ValidationResult{}, err
		}
		staticEpochs[group.Group] = epoch
		for _, broker := range epoch.BrokerIDs {
			if err := shim.AddActive(ctx, shimSubscriber.Binding{Group: group.Group, BrokerID: broker, Epoch: epoch.Epoch, Destination: epoch.Queues[broker]}); err != nil {
				return ValidationResult{}, err
			}
		}
	}

	directory, err := os.MkdirTemp("", "swlb-qualification-outbox-")
	if err != nil {
		return ValidationResult{}, err
	}
	defer os.RemoveAll(directory)
	store, err := outbox.Open(filepath.Join(directory, "outbox.db"), outbox.Limits{MaxMessages: options.OutboxMaxMessages, MaxBytes: options.OutboxMaxBytes})
	if err != nil {
		return ValidationResult{}, err
	}
	defer store.Close()
	publisher, err := shimPublisher.New(shimPublisher.Config{
		Outbox: store, CustomerLibrary: library, Broker: session.BrokerPublisher(),
		Contracts: map[string]shimPublisher.Contract{
			FlightGroup:  {HashContract: routing.FlightOperationsDomain, LibraryVersion: qualificationLibraryVersion},
			BaggageGroup: {HashContract: routing.BaggageDomain, LibraryVersion: qualificationLibraryVersion},
		},
	})
	if err != nil {
		return ValidationResult{}, err
	}
	if err := enableStaticEpoch(ctx, plan, staticEpochs, queues); err != nil {
		return ValidationResult{}, err
	}
	for _, group := range plan.Groups {
		epoch := staticEpochs[group.Group]
		snapshot := activeSnapshotForEpoch(plan.Namespace, group, epoch, 1)
		if err := publisher.ApplyMembership(snapshot); err != nil {
			return ValidationResult{}, err
		}
	}
	work := messages(options.RunID, options.Keys, options.EventsPerKey, options.Now)
	started := options.Now()
	pacer := newWorkloadPacer(started, len(work), options.TargetDuration)
	dispatcher, err := shimPublisher.NewAsyncDispatcher(publisher, session.AsyncBrokerPublisher(), shimPublisher.AsyncDispatcherConfig{
		MaxInFlight: options.MaxInFlight, MaxInFlightPerBroker: options.MaxInFlightPerBroker,
		AckTimeout: options.PublishTimeout, CompletionBatchSize: 1024,
		CompletionFlushPeriod: 10 * time.Millisecond, AttemptObserver: ledger,
	})
	if err != nil {
		return ValidationResult{}, err
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), options.PublishTimeout)
		defer cancelClose()
		_ = dispatcher.Close(closeCtx)
	}()
	dispatchCtx, stopDispatch := context.WithCancel(ctx)
	dispatchResult := make(chan error, 1)
	go func() {
		dispatchResult <- dispatchContinuously(dispatchCtx, dispatcher, options.MaxInFlight, options.DispatchIdlePollInterval)
	}()
	var dispatchErr error
	var dispatchFinished bool
	stopDispatcher := func() error {
		stopDispatch()
		if !dispatchFinished {
			dispatchErr = <-dispatchResult
			dispatchFinished = true
		}
		if dispatchErr != nil && !errors.Is(dispatchErr, context.Canceled) {
			return dispatchErr
		}
		return nil
	}
	checkDispatcher := func(wait bool) (bool, error) {
		if !dispatchFinished {
			if wait {
				select {
				case dispatchErr = <-dispatchResult:
					dispatchFinished = true
				case <-ctx.Done():
					return true, ctx.Err()
				case <-time.After(time.Millisecond):
				}
			} else {
				select {
				case dispatchErr = <-dispatchResult:
					dispatchFinished = true
				default:
				}
			}
		}
		if !dispatchFinished {
			return false, nil
		}
		if dispatchErr == nil {
			return true, errors.New("qualification: publisher dispatcher stopped unexpectedly")
		}
		return true, dispatchErr
	}
	dispatchStopped := false
	defer func() {
		if !dispatchStopped {
			_ = stopDispatcher()
		}
	}()

	for offset := 0; offset < len(work); {
		end := min(offset+options.AcceptBatchSize, len(work))
		if err := pacer.Wait(ctx, end); err != nil {
			return ledger.result(started, options.Now()), err
		}
		for store.Stats().Messages+end-offset > options.OutboxMaxMessages {
			if stopped, err := checkDispatcher(true); stopped {
				return ledger.result(started, options.Now()), err
			}
		}
		if stopped, err := checkDispatcher(false); stopped {
			return ledger.result(started, options.Now()), err
		}
		// Stamp the application submission boundary immediately before durable
		// acceptance. Generating the full paced workload up front must not make later
		// messages appear to have spent the pacing interval in the publish path.
		sentAt := options.Now().UTC()
		for index := offset; index < end; index++ {
			work[index].Headers[HeaderSentNanos] = strconv.FormatInt(sentAt.UnixNano(), 10)
		}
		acceptStarted := time.Now()
		receipts, err := publisher.AcceptBatch(work[offset:end])
		acceptDuration := time.Since(acceptStarted)
		if err != nil {
			return ledger.result(started, options.Now()), err
		}
		ledger.observeAcceptance(acceptDuration, len(receipts))
		ledger.observeOutbox(store.Stats())
		for index, msg := range work[offset:end] {
			receipt := receipts[index]
			ledger.expect(msg.EventID, expectedEvent{group: receipt.Group, hash: receipt.Hash, broker: receipt.Broker, sequence: mustSequence(msg), sentAt: time.Unix(0, mustInt64(msg.Headers[HeaderSentNanos]))})
		}
		offset = end
	}
	if err := ledger.waitPositiveACKs(ctx, uint64(len(work))); err != nil {
		return ledger.result(started, options.Now()), err
	}
	if err := stopDispatcher(); err != nil {
		return ledger.result(started, options.Now()), err
	}
	dispatchStopped = true
	waitErr := ledger.wait(ctx)
	ended := options.Now()
	result := ledger.result(started, ended)
	if waitErr != nil {
		return result, waitErr
	}
	if result.Missing != 0 || result.Duplicates != 0 || result.OutOfOrder != 0 || result.UnexpectedBroker != 0 {
		return result, errors.New("qualification: traffic invariants failed")
	}
	for _, broker := range []string{"broker-a", "broker-b", "broker-c"} {
		if result.BrokerDistribution[broker] == 0 {
			return result, errors.New("qualification: traffic did not reach every broker")
		}
	}
	return result, nil
}

type workloadPacer struct {
	started  time.Time
	interval time.Duration
}

func newWorkloadPacer(started time.Time, events int, duration time.Duration) workloadPacer {
	if events <= 0 || duration <= 0 {
		return workloadPacer{}
	}
	return workloadPacer{started: started, interval: duration / time.Duration(events)}
}

func (p workloadPacer) Wait(ctx context.Context, admitted int) error {
	if p.interval <= 0 {
		return nil
	}
	deadline := p.started.Add(time.Duration(admitted) * p.interval)
	delay := time.Until(deadline)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitForDispatchCapacity(ctx context.Context, dispatcher *shimPublisher.AsyncDispatcher) error {
	for _, group := range []string{FlightGroup, BaggageGroup} {
		status := dispatcher.Status(group)
		if status.LastError != nil {
			return status.LastError
		}
		if status.AckUncertain != 0 || status.Uncertain != 0 {
			return errors.New("qualification: publisher acknowledgement became uncertain")
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		return nil
	}
}

func dispatchContinuously(ctx context.Context, dispatcher *shimPublisher.AsyncDispatcher, maxInFlight int, idlePollInterval time.Duration) error {
	idle := time.NewTicker(idlePollInterval)
	defer idle.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		report, err := dispatcher.Dispatch(ctx, maxInFlight)
		if err != nil && !errors.Is(err, integration.ErrPublisherBackpressure) {
			return err
		}
		for _, group := range []string{FlightGroup, BaggageGroup} {
			status := dispatcher.Status(group)
			if status.LastError != nil {
				return status.LastError
			}
			if status.AckUncertain != 0 || status.Uncertain != 0 {
				return errors.New("qualification: publisher acknowledgement became uncertain")
			}
		}
		if report.Attempted != 0 && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-idle.C:
		}
	}
}

func mustSequence(message customer.MessageView) uint64 {
	value, _ := strconv.ParseUint(message.Headers[HeaderSequence], 10, 64)
	return value
}
func mustInt64(value string) int64 { result, _ := strconv.ParseInt(value, 10, 64); return result }
