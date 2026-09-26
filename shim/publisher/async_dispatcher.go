package publisher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/outbox"
)

var ErrDispatcherClosed = errors.New("async dispatcher is closed")

const (
	completionRetryInitialDelay = 10 * time.Millisecond
	completionRetryMaxDelay     = time.Second
)

// PublishAttempt uniquely identifies one durable submission attempt. Adapters
// must carry it through their native correlation object and return it unchanged
// in the terminal result. Number is the outbox attempt count after the durable
// in-flight transition.
type PublishAttempt struct {
	EventID string
	Number  uint64
	Epoch   uint64
}

// AsyncPublishResult is one terminal native publish result. Acknowledged means a
// positive broker ACK. Rejected is safe to retry. Unknown, including an ACK
// timeout or disconnect after submission, is retained as ACK-uncertain.
type AsyncPublishResult struct {
	Attempt PublishAttempt
	Outcome PublishOutcome
	Err     error
}

// AsyncPublishFuture resolves one native persistent publish. Await cancellation
// does not imply that the broker rejected the message and is therefore treated
// as an ambiguous outcome by AsyncDispatcher.
type AsyncPublishFuture interface {
	Await(context.Context) (AsyncPublishResult, error)
}

// AsyncBrokerPublisher is the broker-independent adapter seam. Implementations
// must be safe for concurrent PublishAsync calls, including calls for the same
// broker. An error returned directly by PublishAsync means the native client
// definitively did not accept the submission. Once a future is returned, only
// its terminal result decides whether the durable record may be deleted or
// retried.
type AsyncBrokerPublisher interface {
	PublishAsync(context.Context, string, BrokerMessage, PublishAttempt) (AsyncPublishFuture, error)
}

// TerminalAttemptObservation is redacted performance evidence for one native
// submission. BrokerLatency measures from immediately before PublishAsync until
// its terminal result is available. The dispatcher emits it only after that
// result has been durably applied to the outbox. Event IDs, broker IDs, payloads,
// properties, destinations, and errors are deliberately absent.
type TerminalAttemptObservation struct {
	Group         string
	AttemptNumber uint64
	Epoch         uint64
	Outcome       PublishOutcome
	BrokerLatency time.Duration
}

// TerminalAttemptObserver receives terminal attempt evidence synchronously after
// durable completion. Implementations should return quickly and must not retain
// dispatcher-owned data; observer panics are isolated from publish correctness.
type TerminalAttemptObserver interface {
	ObserveTerminalAttempt(TerminalAttemptObservation)
}

// TerminalAttemptObserverFunc adapts a function to TerminalAttemptObserver.
type TerminalAttemptObserverFunc func(TerminalAttemptObservation)

func (f TerminalAttemptObserverFunc) ObserveTerminalAttempt(observation TerminalAttemptObservation) {
	f(observation)
}

// AsyncDispatcherConfig bounds unresolved native submissions and controls how
// terminal results are coalesced into bbolt transactions. AttemptObserver is
// optional and adds no per-attempt clock reads when absent.
type AsyncDispatcherConfig struct {
	// MaxInFlight bounds unresolved submissions across all brokers. When
	// MaxInFlightPerBroker is set, it additionally prevents a single broker from
	// consuming capacity provisioned on independent native publishers.
	MaxInFlight           int
	MaxInFlightPerBroker  int
	AckTimeout            time.Duration
	CompletionBatchSize   int
	CompletionFlushPeriod time.Duration
	AttemptObserver       TerminalAttemptObserver
}

// DispatcherStatus is safe to use as publisher transition-ACK evidence.
// Quiescent becomes true only after PAUSED is installed and no native attempt,
// durable in-flight record, or ACK-uncertain durable record remains.
type DispatcherStatus struct {
	Group           string
	Phase           control.Phase
	Epoch           uint64
	TransitionID    string
	Paused          bool
	Quiescent       bool
	InFlight        int
	NativeInFlight  int
	DurableInFlight int
	AckUncertain    int
	Acknowledged    uint64
	Rejected        uint64
	Uncertain       uint64
	LastError       error
}

type dispatcherCounters struct {
	acknowledged uint64
	rejected     uint64
	uncertain    uint64
	lastError    error
}

type pendingAttempt struct {
	attempt PublishAttempt
	group   string
	broker  string
}

type completedAttempt struct {
	attempt       PublishAttempt
	outcome       PublishOutcome
	err           error
	brokerLatency time.Duration
}

type completionStore interface {
	CompleteBatch([]outbox.Completion) ([]outbox.Completion, error)
}

// AsyncDispatcher durably admits one record per ordering lane, submits
// independent lanes asynchronously, and batches terminal outbox mutations. It
// is safe for concurrent Dispatch, Status, WaitQuiescent, and Close calls.
type AsyncDispatcher struct {
	publisher       *Publisher
	broker          AsyncBrokerPublisher
	completionStore completionStore
	config          AsyncDispatcherConfig

	dispatchMu sync.Mutex
	commitMu   sync.Mutex
	mu         sync.Mutex
	pending    map[string]pendingAttempt
	counters   map[string]*dispatcherCounters
	changed    chan struct{}
	closed     bool
	stopped    bool

	completions      chan completedAttempt
	persistenceRetry chan completedAttempt
	stop             chan struct{}
	done             chan struct{}
}

// NewAsyncDispatcher creates a high-throughput dispatcher over an existing
// Publisher's membership and durable outbox. The publisher's synchronous APIs
// remain available, but callers must not mix synchronous and asynchronous
// Dispatch calls for the same outbox.
func NewAsyncDispatcher(publisher *Publisher, broker AsyncBrokerPublisher, config AsyncDispatcherConfig) (*AsyncDispatcher, error) {
	if publisher == nil || broker == nil || config.MaxInFlight <= 0 || config.AckTimeout <= 0 {
		return nil, fmt.Errorf("%w: publisher, async broker, positive max in-flight, and ACK timeout are required", ErrInvalidConfig)
	}
	if config.MaxInFlightPerBroker < 0 || config.MaxInFlightPerBroker > config.MaxInFlight {
		return nil, fmt.Errorf("%w: per-broker max in-flight must be between zero and the global maximum", ErrInvalidConfig)
	}
	if config.CompletionBatchSize < 0 || config.CompletionFlushPeriod < 0 {
		return nil, fmt.Errorf("%w: completion batch settings must not be negative", ErrInvalidConfig)
	}
	if config.CompletionBatchSize == 0 {
		config.CompletionBatchSize = 64
	}
	if config.CompletionFlushPeriod == 0 {
		config.CompletionFlushPeriod = time.Millisecond
	}
	d := &AsyncDispatcher{
		publisher:        publisher,
		broker:           broker,
		completionStore:  publisher.outbox,
		config:           config,
		pending:          make(map[string]pendingAttempt, config.MaxInFlight),
		counters:         make(map[string]*dispatcherCounters),
		changed:          make(chan struct{}),
		completions:      make(chan completedAttempt, config.MaxInFlight),
		persistenceRetry: make(chan completedAttempt, config.MaxInFlight),
		stop:             make(chan struct{}),
		done:             make(chan struct{}),
	}
	go d.runCompletions()
	return d, nil
}

// Dispatch durably marks a batch in flight before making any native submission.
// Each admitted record is the sole pending head of its ordering lane, so native
// admission may proceed concurrently even when several lanes target one broker.
// The report covers admission and immediate known rejections; asynchronous
// terminal counts are available from Status.
func (d *AsyncDispatcher) Dispatch(ctx context.Context, limit int) (DispatchReport, error) {
	var report DispatchReport
	if limit <= 0 {
		return report, nil
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}

	d.dispatchMu.Lock()
	defer d.dispatchMu.Unlock()

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return report, ErrDispatcherClosed
	}
	available := d.config.MaxInFlight - len(d.pending)
	pendingByBroker := make(map[string]int)
	if d.config.MaxInFlightPerBroker > 0 {
		for _, pending := range d.pending {
			pendingByBroker[pending.broker]++
		}
	}
	d.mu.Unlock()
	if available <= 0 {
		return report, nil
	}
	if limit > available {
		limit = available
	}

	// This read lock is held through native admission. ApplyMembership(PAUSED)
	// therefore cannot return until every old-epoch submission has either entered
	// the native publisher or been durably returned to ready.
	d.publisher.mu.RLock()
	defer d.publisher.mu.RUnlock()

	activeEpochs := make(map[string]uint64, len(d.publisher.membership))
	var brokerSlots map[string]int
	if d.config.MaxInFlightPerBroker > 0 {
		brokerSlots = make(map[string]int)
	}
	for group, snapshot := range d.publisher.membership {
		if snapshot.Phase != control.PhaseActive && snapshot.Phase != control.PhasePrepare {
			continue
		}
		activeEpochs[group] = snapshot.Epoch
		if brokerSlots != nil {
			for _, broker := range snapshot.CurrentMembership {
				brokerSlots[broker] = max(0, d.config.MaxInFlightPerBroker-pendingByBroker[broker])
			}
		}
	}
	records := d.publisher.outbox.EligibleFor(limit, activeEpochs, brokerSlots)
	if len(records) == 0 {
		return report, nil
	}

	ids := make([]string, len(records))
	for i := range records {
		ids[i] = records[i].EventID
	}
	if err := d.publisher.outbox.MarkInFlightBatch(ids); err != nil {
		return report, err
	}

	d.mu.Lock()
	for _, record := range records {
		attempt := PublishAttempt{EventID: record.EventID, Number: record.Attempts + 1, Epoch: record.Epoch}
		d.pending[record.EventID] = pendingAttempt{attempt: attempt, group: record.Group, broker: record.Broker}
	}
	d.signalLocked()
	d.mu.Unlock()
	report.Attempted = len(records)

	var submissions sync.WaitGroup
	immediate := make(chan completedAttempt, len(records))
	for _, record := range records {
		record := record
		submissions.Add(1)
		go func() {
			attempt := PublishAttempt{EventID: record.EventID, Number: record.Attempts + 1, Epoch: record.Epoch}
			startedAt := time.Time{}
			if d.config.AttemptObserver != nil {
				startedAt = time.Now()
			}
			future, err := d.broker.PublishAsync(ctx, record.Broker, brokerMessage(record), attempt)
			if err != nil {
				immediate <- completedAttempt{attempt: attempt, outcome: OutcomeRejected, err: err, brokerLatency: elapsedSince(startedAt)}
				submissions.Done()
				return
			}
			if future == nil {
				immediate <- completedAttempt{attempt: attempt, outcome: OutcomeRejected, err: errors.New("async broker returned a nil future"), brokerLatency: elapsedSince(startedAt)}
				submissions.Done()
				return
			}
			// Admission is complete before waiting for the terminal result. Reusing
			// this goroutine avoids a second goroutine per successful submission.
			submissions.Done()
			d.await(future, attempt, startedAt)
		}()
	}
	submissions.Wait()
	close(immediate)

	var rejected []completedAttempt
	var dispatchErrors []error
	for completion := range immediate {
		rejected = append(rejected, completion)
		dispatchErrors = append(dispatchErrors, fmt.Errorf("publish %q: %w", completion.attempt.EventID, errors.Join(ErrPublishRejected, ErrPublishRetryable, completion.err)))
	}
	if len(rejected) != 0 {
		for _, completion := range d.applyCompletions(rejected) {
			// Preserve the terminal result for the completion worker's bounded
			// persistence retry. The native broker call is never repeated here.
			d.persistenceRetry <- completion
		}
		report.Rejected = len(rejected)
	}
	return report, errors.Join(dispatchErrors...)
}

func (d *AsyncDispatcher) await(future AsyncPublishFuture, attempt PublishAttempt, startedAt time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.AckTimeout)
	result, err := future.Await(ctx)
	cancel()
	completion := completedAttempt{attempt: attempt, outcome: OutcomeUnknown, err: err, brokerLatency: elapsedSince(startedAt)}
	if err == nil {
		completion.err = result.Err
		switch {
		case result.Attempt != attempt:
			completion.err = fmt.Errorf("publish correlation mismatch: got %+v, want %+v", result.Attempt, attempt)
		case result.Outcome == OutcomeAcknowledged && result.Err == nil:
			completion.outcome = OutcomeAcknowledged
		case result.Outcome == OutcomeRejected:
			completion.outcome = OutcomeRejected
		default:
			completion.outcome = OutcomeUnknown
		}
	}
	d.completions <- completion
}

func elapsedSince(startedAt time.Time) time.Duration {
	if startedAt.IsZero() {
		return 0
	}
	return time.Since(startedAt)
}

func (d *AsyncDispatcher) runCompletions() {
	defer close(d.done)
	batch := make([]completedAttempt, 0, d.config.CompletionBatchSize)
	retrying := make(map[string]completedAttempt)
	var retryTimer *time.Timer
	var retry <-chan time.Time
	retryDelay := completionRetryInitialDelay
	stopRetryTimer := func() {
		if retryTimer != nil && !retryTimer.Stop() {
			select {
			case <-retryTimer.C:
			default:
			}
		}
		retry = nil
	}
	scheduleRetry := func() {
		if len(retrying) == 0 || retry != nil {
			return
		}
		if retryTimer == nil {
			retryTimer = time.NewTimer(retryDelay)
		} else {
			retryTimer.Reset(retryDelay)
		}
		retry = retryTimer.C
		retryDelay = min(retryDelay*2, completionRetryMaxDelay)
	}
	defer stopRetryTimer()

	for {
		select {
		case <-d.stop:
			return
		case completion := <-d.persistenceRetry:
			retrying[completion.attempt.EventID] = completion
			batch = batch[:0]
			scheduleRetry()
			continue
		case <-retry:
			retry = nil
			batch = batch[:0]
			for _, completion := range retrying {
				batch = append(batch, completion)
			}
		case first := <-d.completions:
			batch = append(batch[:0], first)
			timer := time.NewTimer(d.config.CompletionFlushPeriod)
		collect:
			for len(batch) < d.config.CompletionBatchSize {
				select {
				case completion := <-d.completions:
					batch = append(batch, completion)
				case <-timer.C:
					// Receipts may already be runnable when the timer fires. Drain what is
					// queued before committing so a short flush period does not fragment a
					// burst into one durable transaction per scheduler turn.
					for len(batch) < d.config.CompletionBatchSize {
						select {
						case completion := <-d.completions:
							batch = append(batch, completion)
						default:
							break collect
						}
					}
					break collect
				case <-d.stop:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					failed := d.applyCompletions(batch)
					for _, completion := range failed {
						retrying[completion.attempt.EventID] = completion
					}
					return
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		failed := d.applyCompletions(batch)
		for _, completion := range batch {
			delete(retrying, completion.attempt.EventID)
		}
		for _, completion := range failed {
			retrying[completion.attempt.EventID] = completion
		}
		if len(retrying) == 0 {
			retryDelay = completionRetryInitialDelay
			stopRetryTimer()
		}
		scheduleRetry()
	}
}

// applyCompletions durably applies every matching terminal result it can. A
// result is returned only when persistence itself failed; stale or externally
// resolved attempts are final and do not need another retry.
func (d *AsyncDispatcher) applyCompletions(batch []completedAttempt) []completedAttempt {
	d.commitMu.Lock()

	updates := make([]outbox.Completion, len(batch))
	for i, completion := range batch {
		outcome := outbox.CompletionAckUncertain
		reason := errorText(completion.err, ErrAckUncertain)
		switch completion.outcome {
		case OutcomeAcknowledged:
			outcome = outbox.CompletionAcknowledged
			reason = ""
		case OutcomeRejected:
			outcome = outbox.CompletionRejected
			reason = errorText(completion.err, ErrPublishRejected)
		}
		updates[i] = outbox.Completion{
			EventID:  completion.attempt.EventID,
			Attempts: completion.attempt.Number,
			Epoch:    completion.attempt.Epoch,
			Outcome:  outcome,
			Reason:   reason,
		}
	}
	applied, err := d.completionStore.CompleteBatch(updates)
	var failedIDs map[string]struct{}
	var failed []completedAttempt
	if err != nil {
		failedIDs = make(map[string]struct{})
		// A transient transaction failure must not strand every unrelated lane in
		// the batch. Retry separately on this exceptional path; successful attempts
		// can then release their pending slots while failed ones remain pending for
		// the bounded background persistence retry.
		applied = applied[:0]
		failed = make([]completedAttempt, 0, len(batch))
		for i, update := range updates {
			completed, retryErr := d.completionStore.CompleteBatch([]outbox.Completion{update})
			if retryErr != nil {
				failed = append(failed, batch[i])
				failedIDs[update.EventID] = struct{}{}
				continue
			}
			applied = append(applied, completed...)
		}
		if len(failed) != 0 {
			d.recordPersistenceErrors(failed, err)
		}
	}
	appliedIDs := make(map[string]struct{}, len(applied))
	for _, completion := range applied {
		appliedIDs[completion.EventID] = struct{}{}
	}

	d.mu.Lock()
	changed := false
	observations := make([]TerminalAttemptObservation, 0, len(applied))
	for _, completion := range batch {
		pending, ok := d.pending[completion.attempt.EventID]
		if !ok || pending.attempt != completion.attempt {
			continue
		}
		if _, persistenceFailed := failedIDs[completion.attempt.EventID]; persistenceFailed {
			continue
		}
		if _, ok := appliedIDs[completion.attempt.EventID]; ok {
			counter := d.counterLocked(pending.group)
			switch completion.outcome {
			case OutcomeAcknowledged:
				counter.acknowledged++
			case OutcomeRejected:
				counter.rejected++
				counter.lastError = errors.Join(ErrPublishRejected, completion.err)
			default:
				counter.uncertain++
				counter.lastError = errors.Join(ErrAckUncertain, completion.err)
			}
			if d.config.AttemptObserver != nil {
				observations = append(observations, TerminalAttemptObservation{
					Group: pending.group, AttemptNumber: completion.attempt.Number, Epoch: completion.attempt.Epoch,
					Outcome: completion.outcome, BrokerLatency: completion.brokerLatency,
				})
			}
		}
		// A non-applied result is stale or was already resolved by an operator.
		// In either case it no longer represents a native in-flight attempt.
		delete(d.pending, completion.attempt.EventID)
		changed = true
	}
	if changed {
		d.signalLocked()
	}
	d.mu.Unlock()
	d.commitMu.Unlock()
	for _, observation := range observations {
		d.notifyAttemptObserver(observation)
	}
	return failed
}

func (d *AsyncDispatcher) notifyAttemptObserver(observation TerminalAttemptObservation) {
	defer func() { _ = recover() }()
	d.config.AttemptObserver.ObserveTerminalAttempt(observation)
}

func (d *AsyncDispatcher) recordPersistenceErrors(batch []completedAttempt, err error) {
	d.mu.Lock()
	for _, completion := range batch {
		if pending, ok := d.pending[completion.attempt.EventID]; ok && pending.attempt == completion.attempt {
			d.counterLocked(pending.group).lastError = err
		}
	}
	d.signalLocked()
	d.mu.Unlock()
}

func (d *AsyncDispatcher) counterLocked(group string) *dispatcherCounters {
	counter := d.counters[group]
	if counter == nil {
		counter = &dispatcherCounters{}
		d.counters[group] = counter
	}
	return counter
}

func (d *AsyncDispatcher) signalLocked() {
	close(d.changed)
	d.changed = make(chan struct{})
}

// Status reports durable and native state for one scaling group.
func (d *AsyncDispatcher) Status(group string) DispatcherStatus {
	status := DispatcherStatus{Group: group}
	d.publisher.mu.RLock()
	if snapshot, ok := d.publisher.membership[group]; ok {
		status.Phase = snapshot.Phase
		status.Epoch = snapshot.Epoch
		status.Paused = snapshot.Phase == control.PhasePaused
		if snapshot.Transition != nil {
			status.TransitionID = snapshot.Transition.ID
		}
	}
	d.publisher.mu.RUnlock()

	// Completion persistence and in-memory accounting are published as one status
	// observation, even though the outbox and counters use distinct locks.
	d.commitMu.Lock()
	defer d.commitMu.Unlock()
	for _, record := range d.publisher.outbox.Records() {
		if record.Group != group {
			continue
		}
		switch record.State {
		case outbox.StateInFlight:
			status.DurableInFlight++
		case outbox.StateAckUncertain:
			status.AckUncertain++
		}
	}
	d.mu.Lock()
	for _, pending := range d.pending {
		if pending.group == group {
			status.NativeInFlight++
		}
	}
	if counter := d.counters[group]; counter != nil {
		status.Acknowledged = counter.acknowledged
		status.Rejected = counter.rejected
		status.Uncertain = counter.uncertain
		status.LastError = counter.lastError
	}
	d.mu.Unlock()
	status.InFlight = max(status.NativeInFlight, status.DurableInFlight)
	status.Quiescent = status.Paused && status.NativeInFlight == 0 && status.DurableInFlight == 0 && status.AckUncertain == 0
	return status
}

// WaitQuiescent waits for PAUSED to be installed and every admitted native
// attempt for group to reach a durable terminal state. It returns the exact
// status suitable for deciding whether a transition acknowledgement is ready.
func (d *AsyncDispatcher) WaitQuiescent(ctx context.Context, group string) (DispatcherStatus, error) {
	poll := time.NewTicker(d.config.CompletionFlushPeriod)
	defer poll.Stop()
	for {
		// Capture the notification channel before reading status. A completion
		// between these operations closes this channel, avoiding a lost wake-up.
		d.mu.Lock()
		changed := d.changed
		d.mu.Unlock()
		status := d.Status(group)
		if status.Quiescent {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return d.Status(group), ctx.Err()
		case <-changed:
		case <-poll.C:
			// Membership changes occur on Publisher, so poll to observe PAUSED even
			// when there was no in-flight completion to signal this dispatcher.
		}
	}
}

// Close stops new dispatch admission and waits for all admitted attempts to
// become durably acknowledged, rejected, or ACK-uncertain. If ctx expires the
// dispatcher remains alive to resolve its outstanding futures, and Close may be
// called again.
func (d *AsyncDispatcher) Close(ctx context.Context) error {
	d.mu.Lock()
	d.closed = true
	d.signalLocked()
	d.mu.Unlock()
	for {
		d.mu.Lock()
		if d.stopped {
			d.mu.Unlock()
			return nil
		}
		if len(d.pending) == 0 {
			d.stopped = true
			close(d.stop)
			d.mu.Unlock()
			select {
			case <-d.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		changed := d.changed
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
