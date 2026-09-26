package publisher

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
	"github.com/solacese/solace-workload-balancer/routing"
)

type asyncBrokerFunc func(context.Context, string, BrokerMessage, PublishAttempt) (AsyncPublishFuture, error)

func (f asyncBrokerFunc) PublishAsync(ctx context.Context, broker string, message BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
	return f(ctx, broker, message, attempt)
}

type channelPublishFuture struct {
	result <-chan AsyncPublishResult
}

type asyncFutureFunc func(context.Context) (AsyncPublishResult, error)

func (f asyncFutureFunc) Await(ctx context.Context) (AsyncPublishResult, error) {
	return f(ctx)
}

func (f channelPublishFuture) Await(ctx context.Context) (AsyncPublishResult, error) {
	select {
	case result := <-f.result:
		return result, nil
	case <-ctx.Done():
		return AsyncPublishResult{}, ctx.Err()
	}
}

func resolvedFuture(result AsyncPublishResult) AsyncPublishFuture {
	ch := make(chan AsyncPublishResult, 1)
	ch <- result
	return channelPublishFuture{result: ch}
}

type faultingCompletionStore struct {
	store   *outbox.Store
	failing atomic.Bool
	calls   atomic.Int64
	called  chan int64
	err     error
}

func newFaultingCompletionStore(store *outbox.Store) *faultingCompletionStore {
	faults := &faultingCompletionStore{
		store:  store,
		called: make(chan int64, 64),
		err:    errors.New("injected completion persistence failure"),
	}
	faults.failing.Store(true)
	return faults
}

func (s *faultingCompletionStore) CompleteBatch(completions []outbox.Completion) ([]outbox.Completion, error) {
	fail := s.failing.Load()
	call := s.calls.Add(1)
	select {
	case s.called <- call:
	default:
	}
	if fail {
		return nil, s.err
	}
	return s.store.CompleteBatch(completions)
}

func waitForCompletionCalls(t *testing.T, store *faultingCompletionStore, want int64) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for store.calls.Load() < want {
		select {
		case <-store.called:
		case <-timer.C:
			t.Fatalf("completion persistence calls = %d, want at least %d", store.calls.Load(), want)
		}
	}
}

func TestAcceptBatchIsAtomicAndOrdered(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	messages := []customer.MessageView{message("event-1", "key-1"), message("event-2", "key-2")}
	receipts, err := publisher.AcceptBatch(messages)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 2 || receipts[0].EventID != "event-1" || receipts[1].EventID != "event-2" {
		t.Fatalf("receipts = %#v", receipts)
	}
	records := store.Records()
	if len(records) != 2 || records[0].EventID != "event-1" || records[1].EventID != "event-2" {
		t.Fatalf("records = %#v", records)
	}

	_, err = publisher.AcceptBatch([]customer.MessageView{message("event-3", "key-3"), message("event-1", "duplicate")})
	if !errors.Is(err, outbox.ErrDuplicate) {
		t.Fatalf("duplicate batch error = %v", err)
	}
	if records := store.Records(); len(records) != 2 {
		t.Fatalf("partial batch persisted: %#v", records)
	}
}

func TestAsyncDispatcherMarksWholeBatchInFlightBeforeSubmission(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	if _, err := publisher.AcceptBatch([]customer.MessageView{
		message("event-1", "key-1"), message("event-2", "key-2"), message("event-3", "key-3"),
	}); err != nil {
		t.Fatal(err)
	}
	var checked atomic.Bool
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		if checked.CompareAndSwap(false, true) {
			for _, eventID := range []string{"event-1", "event-2", "event-3"} {
				record, err := store.Get(eventID)
				if err != nil || record.State != outbox.StateInFlight || record.Attempts != 1 {
					t.Errorf("record %q at first submit = %#v, %v", eventID, record, err)
				}
			}
		}
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher := newTestDispatcher(t, publisher, broker, 3)
	report, err := dispatcher.Dispatch(context.Background(), 3)
	if err != nil || report.Attempted != 3 {
		t.Fatalf("Dispatch() = %+v, %v", report, err)
	}
	waitForOutboxCount(t, store, 0)
}

func TestAsyncDispatcherOneInFlightPerLaneAndBounded(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	if _, err := publisher.AcceptBatch([]customer.MessageView{
		message("lane-a-1", "lane-a"), message("lane-a-2", "lane-a"),
		message("lane-b-1", "lane-b"), message("lane-c-1", "lane-c"),
	}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	calls := make([]string, 0, 4)
	results := make(map[string]chan AsyncPublishResult)
	broker := asyncBrokerFunc(func(_ context.Context, _ string, message BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		mu.Lock()
		calls = append(calls, message.EventID)
		ch := make(chan AsyncPublishResult, 1)
		results[message.EventID] = ch
		mu.Unlock()
		return channelPublishFuture{result: ch}, nil
	})
	dispatcher := newTestDispatcher(t, publisher, broker, 2)
	report, err := dispatcher.Dispatch(context.Background(), 10)
	if err != nil || report.Attempted != 2 {
		t.Fatalf("first Dispatch() = %+v, %v", report, err)
	}
	if report, err := dispatcher.Dispatch(context.Background(), 10); err != nil || report.Attempted != 0 {
		t.Fatalf("bounded Dispatch() = %+v, %v", report, err)
	}
	mu.Lock()
	firstCalls := append([]string(nil), calls...)
	mu.Unlock()
	if contains(firstCalls, "lane-a-2") {
		t.Fatalf("same-lane follower submitted early: %v", firstCalls)
	}

	mu.Lock()
	firstID := calls[0]
	firstResult := results[firstID]
	mu.Unlock()
	firstRecord, err := store.Get(firstID)
	if err != nil {
		t.Fatal(err)
	}
	firstResult <- AsyncPublishResult{Attempt: PublishAttempt{EventID: firstID, Number: firstRecord.Attempts, Epoch: firstRecord.Epoch}, Outcome: OutcomeAcknowledged}
	waitForRecordMissing(t, store, firstID)
	waitForPendingCount(t, dispatcher, 1)

	report, err = dispatcher.Dispatch(context.Background(), 10)
	if err != nil || report.Attempted != 1 {
		t.Fatalf("refill Dispatch() = %+v, %v", report, err)
	}
	mu.Lock()
	allCalls := append([]string(nil), calls...)
	pendingResults := make(map[string]chan AsyncPublishResult, len(results))
	for eventID, ch := range results {
		pendingResults[eventID] = ch
	}
	mu.Unlock()
	if firstID == "lane-a-1" && !contains(allCalls, "lane-a-2") {
		t.Fatalf("same-lane follower did not become eligible: %v", allCalls)
	}
	for eventID, ch := range pendingResults {
		if eventID == firstID {
			continue
		}
		record, getErr := store.Get(eventID)
		if getErr == nil && record.State == outbox.StateInFlight {
			ch <- AsyncPublishResult{Attempt: PublishAttempt{EventID: eventID, Number: record.Attempts, Epoch: record.Epoch}, Outcome: OutcomeAcknowledged}
		}
	}
}

func TestAsyncDispatcherAdmitsIndependentLanesConcurrently(t *testing.T) {
	const recordsPerBroker = 16
	tests := []struct {
		name       string
		membership []string
	}{
		{name: "one broker", membership: []string{"broker-a"}},
		{name: "multiple brokers", membership: []string{"broker-a", "broker-b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			publisher, store, _ := newTestPublisherWithLimits(t, outbox.Limits{
				MaxMessages: recordsPerBroker * len(test.membership),
				MaxBytes:    1 << 20,
			})
			apply(t, publisher, activeSnapshot(1, 1, test.membership...))
			messages := make([]customer.MessageView, 0, recordsPerBroker*len(test.membership))
			for _, brokerID := range test.membership {
				for i := 0; i < recordsPerBroker; i++ {
					key := keyForBroker(t, brokerID, test.membership, i)
					messages = append(messages, message(fmt.Sprintf("%s-%d", brokerID, i), key))
				}
			}
			if _, err := publisher.AcceptBatch(messages); err != nil {
				t.Fatal(err)
			}

			var mu sync.Mutex
			activeByBroker := make(map[string]int)
			maxByBroker := make(map[string]int)
			activeTotal, maxTotal := 0, 0
			entered := make(chan string, len(messages))
			release := make(chan struct{})
			broker := asyncBrokerFunc(func(_ context.Context, brokerID string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
				mu.Lock()
				activeByBroker[brokerID]++
				activeTotal++
				maxByBroker[brokerID] = max(maxByBroker[brokerID], activeByBroker[brokerID])
				maxTotal = max(maxTotal, activeTotal)
				mu.Unlock()
				entered <- brokerID
				<-release
				mu.Lock()
				activeByBroker[brokerID]--
				activeTotal--
				mu.Unlock()
				return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
			})
			dispatcher := newTestDispatcher(t, publisher, broker, len(messages))
			var releaseOnce sync.Once
			releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseAll)

			dispatched := make(chan error, 1)
			go func() {
				_, err := dispatcher.Dispatch(context.Background(), len(messages))
				dispatched <- err
			}()

			enteredByBroker := make(map[string]int)
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			for total := 0; total < len(messages); total++ {
				select {
				case brokerID := <-entered:
					enteredByBroker[brokerID]++
				case <-deadline.C:
					t.Fatalf("only %d/%d native calls entered concurrently: %v", total, len(messages), enteredByBroker)
				}
			}
			releaseAll()
			if err := <-dispatched; err != nil {
				t.Fatal(err)
			}
			waitForOutboxCount(t, store, 0)

			mu.Lock()
			defer mu.Unlock()
			if maxTotal != len(messages) {
				t.Fatalf("maximum global submission concurrency = %d, want %d", maxTotal, len(messages))
			}
			for _, brokerID := range test.membership {
				if maxByBroker[brokerID] != recordsPerBroker {
					t.Fatalf("maximum concurrent calls for %s = %d, want %d", brokerID, maxByBroker[brokerID], recordsPerBroker)
				}
			}
		})
	}
}

func TestAsyncDispatcherLimitsPendingPerBroker(t *testing.T) {
	const perBroker = 2
	membership := []string{"broker-a", "broker-b"}
	publisher, store, _ := newTestPublisherWithLimits(t, outbox.Limits{MaxMessages: 16, MaxBytes: 1 << 20})
	apply(t, publisher, activeSnapshot(1, 1, membership...))
	messages := make([]customer.MessageView, 0, 8)
	for _, brokerID := range membership {
		for i := 0; i < 4; i++ {
			messages = append(messages, message(fmt.Sprintf("%s-%d", brokerID, i), keyForBroker(t, brokerID, membership, i)))
		}
	}
	if _, err := publisher.AcceptBatch(messages); err != nil {
		t.Fatal(err)
	}

	entered := make(chan string, len(messages))
	release := make(chan struct{})
	broker := asyncBrokerFunc(func(_ context.Context, brokerID string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		entered <- brokerID
		<-release
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher, err := NewAsyncDispatcher(publisher, broker, AsyncDispatcherConfig{
		MaxInFlight: 8, MaxInFlightPerBroker: perBroker, AckTimeout: time.Second,
		CompletionBatchSize: 8, CompletionFlushPeriod: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseAll()
		_ = dispatcher.Close(context.Background())
	})

	dispatched := make(chan DispatchReport, 1)
	go func() {
		report, _ := dispatcher.Dispatch(context.Background(), len(messages))
		dispatched <- report
	}()
	counts := make(map[string]int)
	for total := 0; total < len(membership)*perBroker; total++ {
		select {
		case brokerID := <-entered:
			counts[brokerID]++
		case <-time.After(time.Second):
			t.Fatalf("only %d submissions entered: %v", total, counts)
		}
	}
	select {
	case brokerID := <-entered:
		t.Fatalf("broker %s exceeded per-broker pending budget: %v", brokerID, counts)
	case <-time.After(10 * time.Millisecond):
	}
	releaseAll()
	if report := <-dispatched; report.Attempted != len(membership)*perBroker {
		t.Fatalf("attempted = %d, want %d", report.Attempted, len(membership)*perBroker)
	}
	waitForOutboxCount(t, store, len(messages)-len(membership)*perBroker)
}

func TestAsyncDispatcherConcurrentAdmissionHoldsMembershipUntilAllCallsReturn(t *testing.T) {
	const messageCount = 8
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	messages := make([]customer.MessageView, messageCount)
	for i := range messages {
		messages[i] = message(fmt.Sprintf("event-%d", i), fmt.Sprintf("key-%d", i))
	}
	if _, err := publisher.AcceptBatch(messages); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{}, messageCount)
	release := make(chan struct{})
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		entered <- struct{}{}
		<-release
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher := newTestDispatcher(t, publisher, broker, messageCount)
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	dispatched := make(chan error, 1)
	go func() {
		_, err := dispatcher.Dispatch(context.Background(), messageCount)
		dispatched <- err
	}()
	for i := 0; i < messageCount; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("only %d/%d native calls entered", i, messageCount)
		}
	}

	paused := make(chan error, 1)
	go func() { paused <- publisher.ApplyMembership(pausedSnapshot(2, 1)) }()
	select {
	case err := <-paused:
		t.Fatalf("ApplyMembership returned before native admission completed: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	releaseAll()
	if err := <-dispatched; err != nil {
		t.Fatal(err)
	}
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	waitForOutboxCount(t, store, 0)
	if status := dispatcher.Status("orders"); !status.Paused || status.Epoch != 1 {
		t.Fatalf("status after pause = %+v", status)
	}
}

func TestAsyncDispatcherConcurrentAdmissionPreservesSameKeyOrder(t *testing.T) {
	const (
		sameKeyRecords  = 32
		independentKeys = 64
		maxInFlight     = 16
	)
	publisher, store, _ := newTestPublisherWithLimits(t, outbox.Limits{
		MaxMessages: sameKeyRecords + independentKeys,
		MaxBytes:    1 << 20,
	})
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	messages := make([]customer.MessageView, 0, sameKeyRecords+independentKeys)
	for i := 0; i < sameKeyRecords; i++ {
		messages = append(messages, message(fmt.Sprintf("same-%02d", i), "same-key"))
	}
	for i := 0; i < independentKeys; i++ {
		messages = append(messages, message(fmt.Sprintf("other-%02d", i), fmt.Sprintf("other-key-%02d", i)))
	}
	if _, err := publisher.AcceptBatch(messages); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	activeSameKey := 0
	maxActiveSameKey := 0
	sameKeyOrder := make([]string, 0, sameKeyRecords)
	broker := asyncBrokerFunc(func(_ context.Context, _ string, message BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		isSameKey := strings.HasPrefix(message.EventID, "same-")
		if isSameKey {
			mu.Lock()
			activeSameKey++
			maxActiveSameKey = max(maxActiveSameKey, activeSameKey)
			sameKeyOrder = append(sameKeyOrder, message.EventID)
			mu.Unlock()
		}
		return asyncFutureFunc(func(context.Context) (AsyncPublishResult, error) {
			runtime.Gosched()
			if isSameKey {
				mu.Lock()
				activeSameKey--
				mu.Unlock()
			}
			return AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}, nil
		}), nil
	})
	dispatcher := newTestDispatcher(t, publisher, broker, maxInFlight)
	deadline := time.Now().Add(5 * time.Second)
	for store.Stats().Messages != 0 && time.Now().Before(deadline) {
		if _, err := dispatcher.Dispatch(context.Background(), maxInFlight); err != nil {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	waitForOutboxCount(t, store, 0)
	waitForPendingCount(t, dispatcher, 0)

	mu.Lock()
	defer mu.Unlock()
	if maxActiveSameKey != 1 {
		t.Fatalf("same-key native concurrency = %d, want 1", maxActiveSameKey)
	}
	if len(sameKeyOrder) != sameKeyRecords {
		t.Fatalf("same-key submissions = %d, want %d: %v", len(sameKeyOrder), sameKeyRecords, sameKeyOrder)
	}
	for i, eventID := range sameKeyOrder {
		want := fmt.Sprintf("same-%02d", i)
		if eventID != want {
			t.Fatalf("same-key submission %d = %q, want %q; order=%v", i, eventID, want, sameKeyOrder)
		}
	}
}

func TestAsyncDispatcherClassifiesImmediateAndAmbiguousFailures(t *testing.T) {
	t.Run("immediate rejection is retryable", func(t *testing.T) {
		publisher, store, _ := newTestPublisher(t)
		apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
		receipt, err := publisher.Accept(message("event-1", "key"))
		if err != nil {
			t.Fatal(err)
		}
		broker := asyncBrokerFunc(func(context.Context, string, BrokerMessage, PublishAttempt) (AsyncPublishFuture, error) {
			return nil, errors.New("native buffer rejected")
		})
		dispatcher := newTestDispatcher(t, publisher, broker, 1)
		report, err := dispatcher.Dispatch(context.Background(), 1)
		if !errors.Is(err, ErrPublishRejected) || !strings.Contains(err.Error(), "native buffer rejected") || report.Rejected != 1 {
			t.Fatalf("Dispatch() = %+v, %v", report, err)
		}
		record, err := store.Get(receipt.EventID)
		if err != nil || record.State != outbox.StateReady || record.Attempts != 1 {
			t.Fatalf("rejected record = %#v, %v", record, err)
		}
	})

	t.Run("disconnect after submission is uncertain", func(t *testing.T) {
		publisher, store, _ := newTestPublisher(t)
		apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
		receipt, err := publisher.Accept(message("event-1", "key"))
		if err != nil {
			t.Fatal(err)
		}
		broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
			return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeUnknown, Err: errors.New("connection lost after send")}), nil
		})
		dispatcher := newTestDispatcher(t, publisher, broker, 1)
		if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		waitForState(t, store, receipt.EventID, outbox.StateAckUncertain)
		status := dispatcher.Status("orders")
		if status.Uncertain != 1 || status.InFlight != 0 || status.AckUncertain != 1 {
			t.Fatalf("status = %+v", status)
		}
	})

	t.Run("ACK timeout is uncertain", func(t *testing.T) {
		publisher, store, _ := newTestPublisher(t)
		apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
		receipt, err := publisher.Accept(message("event-1", "key"))
		if err != nil {
			t.Fatal(err)
		}
		never := make(chan AsyncPublishResult)
		broker := asyncBrokerFunc(func(context.Context, string, BrokerMessage, PublishAttempt) (AsyncPublishFuture, error) {
			return channelPublishFuture{result: never}, nil
		})
		dispatcher, err := NewAsyncDispatcher(publisher, broker, AsyncDispatcherConfig{
			MaxInFlight: 1, AckTimeout: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = dispatcher.Close(context.Background()) })
		if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		waitForState(t, store, receipt.EventID, outbox.StateAckUncertain)
	})
}

func TestAsyncDispatcherRetriesTerminalPersistenceWithoutRepublishing(t *testing.T) {
	tests := []struct {
		name       string
		outcome    PublishOutcome
		resultErr  error
		wantState  outbox.State
		wantAcked  uint64
		wantReject uint64
		wantUnsure uint64
	}{
		{name: "ACK", outcome: OutcomeAcknowledged, wantAcked: 1},
		{name: "rejection", outcome: OutcomeRejected, resultErr: errors.New("broker rejected"), wantState: outbox.StateReady, wantReject: 1},
		{name: "uncertain", outcome: OutcomeUnknown, resultErr: errors.New("connection lost"), wantState: outbox.StateAckUncertain, wantUnsure: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			publisher, store, _ := newTestPublisher(t)
			apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
			if _, err := publisher.Accept(message("event-1", "key")); err != nil {
				t.Fatal(err)
			}
			var submissions atomic.Int64
			broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
				submissions.Add(1)
				return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: test.outcome, Err: test.resultErr}), nil
			})
			dispatcher := newTestDispatcher(t, publisher, broker, 1)
			faults := newFaultingCompletionStore(store)
			dispatcher.completionStore = faults

			if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			// First the batch and single-record fallback fail. A delayed background
			// retry must retain the exact terminal result rather than broker-submit it.
			waitForCompletionCalls(t, faults, 2)
			waitForPendingCount(t, dispatcher, 1)
			record, err := store.Get("event-1")
			if err != nil || record.State != outbox.StateInFlight || record.Attempts != 1 {
				t.Fatalf("record while persistence fails = %#v, %v", record, err)
			}
			firstRetryCalls := faults.calls.Load()
			time.Sleep(completionRetryInitialDelay / 2)
			if got := faults.calls.Load(); got != firstRetryCalls {
				t.Fatalf("persistence retry busy-looped: calls advanced from %d to %d", firstRetryCalls, got)
			}
			apply(t, publisher, pausedSnapshot(2, 1))
			blockedCtx, blockedCancel := context.WithTimeout(context.Background(), completionRetryInitialDelay/2)
			if status, waitErr := dispatcher.WaitQuiescent(blockedCtx, "orders"); !errors.Is(waitErr, context.DeadlineExceeded) || status.Quiescent {
				blockedCancel()
				t.Fatalf("WaitQuiescent while persistence fails = %+v, %v", status, waitErr)
			}
			blockedCancel()

			faults.failing.Store(false)
			waitForCompletionCalls(t, faults, firstRetryCalls+1)
			waitForPendingCount(t, dispatcher, 0)
			if got := submissions.Load(); got != 1 {
				t.Fatalf("native submissions = %d, want exactly 1", got)
			}
			if test.outcome == OutcomeAcknowledged {
				if _, err := store.Get("event-1"); !errors.Is(err, outbox.ErrNotFound) {
					t.Fatalf("ACKed record remains: %v", err)
				}
			} else {
				record = waitForState(t, store, "event-1", test.wantState)
				if record.Attempts != 1 || record.Epoch != 1 {
					t.Fatalf("scoped completion changed attempt = %#v", record)
				}
			}
			status := dispatcher.Status("orders")
			if status.Acknowledged != test.wantAcked || status.Rejected != test.wantReject || status.Uncertain != test.wantUnsure {
				t.Fatalf("status = %+v", status)
			}
		})
	}
}

func TestAsyncDispatcherImmediateRejectionPersistenceRetryIsDelayed(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	if _, err := publisher.Accept(message("event-1", "key")); err != nil {
		t.Fatal(err)
	}
	var submissions atomic.Int64
	broker := asyncBrokerFunc(func(context.Context, string, BrokerMessage, PublishAttempt) (AsyncPublishFuture, error) {
		submissions.Add(1)
		return nil, errors.New("native buffer rejected")
	})
	dispatcher := newTestDispatcher(t, publisher, broker, 1)
	faults := newFaultingCompletionStore(store)
	dispatcher.completionStore = faults

	if _, err := dispatcher.Dispatch(context.Background(), 1); !errors.Is(err, ErrPublishRejected) {
		t.Fatalf("Dispatch error = %v, want rejection", err)
	}
	waitForCompletionCalls(t, faults, 2)
	calls := faults.calls.Load()
	time.Sleep(completionRetryInitialDelay / 2)
	if got := faults.calls.Load(); got != calls {
		t.Fatalf("immediate rejection retry was not delayed: calls advanced from %d to %d", calls, got)
	}
	faults.failing.Store(false)
	waitForCompletionCalls(t, faults, calls+1)
	waitForState(t, store, "event-1", outbox.StateReady)
	waitForPendingCount(t, dispatcher, 0)
	if got := submissions.Load(); got != 1 {
		t.Fatalf("native submissions = %d, want 1", got)
	}
}

func TestAsyncDispatcherPersistenceRetryIsAttemptScoped(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	if _, err := publisher.Accept(message("event-1", "key")); err != nil {
		t.Fatal(err)
	}
	var submissions atomic.Int64
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		submissions.Add(1)
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher := newTestDispatcher(t, publisher, broker, 1)
	faults := newFaultingCompletionStore(store)
	dispatcher.completionStore = faults

	if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitForCompletionCalls(t, faults, 2)
	if err := store.MarkRejected("event-1", "operator resolved attempt"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkInFlight("event-1"); err != nil {
		t.Fatal(err)
	}
	faults.failing.Store(false)
	waitForCompletionCalls(t, faults, 3)
	waitForPendingCount(t, dispatcher, 0)

	record, err := store.Get("event-1")
	if err != nil || record.State != outbox.StateInFlight || record.Attempts != 2 || record.Epoch != 1 {
		t.Fatalf("newer durable attempt changed by stale retry = %#v, %v", record, err)
	}
	if got := submissions.Load(); got != 1 {
		t.Fatalf("native submissions = %d, want 1", got)
	}
}

func TestAsyncDispatcherCloseTimesOutDuringPersistenceFailureThenRecovers(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	if _, err := publisher.Accept(message("event-1", "key")); err != nil {
		t.Fatal(err)
	}
	var submissions atomic.Int64
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		submissions.Add(1)
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher, err := NewAsyncDispatcher(publisher, broker, AsyncDispatcherConfig{
		MaxInFlight: 1, AckTimeout: time.Second, CompletionBatchSize: 1, CompletionFlushPeriod: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	faults := newFaultingCompletionStore(store)
	dispatcher.completionStore = faults
	if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitForCompletionCalls(t, faults, 2)

	closeCtx, cancel := context.WithTimeout(context.Background(), completionRetryInitialDelay/2)
	defer cancel()
	if err := dispatcher.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close during persistence failure = %v, want deadline", err)
	}
	if status := dispatcher.Status("orders"); status.NativeInFlight != 1 || status.DurableInFlight != 1 {
		t.Fatalf("status after timed-out close = %+v", status)
	}
	faults.failing.Store(false)
	waitForCompletionCalls(t, faults, 3)

	recoverCtx, recoverCancel := context.WithTimeout(context.Background(), time.Second)
	defer recoverCancel()
	if err := dispatcher.Close(recoverCtx); err != nil {
		t.Fatalf("Close after persistence recovery = %v", err)
	}
	if _, err := store.Get("event-1"); !errors.Is(err, outbox.ErrNotFound) {
		t.Fatalf("ACKed record remains after close recovery: %v", err)
	}
	if got := submissions.Load(); got != 1 {
		t.Fatalf("native submissions = %d, want 1", got)
	}
}

func TestAsyncDispatcherObserverReportsOnlyDurablyAppliedRedactedTerminals(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	if _, err := publisher.Accept(message("secret-event-id", "secret-business-key")); err != nil {
		t.Fatal(err)
	}
	observed := make(chan TerminalAttemptObservation, 1)
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher, err := NewAsyncDispatcher(publisher, broker, AsyncDispatcherConfig{
		MaxInFlight: 1, AckTimeout: time.Second, CompletionBatchSize: 1,
		CompletionFlushPeriod: time.Millisecond,
		AttemptObserver:       TerminalAttemptObserverFunc(func(observation TerminalAttemptObservation) { observed <- observation }),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dispatcher.Close(context.Background()) })
	if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	select {
	case observation := <-observed:
		if observation.Group != "orders" || observation.AttemptNumber != 1 || observation.Epoch != 1 || observation.Outcome != OutcomeAcknowledged || observation.BrokerLatency < 0 {
			t.Fatalf("observation = %+v", observation)
		}
		if _, err := store.Get("secret-event-id"); !errors.Is(err, outbox.ErrNotFound) {
			t.Fatalf("observer ran before durable ACK deletion: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal attempt observer was not called")
	}
}

func TestAsyncDispatcherObserverPanicDoesNotChangeCompletion(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	if _, err := publisher.Accept(message("event-1", "key")); err != nil {
		t.Fatal(err)
	}
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher, err := NewAsyncDispatcher(publisher, broker, AsyncDispatcherConfig{
		MaxInFlight: 1, AckTimeout: time.Second, CompletionBatchSize: 1,
		CompletionFlushPeriod: time.Millisecond,
		AttemptObserver:       TerminalAttemptObserverFunc(func(TerminalAttemptObservation) { panic("observer") }),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dispatcher.Close(context.Background()) })
	if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitForOutboxCount(t, store, 0)
	waitForPendingCount(t, dispatcher, 0)
}

func TestAsyncDispatcherRejectsMismatchedAttemptCorrelation(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	receipt, err := publisher.Accept(message("event-1", "key"))
	if err != nil {
		t.Fatal(err)
	}
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		wrong := attempt
		wrong.Number++
		return resolvedFuture(AsyncPublishResult{Attempt: wrong, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher := newTestDispatcher(t, publisher, broker, 1)
	if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitForState(t, store, receipt.EventID, outbox.StateAckUncertain)
}

func TestAsyncDispatcherBatchAcknowledgementsDeleteRecords(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	messages := make([]customer.MessageView, 32)
	for i := range messages {
		messages[i] = message(fmt.Sprintf("event-%d", i), fmt.Sprintf("key-%d", i))
	}
	if _, err := publisher.AcceptBatch(messages); err != nil {
		t.Fatal(err)
	}
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher := newTestDispatcher(t, publisher, broker, len(messages))
	if report, err := dispatcher.Dispatch(context.Background(), len(messages)); err != nil || report.Attempted != len(messages) {
		t.Fatalf("Dispatch() = %+v, %v", report, err)
	}
	waitForOutboxCount(t, store, 0)
	waitForPendingCount(t, dispatcher, 0)
	status := dispatcher.Status("orders")
	if status.Acknowledged != uint64(len(messages)) {
		t.Fatalf("status = %+v", status)
	}
}

func TestAsyncDispatcherPauseAndQuiesceStatus(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	if _, err := publisher.Accept(message("event-1", "key")); err != nil {
		t.Fatal(err)
	}
	result := make(chan AsyncPublishResult, 1)
	attempts := make(chan PublishAttempt, 1)
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		attempts <- attempt
		return channelPublishFuture{result: result}, nil
	})
	dispatcher := newTestDispatcher(t, publisher, broker, 1)
	if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	attempt := <-attempts
	apply(t, publisher, pausedSnapshot(2, 1))
	status := dispatcher.Status("orders")
	if !status.Paused || status.Quiescent || status.InFlight != 1 || status.TransitionID != "transition-1" {
		t.Fatalf("paused status = %+v", status)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	quiesced := make(chan DispatcherStatus, 1)
	go func() {
		status, _ := dispatcher.WaitQuiescent(waitCtx, "orders")
		quiesced <- status
	}()
	result <- AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}
	status = <-quiesced
	if !status.Quiescent || status.InFlight != 0 || status.Acknowledged != 1 {
		t.Fatalf("quiescent status = %+v", status)
	}
	if stats := store.Stats(); stats.Messages != 0 {
		t.Fatalf("outbox messages = %d", stats.Messages)
	}
}

func TestAsyncDispatcherAckUncertainPreventsQuiescenceUntilScopedResolution(t *testing.T) {
	publisher, store, _ := newTestPublisher(t)
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	receipt, err := publisher.Accept(message("event-1", "key"))
	if err != nil {
		t.Fatal(err)
	}
	attempts := make(chan PublishAttempt, 1)
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		attempts <- attempt
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeUnknown, Err: errors.New("ACK timeout")}), nil
	})
	dispatcher := newTestDispatcher(t, publisher, broker, 1)
	if _, err := dispatcher.Dispatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	attempt := <-attempts
	waitForState(t, store, receipt.EventID, outbox.StateAckUncertain)
	apply(t, publisher, pausedSnapshot(2, 1))

	status := dispatcher.Status("orders")
	if status.Quiescent || status.AckUncertain != 1 || status.Uncertain != 1 {
		t.Fatalf("uncertain paused status = %+v", status)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if status, err := dispatcher.WaitQuiescent(waitCtx, "orders"); !errors.Is(err, context.DeadlineExceeded) || status.Quiescent {
		t.Fatalf("WaitQuiescent with uncertain record = %+v, %v", status, err)
	}

	if err := publisher.Ack(attempt.EventID, attempt.Number, attempt.Epoch); err != nil {
		t.Fatal(err)
	}
	resolvedCtx, resolvedCancel := context.WithTimeout(context.Background(), time.Second)
	defer resolvedCancel()
	status, err = dispatcher.WaitQuiescent(resolvedCtx, "orders")
	if err != nil || !status.Quiescent || status.AckUncertain != 0 || status.Uncertain != 1 {
		t.Fatalf("WaitQuiescent after scoped ACK = %+v, %v", status, err)
	}
}

func TestAsyncDispatcherRestartRetainsAttemptAndDoesNotReassignUncertain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store, err := outbox.Open(path, outbox.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := New(Config{
		Outbox: store, CustomerLibrary: fixedLibrary{}, Broker: &fakeBroker{},
		ScalingGroup: "orders", HashContract: "sha256/customer-v1", LibraryVersion: "library-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	receipt, err := publisher.Accept(message("event-1", "key"))
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a process crash in the exact durable window after the dispatcher
	// commits in-flight state and before a native receipt is observed.
	if err := store.MarkInFlightBatch([]string{receipt.EventID}); err != nil {
		t.Fatal(err)
	}
	inFlight, err := store.Get(receipt.EventID)
	if err != nil || inFlight.State != outbox.StateInFlight || inFlight.Attempts != 1 {
		t.Fatalf("before restart = %#v, %v", inFlight, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := outbox.Open(path, outbox.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	restarted, err := New(Config{
		Outbox: recovered, CustomerLibrary: fixedLibrary{}, Broker: &fakeBroker{},
		ScalingGroup: "orders", HashContract: "sha256/customer-v1", LibraryVersion: "library-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	apply(t, restarted, activeSnapshot(2, 2, "broker-b"))
	record, err := recovered.Get(receipt.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != outbox.StateAckUncertain || record.Attempts != 1 || record.Epoch != 1 || record.Broker != "broker-a" {
		t.Fatalf("recovered record was lost or reassigned = %#v", record)
	}
}

func TestAsyncDispatcherImmediateReceiptsAcrossDistinctLanes(t *testing.T) {
	const messageCount = 1024
	publisher, store, _ := newTestPublisherWithLimits(t, outbox.Limits{
		MaxMessages: messageCount + 1,
		MaxBytes:    4 << 20,
	})
	apply(t, publisher, activeSnapshot(1, 1, "broker-a"))
	messages := make([]customer.MessageView, messageCount)
	for i := range messages {
		messages[i] = message(fmt.Sprintf("event-%04d", i), fmt.Sprintf("key-%04d", i))
	}
	if _, err := publisher.AcceptBatch(messages); err != nil {
		t.Fatal(err)
	}
	var submissions atomic.Int64
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		submissions.Add(1)
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher, err := NewAsyncDispatcher(publisher, broker, AsyncDispatcherConfig{
		MaxInFlight: messageCount, AckTimeout: time.Second,
		CompletionBatchSize: 256, CompletionFlushPeriod: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dispatcher.Close(context.Background()) })

	report, err := dispatcher.Dispatch(context.Background(), messageCount)
	if err != nil || report.Attempted != messageCount {
		t.Fatalf("Dispatch() = %+v, %v", report, err)
	}
	waitForOutboxCount(t, store, 0)
	waitForPendingCount(t, dispatcher, 0)
	status := dispatcher.Status("orders")
	if got := submissions.Load(); got != messageCount {
		t.Fatalf("submissions = %d, want %d", got, messageCount)
	}
	if status.Acknowledged != messageCount || status.Rejected != 0 || status.Uncertain != 0 || status.InFlight != 0 {
		t.Fatalf("status = %+v", status)
	}
}

func BenchmarkPublisherAcceptBatch100(b *testing.B) {
	publisher, err := benchmarkPublisher(b, b.N*100+1)
	if err != nil {
		b.Fatal(err)
	}
	messages := make([]customer.MessageView, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for i := range messages {
			messages[i] = message(fmt.Sprintf("event-%d-%d", iteration, i), fmt.Sprintf("key-%d", i))
		}
		if _, err := publisher.AcceptBatch(messages); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAsyncDispatcherSubmit100(b *testing.B) {
	benchmarkAsyncDispatcherReceipts(b, 100, time.Microsecond, 0)
}

// BenchmarkAsyncDispatcherConcurrentAdmission100 models native submission work
// for 100 independent lane heads targeting one broker. Sequential per-broker
// admission would pay admissionLatency once per record rather than once per wave.
func BenchmarkAsyncDispatcherConcurrentAdmission100(b *testing.B) {
	benchmarkAsyncDispatcherReceipts(b, 100, time.Microsecond, time.Millisecond)
}

// BenchmarkAsyncDispatcherDistinctKeys1024 exercises the brokerless hot path
// with enough independent lanes to expose admission, completion fragmentation,
// and outbox candidate-scan costs.
func BenchmarkAsyncDispatcherDistinctKeys1024(b *testing.B) {
	benchmarkAsyncDispatcherReceipts(b, 1024, time.Millisecond, 0)
}

func benchmarkAsyncDispatcherReceipts(b *testing.B, batchSize int, flushPeriod, admissionLatency time.Duration) {
	publisher, err := benchmarkPublisher(b, b.N*batchSize+1)
	if err != nil {
		b.Fatal(err)
	}
	broker := asyncBrokerFunc(func(_ context.Context, _ string, _ BrokerMessage, attempt PublishAttempt) (AsyncPublishFuture, error) {
		if admissionLatency > 0 {
			time.Sleep(admissionLatency)
		}
		return resolvedFuture(AsyncPublishResult{Attempt: attempt, Outcome: OutcomeAcknowledged}), nil
	})
	dispatcher, err := NewAsyncDispatcher(publisher, broker, AsyncDispatcherConfig{
		MaxInFlight: batchSize, AckTimeout: time.Second,
		CompletionBatchSize: min(batchSize, 256), CompletionFlushPeriod: flushPeriod,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = dispatcher.Close(context.Background()) }()
	messages := make([]customer.MessageView, batchSize)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for i := range messages {
			messages[i] = message(fmt.Sprintf("event-%d-%d", iteration, i), fmt.Sprintf("key-%d", i))
		}
		if _, err := publisher.AcceptBatch(messages); err != nil {
			b.Fatal(err)
		}
		for {
			report, err := dispatcher.Dispatch(context.Background(), len(messages))
			if err != nil {
				b.Fatal(err)
			}
			if report.Attempted == len(messages) {
				break
			}
			runtime.Gosched()
		}
		for publisher.outbox.Stats().Messages != 0 {
			runtime.Gosched()
		}
		for {
			dispatcher.mu.Lock()
			pending := len(dispatcher.pending)
			dispatcher.mu.Unlock()
			if pending == 0 {
				break
			}
			runtime.Gosched()
		}
	}
}

func benchmarkPublisher(tb testing.TB, maxMessages int) (*Publisher, error) {
	tb.Helper()
	store, err := outbox.Open(filepath.Join(tb.TempDir(), "outbox.db"), outbox.Limits{MaxMessages: maxMessages, MaxBytes: int64(maxMessages) * 1024})
	if err != nil {
		return nil, err
	}
	tb.Cleanup(func() { _ = store.Close() })
	publisher, err := New(Config{
		Outbox: store, CustomerLibrary: fixedLibrary{}, Broker: &fakeBroker{},
		ScalingGroup: "orders", HashContract: "sha256/customer-v1", LibraryVersion: "library-v1",
	})
	if err != nil {
		return nil, err
	}
	if err := publisher.ApplyMembership(activeSnapshot(1, 1, "broker-a")); err != nil {
		return nil, err
	}
	return publisher, nil
}

func newTestDispatcher(t *testing.T, publisher *Publisher, broker AsyncBrokerPublisher, maxInFlight int) *AsyncDispatcher {
	t.Helper()
	dispatcher, err := NewAsyncDispatcher(publisher, broker, AsyncDispatcherConfig{
		MaxInFlight: maxInFlight, AckTimeout: time.Second,
		CompletionBatchSize: maxInFlight, CompletionFlushPeriod: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := dispatcher.Close(ctx); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return dispatcher
}

func waitForState(t *testing.T, store *outbox.Store, eventID string, state outbox.State) outbox.Record {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		record, err := store.Get(eventID)
		if err == nil && record.State == state {
			return record
		}
		time.Sleep(time.Millisecond)
	}
	record, err := store.Get(eventID)
	t.Fatalf("record %q did not reach %q: %#v, %v", eventID, state, record, err)
	return outbox.Record{}
}

func waitForRecordMissing(t *testing.T, store *outbox.Store, eventID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, err := store.Get(eventID)
		if errors.Is(err, outbox.ErrNotFound) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("record %q was not deleted", eventID)
}

func waitForOutboxCount(t *testing.T, store *outbox.Store, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if store.Stats().Messages == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("outbox message count = %d, want %d", store.Stats().Messages, count)
}

func waitForPendingCount(t *testing.T, dispatcher *AsyncDispatcher, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		dispatcher.mu.Lock()
		pending := len(dispatcher.pending)
		dispatcher.mu.Unlock()
		if pending == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	dispatcher.mu.Lock()
	pending := len(dispatcher.pending)
	dispatcher.mu.Unlock()
	t.Fatalf("pending attempt count = %d, want %d", pending, count)
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func keyForBroker(t *testing.T, target string, membership []string, skip int) string {
	t.Helper()
	matches := 0
	for i := 0; ; i++ {
		key := fmt.Sprintf("key-%s-%d", target, i)
		digest := customer.BusinessHash(sha256.Sum256([]byte(key)))
		broker, err := routing.BrokerForHash(digest, membership)
		if err != nil {
			t.Fatal(err)
		}
		if broker == target {
			if matches == skip {
				return key
			}
			matches++
		}
	}
}
