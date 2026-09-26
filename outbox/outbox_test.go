package outbox

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestRecoveryRetainsInFlightAsAckUncertain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store := openTestStore(t, path, Limits{})
	accepted, err := store.Accept(testRecord("event-1", "key-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Assign(accepted.EventID, Assignment{Epoch: 3, Broker: "broker-a", Destination: "orders"}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkInFlight(accepted.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := openTestStore(t, path, Limits{})
	record, err := recovered.Get(accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != StateAckUncertain {
		t.Fatalf("state = %q, want %q", record.State, StateAckUncertain)
	}
	if record.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", record.Attempts)
	}
	if !strings.Contains(record.LastError, "duplicate") {
		t.Fatalf("recovery warning %q does not mention duplicate possibility", record.LastError)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openTestStore(t, path, Limits{})
	record, err = reopened.Get(accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != StateAckUncertain {
		t.Fatalf("persisted state = %q, want %q", record.State, StateAckUncertain)
	}
}

func TestBoundsRejectWithoutDroppingExistingRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store := openTestStore(t, path, Limits{MaxMessages: 1, MaxBytes: 1 << 20})
	if _, err := store.Accept(testRecord("event-1", "key-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Accept(testRecord("event-2", "key-2")); !errors.Is(err, ErrFull) {
		t.Fatalf("second accept error = %v, want ErrFull", err)
	}
	if records := store.Records(); len(records) != 1 || records[0].EventID != "event-1" {
		t.Fatalf("records after rejected accept = %#v", records)
	}

	probe := testRecord("event-large", "key-large")
	probe.Payload = []byte("payload-too-large")
	byteStore := openTestStore(t, filepath.Join(t.TempDir(), "bounded.db"), Limits{MaxMessages: 10, MaxBytes: acceptedSize(probe) - 1})
	if _, err := byteStore.Accept(probe); !errors.Is(err, ErrFull) {
		t.Fatalf("oversize accept error = %v, want ErrFull", err)
	}
	if stats := byteStore.Stats(); stats.Messages != 0 || stats.Bytes != 0 {
		t.Fatalf("stats after oversize rejection = %+v", stats)
	}
}

func TestStatsRetainProcessHighWaterMarks(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	first := acceptAssigned(t, store, testRecord("event-1", "key-1"))
	second := acceptAssigned(t, store, testRecord("event-2", "key-2"))
	peak := store.Stats()
	if peak.HighWaterMessages != 2 || peak.HighWaterBytes != peak.Bytes {
		t.Fatalf("peak stats = %+v", peak)
	}
	if err := store.MarkInFlightBatch([]string{first.EventID, second.EventID}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteBatch([]Completion{
		{EventID: first.EventID, Attempts: 1, Epoch: first.Epoch, Outcome: CompletionAcknowledged},
		{EventID: second.EventID, Attempts: 1, Epoch: second.Epoch, Outcome: CompletionAcknowledged},
	}); err != nil {
		t.Fatal(err)
	}
	stats := store.Stats()
	if stats.Messages != 0 || stats.Bytes != 0 || stats.HighWaterMessages != 2 || stats.HighWaterBytes != peak.Bytes {
		t.Fatalf("final stats = %+v, peak = %+v", stats, peak)
	}
}

func TestAckUncertaintyRequiresExplicitRetryAndBlocksOnlySameKey(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	first := acceptAssigned(t, store, testRecord("event-1", "key-a"))
	second := acceptAssigned(t, store, testRecord("event-2", "key-a"))
	other := acceptAssigned(t, store, testRecord("event-3", "key-b"))

	if err := store.MarkInFlight(first.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkAckUncertain(first.EventID, "ACK deadline elapsed; duplicate delivery is possible if retried"); err != nil {
		t.Fatal(err)
	}

	eligible := store.Eligible(10)
	if len(eligible) != 1 || eligible[0].EventID != other.EventID {
		t.Fatalf("eligible = %#v, want only independent-key record %q", eligible, other.EventID)
	}
	if record, err := store.Get(second.EventID); err != nil || record.State != StateReady {
		t.Fatalf("same-key follower changed unexpectedly: record=%#v err=%v", record, err)
	}
	if err := store.RetryAckUncertain(first.EventID); err != nil {
		t.Fatal(err)
	}
	eligible = store.Eligible(10)
	if len(eligible) != 2 || eligible[0].EventID != first.EventID || eligible[1].EventID != other.EventID {
		t.Fatalf("eligible after explicit retry = %#v", eligible)
	}
}

func TestStaleAckCannotDeleteNewerRetry(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	record := acceptAssigned(t, store, testRecord("event-1", "key-a"))
	if err := store.MarkInFlight(record.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkAckUncertain(record.EventID, "timeout"); err != nil {
		t.Fatal(err)
	}
	if err := store.RetryAckUncertain(record.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.Assign(record.EventID, Assignment{Epoch: 2, Broker: "broker-b", Destination: "destination-2"}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkInFlight(record.EventID); err != nil {
		t.Fatal(err)
	}

	if err := store.Ack(record.EventID, 1, 1); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("stale Ack error = %v, want ErrInvalidState", err)
	}
	current, err := store.Get(record.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != StateInFlight || current.Attempts != 2 || current.Epoch != 2 {
		t.Fatalf("newer retry changed by stale ACK: %#v", current)
	}
	if err := store.Ack(record.EventID, 2, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(record.EventID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("current attempt remains after matching ACK: %v", err)
	}
}

func TestSameKeyFIFOAcrossRejectedRetry(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	first := acceptAssigned(t, store, testRecord("event-1", "same-key"))
	second := acceptAssigned(t, store, testRecord("event-2", "same-key"))

	eligible := store.Eligible(10)
	if len(eligible) != 1 || eligible[0].EventID != first.EventID {
		t.Fatalf("first eligible = %#v", eligible)
	}
	if err := store.MarkInFlight(first.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRejected(first.EventID, "broker rejected before acceptance"); err != nil {
		t.Fatal(err)
	}
	eligible = store.Eligible(10)
	if len(eligible) != 1 || eligible[0].EventID != first.EventID {
		t.Fatalf("retry eligible = %#v, follower %q overtook", eligible, second.EventID)
	}
	if err := store.MarkInFlight(first.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(first.EventID, 2, first.Epoch); err != nil {
		t.Fatal(err)
	}
	eligible = store.Eligible(10)
	if len(eligible) != 1 || eligible[0].EventID != second.EventID {
		t.Fatalf("eligible after ACK = %#v", eligible)
	}
}

func TestUnassignedRecordBlocksLaterSameKey(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	first, err := store.Accept(testRecord("event-1", "same-key"))
	if err != nil {
		t.Fatal(err)
	}
	second := acceptAssigned(t, store, testRecord("event-2", "same-key"))
	other := acceptAssigned(t, store, testRecord("event-3", "other-key"))

	eligible := store.Eligible(10)
	if len(eligible) != 1 || eligible[0].EventID != other.EventID {
		t.Fatalf("eligible = %#v; unassigned %q should block %q only", eligible, first.EventID, second.EventID)
	}
}

func TestEligibleIndexPreservesOrderAcrossMixedTransitionsAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store := openTestStore(t, path, Limits{})
	firstA := acceptAssigned(t, store, testRecord("a-1", "key-a"))
	blockedB, err := store.Accept(testRecord("b-1", "key-b"))
	if err != nil {
		t.Fatal(err)
	}
	firstC := acceptAssigned(t, store, testRecord("c-1", "key-c"))
	secondA := acceptAssigned(t, store, testRecord("a-2", "key-a"))
	secondB := acceptAssigned(t, store, testRecord("b-2", "key-b"))
	firstD := acceptAssigned(t, store, testRecord("d-1", "key-d"))

	assertEligibleIDs(t, store, 2, firstA.EventID, firstC.EventID)
	assertEligibleIDs(t, store, 10, firstA.EventID, firstC.EventID, firstD.EventID)

	if err := store.MarkInFlightBatch([]string{firstA.EventID, firstC.EventID}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRejected(firstC.EventID, "retry"); err != nil {
		t.Fatal(err)
	}
	assertEligibleIDs(t, store, 10, firstC.EventID, firstD.EventID)

	if err := store.MarkAckUncertain(firstA.EventID, "timeout"); err != nil {
		t.Fatal(err)
	}
	if err := store.Assign(blockedB.EventID, Assignment{Epoch: 1, Broker: "broker-a", Destination: "destination"}); err != nil {
		t.Fatal(err)
	}
	assertEligibleIDs(t, store, 10, blockedB.EventID, firstC.EventID, firstD.EventID)

	if err := store.MarkInFlight(blockedB.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(blockedB.EventID, 1, 1); err != nil {
		t.Fatal(err)
	}
	assertEligibleIDs(t, store, 10, firstC.EventID, secondB.EventID, firstD.EventID)

	if err := store.RetryAckUncertain(firstA.EventID); err != nil {
		t.Fatal(err)
	}
	assertEligibleIDs(t, store, 10, firstA.EventID, firstC.EventID, secondB.EventID, firstD.EventID)
	if err := store.MarkInFlight(firstA.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(firstA.EventID, 2, firstA.Epoch); err != nil {
		t.Fatal(err)
	}
	assertEligibleIDs(t, store, 10, firstC.EventID, secondA.EventID, secondB.EventID, firstD.EventID)

	if err := store.MarkInFlight(firstD.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close reopened store: %v", err)
		}
	})
	assertEligibleIDs(t, store, 10, firstC.EventID, secondA.EventID, secondB.EventID)

	if err := store.RetryAckUncertain(firstD.EventID); err != nil {
		t.Fatal(err)
	}
	assertEligibleIDs(t, store, 10, firstC.EventID, secondA.EventID, secondB.EventID, firstD.EventID)
}

func TestEligibleIndexChangesOnlyAfterDurableCommit(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	first := acceptAssigned(t, store, testRecord("event-1", "key-1"))
	second := acceptAssigned(t, store, testRecord("event-2", "key-2"))
	assertEligibleIDs(t, store, 10, first.EventID, second.EventID)

	injected := errors.New("injected index transition failure")
	store.beforeCommit = func(operation string) error {
		if operation == "mark in flight" {
			return injected
		}
		return nil
	}
	if err := store.MarkInFlight(first.EventID); !errors.Is(err, injected) {
		t.Fatalf("MarkInFlight error = %v, want injected failure", err)
	}
	assertEligibleIDs(t, store, 10, first.EventID, second.EventID)
}

func TestAcceptBatchIsAtomicAndDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store := openTestStore(t, path, Limits{})
	records, err := store.AcceptBatch([]Record{
		testRecord("event-1", "key-1"),
		testRecord("event-2", "key-2"),
		testRecord("event-3", "key-3"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, record := range records {
		if want := uint64(i + 1); record.Sequence != want {
			t.Fatalf("record %d sequence = %d, want %d", i, record.Sequence, want)
		}
	}
	if _, err := store.AcceptBatch([]Record{
		testRecord("event-4", "key-4"),
		testRecord("event-2", "duplicate"),
	}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate batch error = %v, want ErrDuplicate", err)
	}
	if got := store.Records(); len(got) != 3 {
		t.Fatalf("records after rejected batch = %d, want 3", len(got))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Records(); len(got) != 3 || got[2].EventID != "event-3" {
		t.Fatalf("records after reopen = %#v", got)
	}
}

func TestBatchStateTransitionsAreAtomic(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	accepted, err := store.AcceptBatch([]Record{
		testRecord("event-1", "key-1"), testRecord("event-2", "key-2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignments := []AssignmentUpdate{
		{EventID: accepted[0].EventID, Assignment: Assignment{Epoch: 1, Broker: "a", Destination: "orders"}},
		{EventID: accepted[1].EventID, Assignment: Assignment{Epoch: 2, Broker: "b", Destination: "orders"}},
	}
	if err := store.AssignBatch(assignments); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkInFlightBatch([]string{"event-1", "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("invalid batch error = %v, want ErrNotFound", err)
	}
	for _, eventID := range []string{"event-1", "event-2"} {
		record, err := store.Get(eventID)
		if err != nil {
			t.Fatal(err)
		}
		if record.State != StateReady || record.Attempts != 0 {
			t.Fatalf("record after rejected batch = %#v", record)
		}
	}
	if err := store.MarkInFlightBatch([]string{"event-1", "event-2"}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkAckUncertainBatch([]StateUpdate{{EventID: "event-1", Reason: "timeout"}, {EventID: "event-2", Reason: "disconnect"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.RetryAckUncertainBatch([]string{"event-1", "event-2"}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkInFlightBatch([]string{"event-1", "event-2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteBatch([]Completion{
		{EventID: "event-1", Attempts: 2, Epoch: assignments[0].Assignment.Epoch, Outcome: CompletionAcknowledged},
		{EventID: "event-2", Attempts: 2, Epoch: assignments[1].Assignment.Epoch, Outcome: CompletionAcknowledged},
	}); err != nil {
		t.Fatal(err)
	}
	if stats := store.Stats(); stats.Messages != 0 || stats.Bytes != 0 {
		t.Fatalf("stats after ACK batch = %+v", stats)
	}
}

func TestCompleteBatchAppliesMixedMatchingAttemptsAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store := openTestStore(t, path, Limits{})
	records := []Record{
		testRecord("acked", "key-acked"),
		testRecord("rejected", "key-rejected"),
		testRecord("uncertain", "key-uncertain"),
		testRecord("stale", "key-stale"),
	}
	for i := range records {
		records[i].State = StateReady
		records[i].Epoch = 7
		records[i].Broker = "broker-a"
		records[i].Destination = "orders"
	}
	if _, err := store.AcceptBatch(records); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkInFlightBatch([]string{"acked", "rejected", "uncertain", "stale"}); err != nil {
		t.Fatal(err)
	}

	applied, err := store.CompleteBatch([]Completion{
		{EventID: "acked", Attempts: 1, Epoch: 7, Outcome: CompletionAcknowledged},
		{EventID: "rejected", Attempts: 1, Epoch: 7, Outcome: CompletionRejected, Reason: "buffer full"},
		{EventID: "uncertain", Attempts: 1, Epoch: 7, Outcome: CompletionAckUncertain, Reason: "connection lost"},
		{EventID: "stale", Attempts: 2, Epoch: 7, Outcome: CompletionAcknowledged},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 3 {
		t.Fatalf("applied completions = %#v, want 3 matching attempts", applied)
	}
	if _, err := store.Get("acked"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("acked record error = %v, want ErrNotFound", err)
	}
	assertRecord := func(eventID string, state State, reason string) {
		t.Helper()
		record, err := store.Get(eventID)
		if err != nil {
			t.Fatal(err)
		}
		if record.State != state || record.LastError != reason || record.Attempts != 1 {
			t.Fatalf("record %q = %#v, want state=%q reason=%q attempts=1", eventID, record, state, reason)
		}
	}
	assertRecord("rejected", StateReady, "buffer full")
	assertRecord("uncertain", StateAckUncertain, "connection lost")
	assertRecord("stale", StateInFlight, "")
	assertEligibleIDs(t, store, 10, "rejected")

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Get("acked"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reopened acked record error = %v, want ErrNotFound", err)
	}
	for eventID, want := range map[string]State{
		"rejected":  StateReady,
		"uncertain": StateAckUncertain,
		"stale":     StateAckUncertain,
	} {
		record, err := reopened.Get(eventID)
		if err != nil || record.State != want {
			t.Fatalf("reopened record %q = %#v, %v; want state %q", eventID, record, err, want)
		}
	}
}

func TestCompleteBatchWriteFailureLeavesEveryAttemptInFlight(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	first := acceptAssigned(t, store, testRecord("event-1", "key-1"))
	second := acceptAssigned(t, store, testRecord("event-2", "key-2"))
	if err := store.MarkInFlightBatch([]string{first.EventID, second.EventID}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected completion failure")
	store.beforeCommit = func(operation string) error {
		if operation == "complete batch" {
			return injected
		}
		return nil
	}
	_, err := store.CompleteBatch([]Completion{
		{EventID: first.EventID, Attempts: 1, Epoch: first.Epoch, Outcome: CompletionAcknowledged},
		{EventID: second.EventID, Attempts: 1, Epoch: second.Epoch, Outcome: CompletionRejected},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("CompleteBatch error = %v, want injected failure", err)
	}
	for _, eventID := range []string{first.EventID, second.EventID} {
		record, getErr := store.Get(eventID)
		if getErr != nil || record.State != StateInFlight || record.Attempts != 1 {
			t.Fatalf("record %q after rollback = %#v, %v", eventID, record, getErr)
		}
	}
}

func TestInvalidStateTransitionsLeaveRecordUnchanged(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	accepted, err := store.Accept(testRecord("event-1", "key"))
	if err != nil {
		t.Fatal(err)
	}
	assertState := func(want State, attempts uint64) {
		t.Helper()
		record, err := store.Get(accepted.EventID)
		if err != nil {
			t.Fatal(err)
		}
		if record.State != want || record.Attempts != attempts {
			t.Fatalf("record state=%q attempts=%d, want %q/%d", record.State, record.Attempts, want, attempts)
		}
	}
	if err := store.MarkInFlight(accepted.EventID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("unassigned MarkInFlight error = %v", err)
	}
	if err := store.Ack(accepted.EventID, 1, 1); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("unassigned Ack error = %v", err)
	}
	assertState(StateUnassigned, 0)

	assignment := Assignment{Epoch: 1, Broker: "broker", Destination: "destination"}
	if err := store.Assign(accepted.EventID, assignment); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRejected(accepted.EventID, "not attempted"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ready MarkRejected error = %v", err)
	}
	if err := store.MarkAckUncertain(accepted.EventID, "not attempted"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ready MarkAckUncertain error = %v", err)
	}
	if err := store.RetryAckUncertain(accepted.EventID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ready RetryAckUncertain error = %v", err)
	}
	assertState(StateReady, 0)

	if err := store.MarkInFlight(accepted.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.Assign(accepted.EventID, assignment); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("in-flight Assign error = %v", err)
	}
	if err := store.RetryAckUncertain(accepted.EventID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("in-flight RetryAckUncertain error = %v", err)
	}
	assertState(StateInFlight, 1)

	if err := store.MarkAckUncertain(accepted.EventID, "timeout"); err != nil {
		t.Fatal(err)
	}
	if err := store.Assign(accepted.EventID, assignment); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("uncertain Assign error = %v", err)
	}
	if err := store.MarkRejected(accepted.EventID, "too late"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("uncertain MarkRejected error = %v", err)
	}
	assertState(StateAckUncertain, 1)
}

func TestLegacyJSONIsRefusedWithoutModification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.json")
	legacy := []byte(`{"version":1,"next_sequence":1,"records":[]}`)
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, Limits{}); !errors.Is(err, ErrLegacyFormat) {
		t.Fatalf("Open error = %v, want ErrLegacyFormat", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(legacy) {
		t.Fatalf("legacy file was modified: %q", got)
	}
}

func TestConcurrentAcceptPreservesUniquenessAndSequence(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{MaxMessages: 500, MaxBytes: 8 << 20})
	const workers = 20
	const perWorker = 20
	var wg sync.WaitGroup
	errorsCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				eventID := fmt.Sprintf("event-%03d-%03d", worker, i)
				if _, err := store.Accept(testRecord(eventID, fmt.Sprintf("key-%d", worker))); err != nil {
					errorsCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	records := store.Records()
	if len(records) != workers*perWorker {
		t.Fatalf("record count = %d, want %d", len(records), workers*perWorker)
	}
	seen := make(map[string]struct{}, len(records))
	for i, record := range records {
		if want := uint64(i + 1); record.Sequence != want {
			t.Fatalf("sequence[%d] = %d, want %d", i, record.Sequence, want)
		}
		if _, exists := seen[record.EventID]; exists {
			t.Fatalf("duplicate event %q", record.EventID)
		}
		seen[record.EventID] = struct{}{}
	}
}

func TestConcurrentDuplicateAcceptHasSingleWinner(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	const contenders = 32
	start := make(chan struct{})
	results := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.Accept(testRecord("same-event", "key"))
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var successes, duplicates int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrDuplicate):
			duplicates++
		default:
			t.Fatalf("unexpected Accept error: %v", err)
		}
	}
	if successes != 1 || duplicates != contenders-1 {
		t.Fatalf("successes=%d duplicates=%d, want 1 and %d", successes, duplicates, contenders-1)
	}
	if records := store.Records(); len(records) != 1 || records[0].Sequence != 1 {
		t.Fatalf("records = %#v", records)
	}
}

func TestWriteFailureRollsBackMemoryAndDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store := openTestStore(t, path, Limits{})
	injected := errors.New("injected write failure")
	store.beforeCommit = func(string) error { return injected }
	if _, err := store.Accept(testRecord("event-failed", "key")); !errors.Is(err, injected) {
		t.Fatalf("Accept error = %v, want injected failure", err)
	}
	if stats := store.Stats(); stats.Messages != 0 || stats.Bytes != 0 {
		t.Fatalf("stats after failed transaction = %+v", stats)
	}
	store.beforeCommit = nil
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Records(); len(got) != 0 {
		t.Fatalf("persisted records after failed transaction = %#v", got)
	}
	accepted, err := reopened.Accept(testRecord("event-ok", "key"))
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Sequence != 1 {
		t.Fatalf("sequence after rollback = %d, want 1", accepted.Sequence)
	}
}

func TestCloseAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store := openTestStore(t, path, Limits{})
	if _, err := store.Accept(testRecord("event-1", "key")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if _, err := store.Accept(testRecord("event-2", "key")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Accept after Close error = %v, want ErrClosed", err)
	}
	reopened, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if records := reopened.Records(); len(records) != 1 || records[0].EventID != "event-1" {
		t.Fatalf("records after reopen = %#v", records)
	}
}

func TestEligibleForFiltersGroupEpochAndBrokerSlots(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "outbox.db"), Limits{})
	records := readyRecords(6)
	for i := range records {
		records[i].Group = "active"
		records[i].Broker = []string{"broker-a", "broker-a", "broker-b"}[i%3]
	}
	records[0].Group = "paused"
	records[4].Epoch = 2
	if _, err := store.AcceptBatch(records); err != nil {
		t.Fatal(err)
	}
	got := store.EligibleFor(6, map[string]uint64{"active": 1}, map[string]int{"broker-a": 1, "broker-b": 2})
	ids := make([]string, len(got))
	for i := range got {
		ids[i] = got[i].EventID
	}
	want := []string{"event-1", "event-2", "event-5"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("EligibleFor IDs = %v, want %v", ids, want)
	}
}

func BenchmarkAccept(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "outbox.db"), Limits{MaxMessages: b.N + 1, MaxBytes: int64(b.N+1) * 1024})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Accept(testRecord(fmt.Sprintf("event-%d", i), fmt.Sprintf("key-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAcceptBatch100(b *testing.B) {
	benchmarkAcceptBatch(b, 100)
}

func BenchmarkAcceptBatch1000(b *testing.B) {
	benchmarkAcceptBatch(b, 1000)
}

func benchmarkAcceptBatch(b *testing.B, batchSize int) {
	store, err := Open(filepath.Join(b.TempDir(), "outbox.db"), Limits{MaxMessages: b.N*batchSize + 1, MaxBytes: int64(b.N*batchSize+1) * 1024})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	records := make([]Record, batchSize)
	b.ReportMetric(float64(batchSize), "messages/op")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range records {
			records[j] = testRecord(fmt.Sprintf("event-%d-%d", i, j), fmt.Sprintf("key-%d-%d", i, j))
		}
		if _, err := store.AcceptBatch(records); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEligible1000(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "outbox.db"), Limits{MaxMessages: 1001, MaxBytes: 2 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	records := readyRecords(1000)
	if _, err := store.AcceptBatch(records); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := store.Eligible(1000); len(got) != 1000 {
			b.Fatalf("eligible count = %d", len(got))
		}
	}
}

func BenchmarkEligibleForActiveBatch(b *testing.B) {
	const (
		readyLanes = 100_000
		batchSize  = 6_144
	)
	store, err := Open(filepath.Join(b.TempDir(), "outbox.db"), Limits{MaxMessages: readyLanes + 1, MaxBytes: int64(readyLanes+1) * 1024})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	records := readyRecords(readyLanes)
	for i := range records {
		records[i].Group = "active"
		records[i].Broker = fmt.Sprintf("broker-%d", i%3)
	}
	if _, err := store.AcceptBatch(records); err != nil {
		b.Fatal(err)
	}
	brokerSlots := map[string]int{"broker-0": 2_048, "broker-1": 2_048, "broker-2": 2_048}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := store.EligibleFor(batchSize, map[string]uint64{"active": 1}, brokerSlots); len(got) != batchSize {
			b.Fatalf("eligible count = %d", len(got))
		}
	}
}

func BenchmarkEligibleLimitOneScaling(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("blocked-prefix-%d", size), func(b *testing.B) {
			store, err := Open(filepath.Join(b.TempDir(), "outbox.db"), Limits{MaxMessages: size + 1, MaxBytes: int64(size+1) * 1024})
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			records := readyRecords(size)
			for i := 0; i < len(records)-1; i++ {
				records[i].OrderingKey = "blocked-key"
			}
			if _, err := store.AcceptBatch(records); err != nil {
				b.Fatal(err)
			}
			if err := store.MarkInFlight("event-0"); err != nil {
				b.Fatal(err)
			}
			want := fmt.Sprintf("event-%d", size-1)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := store.Eligible(1); len(got) != 1 || got[0].EventID != want {
					b.Fatalf("eligible = %#v, want %q", got, want)
				}
			}
		})
	}
}

func assertEligibleIDs(t *testing.T, store *Store, limit int, want ...string) {
	t.Helper()
	got := store.Eligible(limit)
	ids := make([]string, len(got))
	for i := range got {
		ids[i] = got[i].EventID
	}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("Eligible(%d) IDs = %v, want %v", limit, ids, want)
	}
}

func readyRecords(count int) []Record {
	records := make([]Record, count)
	for i := range records {
		records[i] = testRecord(fmt.Sprintf("event-%d", i), fmt.Sprintf("key-%d", i))
		records[i].State = StateReady
		records[i].Epoch = 1
		records[i].Broker = "broker"
		records[i].Destination = "destination"
	}
	return records
}

func openTestStore(t *testing.T, path string, limits Limits) *Store {
	t.Helper()
	store, err := Open(path, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return store
}

func acceptAssigned(t *testing.T, store *Store, record Record) Record {
	t.Helper()
	record.State = StateReady
	record.Epoch = 1
	record.Broker = "broker-a"
	record.Destination = "destination"
	accepted, err := store.Accept(record)
	if err != nil {
		t.Fatal(err)
	}
	return accepted
}

func testRecord(eventID, key string) Record {
	return Record{
		EventID:        eventID,
		Payload:        []byte("payload-" + eventID),
		Topic:          "orders/created",
		Properties:     map[string]string{"content-type": "application/json"},
		OrderingKey:    key,
		Group:          "orders",
		Hash:           "hash-" + key,
		HashContract:   "sha256-v1",
		LibraryVersion: "test-v1",
	}
}
