// Package outbox provides a durable, Bolt-backed message outbox.
//
// Each mutation is committed in a bbolt write transaction. A successful
// mutation has therefore reached durable storage before it is reported to the
// caller. A Store is safe for concurrent use within one process. bbolt also
// prevents the same database from being opened by multiple processes.
package outbox

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

const databaseFormatVersion uint64 = 1

const (
	defaultMaxMessages = 10_000
	defaultMaxBytes    = 64 << 20
)

var (
	ErrFull          = errors.New("outbox capacity exceeded")
	ErrDuplicate     = errors.New("duplicate event ID")
	ErrNotFound      = errors.New("outbox record not found")
	ErrInvalidRecord = errors.New("invalid outbox record")
	ErrInvalidState  = errors.New("invalid outbox state transition")
	ErrClosed        = errors.New("outbox is closed")
	ErrLegacyFormat  = errors.New("legacy JSON outbox format is not supported")
)

var (
	metadataBucket  = []byte("metadata")
	recordsBucket   = []byte("records_by_sequence")
	eventsBucket    = []byte("sequence_by_event")
	formatKey       = []byte("format_version")
	nextSequenceKey = []byte("next_sequence")
	totalBytesKey   = []byte("total_bytes")
)

// State is the durable delivery state of a record.
type State string

const (
	// StateUnassigned is durable but intentionally has no broker assignment,
	// typically because publishing is paused or membership is unavailable.
	StateUnassigned State = "unassigned"
	// StateReady is assigned and may be selected for a publish attempt.
	StateReady State = "ready"
	// StateInFlight is durably marked before invoking the broker client.
	StateInFlight State = "in_flight"
	// StateAckUncertain means the broker may have accepted the message, but a
	// positive ACK was not observed. It is retained and blocks later records
	// with the same ordering key. Retrying it can produce a duplicate; consumers
	// should deduplicate with EventID.
	StateAckUncertain State = "ack_uncertain"
)

// Record is the durable representation of an accepted publish request.
type Record struct {
	EventID        string            `json:"event_id"`
	Payload        []byte            `json:"payload,omitempty"`
	Topic          string            `json:"topic"`
	Properties     map[string]string `json:"properties,omitempty"`
	OrderingKey    string            `json:"ordering_key"`
	Group          string            `json:"group"`
	Hash           string            `json:"hash"`
	HashContract   string            `json:"hash_contract"`
	LibraryVersion string            `json:"library_version"`
	Epoch          uint64            `json:"epoch,omitempty"`
	Broker         string            `json:"broker,omitempty"`
	Destination    string            `json:"destination,omitempty"`
	State          State             `json:"state"`
	Attempts       uint64            `json:"attempts,omitempty"`
	LastError      string            `json:"last_error,omitempty"`
	Sequence       uint64            `json:"sequence"`
	AcceptedAt     time.Time         `json:"accepted_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	SizeBytes      int64             `json:"size_bytes"`
}

// Assignment identifies the broker route chosen for a durable record.
type Assignment struct {
	Epoch       uint64
	Broker      string
	Destination string
}

// AssignmentUpdate is one item in an atomic AssignBatch operation.
type AssignmentUpdate struct {
	EventID    string
	Assignment Assignment
}

// StateUpdate identifies a record and diagnostic reason for a batch state
// transition.
type StateUpdate struct {
	EventID string
	Reason  string
}

// CompletionOutcome identifies the durable terminal transition for an in-flight
// publish attempt.
type CompletionOutcome uint8

const (
	CompletionAcknowledged CompletionOutcome = iota + 1
	CompletionRejected
	CompletionAckUncertain
)

// Completion identifies one terminal broker result. Attempts and Epoch make a
// late result harmless: CompleteBatch applies it only while the matching durable
// attempt is still in flight.
type Completion struct {
	EventID  string
	Attempts uint64
	Epoch    uint64
	Outcome  CompletionOutcome
	Reason   string
}

// Limits bounds the number and aggregate accepted size of records. Zero values
// select conservative defaults; negative values are rejected.
type Limits struct {
	MaxMessages int
	MaxBytes    int64
}

// Stats describes current outbox utilization and process-lifetime high-water
// marks. A freshly opened existing store starts its high-water marks at the
// recovered utilization.
type Stats struct {
	Messages          int
	Bytes             int64
	HighWaterMessages int
	HighWaterBytes    int64
}

// Store is a durable bbolt-backed outbox.
type Store struct {
	mu sync.RWMutex

	path              string
	limits            Limits
	db                *bolt.DB
	records           map[string]Record
	order             []string
	lanes             map[string]*orderingLane
	readyLanes        readyLaneHeap
	nextSeq           uint64
	bytes             int64
	highWaterMessages int
	highWaterBytes    int64
	deleted           int
	now               func() time.Time

	// beforeCommit is an internal fault-injection seam. Returning an error from
	// it aborts the current transaction; production stores leave it nil.
	beforeCommit func(string) error
}

// orderingLane is the acceptance-ordered queue for one ordering key. Only its
// live head may be present in readyLanes; later records remain blocked without
// being examined by Eligible.
type orderingLane struct {
	key          string
	eventIDs     []string
	head         int
	headSequence uint64
	readyIndex   int
}

// readyLaneHeap orders ready lane heads by global acceptance sequence. Sequence
// numbers are unique, while key provides a defensive deterministic tie-breaker.
type readyLaneHeap []*orderingLane

func (h readyLaneHeap) Len() int { return len(h) }

func (h readyLaneHeap) Less(i, j int) bool {
	if h[i].headSequence != h[j].headSequence {
		return h[i].headSequence < h[j].headSequence
	}
	return h[i].key < h[j].key
}

func (h readyLaneHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].readyIndex = i
	h[j].readyIndex = j
}

func (h *readyLaneHeap) Push(value any) {
	lane := value.(*orderingLane)
	lane.readyIndex = len(*h)
	*h = append(*h, lane)
}

func (h *readyLaneHeap) Pop() any {
	old := *h
	last := len(old) - 1
	lane := old[last]
	old[last] = nil
	lane.readyIndex = -1
	*h = old[:last]
	return lane
}

type readyLaneCandidate struct {
	index int
	lane  *orderingLane
}

type readyLaneCandidateHeap []readyLaneCandidate

func (h readyLaneCandidateHeap) Len() int { return len(h) }

func (h readyLaneCandidateHeap) Less(i, j int) bool {
	if h[i].lane.headSequence != h[j].lane.headSequence {
		return h[i].lane.headSequence < h[j].lane.headSequence
	}
	return h[i].lane.key < h[j].lane.key
}

func (h readyLaneCandidateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *readyLaneCandidateHeap) Push(value any) {
	*h = append(*h, value.(readyLaneCandidate))
}

func (h *readyLaneCandidateHeap) Pop() any {
	old := *h
	last := len(old) - 1
	candidate := old[last]
	*h = old[:last]
	return candidate
}

// Open loads or initializes an outbox at path. Any StateInFlight record found
// during recovery becomes StateAckUncertain: the process cannot know whether
// the broker accepted it before the crash, so automatic deletion or retry
// would be unsafe.
//
// Snapshot-based JSON files from earlier versions are refused with
// ErrLegacyFormat rather than being overwritten or silently converted.
func Open(path string, limits Limits) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("open outbox: empty path")
	}
	if limits.MaxMessages < 0 || limits.MaxBytes < 0 {
		return nil, fmt.Errorf("open outbox: limits must not be negative")
	}
	if limits.MaxMessages == 0 {
		limits.MaxMessages = defaultMaxMessages
	}
	if limits.MaxBytes == 0 {
		limits.MaxBytes = defaultMaxBytes
	}

	newDatabase, err := inspectDatabasePath(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("open outbox: create directory: %w", err)
	}

	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open outbox database: %w", err)
	}
	s := &Store{
		path:    path,
		limits:  limits,
		db:      db,
		records: make(map[string]Record),
		lanes:   make(map[string]*orderingLane),
		nextSeq: 1,
		now:     time.Now,
	}
	if err := s.load(newDatabase); err != nil {
		_ = db.Close()
		return nil, err
	}
	// Persist a newly created database directory entry before Open succeeds. This
	// makes the first subsequently accepted transaction recoverable after a power
	// loss.
	if newDatabase {
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("open outbox: sync directory: %w", err)
		}
	}
	return s, nil
}

// Close releases the database file and its process lock. Close is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	db := s.db
	s.db = nil
	if err := db.Close(); err != nil {
		return fmt.Errorf("close outbox: %w", err)
	}
	return nil
}

// Accept durably appends a record. Success means the record is on durable
// storage, not that a broker has ACKed it.
func (s *Store) Accept(record Record) (Record, error) {
	accepted, err := s.acceptMany("accept", []Record{record})
	if err != nil {
		return Record{}, err
	}
	return accepted[0], nil
}

// AcceptBatch atomically and durably appends records in slice order. If any
// record is invalid or duplicated, capacity would be exceeded, or persistence
// fails, none of the records are accepted.
func (s *Store) AcceptBatch(records []Record) ([]Record, error) {
	return s.acceptMany("accept batch", records)
}

func (s *Store) acceptMany(operation string, input []Record) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpen(); err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	if len(input) == 0 {
		return []Record{}, nil
	}
	if len(input) > s.limits.MaxMessages-len(s.records) {
		return nil, fmt.Errorf("%s: %w (message limit %d)", operation, ErrFull, s.limits.MaxMessages)
	}
	if uint64(len(input)) > ^uint64(0)-s.nextSeq {
		return nil, fmt.Errorf("%s: sequence exhausted", operation)
	}

	seen := make(map[string]struct{}, len(input))
	accepted := make([]Record, len(input))
	now := s.now().UTC()
	var addedBytes int64
	for i, original := range input {
		if err := validateNewRecord(original); err != nil {
			return nil, fmt.Errorf("%s record %d: %w", operation, i, err)
		}
		if _, ok := s.records[original.EventID]; ok {
			return nil, fmt.Errorf("%s %q: %w", operation, original.EventID, ErrDuplicate)
		}
		if _, ok := seen[original.EventID]; ok {
			return nil, fmt.Errorf("%s %q: %w", operation, original.EventID, ErrDuplicate)
		}
		seen[original.EventID] = struct{}{}

		record := cloneRecord(original)
		record.Sequence = s.nextSeq + uint64(i)
		record.AcceptedAt = now
		record.UpdatedAt = now
		record.Attempts = 0
		record.LastError = ""
		if record.State == "" {
			record.State = StateUnassigned
		}
		record.SizeBytes = acceptedSize(record)
		if record.SizeBytes > s.limits.MaxBytes-s.bytes-addedBytes {
			return nil, fmt.Errorf("%s %q: %w (byte limit %d)", operation, record.EventID, ErrFull, s.limits.MaxBytes)
		}
		addedBytes += record.SizeBytes
		accepted[i] = record
	}
	nextSequence := s.nextSeq + uint64(len(accepted))
	totalBytes := s.bytes + addedBytes

	encoded := make([][]byte, len(accepted))
	for i := range accepted {
		data, err := json.Marshal(accepted[i])
		if err != nil {
			return nil, fmt.Errorf("%s %q: encode: %w", operation, accepted[i].EventID, err)
		}
		encoded[i] = data
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		events := tx.Bucket(eventsBucket)
		metadata := tx.Bucket(metadataBucket)
		for i, record := range accepted {
			sequence := uint64Bytes(record.Sequence)
			if err := records.Put(sequence, encoded[i]); err != nil {
				return err
			}
			if err := events.Put([]byte(record.EventID), sequence); err != nil {
				return err
			}
		}
		if err := putUint64(metadata, nextSequenceKey, nextSequence); err != nil {
			return err
		}
		if err := putUint64(metadata, totalBytesKey, uint64(totalBytes)); err != nil {
			return err
		}
		return s.runBeforeCommit(operation)
	}); err != nil {
		return nil, fmt.Errorf("%s: persist: %w", operation, err)
	}

	for _, record := range accepted {
		s.records[record.EventID] = record
		s.order = append(s.order, record.EventID)
		s.indexAccepted(record)
	}
	s.nextSeq = nextSequence
	s.bytes = totalBytes
	s.highWaterMessages = max(s.highWaterMessages, len(s.records))
	s.highWaterBytes = max(s.highWaterBytes, s.bytes)
	return cloneRecords(accepted), nil
}

// Assign durably attaches a route to an unassigned record. Reapplying the same
// assignment is idempotent. A ready record may be reassigned because it has
// either never been attempted or was definitively rejected by its old broker.
// In-flight and ACK-uncertain records cannot be reassigned.
func (s *Store) Assign(eventID string, assignment Assignment) error {
	return s.assignMany("assign", []AssignmentUpdate{{EventID: eventID, Assignment: assignment}})
}

// AssignBatch atomically applies a set of assignments.
func (s *Store) AssignBatch(updates []AssignmentUpdate) error {
	return s.assignMany("assign batch", updates)
}

func (s *Store) assignMany(operation string, updates []AssignmentUpdate) error {
	ids := make([]string, len(updates))
	assignments := make(map[string]Assignment, len(updates))
	for i, update := range updates {
		ids[i] = update.EventID
		assignments[update.EventID] = update.Assignment
	}
	return s.updateMany(operation, ids, func(eventID string, r *Record) error {
		assignment := assignments[eventID]
		if assignment.Epoch == 0 || assignment.Broker == "" || assignment.Destination == "" {
			return fmt.Errorf("%w: incomplete assignment", ErrInvalidRecord)
		}
		if r.State == StateReady && r.Epoch == assignment.Epoch && r.Broker == assignment.Broker && r.Destination == assignment.Destination {
			return nil
		}
		if r.State != StateUnassigned && r.State != StateReady {
			return fmt.Errorf("%w: cannot assign record in state %q after %d attempts", ErrInvalidState, r.State, r.Attempts)
		}
		r.Epoch = assignment.Epoch
		r.Broker = assignment.Broker
		r.Destination = assignment.Destination
		r.State = StateReady
		r.LastError = ""
		return nil
	})
}

// MarkInFlight durably records that the broker call is about to begin.
func (s *Store) MarkInFlight(eventID string) error {
	return s.markInFlightMany("mark in flight", []string{eventID})
}

// MarkInFlightBatch atomically marks ready records in flight.
func (s *Store) MarkInFlightBatch(eventIDs []string) error {
	return s.markInFlightMany("mark in flight batch", eventIDs)
}

func (s *Store) markInFlightMany(operation string, eventIDs []string) error {
	return s.updateMany(operation, eventIDs, func(_ string, r *Record) error {
		if r.State != StateReady {
			return fmt.Errorf("%w: cannot publish record in state %q", ErrInvalidState, r.State)
		}
		r.State = StateInFlight
		r.Attempts++
		r.LastError = ""
		return nil
	})
}

// MarkRejected returns a definitely rejected publish attempt to the ready
// state. The record remains durable and retains its assignment.
func (s *Store) MarkRejected(eventID, reason string) error {
	return s.markRejectedMany("mark rejected", []StateUpdate{{EventID: eventID, Reason: reason}})
}

// MarkRejectedBatch atomically returns in-flight records to the ready state.
func (s *Store) MarkRejectedBatch(updates []StateUpdate) error {
	return s.markRejectedMany("mark rejected batch", updates)
}

func (s *Store) markRejectedMany(operation string, updates []StateUpdate) error {
	ids, reasons := splitStateUpdates(updates)
	return s.updateMany(operation, ids, func(eventID string, r *Record) error {
		if r.State != StateInFlight {
			return fmt.Errorf("%w: cannot reject record in state %q", ErrInvalidState, r.State)
		}
		r.State = StateReady
		r.LastError = reasons[eventID]
		return nil
	})
}

// MarkAckUncertain retains an attempted record when a positive broker ACK was
// not observed. It deliberately does not retry: a retry may create a duplicate.
func (s *Store) MarkAckUncertain(eventID, reason string) error {
	return s.markAckUncertainMany("mark ACK uncertain", []StateUpdate{{EventID: eventID, Reason: reason}})
}

// MarkAckUncertainBatch atomically retains attempted records whose ACK outcome
// is unknown.
func (s *Store) MarkAckUncertainBatch(updates []StateUpdate) error {
	return s.markAckUncertainMany("mark ACK uncertain batch", updates)
}

func (s *Store) markAckUncertainMany(operation string, updates []StateUpdate) error {
	ids, reasons := splitStateUpdates(updates)
	return s.updateMany(operation, ids, func(eventID string, r *Record) error {
		if r.State != StateInFlight {
			return fmt.Errorf("%w: cannot mark uncertain record in state %q", ErrInvalidState, r.State)
		}
		r.State = StateAckUncertain
		r.LastError = reasons[eventID]
		return nil
	})
}

// RetryAckUncertain makes an uncertain record eligible again. The caller must
// explicitly accept the possibility of duplicate delivery.
func (s *Store) RetryAckUncertain(eventID string) error {
	return s.retryAckUncertainMany("retry ACK uncertain", []string{eventID})
}

// RetryAckUncertainBatch atomically makes uncertain records eligible again.
func (s *Store) RetryAckUncertainBatch(eventIDs []string) error {
	return s.retryAckUncertainMany("retry ACK uncertain batch", eventIDs)
}

func (s *Store) retryAckUncertainMany(operation string, eventIDs []string) error {
	return s.updateMany(operation, eventIDs, func(_ string, r *Record) error {
		if r.State != StateAckUncertain {
			return fmt.Errorf("%w: cannot retry record in state %q", ErrInvalidState, r.State)
		}
		r.State = StateReady
		r.LastError = "explicit retry after uncertain ACK; duplicate delivery is possible"
		return nil
	})
}

// Ack removes a record only when event ID, attempt number, and epoch all match
// the current durable in-flight or ACK-uncertain state. Requiring the complete
// attempt identity prevents a delayed ACK from deleting a newer retry.
func (s *Store) Ack(eventID string, attempts, epoch uint64) error {
	applied, err := s.CompleteBatch([]Completion{{EventID: eventID, Attempts: attempts, Epoch: epoch, Outcome: CompletionAcknowledged}})
	if err != nil {
		return err
	}
	if len(applied) != 1 {
		return fmt.Errorf("ack attempt %q: %w: stale attempt %d at epoch %d", eventID, ErrInvalidState, attempts, epoch)
	}
	return nil
}

// AckAttempt is retained as a safe compatibility alias for Ack.
func (s *Store) AckAttempt(eventID string, attempts, epoch uint64) error {
	return s.Ack(eventID, attempts, epoch)
}

// Batched acknowledgements use CompleteBatch so every item carries its attempt
// number and epoch; an event-ID-only batch ACK cannot be made safe.

// CompleteBatch atomically applies mixed broker outcomes for matching in-flight
// attempts. Stale completions and records already resolved by an operator are
// ignored. The returned slice contains exactly the completions committed, in
// input order. Acknowledged records are deleted; rejected records become ready;
// uncertain records are retained as ACK-uncertain.
func (s *Store) CompleteBatch(completions []Completion) ([]Completion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpen(); err != nil {
		return nil, fmt.Errorf("complete batch: %w", err)
	}
	if len(completions) == 0 {
		return []Completion{}, nil
	}

	seen := make(map[string]struct{}, len(completions))
	applied := make([]Completion, 0, len(completions))
	updated := make([]Record, 0, len(completions))
	deleted := make([]Record, 0, len(completions))
	encoded := make([][]byte, 0, len(completions))
	now := s.now().UTC()
	var removedBytes int64
	for _, completion := range completions {
		if completion.EventID == "" {
			return nil, fmt.Errorf("complete batch: %w: empty event ID", ErrInvalidRecord)
		}
		if _, ok := seen[completion.EventID]; ok {
			return nil, fmt.Errorf("complete batch: %w: event ID %q appears more than once in batch", ErrInvalidRecord, completion.EventID)
		}
		seen[completion.EventID] = struct{}{}
		record, ok := s.records[completion.EventID]
		if !ok || record.Attempts != completion.Attempts || record.Epoch != completion.Epoch {
			continue
		}
		switch completion.Outcome {
		case CompletionAcknowledged:
			if record.State != StateInFlight && record.State != StateAckUncertain {
				continue
			}
			deleted = append(deleted, record)
			removedBytes += record.SizeBytes
		case CompletionRejected, CompletionAckUncertain:
			if record.State != StateInFlight {
				continue
			}
			record = cloneRecord(record)
			if completion.Outcome == CompletionRejected {
				record.State = StateReady
			} else {
				record.State = StateAckUncertain
			}
			record.LastError = completion.Reason
			record.UpdatedAt = now
			data, err := json.Marshal(record)
			if err != nil {
				return nil, fmt.Errorf("complete batch %q: encode: %w", completion.EventID, err)
			}
			updated = append(updated, record)
			encoded = append(encoded, data)
		default:
			return nil, fmt.Errorf("complete batch %q: %w: invalid completion outcome %d", completion.EventID, ErrInvalidRecord, completion.Outcome)
		}
		applied = append(applied, completion)
	}
	if len(applied) == 0 {
		return []Completion{}, nil
	}

	totalBytes := s.bytes - removedBytes
	if err := s.db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		events := tx.Bucket(eventsBucket)
		for i, record := range updated {
			if err := records.Put(uint64Bytes(record.Sequence), encoded[i]); err != nil {
				return err
			}
		}
		for _, record := range deleted {
			if err := records.Delete(uint64Bytes(record.Sequence)); err != nil {
				return err
			}
			if err := events.Delete([]byte(record.EventID)); err != nil {
				return err
			}
		}
		if len(deleted) != 0 {
			if err := putUint64(tx.Bucket(metadataBucket), totalBytesKey, uint64(totalBytes)); err != nil {
				return err
			}
		}
		return s.runBeforeCommit("complete batch")
	}); err != nil {
		return nil, fmt.Errorf("complete batch: persist: %w", err)
	}
	for _, record := range updated {
		s.records[record.EventID] = record
		s.refreshLaneForRecord(record)
	}
	for _, record := range deleted {
		delete(s.records, record.EventID)
		s.removeIndexed(record)
	}
	s.bytes = totalBytes
	s.deleted += len(deleted)
	s.compactOrderIfNeeded()
	return append([]Completion(nil), applied...), nil
}

func (s *Store) ackMany(operation string, eventIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpen(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if len(eventIDs) == 0 {
		return nil
	}
	if err := validateUniqueEventIDs(eventIDs); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}

	removed := make([]Record, len(eventIDs))
	var removedBytes int64
	for i, eventID := range eventIDs {
		record, ok := s.records[eventID]
		if !ok {
			return fmt.Errorf("%s %q: %w", operation, eventID, ErrNotFound)
		}
		if record.State != StateInFlight && record.State != StateAckUncertain {
			return fmt.Errorf("%s %q: %w: cannot ack record in state %q", operation, eventID, ErrInvalidState, record.State)
		}
		removed[i] = record
		removedBytes += record.SizeBytes
	}
	totalBytes := s.bytes - removedBytes
	if err := s.db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		events := tx.Bucket(eventsBucket)
		metadata := tx.Bucket(metadataBucket)
		for _, record := range removed {
			if err := records.Delete(uint64Bytes(record.Sequence)); err != nil {
				return err
			}
			if err := events.Delete([]byte(record.EventID)); err != nil {
				return err
			}
		}
		if err := putUint64(metadata, totalBytesKey, uint64(totalBytes)); err != nil {
			return err
		}
		return s.runBeforeCommit(operation)
	}); err != nil {
		return fmt.Errorf("%s: persist: %w", operation, err)
	}
	for _, record := range removed {
		delete(s.records, record.EventID)
		s.removeIndexed(record)
	}
	s.bytes = totalBytes
	s.deleted += len(removed)
	s.compactOrderIfNeeded()
	return nil
}

// Eligible returns, in global acceptance order, at most one ready record per
// ordering key. An earlier unassigned, in-flight, or ACK-uncertain record blocks
// only its own key; unrelated keys can continue.
func (s *Store) Eligible(limit int) []Record {
	return s.EligibleFor(limit, nil, nil)
}

// EligibleFor returns eligible lane heads restricted to the supplied active
// group epochs and available broker slots. A nil epoch map permits every group;
// a nil broker-slot map permits every broker. Input maps are read but not
// modified. Filtering occurs before payloads are cloned, so a dispatcher does
// not copy the entire outbox merely to select a bounded active batch.
func (s *Store) EligibleFor(limit int, epochs map[string]uint64, brokerSlots map[string]int) []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || s.db == nil || len(s.readyLanes) == 0 {
		return nil
	}
	limit = min(limit, len(s.readyLanes))
	eligible := make([]Record, 0, limit)
	usedBrokerSlots := make(map[string]int, len(brokerSlots))

	// Selecting several smallest heap elements must not mutate the shared index
	// under a read lock. Walk the implicit heap with a small candidate heap. When
	// filters are present, skipped heads are traversed without cloning payloads.
	candidates := readyLaneCandidateHeap{{lane: s.readyLanes[0]}}
	for len(eligible) < limit && len(candidates) != 0 {
		candidate := heap.Pop(&candidates).(readyLaneCandidate)
		lane := candidate.lane
		eventID := lane.eventIDs[lane.head]
		record := s.records[eventID]
		matches := true
		if epochs != nil {
			epoch, ok := epochs[record.Group]
			matches = ok && epoch == record.Epoch
		}
		if matches && brokerSlots != nil {
			matches = usedBrokerSlots[record.Broker] < brokerSlots[record.Broker]
		}
		if matches {
			eligible = append(eligible, cloneRecord(record))
			usedBrokerSlots[record.Broker]++
		}

		left := 2*candidate.index + 1
		if left < len(s.readyLanes) {
			heap.Push(&candidates, readyLaneCandidate{index: left, lane: s.readyLanes[left]})
		}
		right := left + 1
		if right < len(s.readyLanes) {
			heap.Push(&candidates, readyLaneCandidate{index: right, lane: s.readyLanes[right]})
		}
	}
	return eligible
}

// Unassigned returns durable unassigned records in acceptance order.
func (s *Store) Unassigned() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return nil
	}
	var records []Record
	for _, eventID := range s.order {
		r, ok := s.records[eventID]
		if ok && r.State == StateUnassigned {
			records = append(records, cloneRecord(r))
		}
	}
	return records
}

// Get returns a copy of one record.
func (s *Store) Get(eventID string) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpen(); err != nil {
		return Record{}, fmt.Errorf("get %q: %w", eventID, err)
	}
	record, ok := s.records[eventID]
	if !ok {
		return Record{}, fmt.Errorf("get %q: %w", eventID, ErrNotFound)
	}
	return cloneRecord(record), nil
}

// Records returns all records in acceptance order.
func (s *Store) Records() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return nil
	}
	records := make([]Record, 0, len(s.records))
	for _, eventID := range s.order {
		if record, ok := s.records[eventID]; ok {
			records = append(records, cloneRecord(record))
		}
	}
	return records
}

// Stats returns current capacity utilization.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return Stats{}
	}
	return Stats{Messages: len(s.records), Bytes: s.bytes, HighWaterMessages: s.highWaterMessages, HighWaterBytes: s.highWaterBytes}
}

func (s *Store) updateMany(operation string, eventIDs []string, mutate func(string, *Record) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpen(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if len(eventIDs) == 0 {
		return nil
	}
	if err := validateUniqueEventIDs(eventIDs); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}

	updated := make([]Record, len(eventIDs))
	encoded := make([][]byte, len(eventIDs))
	now := s.now().UTC()
	for i, eventID := range eventIDs {
		record, ok := s.records[eventID]
		if !ok {
			return fmt.Errorf("%s %q: %w", operation, eventID, ErrNotFound)
		}
		record = cloneRecord(record)
		if err := mutate(eventID, &record); err != nil {
			return fmt.Errorf("%s %q: %w", operation, eventID, err)
		}
		record.UpdatedAt = now
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("%s %q: encode: %w", operation, eventID, err)
		}
		updated[i] = record
		encoded[i] = data
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		for i, record := range updated {
			if err := records.Put(uint64Bytes(record.Sequence), encoded[i]); err != nil {
				return err
			}
		}
		return s.runBeforeCommit(operation)
	}); err != nil {
		return fmt.Errorf("%s: persist: %w", operation, err)
	}
	for _, record := range updated {
		s.records[record.EventID] = record
		s.refreshLaneForRecord(record)
	}
	return nil
}

func (s *Store) load(newDatabase bool) error {
	loaded := make(map[string]Record)
	var order []string
	var totalBytes int64
	var nextSequence uint64
	now := s.now().UTC()

	err := s.db.Update(func(tx *bolt.Tx) error {
		if newDatabase {
			metadata, err := tx.CreateBucket(metadataBucket)
			if err != nil {
				return err
			}
			if _, err := tx.CreateBucket(recordsBucket); err != nil {
				return err
			}
			if _, err := tx.CreateBucket(eventsBucket); err != nil {
				return err
			}
			if err := putUint64(metadata, formatKey, databaseFormatVersion); err != nil {
				return err
			}
			if err := putUint64(metadata, nextSequenceKey, 1); err != nil {
				return err
			}
			if err := putUint64(metadata, totalBytesKey, 0); err != nil {
				return err
			}
			nextSequence = 1
			return nil
		}

		metadata := tx.Bucket(metadataBucket)
		records := tx.Bucket(recordsBucket)
		events := tx.Bucket(eventsBucket)
		if metadata == nil || records == nil || events == nil {
			return fmt.Errorf("missing required bucket")
		}
		version, err := readUint64(metadata, formatKey)
		if err != nil {
			return err
		}
		if version != databaseFormatVersion {
			return fmt.Errorf("unsupported format version %d", version)
		}
		nextSequence, err = readUint64(metadata, nextSequenceKey)
		if err != nil {
			return err
		}
		if nextSequence == 0 {
			return fmt.Errorf("invalid next sequence 0")
		}
		storedBytes, err := readUint64(metadata, totalBytesKey)
		if err != nil {
			return err
		}
		if storedBytes > uint64(^uint64(0)>>1) {
			return fmt.Errorf("invalid byte count %d", storedBytes)
		}

		var maxSequence uint64
		var recovered []Record
		cursor := records.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			if len(key) != 8 {
				return fmt.Errorf("invalid record sequence key")
			}
			sequence := binary.BigEndian.Uint64(key)
			if sequence == 0 || sequence <= maxSequence {
				return fmt.Errorf("invalid record sequence %d", sequence)
			}
			var record Record
			if err := json.Unmarshal(value, &record); err != nil {
				return fmt.Errorf("decode record at sequence %d: %w", sequence, err)
			}
			if record.Sequence != sequence {
				return fmt.Errorf("record %q sequence does not match index", record.EventID)
			}
			if err := validateStoredRecord(record); err != nil {
				return fmt.Errorf("invalid record %q: %w", record.EventID, err)
			}
			if _, exists := loaded[record.EventID]; exists {
				return fmt.Errorf("duplicate event ID %q", record.EventID)
			}
			indexedSequence := events.Get([]byte(record.EventID))
			if !bytes.Equal(indexedSequence, key) {
				return fmt.Errorf("record %q has inconsistent event index", record.EventID)
			}
			if record.SizeBytes > s.limits.MaxBytes-totalBytes {
				return fmt.Errorf("%w (contains more than %d bytes)", ErrFull, s.limits.MaxBytes)
			}
			totalBytes += record.SizeBytes
			loaded[record.EventID] = record
			order = append(order, record.EventID)
			maxSequence = sequence
			if record.State == StateInFlight {
				record.State = StateAckUncertain
				record.LastError = "publish outcome unknown after outbox recovery; retry may duplicate"
				record.UpdatedAt = now
				loaded[record.EventID] = record
				recovered = append(recovered, record)
			}
		}
		if len(loaded) > s.limits.MaxMessages {
			return fmt.Errorf("%w (contains %d messages)", ErrFull, len(loaded))
		}
		if totalBytes != int64(storedBytes) {
			return fmt.Errorf("stored byte count %d does not match records %d", storedBytes, totalBytes)
		}
		indexedEvents := 0
		if err := events.ForEach(func(eventID, sequence []byte) error {
			if len(sequence) != 8 {
				return fmt.Errorf("event %q has invalid sequence index", eventID)
			}
			record, ok := loaded[string(eventID)]
			if !ok || record.Sequence != binary.BigEndian.Uint64(sequence) {
				return fmt.Errorf("event %q has no matching record", eventID)
			}
			indexedEvents++
			return nil
		}); err != nil {
			return err
		}
		if indexedEvents != len(loaded) {
			return fmt.Errorf("event index count does not match record count")
		}
		if maxSequence == ^uint64(0) {
			return fmt.Errorf("sequence exhausted")
		}
		if nextSequence <= maxSequence {
			nextSequence = maxSequence + 1
			if err := putUint64(metadata, nextSequenceKey, nextSequence); err != nil {
				return err
			}
		}
		for _, record := range recovered {
			data, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := records.Put(uint64Bytes(record.Sequence), data); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("load outbox: %w", err)
	}
	s.records = loaded
	s.order = order
	s.nextSeq = nextSequence
	s.bytes = totalBytes
	s.highWaterMessages = len(loaded)
	s.highWaterBytes = totalBytes
	s.rebuildEligibleIndex()
	return nil
}

func (s *Store) rebuildEligibleIndex() {
	s.lanes = make(map[string]*orderingLane)
	s.readyLanes = nil
	for _, eventID := range s.order {
		record, ok := s.records[eventID]
		if ok {
			s.indexAccepted(record)
		}
	}
}

func (s *Store) indexAccepted(record Record) {
	key := effectiveOrderingKey(record)
	lane := s.lanes[key]
	if lane == nil {
		lane = &orderingLane{key: key, readyIndex: -1}
		s.lanes[key] = lane
	}
	lane.eventIDs = append(lane.eventIDs, record.EventID)
	if len(lane.eventIDs) == 1 {
		s.refreshLane(lane)
	}
}

func (s *Store) refreshLaneForRecord(record Record) {
	lane := s.lanes[effectiveOrderingKey(record)]
	if lane != nil && lane.head < len(lane.eventIDs) && lane.eventIDs[lane.head] == record.EventID {
		s.refreshLane(lane)
	}
}

func (s *Store) removeIndexed(record Record) {
	lane := s.lanes[effectiveOrderingKey(record)]
	if lane == nil || lane.head >= len(lane.eventIDs) || lane.eventIDs[lane.head] != record.EventID {
		return
	}
	if lane.readyIndex >= 0 {
		heap.Remove(&s.readyLanes, lane.readyIndex)
	}
	lane.head++
	for lane.head < len(lane.eventIDs) {
		if _, ok := s.records[lane.eventIDs[lane.head]]; ok {
			break
		}
		lane.head++
	}
	if lane.head == len(lane.eventIDs) {
		delete(s.lanes, lane.key)
		return
	}
	if lane.head >= 1024 && lane.head*2 >= len(lane.eventIDs) {
		lane.eventIDs = append([]string(nil), lane.eventIDs[lane.head:]...)
		lane.head = 0
	}
	s.refreshLane(lane)
}

func (s *Store) refreshLane(lane *orderingLane) {
	if lane.readyIndex >= 0 {
		heap.Remove(&s.readyLanes, lane.readyIndex)
	}
	if lane.head >= len(lane.eventIDs) {
		lane.headSequence = 0
		return
	}
	record, ok := s.records[lane.eventIDs[lane.head]]
	if !ok {
		lane.headSequence = 0
		return
	}
	lane.headSequence = record.Sequence
	if record.State == StateReady {
		heap.Push(&s.readyLanes, lane)
	}
}

func inspectDatabasePath(path string) (bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("open outbox: inspect database: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("open outbox: inspect database: %w", err)
	}
	if info.IsDir() {
		return false, fmt.Errorf("open outbox: path is a directory")
	}
	if info.Size() == 0 {
		return true, nil
	}
	prefix := make([]byte, 64)
	n, err := file.Read(prefix)
	if err != nil {
		return false, fmt.Errorf("open outbox: inspect database: %w", err)
	}
	prefix = bytes.TrimSpace(prefix[:n])
	if len(prefix) > 0 && (prefix[0] == '{' || prefix[0] == '[') {
		return false, fmt.Errorf("open outbox: %w; export or remove the old file before opening it", ErrLegacyFormat)
	}
	return false, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (s *Store) ensureOpen() error {
	if s.db == nil {
		return ErrClosed
	}
	return nil
}

func (s *Store) runBeforeCommit(operation string) error {
	if s.beforeCommit != nil {
		return s.beforeCommit(operation)
	}
	return nil
}

func (s *Store) compactOrderIfNeeded() {
	if s.deleted < 1024 && len(s.order) <= 2*len(s.records)+16 {
		return
	}
	order := make([]string, 0, len(s.records))
	for _, eventID := range s.order {
		if _, ok := s.records[eventID]; ok {
			order = append(order, eventID)
		}
	}
	s.order = order
	s.deleted = 0
}

func splitStateUpdates(updates []StateUpdate) ([]string, map[string]string) {
	ids := make([]string, len(updates))
	reasons := make(map[string]string, len(updates))
	for i, update := range updates {
		ids[i] = update.EventID
		reasons[update.EventID] = update.Reason
	}
	return ids, reasons
}

func validateUniqueEventIDs(eventIDs []string) error {
	seen := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		if eventID == "" {
			return fmt.Errorf("%w: empty event ID", ErrInvalidRecord)
		}
		if _, ok := seen[eventID]; ok {
			return fmt.Errorf("%w: event ID %q appears more than once in batch", ErrInvalidRecord, eventID)
		}
		seen[eventID] = struct{}{}
	}
	return nil
}

func validateNewRecord(r Record) error {
	if r.EventID == "" || r.Topic == "" || r.OrderingKey == "" || r.Group == "" || r.Hash == "" || r.HashContract == "" || r.LibraryVersion == "" {
		return fmt.Errorf("%w: event ID, topic, ordering key, group, hash, hash contract, and library version are required", ErrInvalidRecord)
	}
	if r.State != "" && r.State != StateUnassigned && r.State != StateReady {
		return fmt.Errorf("%w: initial state %q is not allowed", ErrInvalidRecord, r.State)
	}
	if r.State == StateReady && (r.Epoch == 0 || r.Broker == "" || r.Destination == "") {
		return fmt.Errorf("%w: ready record requires an assignment", ErrInvalidRecord)
	}
	if r.State != StateReady && (r.Epoch != 0 || r.Broker != "" || r.Destination != "") {
		return fmt.Errorf("%w: unassigned record cannot carry an assignment", ErrInvalidRecord)
	}
	return nil
}

func validateStoredRecord(r Record) error {
	if r.EventID == "" || r.Topic == "" || r.OrderingKey == "" || r.Group == "" || r.Hash == "" || r.HashContract == "" || r.LibraryVersion == "" || r.Sequence == 0 || r.SizeBytes <= 0 || r.SizeBytes != acceptedSize(r) {
		return ErrInvalidRecord
	}
	switch r.State {
	case StateUnassigned:
		if r.Epoch != 0 || r.Broker != "" || r.Destination != "" {
			return ErrInvalidRecord
		}
	case StateReady, StateInFlight, StateAckUncertain:
		if r.Epoch == 0 || r.Broker == "" || r.Destination == "" {
			return ErrInvalidRecord
		}
	default:
		return ErrInvalidRecord
	}
	return nil
}

func acceptedSize(r Record) int64 {
	// Capacity applies to immutable accepted content, not incidental database
	// encoding or later diagnostic strings, so state changes cannot overflow.
	type content struct {
		EventID        string            `json:"event_id"`
		Payload        []byte            `json:"payload,omitempty"`
		Topic          string            `json:"topic"`
		Properties     map[string]string `json:"properties,omitempty"`
		OrderingKey    string            `json:"ordering_key"`
		Group          string            `json:"group"`
		Hash           string            `json:"hash"`
		HashContract   string            `json:"hash_contract"`
		LibraryVersion string            `json:"library_version"`
	}
	data, _ := json.Marshal(content{
		EventID: r.EventID, Payload: r.Payload, Topic: r.Topic, Properties: r.Properties,
		OrderingKey: r.OrderingKey, Group: r.Group, Hash: r.Hash,
		HashContract: r.HashContract, LibraryVersion: r.LibraryVersion,
	})
	return int64(len(data))
}

func effectiveOrderingKey(r Record) string {
	if r.OrderingKey != "" {
		return r.OrderingKey
	}
	return r.Group + "\x00" + r.Hash
}

func cloneRecord(r Record) Record {
	r.Payload = append([]byte(nil), r.Payload...)
	r.Properties = cloneProperties(r.Properties)
	return r
}

func cloneRecords(records []Record) []Record {
	cloned := make([]Record, len(records))
	for i := range records {
		cloned[i] = cloneRecord(records[i])
	}
	return cloned
}

func cloneProperties(properties map[string]string) map[string]string {
	if properties == nil {
		return nil
	}
	cloned := make(map[string]string, len(properties))
	for k, v := range properties {
		cloned[k] = v
	}
	return cloned
}

func uint64Bytes(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func putUint64(bucket *bolt.Bucket, key []byte, value uint64) error {
	return bucket.Put(key, uint64Bytes(value))
}

func readUint64(bucket *bolt.Bucket, key []byte) (uint64, error) {
	value := bucket.Get(key)
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid or missing metadata %q", key)
	}
	return binary.BigEndian.Uint64(value), nil
}
