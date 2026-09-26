package broker0

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	operationStateVersion           = 2
	defaultOperationStoreMaxEntries = 10_000
	defaultRedeliveryHorizon        = 24 * time.Hour
)

var ErrOperationStoreFull = errors.New("broker0: operation store is full within the redelivery horizon")

type operationState struct {
	Version    int                  `json:"version"`
	Operations map[string]time.Time `json:"operations"`
}

type legacyOperationState struct {
	Version    int             `json:"version"`
	Operations map[string]bool `json:"operations"`
}

// OperationStoreOptions bounds retained idempotency keys. Entries are never
// evicted before RedeliveryHorizon: when MaxEntries is reached entirely by
// unexpired entries, Record fails closed instead of allowing a replayed side
// effect through.
type OperationStoreOptions struct {
	MaxEntries        int
	RedeliveryHorizon time.Duration
	Now               func() time.Time
}

func (options OperationStoreOptions) withDefaults() OperationStoreOptions {
	if options.MaxEntries == 0 {
		options.MaxEntries = defaultOperationStoreMaxEntries
	}
	if options.RedeliveryHorizon == 0 {
		options.RedeliveryHorizon = defaultRedeliveryHorizon
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return options
}

// JSONOperationStore is a small crash-durable OperationStore. It atomically
// replaces and synchronizes one bounded JSON document.
type JSONOperationStore struct {
	mu      sync.RWMutex
	path    string
	options OperationStoreOptions
	state   operationState
}

// OpenJSONOperationStore uses conservative defaults. New deployments should use
// OpenJSONOperationStoreWithOptions to align retention with the transport's
// maximum redelivery horizon and expected operation rate.
func OpenJSONOperationStore(path string) (*JSONOperationStore, error) {
	return OpenJSONOperationStoreWithOptions(path, OperationStoreOptions{})
}

func OpenJSONOperationStoreWithOptions(path string, options OperationStoreOptions) (*JSONOperationStore, error) {
	if path == "" {
		return nil, errors.New("broker0: operation store path is required")
	}
	options = options.withDefaults()
	if options.MaxEntries < 1 {
		return nil, errors.New("broker0: operation store max entries must be positive")
	}
	if options.RedeliveryHorizon <= 0 {
		return nil, errors.New("broker0: operation store redelivery horizon must be positive")
	}
	store := &JSONOperationStore{
		path: path, options: options,
		state: operationState{Version: operationStateVersion, Operations: make(map[string]time.Time)},
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("broker0: read operation store: %w", err)
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, fmt.Errorf("broker0: decode operation store: %w", err)
	}
	now := options.Now().UTC()
	switch header.Version {
	case 1:
		var legacy legacyOperationState
		if err := json.Unmarshal(data, &legacy); err != nil {
			return nil, fmt.Errorf("broker0: decode legacy operation store: %w", err)
		}
		if legacy.Operations == nil {
			return nil, errors.New("broker0: operation store has no operations map")
		}
		if len(legacy.Operations) > options.MaxEntries {
			return nil, fmt.Errorf("%w: legacy store has %d entries, maximum is %d", ErrOperationStoreFull, len(legacy.Operations), options.MaxEntries)
		}
		for operationID, completed := range legacy.Operations {
			if !completed {
				return nil, fmt.Errorf("broker0: operation %q has invalid incomplete state", operationID)
			}
			store.state.Operations[operationID] = now
		}
		if err := store.validate(); err != nil {
			return nil, err
		}
		if err := saveOperationState(path, store.state); err != nil {
			return nil, fmt.Errorf("broker0: migrate operation store: %w", err)
		}
	case operationStateVersion:
		if err := json.Unmarshal(data, &store.state); err != nil {
			return nil, fmt.Errorf("broker0: decode operation store: %w", err)
		}
		if err := store.validateEntries(); err != nil {
			return nil, err
		}
		pruned := store.pruneLocked(now)
		if len(store.state.Operations) > store.options.MaxEntries {
			return nil, fmt.Errorf("%w: store has %d unexpired entries, maximum is %d", ErrOperationStoreFull, len(store.state.Operations), store.options.MaxEntries)
		}
		if pruned {
			if err := saveOperationState(path, store.state); err != nil {
				return nil, fmt.Errorf("broker0: prune operation store: %w", err)
			}
		}
	default:
		return nil, fmt.Errorf("broker0: unsupported operation store version %d", header.Version)
	}
	return store, nil
}

func (store *JSONOperationStore) validate() error {
	if err := store.validateEntries(); err != nil {
		return err
	}
	if len(store.state.Operations) > store.options.MaxEntries {
		return fmt.Errorf("%w: store has %d entries, maximum is %d", ErrOperationStoreFull, len(store.state.Operations), store.options.MaxEntries)
	}
	return nil
}

func (store *JSONOperationStore) validateEntries() error {
	if store.state.Operations == nil {
		return errors.New("broker0: operation store has no operations map")
	}
	for operationID, recordedAt := range store.state.Operations {
		if err := validateOperationID(operationID); err != nil {
			return fmt.Errorf("broker0: invalid persisted operation ID: %w", err)
		}
		if recordedAt.IsZero() {
			return fmt.Errorf("broker0: operation %q has no recorded timestamp", operationID)
		}
	}
	return nil
}

func (store *JSONOperationStore) Contains(ctx context.Context, operationID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validateOperationID(operationID); err != nil {
		return false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	recordedAt, ok := store.state.Operations[operationID]
	return ok && !expired(recordedAt, store.options.Now().UTC(), store.options.RedeliveryHorizon), nil
}

func (store *JSONOperationStore) Record(ctx context.Context, operationID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateOperationID(operationID); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.options.Now().UTC()
	if recordedAt, ok := store.state.Operations[operationID]; ok && !expired(recordedAt, now, store.options.RedeliveryHorizon) {
		return nil
	}
	next := operationState{Version: operationStateVersion, Operations: make(map[string]time.Time, len(store.state.Operations)+1)}
	for existing, recordedAt := range store.state.Operations {
		if !expired(recordedAt, now, store.options.RedeliveryHorizon) {
			next.Operations[existing] = recordedAt
		}
	}
	if len(next.Operations) >= store.options.MaxEntries {
		return fmt.Errorf("%w: maximum %d", ErrOperationStoreFull, store.options.MaxEntries)
	}
	next.Operations[operationID] = now
	if err := saveOperationState(store.path, next); err != nil {
		return err
	}
	store.state = next
	return nil
}

func (store *JSONOperationStore) pruneLocked(now time.Time) bool {
	changed := false
	for operationID, recordedAt := range store.state.Operations {
		if expired(recordedAt, now, store.options.RedeliveryHorizon) {
			delete(store.state.Operations, operationID)
			changed = true
		}
	}
	return changed
}

func expired(recordedAt, now time.Time, horizon time.Duration) bool {
	return !recordedAt.Add(horizon).After(now)
}

func saveOperationState(path string, state operationState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("broker0: encode operation store: %w", err)
	}
	data = append(data, '\n')
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("broker0: create operation store directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".broker0-operations-*")
	if err != nil {
		return fmt.Errorf("broker0: create temporary operation store: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("broker0: set operation store permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return fmt.Errorf("broker0: write operation store: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("broker0: sync operation store: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("broker0: close operation store: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("broker0: replace operation store: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("broker0: open operation store directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("broker0: sync operation store directory: %w", err)
	}
	return nil
}
