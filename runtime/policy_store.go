package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/solacese/solace-workload-balancer/policy"
)

const policyEngineStateVersion = 1

// PolicyEngineStateStore is the durable boundary for policy evidence. A Save
// must not return until the state survives process restart.
type PolicyEngineStateStore interface {
	Load() (policy.EngineState, error)
	Save(policy.EngineState) error
}

type policyEngineStateDocument struct {
	Version int                `json:"version"`
	State   policy.EngineState `json:"state"`
}

// JSONPolicyEngineStateStore atomically replaces and synchronizes a private,
// versioned JSON document.
type JSONPolicyEngineStateStore struct {
	Path string
}

func policyEngineStatePath(controllerStatePath string) string {
	return filepath.Join(controllerStatePath+".operations", "policy.json")
}

func (store JSONPolicyEngineStateStore) Load() (policy.EngineState, error) {
	if store.Path == "" {
		return policy.EngineState{}, errors.New("runtime: policy state path is required")
	}
	info, err := os.Lstat(store.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return policy.EngineState{}, nil
	}
	if err != nil {
		return policy.EngineState{}, fmt.Errorf("runtime: inspect policy state: %w", err)
	}
	if !info.Mode().IsRegular() {
		return policy.EngineState{}, errors.New("runtime: policy state is not a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return policy.EngineState{}, errors.New("runtime: policy state permissions must be 0600 or stricter")
	}
	contents, err := os.ReadFile(store.Path)
	if err != nil {
		return policy.EngineState{}, fmt.Errorf("runtime: read policy state: %w", err)
	}
	var document policyEngineStateDocument
	if err := json.Unmarshal(contents, &document); err != nil {
		return policy.EngineState{}, fmt.Errorf("runtime: decode policy state: %w", err)
	}
	if document.Version != policyEngineStateVersion {
		return policy.EngineState{}, fmt.Errorf("runtime: unsupported policy state version %d", document.Version)
	}
	return document.State, nil
}

func (store JSONPolicyEngineStateStore) Save(state policy.EngineState) error {
	if store.Path == "" {
		return errors.New("runtime: policy state path is required")
	}
	contents, err := json.MarshalIndent(policyEngineStateDocument{Version: policyEngineStateVersion, State: state}, "", "  ")
	if err != nil {
		return fmt.Errorf("runtime: encode policy state: %w", err)
	}
	contents = append(contents, '\n')
	directory := filepath.Dir(store.Path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("runtime: create policy state directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("runtime: secure policy state directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".policy-state-*")
	if err != nil {
		return fmt.Errorf("runtime: create temporary policy state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("runtime: set temporary policy state permissions: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return fmt.Errorf("runtime: write temporary policy state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("runtime: sync temporary policy state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("runtime: close temporary policy state: %w", err)
	}
	if err := os.Rename(temporaryPath, store.Path); err != nil {
		return fmt.Errorf("runtime: replace policy state: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("runtime: open policy state directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("runtime: sync policy state directory: %w", err)
	}
	return nil
}
