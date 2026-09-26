package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const stateVersion = 1

// Store persists the complete controller state as an atomically replaced JSON
// document. A successful Save is durable through process restart.
type Store interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type JSONStore struct {
	Path string
}

func (s JSONStore) Load() (PersistentState, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return PersistentState{Version: stateVersion, Groups: make(map[string]*GroupState), History: make(map[string][]TransitionRecord), CleanupEvidence: make(map[string][]CleanupEvidence)}, nil
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("read controller state: %w", err)
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("decode controller state: %w", err)
	}
	if state.Version != stateVersion {
		return PersistentState{}, fmt.Errorf("unsupported controller state version %d", state.Version)
	}
	if state.Groups == nil {
		state.Groups = make(map[string]*GroupState)
	}
	if state.History == nil {
		state.History = make(map[string][]TransitionRecord)
	}
	if state.CleanupEvidence == nil {
		state.CleanupEvidence = make(map[string][]CleanupEvidence)
	}
	return state, nil
}

func (s JSONStore) Save(state PersistentState) error {
	if s.Path == "" {
		return errors.New("state path is required")
	}
	state.Version = stateVersion
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode controller state: %w", err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create controller state directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".controller-state-*")
	if err != nil {
		return fmt.Errorf("create temporary controller state: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("set temporary controller state permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temporary controller state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temporary controller state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary controller state: %w", err)
	}
	if err := os.Rename(tmpName, s.Path); err != nil {
		return fmt.Errorf("replace controller state: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open controller state directory: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync controller state directory: %w", err)
	}
	return nil
}
