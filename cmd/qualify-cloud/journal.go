package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const journalVersion = 1

type ResourceRecord struct {
	Role string `json:"role"`
	ID   string `json:"id"`
}

type Journal struct {
	Version   int              `json:"version"`
	Plan      Plan             `json:"plan"`
	Resources []ResourceRecord `json:"resources"`
}

type journalStore interface {
	Initialize(Journal) error
	Load() (Journal, error)
	Save(Journal) error
	Remove() error
}

type fileJournal struct {
	path string
}

func (store fileJournal) Initialize(journal Journal) error {
	if _, err := os.Lstat(store.path); err == nil {
		return fmt.Errorf("cleanup journal already exists; run --cleanup first")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect cleanup journal: %w", err)
	}
	return store.Save(journal)
}

func (store fileJournal) Load() (Journal, error) {
	info, err := os.Lstat(store.path)
	if err != nil {
		return Journal{}, err
	}
	if !info.Mode().IsRegular() {
		return Journal{}, fmt.Errorf("journal is not a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return Journal{}, fmt.Errorf("journal permissions must be 0600 or stricter")
	}
	contents, err := os.ReadFile(store.path)
	if err != nil {
		return Journal{}, err
	}
	var journal Journal
	if err := json.Unmarshal(contents, &journal); err != nil {
		return Journal{}, fmt.Errorf("decode journal: %w", err)
	}
	if err := validateJournal(journal); err != nil {
		return Journal{}, err
	}
	return journal, nil
}

func (store fileJournal) Save(journal Journal) error {
	if err := validateJournal(journal); err != nil {
		return err
	}
	contents, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		return fmt.Errorf("encode journal: %w", err)
	}
	contents = append(contents, '\n')
	directory := filepath.Dir(store.path)
	createdDirectory := false
	if _, err := os.Stat(directory); errors.Is(err, os.ErrNotExist) {
		createdDirectory = true
	} else if err != nil {
		return fmt.Errorf("inspect journal directory: %w", err)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create journal directory: %w", err)
	}
	if createdDirectory {
		if err := os.Chmod(directory, 0o700); err != nil {
			return fmt.Errorf("secure journal directory: %w", err)
		}
	}
	temporary, err := os.CreateTemp(directory, ".qualification-journal-*")
	if err != nil {
		return fmt.Errorf("create temporary journal: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("set journal permissions: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return fmt.Errorf("write journal: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync journal: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close journal: %w", err)
	}
	if err := os.Rename(temporaryName, store.path); err != nil {
		return fmt.Errorf("replace journal: %w", err)
	}
	if directoryHandle, err := os.Open(directory); err == nil {
		_ = directoryHandle.Sync()
		_ = directoryHandle.Close()
	}
	return nil
}

func (store fileJournal) Remove() error {
	err := os.Remove(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func validateJournal(journal Journal) error {
	if journal.Version != journalVersion {
		return fmt.Errorf("unsupported journal version")
	}
	if err := journal.Plan.validate(); err != nil {
		return fmt.Errorf("invalid journal plan: %w", err)
	}
	seenRoles := make(map[string]struct{}, len(journal.Resources))
	seenIDs := make(map[string]struct{}, len(journal.Resources))
	if len(journal.Resources) > len(journal.Plan.Services) {
		return fmt.Errorf("journal contains too many resources")
	}
	for _, resource := range journal.Resources {
		if resource.Role == "" || resource.ID == "" {
			return fmt.Errorf("journal contains an incomplete resource record")
		}
		if _, exists := seenRoles[resource.Role]; exists {
			return fmt.Errorf("journal contains duplicate roles")
		}
		if _, exists := seenIDs[resource.ID]; exists {
			return fmt.Errorf("journal contains duplicate resource IDs")
		}
		validRole := false
		for _, service := range journal.Plan.Services {
			if resource.Role == service.Role {
				validRole = true
				break
			}
		}
		if !validRole {
			return fmt.Errorf("journal contains a role outside its plan")
		}
		seenRoles[resource.Role] = struct{}{}
		seenIDs[resource.ID] = struct{}{}
	}
	return nil
}
