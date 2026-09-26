package cloud

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const journalSchemaVersion = 1

type JournalService struct {
	Role           string          `json:"role"`
	Identity       ServiceIdentity `json:"identity"`
	State          string          `json:"state"`
	IdempotencyKey string          `json:"idempotencyKey"`
	ServiceID      string          `json:"serviceId,omitempty"`
	OperationID    string          `json:"operationId,omitempty"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

type journalDocument struct {
	SchemaVersion int                       `json:"schemaVersion"`
	PlanSHA256    string                    `json:"planSha256"`
	TokenOwner    string                    `json:"tokenOwner"`
	Services      map[string]JournalService `json:"services"`
}

// Journal is an atomically replaced, owner-private lifecycle journal. Close it
// to release the exclusive process lock.
type Journal struct {
	mu       sync.Mutex
	path     string
	lock     *fileLock
	document journalDocument
}

func OpenJournal(journalPath string, plan QualificationPlan, tokenOwner string) (*Journal, error) {
	if journalPath == "" {
		return nil, errors.New("journal path is required")
	}
	if tokenOwner == "" {
		return nil, errors.New("token owner is required")
	}
	directory := filepath.Dir(journalPath)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		return nil, fmt.Errorf("secure journal directory: %w", err)
	}
	lock, err := acquireFileLock(journalPath + ".lock")
	if err != nil {
		return nil, err
	}
	journal := &Journal{path: journalPath, lock: lock}
	if err := journal.load(plan, tokenOwner); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return journal, nil
}

func (j *Journal) load(plan QualificationPlan, owner string) error {
	file, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		j.document = journalDocument{SchemaVersion: journalSchemaVersion, PlanSHA256: plan.SHA256(), TokenOwner: owner, Services: make(map[string]JournalService)}
		return j.persistLocked()
	}
	if err != nil {
		return fmt.Errorf("open lifecycle journal: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat lifecycle journal: %w", err)
	}
	if info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("lifecycle journal %q permissions %04o are not private", j.path, info.Mode().Perm())
	}
	decoder := json.NewDecoder(io.LimitReader(file, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&j.document); err != nil {
		return fmt.Errorf("decode lifecycle journal: %w", err)
	}
	if j.document.SchemaVersion != journalSchemaVersion {
		return fmt.Errorf("unsupported lifecycle journal schema %d", j.document.SchemaVersion)
	}
	if j.document.PlanSHA256 != plan.SHA256() {
		return errors.New("lifecycle journal qualification plan hash does not match")
	}
	if j.document.TokenOwner != owner {
		return errors.New("lifecycle journal token owner does not match")
	}
	if j.document.Services == nil {
		j.document.Services = make(map[string]JournalService)
	}
	return nil
}

func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.lock == nil {
		return nil
	}
	err := j.lock.Close()
	j.lock = nil
	return err
}

func (j *Journal) Service(role string) (JournalService, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	record, ok := j.document.Services[role]
	return record, ok
}

func (j *Journal) Services() map[string]JournalService {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := make(map[string]JournalService, len(j.document.Services))
	for role, record := range j.document.Services {
		result[role] = record
	}
	return result
}

func (j *Journal) put(record JournalService) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.lock == nil {
		return errors.New("lifecycle journal is closed")
	}
	record.UpdatedAt = time.Now().UTC()
	j.document.Services[record.Role] = record
	return j.persistLocked()
}

func (j *Journal) persistLocked() error {
	directory := filepath.Dir(j.path)
	temporary, err := os.CreateTemp(directory, ".lifecycle-journal-*")
	if err != nil {
		return fmt.Errorf("create temporary lifecycle journal: %w", err)
	}
	temporaryName := temporary.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return fmt.Errorf("secure temporary lifecycle journal: %w", err)
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(j.document); err != nil {
		temporary.Close()
		return fmt.Errorf("encode lifecycle journal: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync lifecycle journal: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close lifecycle journal: %w", err)
	}
	if err := os.Rename(temporaryName, j.path); err != nil {
		return fmt.Errorf("replace lifecycle journal: %w", err)
	}
	if err := os.Chmod(j.path, 0600); err != nil {
		return fmt.Errorf("secure lifecycle journal: %w", err)
	}
	dir, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open journal directory for sync: %w", err)
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return fmt.Errorf("sync journal directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close journal directory: %w", closeErr)
	}
	ok = true
	return nil
}
