package runtime

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/policy"
)

func TestJSONPolicyEngineStateStorePersistsPrivateStateAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controller.json.operations", "policy.json")
	store := JSONPolicyEngineStateStore{Path: path}
	want := policy.EngineState{
		LastEvaluatedAt: time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC),
		PressureSince:   map[string]time.Time{"orders": time.Date(2026, 9, 25, 11, 55, 0, 0, time.UTC)},
	}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("policy state permissions = %04o, want 0600", info.Mode().Perm())
	}
	got, err := (JSONPolicyEngineStateStore{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored policy state = %#v, want %#v", got, want)
	}
}

func TestJSONPolicyEngineStateStoreRejectsInvalidDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(`{"version":2,"state":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (JSONPolicyEngineStateStore{Path: path}).Load(); err == nil {
		t.Fatal("unsupported policy state version accepted")
	}
}

func TestValidateConfiguredPolicyStateRejectsUnknownGroup(t *testing.T) {
	state := policy.EngineState{PressureSince: map[string]time.Time{"removed": time.Now()}}
	if err := validateConfiguredPolicyState(state, []config.ScalingGroup{{ID: "orders"}}); err == nil {
		t.Fatal("policy evidence for unknown group accepted")
	}
}
