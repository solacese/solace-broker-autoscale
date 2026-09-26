package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/qualification"
)

func TestParseOptionsRequiresExactlyOneMode(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "none"},
		{name: "multiple", args: []string{"--preflight", "--run"}},
		{name: "positional", args: []string{"--cleanup", "extra"}},
		{name: "zero timeout", args: []string{"--run", "--timeout=0s"}},
		{name: "unbounded timeout", args: []string{"--run", "--timeout=3h"}},
		{name: "unbounded cleanup", args: []string{"--cleanup", "--cleanup-timeout=31m"}},
		{name: "result replaces journal", args: []string{"--run", "--journal=private.json", "--result=private.json"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseOptions(test.args, io.Discard); err == nil {
				t.Fatal("expected invalid options")
			}
		})
	}
}

func TestParseOptionsAcceptsEachMode(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"preflight", "run", "cleanup"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			options, err := parseOptions([]string{"--" + mode}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if options.mode != mode {
				t.Fatalf("mode = %q, want %q", options.mode, mode)
			}
			if options.evidencePath != defaultEvidencePath {
				t.Fatalf("result path = %q, want %q", options.evidencePath, defaultEvidencePath)
			}
		})
	}
}

func TestHelpSucceedsWithoutCredentialsOrRunner(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"--help"}, dependencies{stdout: &stdout, stderr: &stderr})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Usage: qualify-cloud") || !strings.Contains(stdout.String(), "-preflight") {
		t.Fatalf("help output = %q", stdout.String())
	}
}

func TestDefaultRunHookFailsBeforeProvisioning(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	envPath := filepath.Join(directory, ".env")
	journalPath := filepath.Join(directory, "journal.json")
	if err := os.WriteFile(envPath, []byte(defaultJWTVariable+"=jwt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	var stderr bytes.Buffer
	code := execute(context.Background(), []string{"--run", "--env=" + envPath, "--journal=" + journalPath, "--result=" + filepath.Join(directory, "result.json")}, dependencies{
		runner: runner,
		random: bytes.NewReader(make([]byte, 8)),
		stderr: &stderr,
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if runner.provisionCalls != 0 || len(runner.cleanupCalls) != 0 {
		t.Fatalf("unsafe operations before capability failure: provision=%d cleanup=%d", runner.provisionCalls, len(runner.cleanupCalls))
	}
	if !strings.Contains(stderr.String(), "qualification workload not wired; no services were requested") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestPreflightDoesNotCreateOrJournal(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	envPath := filepath.Join(directory, ".env")
	journalPath := filepath.Join(directory, "private", "journal.json")
	const secret = "header.payload.signature"
	if err := os.WriteFile(envPath, []byte(defaultJWTVariable+"='"+secret+"'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"--preflight", "--env=" + envPath, "--journal=" + journalPath, "--result=" + filepath.Join(directory, "result.json")}, dependencies{
		runner: runner,
		now:    func() time.Time { return time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC) },
		random: bytes.NewReader(make([]byte, 8)),
		stdout: &stdout,
		stderr: &stderr,
	})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr.String())
	}
	if runner.preflightJWT != secret {
		t.Fatal("runner did not receive the raw JWT")
	}
	if runner.provisionCalls != 0 || len(runner.cleanupCalls) != 0 {
		t.Fatalf("preflight performed mutations: provision=%d cleanup=%d", runner.provisionCalls, len(runner.cleanupCalls))
	}
	if _, err := os.Stat(journalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("preflight journal exists: %v", err)
	}
	combined := stdout.String() + stderr.String()
	if strings.Contains(combined, secret) {
		t.Fatal("output leaked JWT")
	}
}

func TestRunCreatesExactPlanAndAlwaysCleansUp(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	envPath := filepath.Join(directory, ".env")
	journalPath := filepath.Join(directory, "journal.json")
	const secret = "raw-secret-jwt"
	if err := os.WriteFile(envPath, []byte(defaultJWTVariable+"="+secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	hookCalled := false
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"--run", "--env=" + envPath, "--journal=" + journalPath, "--result=" + filepath.Join(directory, "result.json")}, dependencies{
		runner: runner,
		qualify: func(_ context.Context, _, _ string, plan Plan, resources []ResourceRecord, _, _ time.Duration) (qualification.Result, error) {
			hookCalled = true
			if err := plan.validate(); err != nil {
				return qualification.Result{}, err
			}
			if len(resources) != 4 {
				return qualification.Result{}, errors.New("hook did not receive four exact IDs")
			}
			return successfulQualificationResult(), nil
		},
		now:    func() time.Time { return time.Date(2026, 9, 25, 12, 34, 56, 0, time.UTC) },
		random: bytes.NewReader([]byte{0, 1, 2, 3, 4, 5, 6, 7}),
		stdout: &stdout,
		stderr: &stderr,
	})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr.String())
	}
	if !hookCalled {
		t.Fatal("qualification hook was not called")
	}
	if runner.provisionCalls != 1 {
		t.Fatalf("provision calls = %d, want 1", runner.provisionCalls)
	}
	if len(runner.cleanupCalls) != 1 {
		t.Fatalf("cleanup calls = %d, want 1", len(runner.cleanupCalls))
	}
	if len(runner.cleanupCalls[0].Resources) != 4 {
		t.Fatalf("cleanup journal has %d IDs, want 4", len(runner.cleanupCalls[0].Resources))
	}
	if _, err := os.Stat(journalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed journal still exists: %v", err)
	}
	combined := stdout.String() + stderr.String()
	if strings.Contains(combined, secret) || strings.Contains(combined, "id-broker") {
		t.Fatal("output leaked a credential or resource ID")
	}
}

func TestRunPassesConfiguredDeadlinesToQualification(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	envPath := filepath.Join(directory, ".env")
	journalPath := filepath.Join(directory, "journal.json")
	if err := os.WriteFile(envPath, []byte(defaultJWTVariable+"=jwt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	var gotRun, gotCleanup time.Duration
	code := execute(context.Background(), []string{"--run", "--env=" + envPath, "--journal=" + journalPath, "--result=" + filepath.Join(directory, "result.json"), "--timeout=7m", "--cleanup-timeout=3m"}, dependencies{
		runner: runner,
		qualify: func(_ context.Context, _, _ string, _ Plan, _ []ResourceRecord, run, cleanup time.Duration) (qualification.Result, error) {
			gotRun, gotCleanup = run, cleanup
			return successfulQualificationResult(), nil
		},
		random: bytes.NewReader(make([]byte, 8)),
	})
	if code != 0 || gotRun != 7*time.Minute || gotCleanup != 3*time.Minute {
		t.Fatalf("exit=%d run=%s cleanup=%s", code, gotRun, gotCleanup)
	}
}

func TestRunCleansUpWhenQualificationFails(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	envPath := filepath.Join(directory, ".env")
	journalPath := filepath.Join(directory, "journal.json")
	if err := os.WriteFile(envPath, []byte(defaultJWTVariable+"=jwt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	code := execute(context.Background(), []string{"--run", "--env=" + envPath, "--journal=" + journalPath, "--result=" + filepath.Join(directory, "result.json")}, dependencies{
		runner: runner,
		qualify: func(context.Context, string, string, Plan, []ResourceRecord, time.Duration, time.Duration) (qualification.Result, error) {
			return qualification.Result{Cleanup: "complete", Scenarios: []qualification.ScenarioResult{{Name: qualification.ScenarioStatic, Status: "FAIL", Reason: "private endpoint https://secret.invalid"}}}, errors.New("qualification failure containing private details")
		},
		random: bytes.NewReader(make([]byte, 8)),
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if len(runner.cleanupCalls) != 1 {
		t.Fatalf("cleanup calls after failure = %d, want 1", len(runner.cleanupCalls))
	}
	if _, err := os.Stat(journalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed journal still exists: %v", err)
	}
}

func TestRunRetainsJournalWhenCleanupFails(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	envPath := filepath.Join(directory, ".env")
	journalPath := filepath.Join(directory, "journal.json")
	if err := os.WriteFile(envPath, []byte(defaultJWTVariable+"=jwt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{cleanupErr: errors.New("cleanup deadline")}
	code := execute(context.Background(), []string{"--run", "--env=" + envPath, "--journal=" + journalPath, "--result=" + filepath.Join(directory, "result.json")}, dependencies{
		runner: runner,
		qualify: func(context.Context, string, string, Plan, []ResourceRecord, time.Duration, time.Duration) (qualification.Result, error) {
			return successfulQualificationResult(), nil
		},
		random: bytes.NewReader(make([]byte, 8)),
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if len(runner.cleanupCalls) != 2 {
		t.Fatalf("cleanup calls = %d, want mandatory attempt plus finalizer", len(runner.cleanupCalls))
	}
	journal, err := (fileJournal{path: journalPath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(journal.Resources) != 4 {
		t.Fatalf("retained journal has %d resources", len(journal.Resources))
	}
}

func TestResolvedPreflightPlanIsJournaledForCleanup(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	envPath := filepath.Join(directory, ".env")
	journalPath := filepath.Join(directory, "journal.json")
	if err := os.WriteFile(envPath, []byte(defaultJWTVariable+"=jwt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{
		preflightPlan: func(plan Plan) Plan {
			for index := range plan.Services {
				plan.Services[index].DatacenterID = "dc-id"
				plan.Services[index].BrokerVersionID = qualificationRelease
				plan.Services[index].ServiceClassID = "developer-id"
				if index == 0 {
					plan.Services[index].ServiceClassID = "enterprise-id"
				}
			}
			return plan
		},
		cleanupErr: errors.New("retain"),
	}
	code := execute(context.Background(), []string{"--run", "--env=" + envPath, "--journal=" + journalPath, "--result=" + filepath.Join(directory, "result.json")}, dependencies{
		runner: runner,
		qualify: func(context.Context, string, string, Plan, []ResourceRecord, time.Duration, time.Duration) (qualification.Result, error) {
			return successfulQualificationResult(), nil
		},
		random: bytes.NewReader(make([]byte, 8)),
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	journal, err := (fileJournal{path: journalPath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range journal.Plan.Services {
		if service.DatacenterID != "dc-id" || service.ServiceClassID == "" || service.BrokerVersionID != qualificationRelease {
			t.Fatalf("unresolved journal service: %#v", service)
		}
	}
}

func TestCleanupUsesOnlyJournaledIDsAndRetainsFailures(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	envPath := filepath.Join(directory, ".env")
	journalPath := filepath.Join(directory, "journal.json")
	if err := os.WriteFile(envPath, []byte(defaultJWTVariable+"=jwt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	plan, err := newPlan(time.Date(2026, 9, 25, 1, 2, 3, 0, time.UTC), bytes.NewReader(make([]byte, 8)))
	if err != nil {
		t.Fatal(err)
	}
	store := fileJournal{path: journalPath}
	journal := Journal{Version: journalVersion, Plan: plan, Resources: []ResourceRecord{
		{Role: "broker-0", ID: "exact-0"},
		{Role: "broker-a", ID: "exact-a"},
	}}
	if err := store.Initialize(journal); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{cleanupErr: errors.New("still busy")}
	code := execute(context.Background(), []string{"--cleanup", "--env=" + envPath, "--journal=" + journalPath, "--result=" + filepath.Join(directory, "result.json")}, dependencies{runner: runner})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if len(runner.cleanupCalls) != 1 || !reflect.DeepEqual(runner.cleanupCalls[0].Resources, journal.Resources) {
		t.Fatalf("cleanup journal = %#v", runner.cleanupCalls)
	}
	retained, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retained.Resources, journal.Resources) {
		t.Fatalf("retained resources = %#v", retained.Resources)
	}
	info, err := os.Stat(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("journal permissions = %04o, want 0600", info.Mode().Perm())
	}
}

func TestRunWritesRedactedEvidenceAfterCleanupFailure(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	envPath := filepath.Join(directory, ".env")
	journalPath := filepath.Join(directory, "journal.json")
	resultPath := filepath.Join(directory, "share", "qualification.json")
	const secret = "secret-token-and-endpoint"
	if err := os.WriteFile(envPath, []byte(defaultJWTVariable+"="+secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{cleanupErr: errors.New("cleanup failed at https://private.invalid/id-service")}
	code := execute(context.Background(), []string{"--run", "--env=" + envPath, "--journal=" + journalPath, "--result=" + resultPath}, dependencies{
		runner: runner,
		qualify: func(context.Context, string, string, Plan, []ResourceRecord, time.Duration, time.Duration) (qualification.Result, error) {
			result := successfulQualificationResult()
			result.Scenarios[0].Status = "FAIL"
			result.Scenarios[0].Reason = "private endpoint https://private.invalid with " + secret
			return result, errors.New("raw failure " + secret)
		},
		random: bytes.NewReader(make([]byte, 8)),
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	contents, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(contents)
	for _, forbidden := range []string{secret, "https://private.invalid", "id-broker", "journal"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("evidence contains forbidden value %q: %s", forbidden, text)
		}
	}
	for _, required := range []string{evidenceSchema, `"cloud_services": "incomplete"`, `"queues": "complete"`, `"overall_status": "FAIL"`, `"reason_class": "scenario failed"`} {
		if !strings.Contains(text, required) {
			t.Fatalf("evidence does not contain %q: %s", required, text)
		}
	}
}

func TestWriteQualificationEvidenceAtomicallyReplacesExistingFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "qualification.json")
	if err := os.WriteFile(path, []byte("previous evidence"), 0o644); err != nil {
		t.Fatal(err)
	}
	evidence := newQualificationEvidence(time.Unix(1, 0), successfulQualificationResult(), "complete", nil)
	if err := writeQualificationEvidence(path, evidence); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "previous evidence") || !strings.Contains(string(contents), evidenceSchema) {
		t.Fatalf("result was not atomically replaced: %s", contents)
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".qualification-result-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files remain: %v", matches)
	}
}

func successfulQualificationResult() qualification.Result {
	return qualification.Result{
		StartedAt: time.Date(2026, 9, 25, 12, 35, 0, 0, time.UTC),
		Duration:  time.Second,
		Cleanup:   "complete",
		Scenarios: []qualification.ScenarioResult{{Name: qualification.ScenarioStatic, Status: "PASS"}},
	}
}

type recordingRunner struct {
	preflightJWT   string
	preflightPlan  func(Plan) Plan
	provisionCalls int
	provisionErr   error
	cleanupCalls   []Journal
	cleanupErr     error
}

func (runner *recordingRunner) Preflight(_ context.Context, jwt, _ string, plan Plan) (Plan, error) {
	runner.preflightJWT = jwt
	if runner.preflightPlan != nil {
		plan = runner.preflightPlan(plan)
	}
	return plan, plan.validate()
}

func (runner *recordingRunner) Provision(_ context.Context, _, _ string, plan Plan, _ string) ([]ResourceRecord, error) {
	runner.provisionCalls++
	resources := make([]ResourceRecord, 0, len(plan.Services))
	for _, service := range plan.Services {
		resources = append(resources, ResourceRecord{Role: service.Role, ID: "id-" + service.Role})
	}
	return resources, runner.provisionErr
}

func (runner *recordingRunner) Cleanup(_ context.Context, _, _ string, journal Journal, _ string) error {
	runner.cleanupCalls = append(runner.cleanupCalls, journal)
	return runner.cleanupErr
}
