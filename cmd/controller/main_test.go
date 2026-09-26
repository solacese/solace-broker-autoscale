package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/solacese/solace-workload-balancer/config"
	swlbruntime "github.com/solacese/solace-workload-balancer/runtime"
)

type fakeProcess struct{ ran bool }

func (p *fakeProcess) Run(context.Context) error { p.ran = true; return nil }

func writeConfig(t *testing.T) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestHelpSucceedsWithoutConfiguration(t *testing.T) {
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{"-help"}, &bytes.Buffer{}, &stderr, nil, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "Usage: controller -config FILE") {
		t.Fatalf("help = %q", stderr.String())
	}
}

func TestConfigIsRequired(t *testing.T) {
	err := run(context.Background(), nil, &bytes.Buffer{}, &bytes.Buffer{}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "-config is required") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateOnlyDoesNotAssembleOrResolveCredentials(t *testing.T) {
	var stdout, stderr bytes.Buffer
	assembled := false
	err := run(context.Background(), []string{"-config", writeConfig(t), "-validate-only"}, &stdout, &stderr,
		func(string) (string, bool) { t.Fatal("credential lookup called"); return "", false },
		func(context.Context, config.Config, swlbruntime.Credentials, *slog.Logger) (process, error) {
			assembled = true
			return nil, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if assembled {
		t.Fatal("validate-only assembled production runtime")
	}
	if !strings.Contains(stdout.String(), "configuration valid") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestProductionNeverFallsBackToDryRun(t *testing.T) {
	var stdout, stderr bytes.Buffer
	values := map[string]string{
		"SOLACE_CONTROL_USERNAME": "workload-controller-1", "SOLACE_CONTROL_PASSWORD": "p",
		"SOLACE_CONTROL_SEMP_USERNAME": "su", "SOLACE_CONTROL_SEMP_PASSWORD": "sp",
		"SOLACE_DATA_USERNAME": "du", "SOLACE_DATA_PASSWORD": "dp",
		"SOLACE_DATA_SEMP_USERNAME": "dsu", "SOLACE_DATA_SEMP_PASSWORD": "dsp",
	}
	want := errors.New("native unavailable")
	err := run(context.Background(), []string{"-config", writeConfig(t)}, &stdout, &stderr,
		func(name string) (string, bool) { value, ok := values[name]; return value, ok },
		func(context.Context, config.Config, swlbruntime.Credentials, *slog.Logger) (process, error) {
			return nil, want
		})
	if !errors.Is(err, want) {
		t.Fatalf("run() error = %v, want %v", err, want)
	}
	if strings.Contains(strings.ToLower(stdout.String()+stderr.String()), "dry-run") {
		t.Fatalf("production output claims dry-run: %q", stdout.String()+stderr.String())
	}
}

func TestProductionStartsAssembledProcess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	values := map[string]string{
		"SOLACE_CONTROL_USERNAME": "workload-controller-1", "SOLACE_CONTROL_PASSWORD": "p",
		"SOLACE_CONTROL_SEMP_USERNAME": "su", "SOLACE_CONTROL_SEMP_PASSWORD": "sp",
		"SOLACE_DATA_USERNAME": "du", "SOLACE_DATA_PASSWORD": "dp",
		"SOLACE_DATA_SEMP_USERNAME": "dsu", "SOLACE_DATA_SEMP_PASSWORD": "dsp",
	}
	instance := &fakeProcess{}
	err := run(context.Background(), []string{"-config", writeConfig(t)}, &stdout, &stderr,
		func(name string) (string, bool) { value, ok := values[name]; return value, ok },
		func(context.Context, config.Config, swlbruntime.Credentials, *slog.Logger) (process, error) {
			return instance, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if !instance.ran {
		t.Fatal("assembled runtime was not started")
	}
}
