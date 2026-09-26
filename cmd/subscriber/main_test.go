package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/customer"
	swlbruntime "github.com/solacese/solace-workload-balancer/runtime"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

func TestHelpSucceedsWithoutRequiredArguments(t *testing.T) {
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{"-help"}, strings.NewReader(""), &bytes.Buffer{}, &stderr, nil, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "Usage: subscriber -config FILE -participant ID") || !strings.Contains(stderr.String(), `"outcome":"ack|retry|reject|release"`) {
		t.Fatalf("help = %q", stderr.String())
	}
}

func TestRequiredArguments(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		err := run(context.Background(), nil, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "-config is required") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("participant", func(t *testing.T) {
		err := run(context.Background(), []string{"-config", writeSubscriberConfig(t)}, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "-participant is required") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestValidateOnlySkipsCredentialsAndAssembly(t *testing.T) {
	path := writeSubscriberConfig(t)
	lookedUp := false
	assembled := false
	var stdout bytes.Buffer
	err := run(context.Background(), []string{"-config", path, "-participant", "flight-subscriber-1", "-validate-only"}, strings.NewReader(""), &stdout, &bytes.Buffer{}, func(string) (string, bool) {
		lookedUp = true
		return "", false
	}, func(context.Context, config.Config, swlbruntime.Credentials, string, customer.CustomerLibrary, shimSubscriber.Handler) (process, error) {
		assembled = true
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if lookedUp || assembled {
		t.Fatalf("lookedUp=%t assembled=%t", lookedUp, assembled)
	}
	if !strings.Contains(stdout.String(), "configuration valid") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestSubscriberRejectsPublisherIdentity(t *testing.T) {
	path := writeSubscriberConfig(t)
	err := run(context.Background(), []string{"-config", path, "-participant", "flight-publisher-1", "-validate-only"}, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "not declared as subscriber") {
		t.Fatalf("error = %v", err)
	}
}

func writeSubscriberConfig(t *testing.T) string {
	t.Helper()
	root := filepath.Clean(filepath.Join("..", ".."))
	data, err := os.ReadFile(filepath.Join(root, "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
