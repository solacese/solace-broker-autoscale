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
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
)

func TestHelpSucceedsWithoutRequiredArguments(t *testing.T) {
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{"-help"}, strings.NewReader(""), &bytes.Buffer{}, &stderr, nil, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "Usage: publisher -config FILE -participant ID") || !strings.Contains(stderr.String(), "payload_base64") {
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
		err := run(context.Background(), []string{"-config", writeConfig(t)}, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "-participant is required") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestValidateOnlySkipsCredentialsAndAssembly(t *testing.T) {
	path := writeConfig(t)
	lookedUp := false
	assembled := false
	var stdout bytes.Buffer
	err := run(context.Background(), []string{"-config", path, "-participant", "flight-publisher-1", "-validate-only"}, strings.NewReader(""), &stdout, &bytes.Buffer{}, func(string) (string, bool) {
		lookedUp = true
		return "", false
	}, func(context.Context, config.Config, swlbruntime.Credentials, string, customer.CustomerLibrary) (process, error) {
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

func TestPublisherRejectsSubscriberIdentity(t *testing.T) {
	path := writeConfig(t)
	err := run(context.Background(), []string{"-config", path, "-participant", "flight-subscriber-1", "-validate-only"}, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "not declared as publisher") {
		t.Fatalf("error = %v", err)
	}
}

func TestPublisherEOFStopsRuntime(t *testing.T) {
	path := writeConfig(t)
	values := participantEnvironment()
	fake := &fakeProcess{started: make(chan struct{}), stopped: make(chan struct{})}
	err := run(context.Background(), []string{"-config", path, "-participant", "flight-publisher-1"}, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}, func(context.Context, config.Config, swlbruntime.Credentials, string, customer.CustomerLibrary) (process, error) {
		return fake, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-fake.started:
	default:
		t.Fatal("runtime was not started")
	}
	select {
	case <-fake.stopped:
	default:
		t.Fatal("runtime was not cancelled on stdin EOF")
	}
}

type fakeProcess struct {
	started chan struct{}
	stopped chan struct{}
}

func (p *fakeProcess) Run(ctx context.Context) error {
	close(p.started)
	<-ctx.Done()
	close(p.stopped)
	return ctx.Err()
}
func (*fakeProcess) Accept(customer.MessageView) (shimPublisher.Receipt, error) {
	return shimPublisher.Receipt{}, nil
}

func writeConfig(t *testing.T) string {
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

func participantEnvironment() map[string]string {
	return map[string]string{
		"SOLACE_FLIGHT_PUBLISHER_CONTROL_USERNAME": "flight-publisher-1", "SOLACE_FLIGHT_PUBLISHER_CONTROL_PASSWORD": "publisher-password",
		"SOLACE_DATA_USERNAME": "data-user", "SOLACE_DATA_PASSWORD": "data-password",
	}
}
