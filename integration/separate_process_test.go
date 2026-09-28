//go:build liveintegration

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

// TestSeparateProcessSmoke launches this repository's actual commands. It
// never provisions resources. The supplied config must point at one existing
// Broker 0 and three existing data brokers, and an authorized external policy
// stimulus must cause EventsA A->AB->ABC->AB. The harness itself validates that
// durable controller membership records that exact sequence while EventsB's
// membership remains unchanged and exercises publisher/subscriber NDJSON.
func TestSeparateProcessSmoke(t *testing.T) {
	configPath := os.Getenv("SWLB_LIVE_CONFIG")
	if configPath == "" {
		t.Skip("set SWLB_LIVE_CONFIG for an authorized existing-broker run")
	}
	root, _ := filepath.Abs("..")
	bin := t.TempDir()
	for _, name := range []string{"controller", "publisher", "subscriber"} {
		command := exec.Command("go", "build", "-o", filepath.Join(bin, name), "./cmd/"+name)
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, output)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	controller := exec.CommandContext(ctx, filepath.Join(bin, "controller"), "-config", configPath)
	controller.Stdout = os.Stdout
	controller.Stderr = os.Stderr
	if err := controller.Start(); err != nil {
		t.Fatal(err)
	}
	controllerDone := make(chan struct{})
	var controllerErr error
	go func() { controllerErr = controller.Wait(); close(controllerDone) }()
	type child struct {
		command *exec.Cmd
		stdin   io.WriteCloser
		stdout  io.ReadCloser
		done    chan struct{}
		waitErr error
	}
	startChild := func(binary, participant string) *child {
		command := exec.CommandContext(ctx, filepath.Join(bin, binary), "-config", configPath, "-participant", participant)
		stdin, err := command.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		stdout, err := command.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		child := &child{command: command, stdin: stdin, stdout: stdout, done: done}
		go func() { child.waitErr = command.Wait(); close(done) }()
		return child
	}
	eventsAPublisher := startChild("publisher", "events-a-publisher-1")
	eventsBPublisher := startChild("publisher", "events-b-publisher-1")
	eventsASubscriber := startChild("subscriber", "events-a-subscriber-1")
	eventsBSubscriber := startChild("subscriber", "events-b-subscriber-1")
	children := []*child{eventsAPublisher, eventsBPublisher, eventsASubscriber, eventsBSubscriber}
	defer func() {
		cancel()
		for _, child := range children {
			_ = child.stdin.Close()
			select {
			case <-child.done:
			case <-time.After(5 * time.Second):
				_ = child.command.Process.Kill()
				<-child.done
			}
		}
		_ = controller.Process.Kill()
		select {
		case <-controllerDone:
		case <-time.After(5 * time.Second):
		}
	}()

	evidence := SmokeEvidence{Submitted: map[string][]string{"events-a": {}, "events-b": {}}, Receipts: map[string]SmokeReceipt{}}
	var mu sync.Mutex
	consumeDeliveries := func(group string, child *child) {
		go func() {
			scanner := bufio.NewScanner(child.stdout)
			encoder := json.NewEncoder(child.stdin)
			order := 0
			for scanner.Scan() {
				var delivery struct {
					DeliveryID string `json:"delivery_id"`
					EventID    string `json:"event_id"`
				}
				if err := json.Unmarshal(scanner.Bytes(), &delivery); err != nil || delivery.DeliveryID == "" || delivery.EventID == "" {
					continue
				}
				mu.Lock()
				evidence.Deliveries = append(evidence.Deliveries, SmokeDelivery{EventID: delivery.EventID, Group: group, Order: order})
				mu.Unlock()
				order++
				if err := encoder.Encode(map[string]string{"delivery_id": delivery.DeliveryID, "outcome": "ack"}); err != nil {
					return
				}
			}
		}()
	}
	consumeDeliveries("events-a", eventsASubscriber)
	consumeDeliveries("events-b", eventsBSubscriber)
	publish := func(group string, child *child, count int, headers map[string]string) {
		encoder := json.NewEncoder(child.stdin)
		scanner := bufio.NewScanner(child.stdout)
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("%s-live-%03d", group, i)
			record := map[string]any{"event_id": id, "topic": "synthetic/events", "headers": headers, "payload_base64": "e30="}
			if err := encoder.Encode(record); err != nil {
				t.Fatal(err)
			}
			if !scanner.Scan() {
				select {
				case err := <-child.done:
					t.Fatalf("%s publisher exited: %v", group, err)
				default:
					t.Fatalf("%s publisher produced no receipt", group)
				}
			}
			var receipt SmokeReceipt
			if err := json.Unmarshal(scanner.Bytes(), &receipt); err != nil {
				t.Fatal(err)
			}
			if receipt.EventID != id || !receipt.DurablyAccepted {
				t.Fatalf("invalid %s receipt %#v", group, receipt)
			}
			mu.Lock()
			evidence.Submitted[group] = append(evidence.Submitted[group], id)
			evidence.Receipts[id] = receipt
			mu.Unlock()
		}
	}
	publish("events-a", eventsAPublisher, 20, map[string]string{"scaling-group": "events-a", "entity_id": "UA", "eventsA-number": "123", "timestamp": "2026-09-25", "sequence": "ORD-LAX"})
	publish("events-b", eventsBPublisher, 20, map[string]string{"scaling-group": "events-b", "entity_id": "UA", "entity_id": "bag-1"})

	statePath := os.Getenv("SWLB_LIVE_CONTROLLER_STATE")
	if statePath == "" {
		t.Fatal("SWLB_LIVE_CONTROLLER_STATE is required")
	}
	a, b, c := os.Getenv("SWLB_LIVE_BROKER_A"), os.Getenv("SWLB_LIVE_BROKER_B"), os.Getenv("SWLB_LIVE_BROKER_C")
	if a == "" || b == "" || c == "" {
		t.Fatal("SWLB_LIVE_BROKER_A/B/C are required for exact membership verification")
	}
	eventsASequence := [][]string{{a}, {a, b}, {a, b, c}, {a, b}}
	deadline := time.Now().Add(15 * time.Minute)
	for time.Now().Before(deadline) {
		select {
		case <-controllerDone:
			t.Fatalf("controller exited during smoke: %v", controllerErr)
		default:
		}
		for _, child := range children {
			select {
			case <-child.done:
				t.Fatalf("participant exited during smoke: %v", child.waitErr)
			default:
			}
		}
		data, err := os.ReadFile(statePath)
		if err == nil {
			var state struct {
				Membership map[string]control.MembershipSnapshot `json:"membership"`
			}
			if json.Unmarshal(data, &state) == nil {
				mu.Lock()
				RecordActiveMembership(&evidence, state.Membership)
				report, verifyErr := VerifySmokeEvidence(evidence, eventsASequence, []string{a})
				mu.Unlock()
				if verifyErr == nil {
					t.Logf("at-least-once duplicate observations: %v", report.Duplicates)
					return
				}
			}
		}
		time.Sleep(time.Second)
	}
	mu.Lock()
	_, verifyErr := VerifySmokeEvidence(evidence, eventsASequence, []string{a})
	mu.Unlock()
	t.Fatalf("live smoke evidence incomplete: %v (authorized policy stimulus must produce exact A->AB->ABC->AB)", verifyErr)

}
