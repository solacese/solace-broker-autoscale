package participantio

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
)

func TestRunPublisherPreservesPayloadAndReturnsDurableReceipt(t *testing.T) {
	payload := []byte{0, 1, 2, 255}
	input := `{"event_id":"event-1","topic":"airline/test","headers":{"scaling-group":"flight-operations"},"payload_base64":"` + base64.StdEncoding.EncodeToString(payload) + `"}` + "\n"
	fake := &fakePublisher{receipt: shimPublisher.Receipt{EventID: "event-1", State: outbox.StateReady, Group: "flight-operations", Hash: strings.Repeat("a", 64), Epoch: 2, Broker: "broker-a"}}
	var output bytes.Buffer
	if err := RunPublisher(context.Background(), strings.NewReader(input), &output, fake); err != nil {
		t.Fatal(err)
	}
	got, ok := fake.message.Payload.([]byte)
	if !ok || !bytes.Equal(got, payload) {
		t.Fatalf("payload = %#v", fake.message.Payload)
	}
	var receipt PublisherReceipt
	if err := json.Unmarshal(output.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.DurablyAccepted || receipt.EventID != "event-1" || receipt.Broker != "broker-a" {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func TestRunPublisherRejectsUnknownFields(t *testing.T) {
	err := RunPublisher(context.Background(), strings.NewReader(`{"event_id":"e","topic":"t","headers":{},"payload_base64":"","secret":"x"}`+"\n"), ioDiscard{}, &fakePublisher{})
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error = %v", err)
	}
}

func TestSubscriberBridgeWaitsForApplicationAck(t *testing.T) {
	inputReader, inputWriter := ioPipe(t)
	outputReader, outputWriter := ioPipe(t)
	bridge, err := NewSubscriberBridge(inputReader, outputWriter)
	if err != nil {
		t.Fatal(err)
	}
	result := handleSubscriberMessage(bridge, "event-1")
	delivery := readSubscriberDelivery(t, outputReader)
	if delivery.PayloadBase64 != base64.StdEncoding.EncodeToString([]byte{0, 255}) {
		t.Fatalf("payload = %q", delivery.PayloadBase64)
	}
	assertSubscriberPending(t, result)
	writeSubscriberOutcome(t, inputWriter, SubscriberOutcome{DeliveryID: delivery.DeliveryID, Outcome: SubscriberAck})
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestSubscriberBridgeRetryRepresentsWithNewDeliveryID(t *testing.T) {
	inputReader, inputWriter := ioPipe(t)
	outputReader, outputWriter := ioPipe(t)
	bridge, err := NewSubscriberBridge(inputReader, outputWriter)
	if err != nil {
		t.Fatal(err)
	}

	firstResult := handleSubscriberMessage(bridge, "event-1")
	first := readSubscriberDelivery(t, outputReader)
	writeSubscriberOutcome(t, inputWriter, SubscriberOutcome{DeliveryID: first.DeliveryID, Outcome: SubscriberRetry})
	assertSubscriberDirective(t, <-firstResult, SubscriberRetry)

	secondResult := handleSubscriberMessage(bridge, "event-1")
	second := readSubscriberDelivery(t, outputReader)
	if second.DeliveryID == first.DeliveryID {
		t.Fatalf("retry reused delivery_id %q", first.DeliveryID)
	}
	if second.EventID != first.EventID || second.Topic != first.Topic || second.PayloadBase64 != first.PayloadBase64 {
		t.Fatalf("re-presented delivery changed: first=%#v second=%#v", first, second)
	}
	writeSubscriberOutcome(t, inputWriter, SubscriberOutcome{DeliveryID: second.DeliveryID, Outcome: SubscriberAck})
	if err := <-secondResult; err != nil {
		t.Fatal(err)
	}
}

func TestSubscriberBridgeRejectAndReleaseDirectives(t *testing.T) {
	for _, outcome := range []string{SubscriberReject, SubscriberRelease} {
		t.Run(outcome, func(t *testing.T) {
			inputReader, inputWriter := ioPipe(t)
			outputReader, outputWriter := ioPipe(t)
			bridge, err := NewSubscriberBridge(inputReader, outputWriter)
			if err != nil {
				t.Fatal(err)
			}
			result := handleSubscriberMessage(bridge, "event-"+outcome)
			delivery := readSubscriberDelivery(t, outputReader)
			writeSubscriberOutcome(t, inputWriter, SubscriberOutcome{DeliveryID: delivery.DeliveryID, Outcome: outcome})
			assertSubscriberDirective(t, <-result, outcome)
		})
	}
}

func TestSubscriberBridgeInvalidResponsesDoNotCorruptPendingDelivery(t *testing.T) {
	for _, test := range []struct {
		name   string
		record func(string) string
	}{
		{name: "invalid JSON", record: func(string) string { return `{"delivery_id":` }},
		{name: "unknown field", record: func(id string) string {
			return fmt.Sprintf(`{"delivery_id":%q,"outcome":"ack","extra":true}`, id)
		}},
		{name: "unknown outcome", record: func(id string) string {
			return fmt.Sprintf(`{"delivery_id":%q,"outcome":"ACK"}`, id)
		}},
		{name: "duplicate field", record: func(id string) string {
			return fmt.Sprintf(`{"delivery_id":%q,"delivery_id":%q,"outcome":"ack"}`, id, id)
		}},
		{name: "trailing value", record: func(id string) string {
			return fmt.Sprintf(`{"delivery_id":%q,"outcome":"ack"} {}`, id)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			inputReader, inputWriter := ioPipe(t)
			outputReader, outputWriter := ioPipe(t)
			bridge, err := NewSubscriberBridge(inputReader, outputWriter)
			if err != nil {
				t.Fatal(err)
			}
			result := handleSubscriberMessage(bridge, "event-1")
			delivery := readSubscriberDelivery(t, outputReader)
			if _, err := fmt.Fprintln(inputWriter, test.record(delivery.DeliveryID)); err != nil {
				t.Fatal(err)
			}
			assertSubscriberPending(t, result)
			writeSubscriberOutcome(t, inputWriter, SubscriberOutcome{DeliveryID: delivery.DeliveryID, Outcome: SubscriberAck})
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSubscriberBridgeUnknownAndDuplicateResponsesDoNotCorruptPendingDelivery(t *testing.T) {
	inputReader, inputWriter := ioPipe(t)
	outputReader, outputWriter := ioPipe(t)
	bridge, err := NewSubscriberBridge(inputReader, outputWriter)
	if err != nil {
		t.Fatal(err)
	}

	firstResult := handleSubscriberMessage(bridge, "event-1")
	first := readSubscriberDelivery(t, outputReader)
	writeSubscriberOutcome(t, inputWriter, SubscriberOutcome{DeliveryID: "delivery-unknown", Outcome: SubscriberAck})
	assertSubscriberPending(t, firstResult)
	writeSubscriberOutcome(t, inputWriter, SubscriberOutcome{DeliveryID: first.DeliveryID, Outcome: SubscriberAck})
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}

	secondResult := handleSubscriberMessage(bridge, "event-2")
	second := readSubscriberDelivery(t, outputReader)
	writeSubscriberOutcome(t, inputWriter, SubscriberOutcome{DeliveryID: first.DeliveryID, Outcome: SubscriberRelease})
	assertSubscriberPending(t, secondResult)
	writeSubscriberOutcome(t, inputWriter, SubscriberOutcome{DeliveryID: second.DeliveryID, Outcome: SubscriberAck})
	if err := <-secondResult; err != nil {
		t.Fatal(err)
	}
}

func handleSubscriberMessage(bridge *SubscriberBridge, eventID string) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- bridge.Handle(context.Background(), customer.MessageView{
			EventID: eventID, Topic: "topic/1", Headers: map[string]string{"x": "y"}, Payload: []byte{0, 255},
		})
	}()
	return result
}

func readSubscriberDelivery(t *testing.T, reader io.Reader) SubscriberDelivery {
	t.Helper()
	var delivery SubscriberDelivery
	if err := json.NewDecoder(reader).Decode(&delivery); err != nil {
		t.Fatal(err)
	}
	return delivery
}

func writeSubscriberOutcome(t *testing.T, writer io.Writer, outcome SubscriberOutcome) {
	t.Helper()
	if err := json.NewEncoder(writer).Encode(outcome); err != nil {
		t.Fatal(err)
	}
}

func assertSubscriberPending(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("subscriber handler completed while response remained pending: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

func assertSubscriberDirective(t *testing.T, err error, want string) {
	t.Helper()
	var directive interface{ SubscriberSettlement() string }
	if !errors.As(err, &directive) || directive.SubscriberSettlement() != want {
		t.Fatalf("directive = %v, want %q", err, want)
	}
}

type fakePublisher struct {
	message customer.MessageView
	receipt shimPublisher.Receipt
	err     error
}

func (f *fakePublisher) Accept(message customer.MessageView) (shimPublisher.Receipt, error) {
	f.message = message
	return f.receipt, f.err
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

func ioPipe(t *testing.T) (*io.PipeReader, *io.PipeWriter) {
	t.Helper()
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.CloseWithError(errors.New("test complete"))
		_ = writer.Close()
	})
	return reader, writer
}
