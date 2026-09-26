package participantio

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/solacese/solace-workload-balancer/customer"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

const MaxRecordBytes = 1 << 20

const (
	SubscriberAck     = "ack"
	SubscriberRetry   = "retry"
	SubscriberReject  = "reject"
	SubscriberRelease = "release"
)

type Publisher interface {
	Accept(customer.MessageView) (shimPublisher.Receipt, error)
}

type PublisherInput struct {
	EventID       string            `json:"event_id"`
	Topic         string            `json:"topic"`
	Headers       map[string]string `json:"headers"`
	PayloadBase64 string            `json:"payload_base64"`
}

type PublisherReceipt struct {
	EventID         string `json:"event_id"`
	DurablyAccepted bool   `json:"durably_accepted"`
	State           string `json:"state"`
	Group           string `json:"group"`
	Hash            string `json:"hash"`
	Epoch           uint64 `json:"epoch,omitempty"`
	Broker          string `json:"broker,omitempty"`
}

func RunPublisher(ctx context.Context, input io.Reader, output io.Writer, publisher Publisher) error {
	if input == nil || output == nil || publisher == nil {
		return errors.New("participantio: publisher input, output, and adapter are required")
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), MaxRecordBytes)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var record PublisherInput
		if err := decodeStrict(scanner.Bytes(), &record); err != nil {
			return fmt.Errorf("participantio: decode publisher input: %w", err)
		}
		payload, err := base64.StdEncoding.Strict().DecodeString(record.PayloadBase64)
		if err != nil {
			return fmt.Errorf("participantio: decode payload for %q: %w", record.EventID, err)
		}
		receipt, err := publisher.Accept(customer.MessageView{EventID: record.EventID, Topic: record.Topic, Headers: record.Headers, Payload: payload})
		if err != nil {
			return err
		}
		if err := encoder.Encode(PublisherReceipt{EventID: receipt.EventID, DurablyAccepted: true, State: string(receipt.State), Group: receipt.Group, Hash: receipt.Hash, Epoch: receipt.Epoch, Broker: receipt.Broker}); err != nil {
			return fmt.Errorf("participantio: write publisher receipt: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("participantio: read publisher input: %w", err)
	}
	return nil
}

type SubscriberDelivery struct {
	DeliveryID    string            `json:"delivery_id"`
	EventID       string            `json:"event_id"`
	Topic         string            `json:"topic"`
	Headers       map[string]string `json:"headers,omitempty"`
	PayloadBase64 string            `json:"payload_base64"`
}

// SubscriberOutcome is one application response. Outcome is case-sensitive:
// ack accepts the native delivery, retry re-presents the retained delivery, and
// reject or release terminally rejects it before unblocking its ordering lane.
type SubscriberOutcome struct {
	DeliveryID string `json:"delivery_id"`
	Outcome    string `json:"outcome"`
}

// SubscriberDirectiveError crosses the application-handler boundary without
// settling the native message. The runtime participant adapter acts on it only
// after the shim has retained and blocked the delivery's business-key lane.
type SubscriberDirectiveError struct {
	outcome string
}

func (e *SubscriberDirectiveError) Error() string {
	return fmt.Sprintf("participantio: application requested subscriber outcome %q", e.outcome)
}

func (e *SubscriberDirectiveError) SubscriberSettlement() string { return e.outcome }

type subscriberOutcomeResult struct {
	outcome SubscriberOutcome
	err     error
}

// SubscriberBridge writes byte-exact deliveries and routes each strict NDJSON
// response to only its matching handler. Invalid, stale, and duplicate responses
// are rejected by ignoring them; they cannot fail an unrelated delivery, and the
// application may send a corrected response for a still-pending delivery ID.
type SubscriberBridge struct {
	output  io.Writer
	writeMu sync.Mutex
	next    atomic.Uint64

	pendingMu sync.Mutex
	pending   map[string]chan subscriberOutcomeResult
	terminal  error
}

func NewSubscriberBridge(input io.Reader, output io.Writer) (*SubscriberBridge, error) {
	if input == nil || output == nil {
		return nil, errors.New("participantio: subscriber input and output are required")
	}
	bridge := &SubscriberBridge{output: output, pending: make(map[string]chan subscriberOutcomeResult)}
	go bridge.readOutcomes(input)
	return bridge, nil
}

func (b *SubscriberBridge) readOutcomes(input io.Reader) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), MaxRecordBytes)
	for scanner.Scan() {
		outcome, err := parseSubscriberOutcome(scanner.Bytes())
		if err != nil {
			continue
		}
		b.pendingMu.Lock()
		destination := b.pending[outcome.DeliveryID]
		if destination != nil {
			delete(b.pending, outcome.DeliveryID)
		}
		b.pendingMu.Unlock()
		if destination != nil {
			destination <- subscriberOutcomeResult{outcome: outcome}
		}
	}
	terminal := scanner.Err()
	if terminal == nil {
		terminal = io.EOF
	}
	terminal = fmt.Errorf("participantio: read subscriber outcome: %w", terminal)

	b.pendingMu.Lock()
	b.terminal = terminal
	pending := b.pending
	b.pending = make(map[string]chan subscriberOutcomeResult)
	b.pendingMu.Unlock()
	for _, destination := range pending {
		destination <- subscriberOutcomeResult{err: terminal}
	}
}

func (b *SubscriberBridge) Handle(ctx context.Context, message customer.MessageView) error {
	payload, payloadOK := message.Payload.([]byte)
	if !payloadOK {
		return errors.New("participantio: subscriber payload is not bytes")
	}
	deliveryID := fmt.Sprintf("delivery-%d", b.next.Add(1))
	resultChannel := make(chan subscriberOutcomeResult, 1)
	b.pendingMu.Lock()
	if b.terminal != nil {
		err := b.terminal
		b.pendingMu.Unlock()
		return err
	}
	b.pending[deliveryID] = resultChannel
	b.pendingMu.Unlock()

	b.writeMu.Lock()
	err := json.NewEncoder(b.output).Encode(SubscriberDelivery{
		DeliveryID: deliveryID, EventID: message.EventID, Topic: message.Topic,
		Headers: message.Headers, PayloadBase64: base64.StdEncoding.EncodeToString(payload),
	})
	b.writeMu.Unlock()
	if err != nil {
		b.removePending(deliveryID, resultChannel)
		return fmt.Errorf("participantio: write subscriber delivery: %w", err)
	}

	select {
	case <-ctx.Done():
		b.removePending(deliveryID, resultChannel)
		return ctx.Err()
	case result := <-resultChannel:
		if result.err != nil {
			return result.err
		}
		switch result.outcome.Outcome {
		case SubscriberAck:
			return nil
		case SubscriberRetry, SubscriberReject, SubscriberRelease:
			return &SubscriberDirectiveError{outcome: result.outcome.Outcome}
		default:
			panic("participantio: validated subscriber outcome escaped parser")
		}
	}
}

func (b *SubscriberBridge) removePending(deliveryID string, destination chan subscriberOutcomeResult) {
	b.pendingMu.Lock()
	if b.pending[deliveryID] == destination {
		delete(b.pending, deliveryID)
	}
	b.pendingMu.Unlock()
}

func (b *SubscriberBridge) Handler() shimSubscriber.Handler {
	return shimSubscriber.HandlerFunc(b.Handle)
}

func parseSubscriberOutcome(data []byte) (SubscriberOutcome, error) {
	var outcome SubscriberOutcome
	if err := decodeStrict(data, &outcome); err != nil {
		return SubscriberOutcome{}, err
	}
	if outcome.DeliveryID == "" {
		return SubscriberOutcome{}, errors.New("delivery_id is required")
	}
	switch outcome.Outcome {
	case SubscriberAck, SubscriberRetry, SubscriberReject, SubscriberRelease:
		return outcome, nil
	default:
		return SubscriberOutcome{}, fmt.Errorf("unsupported subscriber outcome %q", outcome.Outcome)
	}
}

func decodeStrict(data []byte, target any) error {
	if err := rejectDuplicateFields(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func rejectDuplicateFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate field %q", key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return errors.New("malformed object")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return errors.New("malformed array")
		}
	default:
		return errors.New("unexpected closing delimiter")
	}
	return nil
}
