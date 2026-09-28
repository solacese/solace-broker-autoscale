// Command sdkperf-adapter is an optional test-environment bridge between the
// official JCSMP SDKPerf tool and the native Go publisher/subscriber processes.
// It is not built by the default Makefile and is not part of production startup.
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/solacese/solace-workload-balancer/integration"
)

type options struct {
	endpoint, usernameEnv, passwordEnv string
	ingressQueue, resultTopic          string
	group, runID, applicationID        string
	entities                           int
}

type bridgeInput struct {
	Kind          string `json:"kind"`
	DeliveryID    string `json:"delivery_id,omitempty"`
	EventID       string `json:"event_id"`
	PayloadBase64 string `json:"payload_base64,omitempty"`
	Error         string `json:"error,omitempty"`
}

type bridgeOutput struct {
	Kind       string          `json:"kind"`
	Publish    *publisherInput `json:"publish,omitempty"`
	DeliveryID string          `json:"delivery_id,omitempty"`
	EventID    string          `json:"event_id,omitempty"`
	Error      string          `json:"error,omitempty"`
}

type publisherInput struct {
	EventID       string            `json:"event_id"`
	Topic         string            `json:"topic"`
	Headers       map[string]string `json:"headers"`
	PayloadBase64 string            `json:"payload_base64"`
}

type bridge struct {
	options  options
	conn     *integration.AMQPConnection
	receiver *amqp.Receiver
	stdout   *json.Encoder

	writeMu sync.Mutex
	waitMu  sync.Mutex
	waiters map[string]chan error
	seq     atomic.Uint64
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("sdkperf-adapter", flag.ContinueOnError)
	var o options
	flags.StringVar(&o.endpoint, "endpoint", "", "Broker A AMQPS endpoint")
	flags.StringVar(&o.usernameEnv, "username-env", "SWLB_SDKPERF_USERNAME", "environment variable containing the test username")
	flags.StringVar(&o.passwordEnv, "password-env", "SWLB_SDKPERF_PASSWORD", "environment variable containing the test password")
	flags.StringVar(&o.ingressQueue, "ingress-queue", "", "pre-provisioned SDKPerf ingress queue")
	flags.StringVar(&o.resultTopic, "result-topic", "", "SDKPerf result topic")
	flags.StringVar(&o.group, "group", "events-a", "target scaling group")
	flags.StringVar(&o.runID, "run-id", "", "bounded test run identifier")
	flags.StringVar(&o.applicationID, "application-id", "swlb-sdkperf-adapter", "AMQP container ID")
	flags.IntVar(&o.entities, "entities", 100, "number of deterministic entity IDs")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if o.endpoint == "" || o.ingressQueue == "" || o.resultTopic == "" || o.runID == "" || o.entities < 1 || o.entities > 10000 {
		return errors.New("sdkperf-adapter: endpoint, ingress queue, result topic, run ID, and 1..10000 entities are required")
	}
	username, usernameOK := os.LookupEnv(o.usernameEnv)
	password, passwordOK := os.LookupEnv(o.passwordEnv)
	if !usernameOK || !passwordOK || username == "" {
		return errors.New("sdkperf-adapter: configured credential environment variables are unavailable")
	}
	conn, err := integration.ConnectAMQP(ctx, o.endpoint, username, password, o.applicationID, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	receiver, err := conn.Session.NewReceiver(ctx, integration.QueueAddress(o.ingressQueue), &amqp.ReceiverOptions{Credit: 1})
	if err != nil {
		return fmt.Errorf("sdkperf-adapter: attach ingress queue: %w", err)
	}
	defer receiver.Close(ctx)
	b := &bridge{options: o, conn: conn, receiver: receiver, stdout: json.NewEncoder(os.Stdout), waiters: make(map[string]chan error)}
	inputErr := make(chan error, 1)
	go func() { inputErr <- b.readDashboard(ctx, bufio.NewScanner(os.Stdin)) }()
	for {
		select {
		case inputReadErr := <-inputErr:
			return inputReadErr
		default:
		}
		message, receiveErr := receiver.Receive(ctx, nil)
		if receiveErr != nil {
			select {
			case inputReadErr := <-inputErr:
				return errors.Join(receiveErr, inputReadErr)
			default:
				return receiveErr
			}
		}
		if err := b.forwardSource(ctx, message); err != nil {
			_ = receiver.ReleaseMessage(ctx, message)
			return err
		}
		if err := receiver.AcceptMessage(ctx, message); err != nil {
			return fmt.Errorf("sdkperf-adapter: settle ingress: %w", err)
		}
	}
}

func (b *bridge) forwardSource(ctx context.Context, message *amqp.Message) error {
	sequence := b.seq.Add(1)
	eventID := fmt.Sprintf("sdkperf-%s-%06d", b.options.runID, sequence)
	ack := make(chan error, 1)
	b.waitMu.Lock()
	b.waiters[eventID] = ack
	b.waitMu.Unlock()
	defer func() {
		b.waitMu.Lock()
		delete(b.waiters, eventID)
		b.waitMu.Unlock()
	}()
	payload := append([]byte(nil), message.GetData()...)
	record := publisherInput{
		EventID: eventID, Topic: "synthetic/sdkperf", PayloadBase64: base64.StdEncoding.EncodeToString(payload),
		Headers: map[string]string{
			"scaling-group": b.options.group,
			"entity_id":     fmt.Sprintf("entity-%04d", (sequence-1)%uint64(b.options.entities)),
			"event_type":    "sdkperf-test",
			"sequence":      strconv.FormatUint(sequence, 10),
			"timestamp":     time.Now().UTC().Format(time.RFC3339Nano),
		},
	}
	b.writeMu.Lock()
	err := b.stdout.Encode(bridgeOutput{Kind: "publish", Publish: &record})
	b.writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("sdkperf-adapter: write publisher input: %w", err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ack:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("sdkperf-adapter: publisher receipt timed out for %q", eventID)
	}
}

func (b *bridge) readDashboard(ctx context.Context, scanner *bufio.Scanner) error {
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	for scanner.Scan() {
		var input bridgeInput
		if err := json.Unmarshal(scanner.Bytes(), &input); err != nil {
			return fmt.Errorf("sdkperf-adapter: decode dashboard input: %w", err)
		}
		switch input.Kind {
		case "receipt":
			b.waitMu.Lock()
			waiter := b.waiters[input.EventID]
			b.waitMu.Unlock()
			if waiter != nil {
				if input.Error != "" {
					waiter <- errors.New(input.Error)
				} else {
					waiter <- nil
				}
			}
		case "delivery":
			payload, err := base64.StdEncoding.DecodeString(input.PayloadBase64)
			if err == nil {
				sender, senderErr := b.conn.Session.NewSender(ctx, integration.TopicAddress(b.options.resultTopic+"/"+input.EventID), nil)
				if senderErr != nil {
					err = senderErr
				} else {
					out := amqp.NewMessage(payload)
					out.Header = &amqp.MessageHeader{Durable: true}
					out.Properties = &amqp.MessageProperties{MessageID: input.EventID}
					err = sender.Send(ctx, out, nil)
					closeErr := sender.Close(ctx)
					if err == nil {
						err = closeErr
					}
				}
			}
			result := bridgeOutput{Kind: "delivery-result", DeliveryID: input.DeliveryID, EventID: input.EventID}
			if err != nil {
				result.Error = err.Error()
			}
			b.writeMu.Lock()
			writeErr := b.stdout.Encode(result)
			b.writeMu.Unlock()
			if writeErr != nil {
				return fmt.Errorf("sdkperf-adapter: write delivery result: %w", writeErr)
			}
		default:
			return fmt.Errorf("sdkperf-adapter: unsupported dashboard input kind %q", input.Kind)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("sdkperf-adapter: dashboard input closed")
}
