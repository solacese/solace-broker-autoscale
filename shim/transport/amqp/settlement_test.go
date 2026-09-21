package amqp

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
)

// The fixture needs a durable q-go-settlement queue subscribed to autoscale/go/settlement.
func TestRealBrokerRedeliversUntilApplicationAcknowledges(t *testing.T) {
	uri := os.Getenv("SOLACE_GO_AMQP_URI")
	if uri == "" {
		t.Skip("set SOLACE_GO_AMQP_URI for real broker settlement test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	transport := New()
	sender, err := transport.Sender(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	receiver, err := transport.Receiver(ctx, uri, "queue://q-go-settlement")
	if err != nil {
		t.Fatal(err)
	}
	if err = sender.Send(ctx, dispatch.Message{Address: "topic://autoscale/go/settlement", Body: []byte("durable")}); err != nil {
		t.Fatal(err)
	}
	first, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Ack == nil || first.Release == nil {
		t.Fatal("missing application settlement")
	}
	receiver.Close() // Crash before committing the business operation: must not lose the message.
	receiver, err = transport.Receiver(ctx, uri, "queue://q-go-settlement")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	second, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(second.Body) != "durable" {
		t.Fatal("unacknowledged event missing")
	}
	if err = second.Release(ctx); err != nil {
		t.Fatal(err)
	}
	third, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = third.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	if err = third.Ack(ctx); err != nil {
		t.Fatal("ack is not idempotent", err)
	}
	emptyCtx, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	defer stop()
	if _, err = receiver.Receive(emptyCtx); err == nil {
		t.Fatal("acknowledged message redelivered")
	}
}

func TestRealReceiversShareConnectionAndCloseIndependently(t *testing.T) {
	uri := os.Getenv("SOLACE_GO_AMQP_URI")
	if uri == "" {
		t.Skip("set SOLACE_GO_AMQP_URI for real broker test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tr := New()
	a, err := tr.Receiver(ctx, uri, "queue://q-go-settlement")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := tr.Receiver(ctx, uri, "queue://q-go-settlement")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if a.(*receiver).conn != b.(*receiver).conn {
		t.Fatal("receiver connections were not pooled")
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	sender, err := tr.Sender(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err = sender.Send(ctx, dispatch.Message{Address: "topic://autoscale/go/settlement", Body: []byte("pooled")}); err != nil {
		t.Fatal(err)
	}
	msg, err := b.Receive(ctx)
	if err != nil {
		t.Fatal("closing one link broke the other", err)
	}
	if string(msg.Body) != "pooled" {
		t.Fatal(string(msg.Body))
	}
	if err = msg.Ack(ctx); err != nil {
		t.Fatal(err)
	}
}
