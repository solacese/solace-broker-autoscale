package dispatch_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
	"github.com/solacese/solace-broker-autoscale/shim/rules"
	"github.com/solacese/solace-broker-autoscale/shim/transport/memory"
)

// testPlan mirrors testdata/interop_spec.json so routing behavior is anchored to the shared spec.
func testPlan(t *testing.T) *rules.Plan {
	t.Helper()
	rs := []rules.Rule{
		{
			Name: "vip-orders",
			When: rules.Match{
				Topic:   "orders/>",
				Payload: []rules.Predicate{{Op: "in", Path: "priority", Value: []any{"high", "urgent"}}},
			},
			Route: rules.Route{Broker: "broker-vip", Key: "vip.{region}", Topic: "vip/{topic}"},
		},
		{
			Name: "large-orders",
			When: rules.Match{
				Topic:   "orders/*/created",
				Payload: []rules.Predicate{{Op: "gt", Path: "amount", Value: 1000}},
			},
			Route: rules.Route{Broker: "broker-big", Key: "big.{region}"},
		},
	}
	return rules.NewPlan(rs, "broker-bulk")
}

// uriMap resolves the three plan brokers to distinct in-memory URIs.
var uriMap = map[string]string{
	"broker-vip":  "amqp://vip",
	"broker-big":  "amqp://big",
	"broker-bulk": "amqp://bulk",
}

func staticResolver(m map[string]string) dispatch.BrokerResolver {
	return func(_ context.Context, broker string) (string, error) {
		uri, ok := m[broker]
		if !ok {
			return "", errors.New("unknown broker " + broker)
		}
		return uri, nil
	}
}

func TestPublishRoutesAndStampsKey(t *testing.T) {
	tport := memory.New()
	pub := dispatch.NewPublisher(testPlan(t), staticResolver(uriMap), tport, dispatch.RetryPolicy{})
	defer pub.Close()

	res, err := pub.Publish(context.Background(), "orders/eu/created",
		[]byte(`{"priority":"urgent","region":"eu","amount":10}`), map[string]string{"trace": "abc"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision.Broker != "broker-vip" || res.Decision.Rule != "vip-orders" {
		t.Fatalf("decision = %+v", res.Decision)
	}
	if res.Decision.Address != "vip/orders/eu/created" {
		t.Errorf("address = %q", res.Decision.Address)
	}
	if res.Decision.Key != "vip.eu" {
		t.Errorf("key = %q", res.Decision.Key)
	}
	if res.URI != "amqp://vip" {
		t.Errorf("uri = %q", res.URI)
	}

	got := tport.Delivered("amqp://vip")
	if len(got) != 1 {
		t.Fatalf("delivered %d messages to vip", len(got))
	}
	m := got[0]
	if m.GroupID != "vip.eu" {
		t.Errorf("group-id = %q", m.GroupID)
	}
	if m.Properties[dispatch.PartitionKeyProperty] != "vip.eu" {
		t.Errorf("%s = %q", dispatch.PartitionKeyProperty, m.Properties[dispatch.PartitionKeyProperty])
	}
	if m.Properties["trace"] != "abc" {
		t.Errorf("passthrough property lost: %v", m.Properties)
	}
	// nothing should have gone to the other brokers
	if n := len(tport.Delivered("amqp://big")); n != 0 {
		t.Errorf("big received %d", n)
	}
}

func TestPublishFallsThroughToDefault(t *testing.T) {
	tport := memory.New()
	pub := dispatch.NewPublisher(testPlan(t), staticResolver(uriMap), tport, dispatch.RetryPolicy{})
	defer pub.Close()

	// matches no rule: low priority, small amount
	res, err := pub.Publish(context.Background(), "orders/us/created",
		[]byte(`{"priority":"low","amount":5}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision.Broker != "broker-bulk" || res.Decision.Matched {
		t.Fatalf("expected default broker, unmatched; got %+v", res.Decision)
	}
	if res.Decision.Address != "orders/us/created" {
		t.Errorf("default address should be the original topic, got %q", res.Decision.Address)
	}
	if len(tport.Delivered("amqp://bulk")) != 1 {
		t.Error("default message not delivered to bulk")
	}
}

// countingTransport wraps memory transport to count how many senders it opens per URI.
type countingTransport struct {
	inner *memory.Transport
	opens map[string]int
}

func (c *countingTransport) Sender(ctx context.Context, uri string) (dispatch.Sender, error) {
	c.opens[uri]++
	return c.inner.Sender(ctx, uri)
}

func (c *countingTransport) Receiver(ctx context.Context, uri, source string) (dispatch.Receiver, error) {
	return c.inner.Receiver(ctx, uri, source)
}

func TestPublishReusesOneSenderPerBroker(t *testing.T) {
	ct := &countingTransport{inner: memory.New(), opens: map[string]int{}}
	pub := dispatch.NewPublisher(testPlan(t), staticResolver(uriMap), ct, dispatch.RetryPolicy{})
	defer pub.Close()

	for i := 0; i < 3; i++ {
		if _, err := pub.Publish(context.Background(), "orders/eu/created",
			[]byte(`{"priority":"high","region":"eu"}`), nil); err != nil {
			t.Fatal(err)
		}
	}
	if ct.opens["amqp://vip"] != 1 {
		t.Errorf("expected 1 sender open for vip, got %d", ct.opens["amqp://vip"])
	}
}

// flakySender fails the first n sends, then succeeds.
type flakySender struct {
	failuresLeft int
	sent         int
}

func (f *flakySender) Send(_ context.Context, _ dispatch.Message) error {
	if f.failuresLeft > 0 {
		f.failuresLeft--
		return errors.New("transient")
	}
	f.sent++
	return nil
}
func (f *flakySender) Close() error { return nil }

type oneSenderTransport struct{ s dispatch.Sender }

func (o oneSenderTransport) Sender(context.Context, string) (dispatch.Sender, error) {
	return o.s, nil
}
func (o oneSenderTransport) Receiver(context.Context, string, string) (dispatch.Receiver, error) {
	return nil, errors.New("no receiver")
}

func TestPublishRetriesTransientFailures(t *testing.T) {
	fs := &flakySender{failuresLeft: 2}
	pub := dispatch.NewPublisher(testPlan(t), staticResolver(uriMap), oneSenderTransport{fs},
		dispatch.RetryPolicy{MaxAttempts: 3, Backoff: 0})
	defer pub.Close()

	if _, err := pub.Publish(context.Background(), "orders/eu/created",
		[]byte(`{"priority":"high","region":"eu"}`), nil); err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if fs.sent != 1 {
		t.Errorf("expected exactly 1 successful send, got %d", fs.sent)
	}
}

func TestPublishGivesUpAfterMaxAttempts(t *testing.T) {
	fs := &flakySender{failuresLeft: 5}
	pub := dispatch.NewPublisher(testPlan(t), staticResolver(uriMap), oneSenderTransport{fs},
		dispatch.RetryPolicy{MaxAttempts: 2, Backoff: 0})
	defer pub.Close()

	if _, err := pub.Publish(context.Background(), "orders/eu/created",
		[]byte(`{"priority":"high","region":"eu"}`), nil); err == nil {
		t.Error("expected failure after exhausting attempts")
	}
}

func TestPublisherTargets(t *testing.T) {
	pub := dispatch.NewPublisher(testPlan(t), staticResolver(uriMap), memory.New(), dispatch.RetryPolicy{})
	got := pub.Targets()
	sort.Strings(got)
	want := []string{"broker-big", "broker-bulk", "broker-vip"}
	if len(got) != len(want) {
		t.Fatalf("targets = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("targets = %v, want %v", got, want)
		}
	}
}

func TestPublisherListenerRoundTrip(t *testing.T) {
	tport := memory.New()
	plan := testPlan(t)
	resolver := staticResolver(uriMap)

	lis := dispatch.NewListener(plan, resolver, tport)
	defer lis.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subscribe on ">" across every target broker before publishing.
	ch, err := lis.Subscribe(ctx, ">")
	if err != nil {
		t.Fatal(err)
	}

	pub := dispatch.NewPublisher(plan, resolver, tport, dispatch.RetryPolicy{})
	defer pub.Close()

	// vip message and a default (bulk) message land on different brokers.
	if _, err := pub.Publish(ctx, "orders/eu/created",
		[]byte(`{"priority":"urgent","region":"eu"}`), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.Publish(ctx, "orders/us/created",
		[]byte(`{"priority":"low"}`), nil); err != nil {
		t.Fatal(err)
	}

	got := map[string]dispatch.Delivery{}
	timeout := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case d := <-ch:
			got[d.Broker] = d
		case <-timeout:
			t.Fatalf("timed out; received %d of 2 deliveries: %+v", len(got), got)
		}
	}

	vip, ok := got["broker-vip"]
	if !ok {
		t.Fatal("no delivery fanned in from broker-vip")
	}
	if vip.GroupID != "vip.eu" {
		t.Errorf("recovered key = %q, want vip.eu", vip.GroupID)
	}
	if vip.Message.Address != "vip/orders/eu/created" {
		t.Errorf("vip address = %q", vip.Message.Address)
	}
	if _, ok := got["broker-bulk"]; !ok {
		t.Error("no delivery fanned in from broker-bulk")
	}
}
