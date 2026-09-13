// Command shim is a small CLI for the smart shim: it lets you try the rule engine and run the whole
// data path offline, with no broker.
//
//	shim route --spec rules.json --topic orders/eu/created --payload '{"priority":"urgent","region":"eu"}'
//	    Load a portable rule spec and print where one message would be routed.
//
//	shim demo
//	    Run a self-contained publish/subscribe round trip over the in-memory transport, so you can
//	    watch the shim route messages to different brokers and recover partition keys with nothing
//	    installed.
//
//	shim demo --scale
//	    Watch an event-driven, ordering-first scale-up: a topology snapshot pushed over the spine
//	    moves a partition key to a new broker, held on its old broker until in-flight traffic drains
//	    before cutover, so per-key order survives the reassignment.
//
// The route command is the fast way to check that a rule spec does what you think before wiring it
// into an application. The demo is the fast way to see the shim behave end to end.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
	"github.com/solacese/solace-broker-autoscale/shim/rules"
	"github.com/solacese/solace-broker-autoscale/shim/spine"
	"github.com/solacese/solace-broker-autoscale/shim/topology"
	"github.com/solacese/solace-broker-autoscale/shim/transport/memory"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "route":
		err = runRoute(os.Args[2:])
	case "demo":
		err = runDemo(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `shim - smart shim CLI

Usage:
  shim route --spec FILE --topic TOPIC [--payload JSON]
      Print how one message routes under a portable rule spec.

  shim demo [--scale]
      Run an offline publish/subscribe round trip over the in-memory transport.
      With --scale, demonstrate an event-driven, ordering-first scale-up: a topology
      snapshot pushed over the spine moves a key, held on its old broker until it drains.

Run "shim route -h" or "shim demo -h" for command flags.
`)
}

func runRoute(args []string) error {
	fs := flag.NewFlagSet("route", flag.ContinueOnError)
	spec := fs.String("spec", "", "path to a portable rule spec (JSON)")
	topic := fs.String("topic", "", "message topic to route")
	payload := fs.String("payload", "{}", "message payload as JSON (default {})")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *spec == "" || *topic == "" {
		return fmt.Errorf("both --spec and --topic are required")
	}

	data, err := os.ReadFile(*spec)
	if err != nil {
		return fmt.Errorf("read spec: %w", err)
	}
	plan, err := rules.LoadSpec(data)
	if err != nil {
		return fmt.Errorf("load spec: %w", err)
	}
	dec, err := plan.Decide(*topic, []byte(*payload))
	if err != nil {
		return err
	}

	rule := dec.Rule
	if !dec.Matched {
		rule = "default (no rule matched)"
	}
	key := dec.Key
	if key == "" {
		key = "(none)"
	}
	fmt.Printf("topic    %s\n", *topic)
	fmt.Printf("rule     %s\n", rule)
	fmt.Printf("broker   %s\n", dec.Broker)
	fmt.Printf("address  %s\n", dec.Address)
	fmt.Printf("key      %s\n", key)
	return nil
}

// demoSpec is the same three-rule spec shipped in testdata/interop_spec.json, inlined so the demo
// runs with no files. It routes VIP orders to broker-vip, large orders to broker-big, big telemetry
// to broker-bulk, and everything else to the default broker-bulk.
const demoSpec = `{
  "version": 1,
  "default_broker": "broker-bulk",
  "rules": [
    {"name": "vip-orders",
     "when": {"topic": "orders/>", "payload": [{"op": "in", "path": "priority", "value": ["high", "urgent"]}]},
     "route": {"broker": "broker-vip", "key": "vip.{region}", "topic": "vip/{topic}"}},
    {"name": "large-orders",
     "when": {"topic": "orders/*/created", "payload": [{"op": "gt", "path": "amount", "value": 1000}]},
     "route": {"broker": "broker-big", "key": "big.{region}"}},
    {"name": "telemetry-raw",
     "when": {"topic": "telemetry/>", "payload": [{"op": "raw_size_gt", "value": 2048}]},
     "route": {"broker": "broker-bulk"}}
  ]
}`

type demoMsg struct {
	topic   string
	payload string
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	scale := fs.Bool("scale", false, "demonstrate an event-driven scale-up: a topology snapshot pushed over the spine moves a key, held on its old broker until it drains (ordering-first)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scale {
		return runScaleDemo()
	}

	plan, err := rules.LoadSpec([]byte(demoSpec))
	if err != nil {
		return err
	}

	// One in-memory transport backs both the publisher and the listener, and every broker maps to
	// its own URI so we can see which broker each message actually reached.
	tport := memory.New()
	resolve := func(_ context.Context, broker string) (string, error) {
		return "memory://" + broker, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis := dispatch.NewListener(plan, resolve, tport)
	defer lis.Close()
	deliveries, err := lis.Subscribe(ctx, ">")
	if err != nil {
		return err
	}

	pub := dispatch.NewPublisher(plan, resolve, tport, dispatch.RetryPolicy{MaxAttempts: 3, Backoff: 50 * time.Millisecond})
	defer pub.Close()

	msgs := []demoMsg{
		{"orders/eu/created", `{"priority":"urgent","region":"eu","amount":50}`},
		{"orders/us/created", `{"priority":"low","region":"us","amount":9000}`},
		{"orders/eu/updated", `{"priority":"low","region":"eu","amount":10}`},
		{"telemetry/eu/cpu", string(make([]byte, 4096))}, // > 2048 raw bytes
	}

	fmt.Printf("plan targets: %v\n\n", plan.Targets())
	fmt.Println("PUBLISH")
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "  topic\tbroker\taddress\tkey\trule")
	for _, m := range msgs {
		res, err := pub.Publish(ctx, m.topic, []byte(m.payload), map[string]string{"demo": "1"})
		if err != nil {
			return err
		}
		rule := res.Decision.Rule
		if !res.Decision.Matched {
			rule = "default"
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\n", m.topic, res.Decision.Broker, res.Decision.Address, orNone(res.Decision.Key), rule)
	}
	w.Flush()

	// Collect what the listener fanned in from every broker.
	fmt.Println("\nRECEIVE (fanned in from all target brokers)")
	w = tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "  broker\taddress\trecovered-key\tbytes")
	deadline := time.After(2 * time.Second)
	for got := 0; got < len(msgs); got++ {
		select {
		case d := <-deliveries:
			fmt.Fprintf(w, "  %s\t%s\t%s\t%d\n", d.Broker, d.Message.Address, orNone(d.GroupID), len(d.Message.Body))
		case <-deadline:
			w.Flush()
			return fmt.Errorf("timed out waiting for deliveries (%d of %d)", got, len(msgs))
		}
	}
	w.Flush()
	return nil
}

// scaleSink is a minimal spine.TopologySink for the demo: it records the latest applied topology with
// generation-win, exactly like the resolver. It also remembers the previous topology so the demo can
// show the fencer the (prev, next) pair a handoff spans.
type scaleSink struct {
	prev, cur *topology.Topology
}

func (s *scaleSink) Apply(t *topology.Topology) bool {
	if s.cur != nil && t.Gen() <= s.cur.Gen() {
		return false
	}
	s.prev, s.cur = s.cur, t
	return true
}

// runScaleDemo shows the ordering-first, event-driven reassignment path end to end with no broker:
// a topology snapshot is PUSHED over the spine (not polled), it moves one partition key to a new
// broker, and the fencer keeps that key on its old broker until in-flight traffic drains before
// cutting over - so per-key order survives the scale-up.
func runScaleDemo() error {
	shard := "orders"
	tport := memory.New()
	sink := &scaleSink{}

	sub := spine.NewSubscriber(tport, sink)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applied := make(chan int, 4)
	sub.OnApply = func(_ string, gen int, ok bool) {
		if ok {
			applied <- gen
		}
	}
	go func() { _ = sub.Run(ctx, "memory://spine", spine.WildcardTopologySource) }()

	sender, err := tport.Sender(ctx, "memory://spine")
	if err != nil {
		return err
	}

	// Generation 1: three active brokers. Generation 2: a scale-up adds broker-d.
	gen1 := scaleTopo(shard, 1, []string{"broker-a", "broker-b", "broker-c"}, nil)
	// Push gen 1 and wait for it to be applied (the memory transport registers the receiver async).
	if err := pushUntilApplied(ctx, sender, shard, gen1, applied, 1); err != nil {
		return err
	}

	// Find a key that the scale-up will move, so the demo shows a real handoff.
	gen2Preview := scaleTopo(shard, 2, []string{"broker-a", "broker-b", "broker-c", "broker-d"}, nil)
	movedKey, from, to := firstMovedKey(gen1, gen2Preview)
	gen2 := scaleTopo(shard, 2, []string{"broker-a", "broker-b", "broker-c", "broker-d"},
		[]topology.Handoff{{FromBroker: from, ToBroker: to, EffectiveGen: 2, Reason: "scale-up"}})

	fmt.Printf("shard %q, moved key %q: %s -> %s at gen 2\n\n", shard, movedKey, from, to)

	// A deterministic clock so the demo is instantaneous and reproducible.
	clk := &demoClock{t: time.Unix(1000, 0)}
	grace := 30 * time.Second
	fencer := spine.NewFencer(grace)
	fencer.Now = clk.now
	reorder := spine.NewReorderer(grace)
	reorder.Now = clk.now

	fmt.Println("PUBLISH-SIDE FENCE (drain-before-cutover)")
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "  when\troute-to\tstamp-gen\tdraining")

	// While still at gen 1, the key routes to its current owner.
	r, _ := fencer.Route(sink.prev, sink.cur, movedKey)
	fmt.Fprintf(w, "  before scale (gen 1)\t%s\t%d\t%v\n", r.BrokerID, r.Gen, r.Draining)

	// The scale event arrives over the spine: push gen 2 and observe the handoff it introduces.
	if err := pushUntilApplied(ctx, sender, shard, gen2, applied, 2); err != nil {
		w.Flush()
		return err
	}
	fencer.Observe(sink.prev, sink.cur)

	// Immediately after the event: within the grace window, the moved key still goes to the OLD broker.
	r, _ = fencer.Route(sink.prev, sink.cur, movedKey)
	fmt.Fprintf(w, "  event applied, in grace\t%s\t%d\t%v\n", r.BrokerID, r.Gen, r.Draining)

	// After the grace window: cut over to the new owner, stamped at the new generation.
	clk.t = clk.t.Add(grace + time.Second)
	r, _ = fencer.Route(sink.prev, sink.cur, movedKey)
	fmt.Fprintf(w, "  after grace (cutover)\t%s\t%d\t%v\n", r.BrokerID, r.Gen, r.Draining)
	w.Flush()

	// Listener side: the pre-cutover (gen 1) messages and the post-cutover (gen 2) message for the
	// moved key arrive, possibly out of order. The reorderer releases them in (gen, seq) order and
	// never lets the gen-2 message overtake the still-draining gen-1 stream.
	clk.t = time.Unix(1000, 0) // reset the clock for the listener timeline
	fmt.Println("\nLISTEN-SIDE REORDER (per-key generation order)")
	var released []spine.FencedMessage
	released = append(released, reorder.Admit(spine.FencedMessage{Key: movedKey, Gen: 1, Seq: 1})...)
	released = append(released, reorder.Admit(spine.FencedMessage{Key: movedKey, Gen: 1, Seq: 2})...)
	// The new broker is faster: its gen-2 message arrives before gen-1 has drained. It is held.
	released = append(released, reorder.Admit(spine.FencedMessage{Key: movedKey, Gen: 2, Seq: 3})...)
	fmt.Printf("  released before hold elapses: %d (gen-2 is held behind draining gen-1)\n", len(released))
	clk.t = clk.t.Add(grace + time.Second)
	released = append(released, reorder.Tick()...)

	w = tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "  order\tkey\tgen\tseq")
	for i, m := range released {
		fmt.Fprintf(w, "  %d\t%s\t%d\t%d\n", i+1, m.Key, m.Gen, m.Seq)
	}
	w.Flush()
	fmt.Println("\nper-key order preserved across the scale-up: gen-1 messages precede gen-2.")
	return nil
}

func scaleTopo(shard string, gen int, active []string, handoffs []topology.Handoff) *topology.Topology {
	brokers := make([]topology.BrokerRef, 0, len(active))
	for _, id := range active {
		brokers = append(brokers, topology.BrokerRef{
			BrokerID:  id,
			State:     topology.StateActive,
			Endpoints: map[string]string{"amqp": "amqp://" + id + ":5672"},
		})
	}
	return &topology.Topology{
		Version:    topology.Version,
		Shard:      shard,
		Generation: gen,
		EmittedAt:  time.Unix(1000, int64(gen)).UTC().Format(time.RFC3339),
		Brokers:    brokers,
		Handoffs:   handoffs,
	}
}

func firstMovedKey(prev, next *topology.Topology) (key, from, to string) {
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key-%d", i)
		po, ok1 := prev.Owner(k)
		no, ok2 := next.Owner(k)
		if ok1 && ok2 && po != no {
			return k, po, no
		}
	}
	return "", "", ""
}

// pushUntilApplied republishes a last-value topology snapshot until the subscriber applies the wanted
// generation, tolerating the memory transport's asynchronous receiver registration.
func pushUntilApplied(ctx context.Context, sender dispatch.Sender, shard string, t *topology.Topology, applied <-chan int, want int) error {
	body, err := json.Marshal(t)
	if err != nil {
		return err
	}
	msg := dispatch.Message{Address: spine.ControlSource(shard), Body: body}
	deadline := time.After(2 * time.Second)
	for {
		if err := sender.Send(ctx, msg); err != nil {
			return err
		}
		select {
		case gen := <-applied:
			if gen >= want {
				return nil
			}
		case <-deadline:
			return fmt.Errorf("timed out waiting for gen %d to be applied", want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

type demoClock struct{ t time.Time }

func (c *demoClock) now() time.Time { return c.t }

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
