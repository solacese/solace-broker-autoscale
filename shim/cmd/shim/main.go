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
// The route command is the fast way to check that a rule spec does what you think before wiring it
// into an application. The demo is the fast way to see the shim behave end to end.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
	"github.com/solacese/solace-broker-autoscale/shim/rules"
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

  shim demo
      Run an offline publish/subscribe round trip over the in-memory transport.

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
	if err := fs.Parse(args); err != nil {
		return err
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

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
