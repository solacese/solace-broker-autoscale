# Smart shim (Go)

The smart shim is a thin client-side library that sits next to your application's own AMQP client.
For every message it decides three things from the topic and payload, using a set of rules you write:

- which **broker** the message should go to,
- what **partition key** to stamp on it (so related messages stay ordered together), and
- what **address** to publish it to (optionally rewritten).

It is not a proxy. Nothing sits on the network path between your app and the broker. The shim wraps
the client you already use, so you keep your own connection, credentials, and delivery guarantees.

The rules are the same portable spec the Python scaling controller reads and writes, so the routing
you test here is exactly the routing the control plane reasons about. See
[`../guide/rule-spec.md`](../guide/rule-spec.md) for the full spec.

## Try it in 30 seconds

```sh
cd shim
go test ./...
go run ./cmd/shim demo
```

`demo` runs the whole data path offline over an in-memory transport (no broker needed). You will see
the same four messages fan out to three different brokers by payload, and then get fanned back in by
a listener that recovers each partition key:

```
PUBLISH
  topic              broker       address                key     rule
  orders/eu/created  broker-vip   vip/orders/eu/created  vip.eu  vip-orders
  orders/us/created  broker-big   orders/us/created      big.us  large-orders
  orders/eu/updated  broker-bulk  orders/eu/updated      (none)  default
  telemetry/eu/cpu   broker-bulk  telemetry/eu/cpu       (none)  telemetry-raw
```

## Check a rule spec against a message

```sh
go run ./cmd/shim route \
  --spec testdata/interop_spec.json \
  --topic orders/eu/created \
  --payload '{"priority":"urgent","region":"eu"}'
```

This prints the routing decision for one message without touching a broker, which is the fast way to
confirm a spec does what you expect before wiring it into an app.

## How routing works

Rules are evaluated top to bottom, first match wins, and if nothing matches the message goes to the
`default_broker`. A rule matches when both halves hold:

- **topic** matches a Solace pattern (`*` is one level, `>` is the rest and must be last), and
- every **payload** predicate holds (AND). Predicates read dotted JSON paths, for example
  `order.region`, and support operators like `eq`, `in`, `gt`, `prefix`, `contains`, `regex`,
  `exists`, and `raw_size_gt` (which looks at the undecoded byte length). A missing field is simply
  a non-match rather than an error.

The chosen route can template the key and address with `{topic}` and `{dotted.path}` placeholders,
for example `key: "vip.{region}"`.

## Using it in your application

```go
plan, _ := rules.LoadSpec(specJSON)              // portable spec shared with the controller
resolver := myBrokerResolver                     // broker name -> connection URI
tport := amqp.New()                              // real AMQP 1.0 (transport/amqp)

pub := dispatch.NewPublisher(plan, resolver, tport, dispatch.RetryPolicy{MaxAttempts: 3})
defer pub.Close()

res, err := pub.Publish(ctx, "orders/eu/created", payload, map[string]string{"trace": traceID})
// res.Decision tells you which broker, address, key, and rule fired.
```

The consumer side mirrors it:

```go
lis := dispatch.NewListener(plan, resolver, tport)
defer lis.Close()
deliveries, _ := lis.Subscribe(ctx, ">")         // covers every broker the rules can target
for d := range deliveries {
	// d.Broker, d.Message, and d.GroupID (the recovered partition key)
}
```

## Packages

| Package             | What it does                                                                 |
| ------------------- | ---------------------------------------------------------------------------- |
| `rules`             | The rule engine and the portable spec loader (`LoadSpec`). No I/O.           |
| `topology`          | Parses topology snapshots and answers key ownership (rendezvous). No I/O.    |
| `spine`             | Consumes control events (`Subscriber`) and the ordering-first cutover        |
|                     | machinery (`Fencer`, `Reorderer`). Pure but for an injected clock.           |
| `resolve`           | Fail-open resolver: events-primary, HTTP/cache cold-start and fallback.      |
| `dispatch`          | The data path: `PublisherShim`, `ListenerShim`, and the `Transport` seam.    |
| `transport/amqp`    | Real AMQP 1.0 transport (github.com/Azure/go-amqp).                          |
| `transport/memory`  | In-memory transport so the whole shim runs and tests offline.                |
| `cmd/shim`          | The `route` and `demo` (`--scale`) CLI.                                      |

## Cross-language interop

`testdata/interop_spec.json` is emitted by the Python controller and read here by `LoadSpec`. A Go
test (`TestInteropGolden`) asserts it routes to the exact decisions the Python engine computes, and a
Python test asserts the controller still emits that file byte for byte. Neither engine can drift from
the other without a test failing.

## Partition key on the wire

The shim stamps the partition key both as the AMQP **group-id** and as a `saas_partition_key`
application property. Listeners read it back (preferring the property), so consumers can process a
coherent per-key stream without re-running the rules.
