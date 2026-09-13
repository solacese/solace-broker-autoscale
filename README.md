# solace-broker-autoscale

**Autoscale your Solace Cloud brokers as throughput grows, and keep every client pointed at the right broker.**

Vertical scaling on Solace Cloud has a ceiling. Once you are on the largest practical service class
and traffic keeps climbing, there is no bigger broker to buy. This project is the horizontal path:
run more brokers, spread the load, and route each message to where it belongs.

🔗 **[Overview and examples](https://solacese.github.io/solace-broker-autoscale/)**

> Community project. Not a supported Solace product. No warranty. Apache 2.0.
> No measured numbers, prices, or customer data are included. You supply your own.

---

## Two products, one repo

The project ships two independent pieces. Use either on its own.

| Product | Folder | Language | What it does |
|---|---|---|---|
| **Scaling controller** | [`scaling-controller/`](scaling-controller/) | Python | Reads your capacity model and live metrics, decides how many brokers the workload needs, explains why, and can optionally scale the fleet. This is the `solace-autoscale` command. |
| **Smart shim** | [`shim/`](shim/) | Go | A client-side library that wraps your app's own AMQP client. Per message, it applies your routing rules, picks the target broker, stamps a partition key, and (optionally) rewrites the address. No proxy in the data path. |

The controller changes the *number* of brokers. The shim makes each publisher and subscriber follow
that change and route each message deterministically across the fleet.

### Repository layout

```
scaling-controller/   Python control plane (the solace-autoscale CLI, decision engine, assignment service)
shim/                 Go smart shim (rule engine, resolver, publisher + listener, real AMQP, CLI demo)
guide/                Documentation and design records (published to the project site)
examples/             Ready-to-run config, starting with config.example.yaml
deploy/               Deployment assets (Terraform, container, KEDA scaler)
models/               Capacity model files, including the fabricated synthetic sample
resources/            Inputs you supply (performance workbooks); nothing measured is committed
scripts/              Helpers, e.g. regenerating the synthetic model
```

---

## Scaling controller (Python)

```bash
cd scaling-controller
pip install -e '.[compile]'          # installs the solace-autoscale command
```

```bash
# How many brokers does this workload need now?
solace-autoscale recommend --config ../examples/config.example.yaml --metrics metrics.json

# How many would it need if traffic doubled or quadrupled?
solace-autoscale whatif --config ../examples/config.example.yaml --metrics metrics.json --multipliers 1,2,4
```

By default these run against an included sample model (`models/synthetic-v0.json`) whose numbers are
obviously fabricated. Every report flags the sample model and blocks any real change, so you can see
the output safely. To use your own numbers, measure your brokers into a workbook (see
[`guide/benchmark.md`](guide/benchmark.md)), compile it, and point your config at the result:

```bash
solace-autoscale compile --workbook performance.xlsx \
    --service-classes models/service-classes.json --out models/mymodel.json
```

Full commands, settings, and safety controls are in [`scaling-controller/README.md`](scaling-controller/README.md).

---

## Smart shim (Go)

Once a workload spans several brokers, where each message goes stops being arbitrary. The shim wraps
the app's existing AMQP client, so there is no proxy in the data path. Per message it makes one
decision from rules that combine the Solace topic and the JSON payload, picks the target broker, and
stamps a partition key on the wire (AMQP `group-id` plus a `saas_partition_key` property). It is not
round-robin: the same message always routes the same way, so per-key ordering holds, and the listener
side re-runs the same rules to demultiplex a coherent per-key stream.

```yaml
# rule spec (JSON on disk, shown here as YAML for readability): first match wins
default_broker: broker-bulk
rules:
  - name: vip-orders                 # topic AND payload
    when:
      topic: "orders/>"
      payload:
        - { path: priority, op: in, value: [high, urgent] }
    route:
      broker: broker-vip
      key:   "vip.{region}"          # partition key = ordering group, templated from the payload
      topic: "vip/{topic}"           # optional address rewrite
  - name: large-orders
    when: { topic: "orders/>", payload: [ { path: amount, op: gt, value: 1000 } ] }
    route: { broker: broker-big, key: "big.{region}" }
```

Predicate operators: `eq ne in nin gt gte lt lte exists missing prefix contains regex`, plus
`raw_size_gt raw_size_lt raw_prefix` for non-JSON payloads. The rule spec is portable: the Python
controller's `dispatch-test` and the Go shim read the same JSON, proven by a cross-language golden
test. Build and try it from [`shim/`](shim/):

```bash
cd shim
go test ./...
go run ./cmd/shim demo          # runs the rule engine + an in-memory transport, offline
```

See [`shim/README.md`](shim/README.md) for the library API, the `Transport` interface, and the AMQP
transport.

---

## Documentation

- [`guide/architecture.md`](guide/architecture.md) - how the pieces fit together
- [`guide/capacity-model.md`](guide/capacity-model.md) - the model file and how to build it
- [`guide/configuration.md`](guide/configuration.md) - every controller setting
- [`guide/metrics.md`](guide/metrics.md) - where the numbers come from
- [`guide/rule-spec.md`](guide/rule-spec.md) - the portable dispatch rule spec (Python and Go)
- [`guide/client-integration.md`](guide/client-integration.md) - connecting clients
- [`guide/event-spine.md`](guide/event-spine.md) - event-driven, ordering-first reassignment
- [`guide/safety.md`](guide/safety.md) - the safety controls
- [`guide/adr/`](guide/adr/) - the design decisions and the reasons for them

## Development

Each product builds and tests on its own:

```bash
# Python controller
cd scaling-controller && pip install -e '.[dev]'
pytest -m "not integration" -q && ruff check . && mypy solace_autoscale

# Go shim
cd shim && go test ./... && go vet ./...
```

Contributions are welcome. The decision engine and the rule engine are pure functions (inputs in,
result out, no I/O). Please keep them that way.
