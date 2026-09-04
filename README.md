# solace-broker-autoscale

**How many Solace Cloud brokers does a workload need, and when?**

`solace-broker-autoscale` answers that from capacity numbers you measure on your own brokers. It
gives you a recommendation and the reasoning behind it, not just a number. By default it only
advises; scaling the fleet for you is a separate setting you turn on deliberately.

Vertical scaling on Solace Cloud has a limit. Once you are on the largest practical service class
and traffic keeps growing, there is no bigger broker to buy. This tool provides the horizontal path:
run more brokers and spread the load across them.

🔗 **[Overview and examples](https://solacese.github.io/solace-broker-autoscale/)**

> Community project. Not a supported Solace product. No warranty. Apache 2.0.
> No measured numbers, prices, or customer data are included. You supply your own.

---

## Two parts

The project has two independent pieces. You can use either on its own.

- **The controller** is the tool you run. It reads your capacity model and live metrics, decides how
  many brokers the workload needs, and can optionally scale the fleet. This is the `solace-autoscale`
  command described below.
- **The shim** is a thin wrapper around the messaging API your applications already use (AMQP, MQTT,
  REST, or SMF). It sits in front of your existing client, and when brokers are added or removed it
  points the client at the correct broker. It adds no proxy to the data path and needs no change to
  how you publish or consume. It is optional and lives in [`adapters/`](adapters/README.md).

The controller changes the number of brokers. The shim makes each publisher and subscriber follow
that change, using the AMQP (or MQTT, REST, or SMF) client they already have.

---

## The controller

### Install

Requires Python 3.11 or later.

```bash
pip install -e '.[compile]'
```

This installs the `solace-autoscale` command.

### Use it

You need two inputs: a configuration file (start from the included `config.example.yaml`) and a
metrics file, which is a JSON snapshot of your broker's load (format in
[`docs/metrics.md`](docs/metrics.md)).

```bash
# How many brokers does this workload need now?
solace-autoscale recommend --config config.example.yaml --metrics metrics.json

# How many would it need if traffic doubled or quadrupled?
solace-autoscale whatif --config config.example.yaml --metrics metrics.json --multipliers 1,2,4
```

By default these run against an included sample model (`models/synthetic-v0.json`) that contains
obviously fake capacity numbers. Every report flags the sample model, and it is blocked from making
any real change, so you can see the output safely. The tool never acts on fake data.

To use your own numbers, measure your brokers into a workbook (see
[`docs/benchmark.md`](docs/benchmark.md)), compile it into a model file, and point your configuration
at that file:

```bash
solace-autoscale compile --workbook performance.xlsx \
    --service-classes models/service-classes.json --out models/mymodel.json
# then set  capacity.model: models/mymodel.json  in your configuration
```

### Commands

| Command | What it does |
|---|---|
| `recommend` | Reports how many brokers a workload needs, with the reason: which limit is reached, how full each broker is, and the cost. |
| `whatif` | The same recommendation, projected under load multipliers, so you can see the limit before you reach it. |
| `compile` | Turns a measured workbook into a versioned model file. Run this once whenever your numbers change. |
| `monitor` | Watches a live broker over time and records how accurate past advice was. |
| `simulate` | Tests the model across many message sizes and fan-out patterns. |
| `accuracy` | Compares what was predicted against what actually happened, per limit. |
| `shard-advise` | Suggests how to split traffic into groups, from an Event Portal export. |
| `serve` | Runs the assignment service that tells clients which broker to use. |

### Settings

All settings live in one YAML file. Unknown keys are rejected, and the defaults are conservative.
Start from [`config.example.yaml`](config.example.yaml). The settings you are most likely to change:

| Setting | Effect |
|---|---|
| `fleet.min_brokers` / `max_brokers` | The lowest and highest broker count allowed. Reaching the limit is reported, never hidden. |
| `workload.delivery` / `bottleneck` | `direct`, `guaranteed`, or `mixed`; set the limit to check, or let the tool detect it. |
| `metrics.source` | Where the numbers come from: `semp`, `prometheus`, `cloud-api`, or a `static` file. |
| `billing.per_broker_monthly` | Your own prices, used to turn broker counts into a monthly cost. No prices are included. |
| `actuation.mode` | `recommend` (default, advice only), `scale-up-only`, or `full`. |

Full reference: [`docs/configuration.md`](docs/configuration.md).

### Safety

The controller cannot change your brokers unless you turn scaling on, and even then it is
constrained:

- It defaults to advice only. In `recommend` mode, the component that makes changes is never created.
- A sample model or stale metrics block all changes.
- Every real change is written to an audit log first. It also honours a kill-switch file and rate
  limits, requires explicit confirmation, and refuses to delete a broker that still has traffic.
- TLS certificate checking is on by default. Disabling it requires an explicit `--insecure` flag.

See [`docs/safety.md`](docs/safety.md).

---

## The shim

The shim wraps the messaging API your publishers and subscribers already use. You keep your existing
AMQP, MQTT, REST, or SMF client; the shim just tells it which broker to connect to as the fleet
changes. It asks the assignment service which broker to use, then hands your client a normal
connection URL for that protocol. It never carries messages and never handles credentials, so your
data path and authentication are unchanged. If the assignment service is unreachable, it returns the
last known answer rather than failing.

For example, a publisher using AMQP keeps its AMQP client and only adds the lookup:

```python
from solace_autoscale_client import Resolver, amqp_uri

r = Resolver(base_url="https://assign.example.com")
a = r.resolve(shard="shard-a", client_id="orders-publisher", mode="guaranteed", protocol="amqp")
uri = amqp_uri(a)   # pass this URL to your normal AMQP client and connect and publish as usual
```

The same pattern works for MQTT, REST, and SMF. Python and Java versions are provided. See
[`adapters/`](adapters/README.md) and [`docs/client-integration.md`](docs/client-integration.md) for
the full integration options.

---

## Documentation

- [`docs/architecture.md`](docs/architecture.md) - how the pieces fit together
- [`docs/capacity-model.md`](docs/capacity-model.md) - the model file and how to build it
- [`docs/configuration.md`](docs/configuration.md) - every setting
- [`docs/metrics.md`](docs/metrics.md) - where the numbers come from
- [`docs/client-integration.md`](docs/client-integration.md) - connecting clients (the shim)
- [`docs/safety.md`](docs/safety.md) - the safety controls
- [`docs/adr/`](docs/adr/) - the design decisions and the reasons for them

## Development

```bash
pip install -e '.[dev]'    # tests, linters, type checker, compiler
pytest                     # unit tests (live-broker tests are skipped unless requested)
ruff check . && mypy src/solace_autoscale
```

The live-broker tests need extra client libraries and a local broker. See
[`tests/test_integration_broker.py`](tests/test_integration_broker.py) for the Docker command and
run them with `pytest -m integration`.

Contributions are welcome. The decision engine is a pure function (numbers in, recommendation out, no
input or output). Please keep it that way.
