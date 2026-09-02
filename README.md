# solace-autoscale

**How many Solace PubSub+ Cloud brokers does this workload actually need — and when?**

`solace-autoscale` answers that from a broker capacity model you compile yourself, using a pure,
deterministic decision engine, and shows the reasoning instead of hiding it behind a number. When
you're ready, it can scale the fleet to match — but it ships in recommend-only mode and stays there
until you say otherwise.

It exists because vertical scaling on Solace Cloud has a ceiling. Once you're on the largest
practical service class and message sizes or adoption keep growing, there's no supported path
forward. This is the horizontal one.

🔗 **[solacese.github.io/solace-broker-autoscale](https://solacese.github.io/solace-broker-autoscale/)** — overview, worked examples, and quickstart.

> **Community project. Not a supported Solace product. No warranty.** Apache 2.0.
> The repo ships the capacity-model *schema and harness*, never measured numbers, credentials, or
> customer data — you measure your own brokers (see [`benchmark/`](benchmark/README.md)).

---

## Install

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'          # dev extras: tests, lint, types, xlsx compiler
```

Requires Python 3.11+.

## Three commands to a decision

```bash
# 1. compile your measured workbook into a versioned model (also runs in CI)
solace-autoscale compile --workbook performance.xlsx \
    --service-classes models/service-classes.json --out models/mymodel.json

# 2. get a recommendation — with the binding axis, thresholds, warnings, and cost
solace-autoscale recommend --config config.example.yaml --metrics metrics.json

# 3. project the fleet under 2x / 4x load
solace-autoscale whatif --config config.example.yaml --metrics metrics.json --multipliers 1,2,4
```

Until you compile your own model, everything runs against `models/synthetic-v0.json` — obviously
fabricated round numbers, flagged in every report and **blocked from any real action**. Nothing is
guessed; nothing acts on fake data.

## What it does

| Command | Does |
|---|---|
| `compile` | Turn a performance workbook into versioned JSON. The runtime never reads a spreadsheet; every decision records its model version. |
| `recommend` | Per-shard broker count with the full reasoning: binding axis, four utilisation ratios vs. thresholds, derived-vs-configured headroom, cost, and the warnings that matter. |
| `whatif` | Re-run the same pure engine under load multipliers to see where the fleet ceiling bites. |
| `simulate` | Validate the model across the full message-size × fanout × delivery matrix. |
| `monitor` | Scrape SEMP continuously, accrue a rolling window, decide each tick, and record accuracy history. |
| `accuracy` | Predicted vs. actual capacity error per axis and size bucket — flags where the model is optimistic. |
| `shard-advise` | Propose shard boundaries from an Event Portal export (connected components of the app↔topic graph). |
| `serve` | Run the assignment service that maps clients to brokers (per-protocol endpoints, sticky durable placement). |

## You may not need this tool

Adding brokers is the most expensive way to get more capacity, and it is rarely the first thing that
should change. Before reaching for horizontal broker scaling, rule these out — honestly, most
workloads are solved by one of them:

- **Consumer lag or backlog? Scale consumers, not brokers.** The
  [KEDA Solace scaler](https://keda.sh/docs/2.20/scalers/solace-pub-sub/) scales *consumer
  instances* off queue depth, spool usage, and receive rate. It scales a Kubernetes Deployment; it
  never touches the broker. If your symptom is a growing queue or rising consumer lag, that is the
  right tool and this one is unnecessary. There is a direct-messaging variant too,
  [`solace-pub-sub-dm`](https://keda.sh/docs/2.20/scalers/solace-pub-sub/). This tool watches the
  *broker* ceiling, which is a different problem.
- **Throughput-bound within one broker? Use partitioned queues.** Partitioned queues scale consumer
  throughput across partitions on a single broker. Exhaust this before adding brokers — a second
  broker is far more operationally costly than more partitions.
- **Byte-bound on large payloads? Use the claim-check pattern.** Put the payload in object storage
  and publish a reference; consumers fetch the body out of band. For byte-bound workloads this is
  often an order of magnitude cheaper than adding brokers. The decision engine already raises this
  as a warning when the binding axis is bytes on large messages — the advice is the same here.

**This tool is for the remaining case:** a workload that has genuinely exhausted vertical scaling on
the largest practical service class, where the ceiling is *broker* capacity, not consumer capacity,
and the only path left is more brokers with traffic steered across them. If you are not there yet,
one of the above is the cheaper, simpler fix.

## Why the design holds up

- **The decision engine is a pure function** — state in, target state out. No I/O, no network, no
  clock, no logging. That makes it exhaustively testable, and its correctness is the whole product.
  Everything else (metrics, model loading, reports, Cloud calls) lives outside it.
- **Per-axis thresholds.** Messages, bytes, connections, and spool each have their own headroom; the
  engine reports the *binding* axis so you see why, not just how many.
- **Derived headroom.** A threshold isn't a preference — it's derived to leave room for the growth
  that arrives while a broker is still provisioning. Your configured value is a ceiling, never a
  floor.
- **No proxy in the data path.** Every component is control plane; client traffic goes straight to
  brokers.

## Safety (Phase 4, off by default)

- `actuation.mode` defaults to `recommend`, and stays there — in recommend mode the actuator is
  **never even constructed**. Turning on scaling is a deliberate config edit.
- A synthetic capacity model, or stale metrics, **hard-blocks** all actuation.
- The actuator writes an audit record *before* every call, honours a kill switch and rate limits,
  and refuses to delete a broker that still has queues, consumers, flows, or spooled messages.
- `actuation.require_confirmation` (on by default) is **enforced**: a real (non-dry-run) operation
  is refused, and audited as `refused`, unless the caller explicitly confirmed it. The gate never
  prompts — it stays deterministic — so a caller collects the operator's confirmation and passes it
  in.
- **TLS verification is on by default** everywhere the tool connects to a broker. `monitor` accepts
  an explicit `--insecure` flag to disable certificate verification; it must be passed deliberately
  and prints a stderr warning naming the host it applies to.

See [`docs/safety.md`](docs/safety.md).

## Customize

Configuration is one validated YAML file — unknown keys are rejected, defaults are conservative.
Start from [`config.example.yaml`](config.example.yaml). The knobs you'll reach for first:

| Setting | Effect |
|---|---|
| `fleet.min_brokers` / `max_brokers` | Clamp every recommendation; hitting the ceiling is reported, never silently absorbed. |
| `topology.mode` | `sharded` \| `mesh` \| `hybrid`. Mesh adds inter-broker link cost to the bytes/spool axes — deliberately. |
| `workload.delivery` / `bottleneck` | `direct`/`guaranteed`/`mixed`; force an axis or let it auto-detect the binding one. |
| `policy.headroom.mode` | `derived` (safe, adapts to growth + provisioning time) or `fixed`. |
| `metrics.source` | `semp` \| `prometheus` \| `cloud-api` \| `static`. SEMPv2 field names are verified against a real broker; sources without a captured sample are documented stubs, not guesses. |
| `billing.model` + `per_broker_monthly` | Committed billing suppresses scale-down; add your own price table to turn broker counts into monthly dollars (no pricing ships with the tool). |
| `actuation.mode` | `recommend` \| `scale-up-only` \| `full`, all behind the safety layer. |

Full reference: [`docs/configuration.md`](docs/configuration.md).

## Make it yours

- **Your capacity numbers** are yours to measure — [`benchmark/`](benchmark/README.md) explains the
  workbook shape the compiler expects and how to produce it. Compiled models built from real data
  are gitignored.
- **Client integration** offers three tiers so each app picks its own (DNS, a thin resolver, or a
  full SDK wrapper) — see [`docs/client-integration.md`](docs/client-integration.md) and
  [`adapters/`](adapters/README.md).
- **Broker configuration** is templated with Terraform in [`deploy/terraform/`](deploy/terraform/)
  so every broker in a shard is configured identically.

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — components and data flow
- [`docs/capacity-model.md`](docs/capacity-model.md) — workbook shape, compiled schema, open questions
- [`docs/configuration.md`](docs/configuration.md) — every setting and its behaviour
- [`docs/metrics.md`](docs/metrics.md) — verified metric field mapping (SEMPv2)
- [`docs/client-integration.md`](docs/client-integration.md) — the three integration tiers
- [`docs/safety.md`](docs/safety.md) — the actuator guardrails
- [`docs/adr/`](docs/adr/) — the decisions and why

## Develop

```bash
pytest                           # fast unit suite (integration tests are deselected by default)
ruff check . && mypy src/solace_autoscale
```

A bare `pytest` is green out of the box: `[tool.pytest.ini_options]` sets
`addopts = '-m "not integration"'`, so the live-broker tests never run unless you ask for them.

To run the integration tests you need the optional protocol clients and a local broker:

```bash
pip install -e '.[dev,integration]'   # adds paho-mqtt, qpid-proton, solace-pubsubplus, requests

# start a local Solace PubSub+ broker (ports match tests/test_integration_broker.py defaults)
docker run -d --name solace-dev --shm-size=1g --ulimit nofile=1048576:1048576 \
  -p 8081:8080 -p 55556:55555 -p 5673:5672 -p 1884:1883 -p 9001:9000 \
  -e username_admin_globalaccesslevel=admin -e username_admin_password=admin \
  solace/solace-pubsub-standard

pytest -m integration                 # live-broker protocol tests
```

CI runs lint, types, unit tests, the compiler (against the synthetic model), and — in a separate
job with a real broker service — the live-broker protocol integration tests. Contributions welcome —
keep the decision engine pure.
