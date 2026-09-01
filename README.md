# solace-autoscale

**How many Solace PubSub+ Cloud brokers does this workload actually need — and when?**

`solace-autoscale` answers that from a broker capacity model you compile yourself, using a pure,
deterministic decision engine, and shows the reasoning instead of hiding it behind a number. When
you're ready, it can scale the fleet to match — but it ships in recommend-only mode and stays there
until you say otherwise.

It exists because vertical scaling on Solace Cloud has a ceiling. Once you're on the largest
practical service class and message sizes or adoption keep growing, there's no supported path
forward. This is the horizontal one.

🔗 **[solacese.github.io/solace-autoscale](https://solacese.github.io/solace-autoscale/)** — overview, worked examples, and quickstart.

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
pytest -m "not integration"      # fast unit suite
pytest -m integration            # live-broker protocol tests (needs a local Solace broker)
ruff check . && mypy src/solace_autoscale
```

CI runs lint, types, unit tests, the compiler (against the synthetic model), and the live-broker
protocol integration tests. Contributions welcome — keep the decision engine pure.
