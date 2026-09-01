# solace-autoscale

Tells an operator how many Solace PubSub+ Cloud brokers a workload needs, and (optionally) scales
the fleet to match.

It exists because vertical scaling on Solace Cloud has a ceiling. Once a customer is on the largest
practical service class and their message sizes or adoption keep growing, there is no supported path
forward. This tool provides the horizontal one.

> **Community project. Not a supported Solace product. No warranty.** See `LICENSE` (Apache 2.0).
> This repository contains no Solace-internal figures, credentials, customer names, or account
> identifiers. The tool ships the capacity-model *schema and harness*, not measured numbers — you
> measure your own brokers (see `benchmark/README.md`).

## What it does

- **Recommends** a per-shard broker count from observed metrics and a compiled capacity model, with
  a full explanation: the binding axis, all four utilisation ratios against their effective
  thresholds, derived-vs-configured headroom, and the warnings that matter (hot shard, ceiling hit,
  byte-bound-at-large-messages, model extrapolation, unsafe headroom, insufficient window).
- **Tracks accuracy**: records every recommendation and, when later samples arrive, reports
  predicted-vs-actual capacity error per axis and size bucket — because an optimistic model is a
  dangerous one.
- **Advises shard boundaries** from an Event Portal export (weakly-connected components of the
  application↔topic graph).
- **Assigns** clients to brokers via a small HTTP service and three tiers of client integration
  (DNS, resolver, SDK wrapper) — never a proxy in the data path.
- **Actuates** (opt-in, off by default) against the Solace Cloud (Mission Control) REST API, behind
  a safety layer that refuses anything unsafe.

## The core is a pure function

The decision engine (`decision/engine.py`) is a pure function: state in, target state out. No I/O, no
network, no clock reads, no logging. This makes it exhaustively testable, and its correctness is the
product. Everything else — collecting metrics, loading the model, writing reports, calling the Cloud
API — lives outside it.

## Safety, up front

- `actuation.mode` defaults to `recommend` and stays there. In recommend mode the actuator is never
  even constructed.
- A synthetic capacity model hard-blocks all actuation.
- Stale metrics block both decisions and actuation.
- The actuator writes an audit record before every call, honours a kill switch, rate limits, and
  refuses to delete a broker that still has queues, consumers, flows, or spooled messages.

See `docs/safety.md`.

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'

# 1. compile your measured workbook into a versioned model (never read at runtime)
solace-autoscale compile --workbook performance.xlsx \
    --service-classes models/service-classes.json --out models/mymodel.json

# 2. get a recommendation (uses a static metrics window here; SEMP/Cloud collectors documented)
solace-autoscale recommend --config config.example.yaml --metrics metrics-sample.json

# 3. validate the model across the full size/fanout matrix
solace-autoscale simulate --config config.example.yaml

# 4. propose shard boundaries from an Event Portal export
solace-autoscale shard-advise --export export.json
```

Until you compile a real model, the tool runs against `models/synthetic-v0.json` — obviously
fabricated round numbers, flagged in every report, and blocked from actuation.

## Documentation

- `docs/architecture.md` — components and data flow
- `docs/capacity-model.md` — workbook shape, compiled schema, open questions
- `docs/configuration.md` — every setting and its behaviour
- `docs/metrics.md` — verified metric field mapping (SEMPv2) and what is still an open question
- `docs/safety.md` — the actuator guardrails
- `docs/adr/` — the decisions and why

## Status

Built in phases:

1. ✅ Config, capacity compiler, decision engine, metrics collectors, report, simulator, shard advisor
2. ✅ Prediction accuracy tracking, derived headroom advisor
3. ✅ Assignment service, DNS tier, protocol adapters, SDK wrappers
4. ✅ Actuator, warm pool, drain controller, Terraform — **all off by default**
