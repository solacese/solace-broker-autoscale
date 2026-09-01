# Architecture

`solace-autoscale` is entirely **control plane**. No component carries a client message (ADR 0002).

## Data flow

```
performance.xlsx ──(build-time)──▶ capacity/compile.py ──▶ models/*.json (versioned)
                                                                 │
metrics source ──▶ metrics/*.py ──▶ MetricSample window ─────────┤
   (SEMPv2 / Cloud / Prometheus / static)                        ▼
                                              decision/engine.py  (PURE)
config.yaml ──▶ config.py ──────────────────────────────▶  ShardDecision
                                                                 │
                                          ┌──────────────────────┼───────────────────────┐
                                          ▼                      ▼                        ▼
                                  report/markdown.py     accuracy/recorder.py      actuator/* (opt-in)
                                  report/json.py         accuracy/report.py        safety.py → Solace Cloud API
```

## Components

| Package | Role | I/O? |
|---|---|---|
| `config` | Load + validate YAML (Pydantic, reject unknown keys) | reads config file |
| `capacity/compile` | xlsx → versioned JSON at build time | reads xlsx (build only) |
| `capacity/model` + `schema` | Runtime capacity lookup + interpolation | reads model JSON |
| `metrics/*` | Broker metrics → normalised `MetricSample` | network / file |
| `decision/engine` | **Pure** state→target function | none |
| `decision/headroom` | Derived headroom + window (pure) | none |
| `accuracy/*` | Record recommendations, report predicted-vs-actual | SQLite |
| `report/*` | Markdown + JSON from one structure | none (returns strings) |
| `simulator/workload` | Synthetic workload matrix + model validation | none |
| `portal/shard_advisor` | Event Portal export → shard boundaries | reads export |
| `assignment/*` | HTTP service + durable placement store | HTTP, SQLite/Postgres |
| `actuator/*` | Solace Cloud API calls behind safety layer | network (opt-in) |
| `dns/updater` | Tier-0 DNS records per shard | network (opt-in) |

## The purity boundary

The decision engine takes a `DecisionRequest` (config + model + shard samples + `now` +
`last_decision_at` + optional measured `minutes_to_capacity`) and returns a `ShardDecision`. It never
imports `httpx`, never reads a clock, never logs. `now` and timestamps are supplied by the caller.
This is enforced by review and by the test that asserts identical output for identical input.

The actuator is the **only** component permitted to call the Solace Cloud REST API. In `recommend`
mode it is never constructed.
