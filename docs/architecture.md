# Architecture

For the implemented unattended managed-queue workflow, use [Automatic scaling](automatic-scaling.md), including its complete YAML and recovery behavior.

The controller and assignment service are **control plane**. Application adapters keep a local retry buffer and send messages directly to brokers; there is no central message proxy (ADR 0002).

## Data flow

```
performance.xlsx ──(build-time)──▶ capacity/compile.py ──▶ models/*.json (versioned)
                                                                 │
metrics source ──▶ metrics/*.py ──▶ MetricSample window ─────────┤
   (SEMPv2 / static; other collectors are placeholders)                        ▼
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
| `assignment/*` | HTTP service + durable placement store | HTTP, SQLite (Postgres not implemented) |
| `controller/*` | Load-aware partition moves, persisted fencing/drain/cutover, optional warm provisioning | SEMP, Cloud, SQLite (opt-in) |
| `actuator/*` | Solace Cloud API calls behind safety layer | network (opt-in) |
| `dns/updater` (in `solace_autoscale.dns`) | Tier-0 DNS records per shard | network (opt-in) |

## The purity boundary

The decision engine takes a `DecisionRequest` (config + model + shard samples + `now` +
`last_decision_at` + optional measured `minutes_to_capacity`) and returns a `ShardDecision`. It never
imports `httpx`, never reads a clock, never logs. `now` and timestamps are supplied by the caller.
This is enforced by review and by the test that asserts identical output for identical input.

The actuator is the **only** component permitted to call the Solace Cloud REST API. In `recommend`
mode it is never constructed.

## Interaction with a consumer autoscaler (KEDA)

This tool scales **brokers**. A consumer autoscaler such as the
[KEDA Solace scaler](https://keda.sh/docs/2.20/scalers/solace-pub-sub/) scales **consumers**. In a
system that runs both, they are two independent control loops observing overlapping signals with no
awareness of each other - the classic setup for oscillation.

**The coupling.** KEDA scaling consumers *up* (because a queue is backing up) increases egress load
on the broker, which pushes broker utilisation toward the ceiling this tool watches. This tool then
adds a broker. Adding a broker changes the queue distribution - the very signal KEDA is measuring -
so KEDA re-evaluates, and the cycle can repeat. Neither loop knows the other exists.

**Symptoms.** Broker count and consumer replica count that ratchet up together and never settle;
scale actions that each look locally correct but chase each other; a fleet that grows under a load
that a stable configuration would have absorbed.

**Mitigations** - the goal is to make the two loops operate on clearly separated timescales so the
slower one (brokers) cannot chase the faster one (consumers):

- **Asymmetric windows.** Scale *up* deliberately and scale *down* slowly. This tool already derives
  `scale_up_window` from provisioning time and holds a long `scale_down_window` before removing a
  broker, so a transient consumer-driven spike does not immediately add a broker, and a dip does not
  immediately remove one.
- **Cooldowns.** `policy.cooldown` suppresses a second broker action for a fixed period after one
  fires, giving the consumer loop time to converge before this tool reacts again.
- **Order the timescales explicitly.** Set this tool's `scale_up_window` **longer than** the KEDA
  `cooldownPeriod`. The broker loop must be the slower of the two: consumers should reach a stable
  count for the current broker fleet *before* this tool concludes the broker itself is the ceiling.
  If the broker loop reacts faster than the consumer loop settles, it will add brokers in response
  to load that the consumer loop was already resolving.

A simulator scenario (`simulator/workload.py::consumer_reaction_window`) models a consumer count
rising in response to backlog and asserts the decision engine does not oscillate under it; see
`tests/test_simulator.py`.

## Current integration boundary

The diagram describes the intended composition. The CLI recommendation/monitor paths do not call
an actuator, maintain a warm pool or execute Terraform. Prometheus and Cloud API metrics collectors
are placeholders. The separate opt-in `run` controller connects managed queue handover and optional Cloud warm
provisioning with persistent reconciliation state. See [the simple guide](autoscaler-simple-guide.md).

## Managed automatic path

`run` reads per-partition queue counters, selects a fitting move and advances a persisted migration. It shares ownership state with `serve`. Worker discovery pre-binds destination consumers; broker ingress fencing rejects stale publishers; the outbox retries after cutover. The controller is a separate loop from the pure recommendation engine and uses its own trigger/target settings. See [automatic scaling](automatic-scaling.md).

## Native topics and the application shim

`MessagingClient` derives the key from configured topic levels, selects the recorded owner and
publishes one owner-prefixed SMF topic. `TopicRegistry` persists subscription groups. `QueueManager`
installs their native queue subscriptions and fences every group during a partition move.
Solace performs fanout; subscriber shims discover group queues and restore the logical topic.
See [native messaging](native-messaging.md) for guarantees and limits.
