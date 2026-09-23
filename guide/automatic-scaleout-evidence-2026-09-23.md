# Automatic 1→2→3→4 scale-out evidence — 23 September 2026

## Result

A bounded local run automatically activated three pre-provisioned warm brokers in sequence. The production `Controller.tick()` path used real monotonic time, real managed-queue cumulative counters, real VPN totals, the constrained planner and the production migration engine. It moved partition 9 from broker A to B, partition 4 from A to C, then partition 2 from A to D. No assignment or owner was changed manually.

The final run passed in **94.14 seconds**:

```text
Broker 28400: message spool and durable queues ready
Broker 28401: message spool and durable queues ready
Broker 28402: message spool and durable queues ready
Broker 28403: message spool and durable queues ready
.                                                                        [100%]
1 passed in 94.14s (0:01:34)
```

Raw machine-readable evidence is retained under ignored `state/claude-directed-qualification/automatic-scaleout-20260923-133601/`. The compact sanitized aggregate is [`local-automatic-scale-out.json`](../examples/qualification-evidence/2026-09-23/local-automatic-scale-out.json).

## Exact checks

- Four independent `solace/solace-pubsub-standard` containers, image digest prefix `sha256:05f80ec7bd38`, used loopback SEMP/SMF ports 28400–28503.
- Twelve partitions initially belonged to broker A. B, C and D were durable warm entries.
- The native `MessagingClient` published **1,752 original event IDs**; two identical wildcard subscriber groups produced **3,504 expected deliveries**.
- Every group received every accepted ID exactly once: **0 missing IDs, 0 duplicate IDs, 0 partition-order violations**.
- Owner cardinality progressed **1 → 2 → 3 → 4**. The three completed handovers were A/p9→B, A/p4→C and A/p2→D.
- Every handover followed `preparing → fencing → draining → activating → complete`. At completion the source was drained and ingress-disabled, while the target was ingress-enabled.
- Before each migration, real owner queue counters increased by the exact two-group copy count.
- After each cutover, a separate 24-event burst crossed all 12 partitions. Every current owner recorded four queue copies per partition (two original events × two groups). This includes broker D after the final cutover, proving the fourth owner received real traffic rather than merely appearing in the assignment store.
- Exactly one ingress-enabled group queue remained for each `(partition, group)`, and it matched the durable owner.
- Observation-only instrumentation recorded the unmodified `PlacementProblem` inputs and planner outcome in the ignored raw report.

## Functional threshold, not capacity

This run deliberately uses an **invented test-only capacity envelope** of 30 fanout-adjusted messages/s, trigger utilization 0.20 and target utilization 0.15. It exists only to make a low-volume local functional run exercise automatic decisions. It was compiled through the production profile path because the controller correctly rejects profiles marked synthetic, but every provenance field labels it invented and test-only.

Do not use this evidence to train a model, set a production threshold, infer PubSub+ Standard capacity, compare Cloud tiers or claim an SLA. A customer qualification must use the measured profile matching its provider, exact broker version, service class, payload distribution, fanout and feature set.

## Scope boundaries

- The four warm inventory entries were independent standalone Standard containers. An HA mate is not a capacity unit, and this run does not qualify HA failover.
- Publishing was paused while each handover completed, then resumed for an explicit post-cutover burst. This sequential case does not establish uninterrupted publishing under sustained overload during migration.
- Capacity was already present. This proves activation and workload movement, not Solace Cloud service creation or warm-pool replenishment.
- It does not cover automatic scale-in, backlog copying, arbitrary existing queues, DR, transaction/XA/replay/tracing interaction during migration, saturation or soak.
- Restart safety remains covered by the separate migration phase/recovery tests; it was not repeated in this bounded sequential case.

## Reproduce

Run only when native/performance qualification brokers are absent:

```bash
./scripts/qualify-automatic-scaleout.sh
```

The harness refuses to adopt existing names, checks the performance marker, binds only loopback ports 28400–28503, records exact created container IDs and removes only those IDs on every exit. Each default invocation uses a timestamped ignored result directory so a stale report cannot be mistaken for the current run.
