# Before production

Status: suitable for a bounded v1 customer alpha on dedicated, isolated non-production brokers; not yet qualified for unattended production payments. Follow the [customer-alpha runbook](customer-alpha.md) for the supported Python SMF starting path, acceptance evidence, and safe shutdown.

Python SMF and Go AMQP share the controller's persisted partition ownership and topic/subscriber contract. The managed Go `messaging` package adds a synced durable outbox, bounded per-partition delivery, native subscription discovery, reconnect and migration recovery. The legacy Go rules/spine path remains separate. Capability/domain placement beyond the managed baseline is experimental, capacity remains customer-specific and uncalibrated, and durability-preserving Go batching remains an open production gate.

## Required release gates

| Priority | Gate | Evidence needed |
|---|---|---|
| P0 | Choose one supported end-to-end path | Choose managed Python SMF or managed Go AMQP and qualify that exact client/protocol combination. Both have local migration evidence; legacy Go topology routing is a different contract. |
| P0 | Prove message outcomes under failure | Kill publisher, consumer and controller during every migration phase; interrupt ACK delivery and networking; fill disks; restart brokers. Reconcile accepted event IDs against durable business effects: no missing accepted events and harmless duplicates. |
| P0 | Measure the complete system | Sustained load and long soak at the actual payload mix, fanout, account skew, queue count and subscriber count. Record publication acceptance latency, broker receipt latency, processing latency, migration pause and recovery time. Set explicit pass/fail SLOs before the run. No current run establishes saturation or soak behavior. |
| P0 | Remove the managed Go durable-storage bottleneck | The [bounded filesystem profile](router-performance-review-2026-09-23.md) measured only 61–68 messages/s for a 4 KiB enqueue-plus-confirmed-delete cycle because each message pays two synchronous bbolt commits. Implement durability-preserving batching or select a qualified store, then repeat crash/order tests and a steady-state saturation matrix before setting a throughput SLO. |
| P0 | Complete Cloud operations qualification | Prior isolated runs exercised creation, readiness, quota rejection, TLS workload access and exact-ID deletion. No Enterprise 100K quota was available. Do not create more Cloud resources while billing/credit status is unknown; once authorized, test permission errors, API timeout crash windows, credential rotation, and the repaired end-to-end workflow without touching existing services. |
| P0 | Qualify feature combinations through managed migration | [Native transactions, XA, ordinary-queue replay, tracing and HA](native-feature-evidence.md) passed bounded independent tests, but the managed handover path has not exercised transaction/XA state, replay-active catch-up, trace completeness across cutover, or HA failover interaction. DR remains not tested. |
| P0 | Recovery and ownership | Define RPO/RTO, back up and restore controller state and publisher outboxes, verify single-writer enforcement, document disk/host loss and rollback. Multi-host controller failover is not implemented. |
| P0 | Security and operations | TLS verification, least-privilege broker/Cloud/API identities, secret rotation, authenticated control topics, alerts and audited operator recovery. Provide on-call procedures for stalled drain, unavailable capacity, full outbox and expired telemetry. |
| P1 | Capacity accounting and cost | Calibrate original publication versus subscription-copy counters against measured profiles; validate polling at fleet scale and consumer bottleneck detection. Define a total fleet budget, since current broker ceilings apply per workload. |
| P1 | Policy lifecycle | Exercise additive workload rollout, queue readiness and mixed client versions. Define supported subscriber/route migrations and rollback; existing ordering contracts cannot be silently rewritten. |

Start with one isolated workload, spare capacity and a small canary. Expand only after the gates pass. Automatic scale-in, copying stored backlog and arbitrary existing queues remain outside the initial supported scope. The optimizer is deterministic and constraint-aware, but functional reduced-threshold runs do not train or validate an ML capacity model and must not be used to infer one.

The 22–23 September 2026 Cloud qualification found no Enterprise 100K quota, then successfully exercised the managed two-service handover on two Enterprise 5K HA services: 112 accepted events reached both subscriber groups with zero duplicates and preserved per-account order through publisher SIGKILL/restart. The largest available single-service tier, Enterprise 10K HA, also completed four bounded AMQP-over-TLS payload/fanout/slow-consumer cases with no missing IDs, duplicates or per-account order violations. The sequential remote-client result validates behavior, not broker throughput or capacity. All exact-ID services were deleted. See [the qualification record](cloud-qualification-2026-09-22.md).

## Improvements included in the main integration

- Kept main's Go rules, topology, event spine, Mission Control identifiers and reorganized folders.
- Added the simple YAML, durable native SMF client, measured capacity workflow and broker-fenced migration.
- Fixed Go AMQP acknowledgement-before-processing: the application now explicitly acknowledges or releases deliveries.
- Added bounded asynchronous Go publishing with observable receipts, input copies, backpressure and stop-on-failure behavior.
- Marked outgoing AMQP messages durable.
- Stopped Go assignment fallback after authorization/policy rejection and separated protocol caches.
- Authenticated the topology endpoint and refused topology hashing in managed native mode.
- Kept both Python and Go checks in CI, including real-broker migration and AMQP redelivery.

## Go qualification and limits

The managed `messaging` client has durable publication and uses the recorded migration contract. Its real two-broker test covers native fanout, source fencing, publisher SIGKILL with pending events, recovery on the same disk and ordered cutover. Receiver links share broker connections. These tests do not establish throughput, disk-loss recovery, every migration failure phase or production HA behavior.

The legacy `dispatch.AsyncPublisher` remains memory-only. Its topology/event-spine behavior is not the managed path. Neither client creates a global business order across concurrent publisher processes. Poison events block a managed queue and require operator intervention; no automatic discard is enabled.

## Verification of this integration

The combined Python unit suite, Go race tests, lint/type checks and real-broker tests are run before publication. Real-broker coverage includes native topic fanout, ordered cutover, an ordered burst during a controller outage, legacy migration and Go AMQP redelivery after a receiver closes without acknowledging. These functional checks are not a throughput or production availability certification.

Integration evidence (18 September 2026): **368 Python tests passed, 4 optional tests skipped; 2 real-broker Python migration tests passed in 5.95 seconds.** Go tests passed with the race detector, including the real AMQP settlement test when its fixture was enabled. Ruff, mypy, go vet and go build passed. The project page was checked in a browser and its YAML copy control worked. Production Cloud mutations were not performed.

Managed Go addition (21 September 2026): the local manager scenario reconciles 112 accepted event IDs in each of ledger and audit, including 24 buffered through publisher SIGKILL and migration. It uses real AMQP and SEMP, with a deliberately reduced demo threshold. CI repeats the scenario with the Go race detector. See [demo evidence and limitations](../examples/manager-demo/README.md).

Local checks for that addition: **371 Python unit tests passed, 4 optional skips; four live-broker integration tests passed**, including Go-to-Python and Python-to-Go native payload interoperability. Go race tests included real AMQP settlement and pooled-link independence; vet/build and Python lint/type checks passed. The manager presentation's replay, crash step and final reconciliation were checked in a browser. Subsequent isolated Cloud lifecycle and functional workload checks are recorded above; production capacity and full failure qualification remain open.

The 23 September follow-up added real local static 2/3/4-broker routing evidence, open-loop multiprocess workload measurements, conservative fanout attribution, lifecycle crash-recovery hardening and a bounded [automatic 1→2→3→4 scale-out run](automatic-scaleout-evidence-2026-09-23.md). In that automatic case, real queue/VPN telemetry drove three `Controller.tick()` decisions; 1,752 accepted IDs produced 3,504 reconciled two-group deliveries, and post-cutover bursts reached every new owner including the fourth broker. The capacity profile and thresholds were invented solely for functional triggering, so these results remain below-saturation behavior evidence and do not set production capacity, Cloud provisioning or availability expectations. See the [feature qualification matrix](feature-qualification-matrix-2026-09-23.md).

A separate bounded native harness qualified local JMS transactions, rollback/redelivery, open-transaction client crash, XA prepare/commit/rollback and prepared-branch restart recovery, ordinary exclusive-queue replay followed by new live traffic, exact-ID native tracing, and eight-message three-node Standard HA failover. See the [native feature record](native-feature-evidence.md) and [sanitized aggregate](../examples/qualification-evidence/2026-09-23/native-features.json). Those results prove the listed broker semantics only: they do not qualify those features during managed migration, HA transaction state, replay overlap, failback, DR, saturation or soak. Customer trials must match broker class/version, client, profile and feature combination; the TLS demo is only a baseline.

The [router performance review](router-performance-review-2026-09-23.md) is a user-facing release gate: on this local filesystem, a normal-sync 4 KiB outbox enqueue plus confirmed deletion sustained 61–68 messages/s. The test isolates synchronous durable storage as the dominant local cost; it is not broker capacity, but production throughput claims should wait for a durability-preserving design change and requalification.

The bounded Cloud runner performs in-process and workflow-failure cleanup from an exact-ID journal. A hosted-runner or control-plane disappearance can terminate both the workload and its cleanup step, and there is no independent external reaper in this repository. Therefore the validated budget is an exposure bound, not a provider-enforced spending cap. Retain the private journal outside an ephemeral runner or add an independently hosted cleanup/reconciliation job before unattended use.
