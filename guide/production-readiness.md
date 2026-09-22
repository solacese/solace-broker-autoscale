# Before production

Status: suitable for an isolated pilot; not yet qualified for unattended production payments.

Python SMF and Go AMQP now share the controller's persisted partition ownership and topic/subscriber contract. The managed Go `messaging` package adds a synced durable outbox, bounded per-partition delivery, native subscription discovery, reconnect and migration recovery. The legacy Go rules/spine path remains separate.

## Required release gates

| Priority | Gate | Evidence needed |
|---|---|---|
| P0 | Choose one supported end-to-end path | Choose managed Python SMF or managed Go AMQP and qualify that exact client/protocol combination. Both have local migration evidence; legacy Go topology routing is a different contract. |
| P0 | Prove message outcomes under failure | Kill publisher, consumer and controller during every migration phase; interrupt ACK delivery and networking; fill disks; restart brokers. Reconcile accepted event IDs against durable business effects: no missing accepted events and harmless duplicates. |
| P0 | Measure the complete system | Sustained load and long soak at the actual payload mix, fanout, account skew, queue count and subscriber count. Record publication acceptance latency, broker receipt latency, processing latency, migration pause and recovery time. Set explicit pass/fail SLOs before the run. |
| P0 | Validate actual Cloud operations | Exercise creation, readiness, quotas, permission errors, API timeouts, regional endpoints and credential rotation in an isolated organization. Verify reconciliation does not create duplicate services. Cloud provisioning currently has mocked coverage. |
| P0 | Recovery and ownership | Define RPO/RTO, back up and restore controller state and publisher outboxes, verify single-writer enforcement, document disk/host loss and rollback. Multi-host controller failover is not implemented. |
| P0 | Security and operations | TLS verification, least-privilege broker/Cloud/API identities, secret rotation, authenticated control topics, alerts and audited operator recovery. Provide on-call procedures for stalled drain, unavailable capacity, full outbox and expired telemetry. |
| P1 | Capacity accounting and cost | Calibrate original publication versus subscription-copy counters against measured profiles; validate polling at fleet scale and consumer bottleneck detection. Define a total fleet budget, since current broker ceilings apply per workload. |
| P1 | Policy lifecycle | Exercise additive workload rollout, queue readiness and mixed client versions. Define supported subscriber/route migrations and rollback; existing ordering contracts cannot be silently rewritten. |

Start with one isolated workload, spare capacity and a small canary. Expand only after the gates pass. Automatic scale-in, copying stored backlog and arbitrary existing queues remain outside the initial supported scope.

The 22 September 2026 Enterprise 100K Cloud attempt passed region/version/service discovery but was blocked before allocation by the organization's 100K service-class limit. No service was created, so it adds control-plane validation but no throughput or Cloud handover evidence. See [the qualification record](cloud-qualification-2026-09-22.md).

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

Local checks for that addition: **371 Python unit tests passed, 4 optional skips; four live-broker integration tests passed**, including Go-to-Python and Python-to-Go native payload interoperability. Go race tests included real AMQP settlement and pooled-link independence; vet/build and Python lint/type checks passed. The manager presentation's replay, crash step and final reconciliation were checked in a browser. Cloud operations and production load qualification remain untested.
