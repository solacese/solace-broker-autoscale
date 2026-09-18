# Before production

Status: suitable for an isolated pilot; not yet qualified for unattended production payments.

The repository now preserves both the Go rules/event-spine work and the Python managed native SMF controller. They solve overlapping problems with different ownership contracts. The simple workload YAML currently controls the Python managed path. Do not combine both ownership mechanisms for the same workload.

## Required release gates

| Priority | Gate | Evidence needed |
|---|---|---|
| P0 | Choose one supported end-to-end path | For the first release, use native SMF with the durable Python client. If Go is required, implement durable publication and the same persisted ownership/migration contract before claiming parity. |
| P0 | Prove message outcomes under failure | Kill publisher, consumer and controller during every migration phase; interrupt ACK delivery and networking; fill disks; restart brokers. Reconcile accepted event IDs against durable business effects: no missing accepted events and harmless duplicates. |
| P0 | Measure the complete system | Sustained load and long soak at the actual payload mix, fanout, account skew, queue count and subscriber count. Record publication acceptance latency, broker receipt latency, processing latency, migration pause and recovery time. Set explicit pass/fail SLOs before the run. |
| P0 | Validate actual Cloud operations | Exercise creation, readiness, quotas, permission errors, API timeouts, regional endpoints and credential rotation in an isolated organization. Verify reconciliation does not create duplicate services. Cloud provisioning currently has mocked coverage. |
| P0 | Recovery and ownership | Define RPO/RTO, back up and restore controller state and publisher outboxes, verify single-writer enforcement, document disk/host loss and rollback. Multi-host controller failover is not implemented. |
| P0 | Security and operations | TLS verification, least-privilege broker/Cloud/API identities, secret rotation, authenticated control topics, alerts and audited operator recovery. Provide on-call procedures for stalled drain, unavailable capacity, full outbox and expired telemetry. |
| P1 | Capacity accounting and cost | Calibrate original publication versus subscription-copy counters against measured profiles; validate polling at fleet scale and consumer bottleneck detection. Define a total fleet budget, since current broker ceilings apply per workload. |
| P1 | Policy lifecycle | Exercise additive workload rollout, queue readiness and mixed client versions. Define supported subscriber/route migrations and rollback; existing ordering contracts cannot be silently rewritten. |

Start with one isolated workload, spare capacity and a small canary. Expand only after the gates pass. Automatic scale-in, copying stored backlog and arbitrary existing queues remain outside the initial supported scope.

## Improvements included in the main integration

- Kept main's Go rules, topology, event spine, Mission Control identifiers and reorganized folders.
- Added the simple YAML, durable native SMF client, measured capacity workflow and broker-fenced migration.
- Fixed Go AMQP acknowledgement-before-processing: the application now explicitly acknowledges or releases deliveries.
- Added bounded asynchronous Go publishing with observable receipts, input copies, backpressure and stop-on-failure behavior.
- Marked outgoing AMQP messages durable.
- Stopped Go assignment fallback after authorization/policy rejection and separated protocol caches.
- Authenticated the topology endpoint and refused topology hashing in managed native mode.
- Kept both Python and Go checks in CI, including real-broker migration and AMQP redelivery.

## Remaining Go-specific work

The async queue is memory-only, so a process crash can lose accepted-in-memory work unless the application retains it. The event-spine cache still needs an explicit staleness/recovery policy. Listener reconnect, visible failure reporting, poison-message handling and operational topology ownership need qualification. Concurrent publishers do not create a global per-key business order.

## Verification of this integration

The combined Python unit suite, Go race tests, lint/type checks and real-broker tests are run before publication. Real-broker coverage includes native topic fanout, ordered cutover, an ordered burst during a controller outage, legacy migration and Go AMQP redelivery after a receiver closes without acknowledging. These functional checks are not a throughput or production availability certification.

Integration evidence (18 September 2026): **368 Python tests passed, 4 optional tests skipped; 2 real-broker Python migration tests passed in 5.95 seconds.** Go tests passed with the race detector, including the real AMQP settlement test when its fixture was enabled. Ruff, mypy, go vet and go build passed. The project page was checked in a browser and its YAML copy control worked. Production Cloud mutations were not performed.
