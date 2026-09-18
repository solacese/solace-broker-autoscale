# Product review: purpose, readiness and configuration

Review date: 18 September 2026. This is a review of the current local implementation, not a production certification. The original findings below describe the reviewed baseline; the implementation update records what changed.

## Implementation update

The current branch now has independent asynchronous broker-receipt delivery, bounded partition concurrency, bounded stale ownership, relevant-workload subscription discovery, a shared consumer handler pool, additive route checks and a working short YAML compiler with read-only `explain`. Spool backlog is no longer subtracted as if a move transferred stored messages. See [the current policy guide](simple-policy.md).

Real local two-broker native topic and legacy migration tests passed after these changes. The original blockers below remain useful context, but the first two are no longer descriptions of the current delivery loop. Queue telemetry batching, full fanout calibration, production load and operational qualification remain open. This is still a controlled-pilot implementation, not certified unattended payment infrastructure.

## Decision

Keep the project, narrow its first supported use case, and pause feature expansion while hardening delivery and operations. The strongest product promise is: define business topics and ordering boundaries, then let the system place and move divisible workload across a managed Solace broker fleet.

Current classification: engineering prototype suitable for a controlled lab pilot. It is not ready for unattended customer payment traffic. Unit coverage and successful two-broker handover tests prove specific behavior, not sustained throughput, latency or operational availability.

## Where horizontal scaling helps

| Situation | Reason to spread load | Required condition |
|---|---|---|
| Sustained payment burst | Independent account partitions exceed available broker capacity | Consumers have headroom, spare brokers exist, migration latency fits the workload |
| New business domains | Traffic and isolation needs grow across multiple workloads | Planned onboarding and separate policies without disrupting current routes |
| Expanding device fleet | Many independent device streams increase aggregate message/byte rates | Keys distribute adequately and connection/queue limits are separately validated |

A queue backlog is not by itself a reason to add brokers. A slow database, saturated consumer, stalled audit group or indivisible hot account can remain the bottleneck. Short bursts may pass before a safe migration completes. Buffering and reserved capacity are part of the response.

## Scope relative to native Solace capabilities

Solace partitioned queues distribute key-related messages among consumer flows on a broker. They are a candidate for reducing application-managed consumer machinery, not an automatic substitute for cross-broker ownership migration. Evaluate them against the current design with a prototype and benchmarks.

DMR handles subscription-driven forwarding between brokers. It is not proof that our application sharding design transparently interoperates with an existing mesh, nor a drop-in load-aware queue migration system. Solace's documentation distinguishes DMR clustering from typical Cloud service topology.

The current shim uses native SMF and queue subscriptions, but adds owner-prefixed wire topics and a managed application contract. Position this accurately as a managed broker-fleet layer. Existing arbitrary applications do not adopt it automatically.

Primary references:
- https://docs.solace.com/Messaging/Guaranteed-Msg/Partitioned-Queue-Messaging.htm
- https://docs.solace.com/Get-Started/dynamic-message-routing.htm

## Readiness blockers observed in code

1. **Publisher throughput architecture.** `MessagingClient._cycle()` calls `DurableOutbox.flush()` once per outbox, whose default limit is 100 attempts, and the background loop waits one second by default after each cycle. Each send synchronously waits for acknowledgement. For a continuously backlogged single outbox, this implies fewer than about 100 attempts/second in steady state at defaults, before other I/O. This is a code-derived bound, not a benchmark. Separate delivery scheduling from policy refresh, support bounded asynchronous in-flight sends while preserving per-key order and durable acknowledgement bookkeeping, then measure the result.
   - `adapters/python/solace_autoscale_client/messaging.py:201`
   - `adapters/python/solace_autoscale_client/outbox.py:75`
   - `adapters/python/solace_autoscale_client/managed_smf.py:72`

2. **Control-plane outage blocks delivery.** `_cycle()` refreshes policy and synchronizes consumers before flushing. A failed control-plane call prevents that cycle's send work even if a previously known broker is healthy. Introduce an explicit bounded stale-policy strategy with persisted ownership versions and broker-enforced fencing. Never solve this by ignoring fences or routing-contract mismatches.

3. **Queue, thread and management-call growth.** Each group gets queues for every managed partition, including unrelated shards. Each subscribed worker opens a receiver thread for each discovered location. With 128 partitions and 10 groups, the current model creates 1,280 owned group queues per shard before old-owner queues and migration copies. The queue monitor performs three SEMP requests per group queue, which would be 3,840 requests per full scrape in that example. Replica counts further increase receivers/threads. Scope groups to relevant routes, bound workers and batch telemetry. Benchmark native partitioned queues as an alternative within each broker.
   - `src/solace_autoscale/assignment/service.py:253`
   - `src/solace_autoscale/controller/semp.py:153`
   - `src/solace_autoscale/controller/semp.py:220`
   - `adapters/python/solace_autoscale_client/managed_smf.py:123`

4. **Capacity semantics need qualification.** The native queue manager sums copies across subscription groups. The controller treats their rate and byte deltas as capacity inputs and also uses configured fanout. Verify original-publication, delivery-copy, ingress and egress units against the imported profile to avoid double counting. The planner includes spool pressure in a move vector, while migration drains existing backlog on the source instead of copying it. Distinguish movable future traffic from backlog that will stay on the old broker during the handover. Consumer processing rate and connection pressure need explicit treatment.
   - `src/solace_autoscale/controller/runtime.py:216`
   - `src/solace_autoscale/controller/planner.py`
   - `src/solace_autoscale/controller/migration.py`

5. **Safe onboarding is missing.** The durable contract correctly rejects changed topic routes, including additions. That prevents silent remapping, but does not implement the desired new-use-case onboarding flow. Add a versioned additive policy update with overlap checks, resource preparation, readiness and rollback rules. Never tell users to delete their database to update routing.
   - `src/solace_autoscale/assignment/topics.py:61`

6. **Operational qualification is incomplete.** Cloud creation remains mock-tested. Validate actual Cloud provisioning, credential rotation, TLS/network failures, restart/disk-loss behavior, consumer failures, duplicate handling and kill-switch recovery. Define and measure acceptable publish latency, handover pause, backlog recovery and sustained rates. Test the shim and controller together with the real profile, not only the broker in isolation.

## A simpler customer contract

The native example currently has 81 lines before the separate inventory. It exposes multiple overlapping configuration blocks, duplicate global/per-shard thresholds and internal details such as partition count, observation periods and local state paths. The problem is the public abstraction, not YAML itself.

Two layers should exist:

- A platform connection profile, configured once by an operator, owns Cloud credentials, network, broker tier/version, measured profile and durable controller deployment.
- A short application policy owns business topic families, ordering intent, subscriber groups and broker limits. The tool expands versioned defaults into an inspectable effective configuration. Advanced overrides remain available for qualified operators.

**Proposed customer-facing design, not currently executable:**

```yaml
version: 1
connection: production-solace
scaling:
  mode: automatic
  max_brokers: 4
  warm_brokers: 1
workloads:
  payments:
    topic: payments/{account}/{event}
    keep_together: account
    subscribers:
      ledger: payments/>
      audit: payments/*/created
```

Named topic fields would compile to native wildcard matching and key extraction. Names express intent more clearly than numeric topic indexes. This requires a real parser/compiler change, compatibility checks and a migration plan. It cannot be implemented by merely changing the example file.

Ordering, no silent loss, positive acknowledgement and safe drain checks remain enforced invariants. They should not become checkboxes a customer can accidentally disable. Unknown options still fail validation. A generated explanation should show all effective thresholds, reserved headroom and resource estimates.

## Feature decisions

| Decision | Scope |
|---|---|
| Keep | Native topic fanout, business-key locality, durable ownership, bounded outbox, fenced migration, measured profiles |
| Fix first | Delivery scheduling, control-plane resilience, fanout/resource accounting, group scope, safe configuration evolution |
| Add next | Policy validation and preview, explain decisions, actionable runtime status, bottleneck diagnosis |
| Hide from normal setup | Hash choice, partition IDs/count, cache leases, SEMP polling details, database paths, duplicated thresholds |
| Defer | More messaging protocols, DMR automation, multi-region placement, speculative priorities and complex presets |
| Later release | Automatic scale-in/hibernation after safe scale-out and recovery qualification |

Do not delete lower-level code merely to simplify the product. Keep it internal or in advanced documentation until it has a supported customer use case.

## Release gate

A first supported release should target one homogeneous managed Solace Cloud fleet, SMF guaranteed delivery, topic-derived ordering and a small supported shim surface. First complete a representative end-to-end load test and failure campaign, then a monitored Cloud pilot. Publish measured limits for rates, payloads, fanout, groups, partition counts, pauses and recovery. Only then describe the system as ready for unattended production operation.
