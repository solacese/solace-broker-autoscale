# Before and after an intelligent broker split

These runnable examples use the current constrained planner. All percentages are **synthetic fractions of capacity**, not measured 100K throughput. They illustrate decisions after sustained overload; they do not prove Cloud performance or migration safety. The planning trigger is 80%, and a destination must stay within 65% on every capacity axis.

## 1. A payment burst

Publishers use `payments/{account}/{event}`. Hashing the account selects a stable partition; the controller records which broker owns that partition. Suppose two partition bundles now consume 50% and 45% of message capacity on A.

| Message capacity used | Before | After |
|---|---:|---:|
| Broker A | 95% | 50% |
| Broker B, initially warm | 0% | 45% |

The planner moves partition 1, including its processor and audit queues. Every account mapped to that partition follows it. It does not alternate individual payments between brokers, and it cannot split an indivisible hot account simply to get a better balance. Account names in a demo must be resolved with the actual routing hash rather than assigned arbitrarily.

The publisher shim resolves the recorded owner and sends directly to it. During the handover it retains unacknowledged publications in its durable outbox. The subscriber shim discovers and binds the destination queues. Source queues drain before ownership changes; retries require application event-ID deduplication. This is not global ordering across independent publishers.

## 2. More activity over time

A managed workload grows as more customers or applications publish into its configured topic family. Three existing partition bundles consume 35%, 30% and 25% of message capacity.

| Message capacity used | Before | After |
|---|---:|---:|
| Broker A | 90% | 55% |
| Broker B, initially warm | 0% | 35% |

The planner moves partition 0 to B and leaves the other two on A. Business topic names and subscriber group names stay the same. The current runtime optimizes within a shard and uses a homogeneous broker tier. This example does not claim arbitrary placement of separate workloads onto a shared heterogeneous fleet. New topic contracts still need explicit configuration; the controller does not guess business affinities from names.

## 3. Large payloads saturate bandwidth

A contains a byte-heavy partition (20% messages, 60% bytes) and a smaller-message partition (30% messages, 35% bytes). B already carries unrelated operator-attributed fixed load (45% messages, 4% bytes).

| Broker | Before: messages / bytes | After: messages / bytes |
|---|---:|---:|
| A | 50% / 95% | 30% / 35% |
| B | 45% / 4% | 65% / 64% |

The planner moves the byte-heavy partition 0. Moving partition 1 instead would push B to 75% message capacity, above the destination target. Message count alone would miss the reason for this choice. Fixed load in this example is supplied by the operator; it is not automatically learned by the runtime.

## When the correct decision is to refuse

If the workload declares local/XA transactions, DR replication, replay or tracing, the current compatibility gate pins it. A DR standby is never used as a warm scale-out destination. This is a documented limitation, not an optimized transaction-aware split. Qualification of transaction boundaries, feature interactions and failure recovery is required before changing that gate.

## Reproduce the decisions

Install the controller from the repository root, then run:

```sh
solace-autoscale placement-compare --manifest examples/placement/customer-scenarios/payment-burst.json
solace-autoscale placement-compare --manifest examples/placement/customer-scenarios/gradual-growth.json
solace-autoscale placement-compare --manifest examples/placement/customer-scenarios/large-payload-burst.json
```

Each command compares the current one-move planner with a bounded exact solver. For actual queue movement and crash recovery, use the [two-broker manager demo](../examples/manager-demo/README.md). That demo is functional evidence with a reduced capacity budget, not a 100K benchmark.

## Cloud qualification before a customer demonstration

Enterprise 100K is a service class with connection and resource limits, not a message-throughput promise. See [Solace service-class limits](https://docs.solace.com/Cloud/service-class-limits.htm).

1. Provision an isolated Enterprise 100K HA service in the approved region. Record exact broker/API versions, protocols, features, price basis and lifetime budget. Use synthetic payloads and dedicated credentials.
2. Establish a single-service baseline with small, medium and large payloads; fanout 1 and 2; steady traffic and bursts; fast and slow consumers. Record accepted/persisted/processed unique IDs, duplicates, order violations, throughput, p95/p99 latency, spool, connections and client-side bottlenecks.
3. Repeat with individual supported features and selected combinations. Transaction commit/rollback and DR failure behavior need dedicated clients/topology; do not infer them from ordinary queue counters.
4. Add a second independent matching service for split tests. A single HA service's standby is not that second service. Compare the same offered workload before/after, including handover pause and recovery time. A third service is needed to validate a three-service outcome.
5. Inject publisher/controller restarts, temporary connectivity loss and slow consumers during each handover phase. Verify ownership, event reconciliation and bounded buffering; define acceptance thresholds before the run.
6. Delete only the test services after authorized cleanup, verify deletion, and retain redacted reports with configuration/model digests. Keep raw account data and credentials out of Git.

A successful single-service behavior test is useful but does not qualify splitting, disaster recovery, or production readiness. Machine sizing and load-generator placement must be sufficient to identify the broker bottleneck instead of measuring a laptop or WAN link.
