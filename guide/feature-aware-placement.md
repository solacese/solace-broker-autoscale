# Feature-aware placement: evidence and first safety boundary

Status: the controller now fails closed before automatic movement when a workload declares broker-local features that the managed handover has not qualified. This is a correctness gate, not a capacity model and not a claim that unsupported combinations can never be implemented.

## What the broker guarantees

| Feature | Documented boundary | Placement consequence |
|---|---|---|
| Local transactions | A local transaction is an atomic unit of Guaranteed-message operations using one broker resource. Direct messages are excluded. | Treat the complete transaction as indivisible. The controller cannot infer an open/settled transaction boundary from queue depth, so automatic migration is pinned. |
| XA transactions | A branch is atomic and may participate with other networked resources under an external transaction manager. Operations block; Solace does not supply that manager. | The existing queue drain cannot prove a distributed transaction is resolved. Automatic migration is pinned. |
| Persistence | The managed controller supports Guaranteed messages only. It fences ingress and waits for ready, unacknowledged and spooled state to drain before changing ownership. | Direct delivery is rejected at controller startup. Persistent backlog remains on the source and is never copied. |
| Fanout | Each durable subscriber group owns an independent queue copy. The measured profile retains ingress and egress at the requested fanout. | Every group queue is prepared, fenced and drained as one partition boundary; fanout is capacity amplification, not permission to split the group set. |
| HA | One messaging node is active and carries client traffic; its mate is passive and carries no messaging traffic. Guaranteed messages/state and configuration are synchronized for takeover. | One HA service is one active capacity unit. Do not count its internal standby as another broker. |
| DR replication | Same-named Message VPNs form active and standby sites. Guaranteed messages, acknowledgements and transaction outcomes can be replicated. Site switching is operationally distinct from HA failover. | A DR peer is protection capacity, not ordinary active or warm autoscaling capacity. The controller records `role: dr` but never places or migrates to it. |
| Replay | Replay is per Message VPN, retains publication order across topics and targets the requested endpoint. Native Solace partitioned queues have separate replay restrictions. | This controller creates ordinary queues per application partition; those are **not** native Solace partitioned queues. Replay remains pinned because catch-up and migration interactions are unqualified here, not because the application partitioning automatically inherits the native restriction. |
| Distributed tracing | Broker tracing emits spans for receive, enqueue, delivery, acknowledgements, deletion and movement. Local-transaction publications do not generate trace messages. | Tracing has measured standalone profiles but no migration qualification here. It remains pinned rather than receiving an invented multiplier. |

Official references:

- [Using Local Transactions](https://docs.solace.com/API/API-Developer-Guide/Using-Local-Transactions.htm)
- [Using XA Transactions](https://docs.solace.com/API/Solace-Jakarta-API/Using-XA-Transactions.htm)
- [Configuring Queues](https://docs.solace.com/Messaging/Guaranteed-Msg/Configuring-Queues.htm)
- [High Availability for Software Event Brokers](https://docs.solace.com/Features/HA-Redundancy/SW-Broker-Redundancy-and-Fault-Tolerance.htm)
- [How Replication Works](https://docs.solace.com/Features/DR-Replication/How-Replication-Works.htm)
- [Synchronous and Asynchronous Message Replication](https://docs.solace.com/Features/DR-Replication/Sync-Asynch-Replication.htm)
- [Message Replay](https://docs.solace.com/Features/Replay/Message-Replay-Overview.htm)
- [Distributed Tracing](https://docs.solace.com/Features/Distributed-Tracing/Distributed-Tracing-Overview.htm)
- [Distributed Tracing Version Compatibility](https://docs.solace.com/Features/Distributed-Tracing/Distributed-Tracing-Receiver-Versions.htm)

## Configuration and behavior

The small business policy remains unchanged. Declare advanced evidence in its referenced operator profile:

```yaml
automation:
  shards:
    payments:
      features:
        transactions: none             # none | local | xa
        replication: none              # none | disaster-recovery
        replay: false
        tracing: false
```

`capacity.scenario: replay` or `tracing` also declares that feature. This prevents a measured capacity choice from accidentally bypassing the migration gate. `scenario: worst` does not mean replay and tracing together; it remains a conservative envelope of separate observations.

The controller persists this feature contract beside its routing and topic contracts. Existing shard requirements cannot change on restart, because doing so could reinterpret durable ownership or an in-flight handover. Adding a new shard is allowed. Upgrading a non-empty pre-contract database fails closed; review the normalized contract and run `solace-autoscale adopt-feature-contract --config POLICY --yes` once rather than silently asserting that historic traffic used no broker-local feature.

When sustained load would otherwise cause a move, an unqualified shard returns `feature-pinned` with a stable reason and leaves warm services, queue ingress and ownership unchanged. The planner still handles the supported guaranteed/persistent baseline deterministically. Compatibility is checked once before planning and again immediately before durable migration intent is recorded.

## Capacity roles

- **Active:** receives traffic and counts toward observed runtime capacity.
- **Warm:** a provisioned, ready service that can be activated as a placement destination. It is billed and consumes the workload's broker ceiling.
- **HA standby:** an internal passive member of one HA service. It is not an inventory row or separate throughput unit.
- **DR:** a separately inventoried protection site. It is excluded from active telemetry, queue bootstrap, candidate selection and warm-pool activation.

## Deterministic and constrained baselines

The production planner now exhaustively evaluates the finite legal **one-move** neighborhood. This is exact for the action the controller can execute safely; it does not persist a speculative batch. Hard capability, domain, role, pin, capacity and broker-count constraints are filtered first. Remaining assignments are compared lexicographically by overloaded axes, fleet peak, excess above target, warm activation, migration disruption, scarce-capability use, requested domain spread and stable IDs.

A dependency-free exhaustive global solver is available only for small synthetic validation manifests:

```bash
solace-autoscale placement-compare --manifest examples/placement/cross-axis.json
```

It independently validates complete assignments, refuses a state space above its configured bound, and reports both the exact one-step result and the global optimum. This exposes limitations of repeated local decisions without letting an offline plan bypass fresh telemetry or the durable handover. Unknown features or combinations produce no valid production move.

## Empirical gates before expanding support

- Observe and deliberately interrupt local/XA transactions during every handover phase; prove that no open or in-doubt transaction crosses ownership.
- Measure replay catch-up with concurrent live traffic, retention pressure and HA events on the ordinary queues created per application partition. Do not conflate these queues with native Solace partitioned queues.
- Measure tracing overhead and span completeness across enqueue, delivery, migration, HA and DR failures using the exact deployed broker, collector and API versions.
- Exercise synchronous/asynchronous DR link loss, backlog, reject-on-sync-ineligible, failover and failback. Define RPO/RTO and prove message reconciliation.
- Measure feature combinations directly. Separate replay and tracing profiles do not establish replay-plus-tracing capacity.

Use [qualification-manifest.example.json](../examples/placement/qualification-manifest.example.json) as the versioned checklist for real runs. Its zero values are unset criteria, not measurements. Generated results must include the capacity-model digest and remain outside Git when they contain real/customer data.

Until those tests exist, the gate remains conservative and no predictive model may override it. ML is intentionally deferred; if added later, it may estimate performance and uncertainty only, while these hard constraints remain authoritative.
