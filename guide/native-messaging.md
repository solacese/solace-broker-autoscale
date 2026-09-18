# One API for native Solace topics

Your application publishes a business topic and subscribes to a topic pattern. The Python shim selects the broker, maintains connections and consumers, buffers publications on disk, and follows controller-managed moves. Messages go directly to Solace over SMF.

```python
from solace_autoscale_client import MessagingClient

with MessagingClient(
    "https://controller.example.com",
    state_dir="./publisher-state",  # Persistent disk; unique per client process.
    credentials=credentials_for_broker,
    api_key=controller_key,
) as client:
    client.subscribe("payments/>", group="ledger", handler=commit_payment)
    client.publish("payments/account-42/created", {"amount": 25}, event_id="payment-123")
    client.flush(timeout=30)  # True means the broker acknowledged all pending publications.
```

The handler receives a `Message` with `topic`, `payload` and `event_id`. Return only after committing the business transaction. Deduplicate `event_id` in that same transaction: delivery is **at least once**, including uncertain acknowledgements and retries. `publish()` confirms local durable acceptance; it does not wait for broker delivery. A full outbox refuses new work instead of dropping it. Each shard has its own configured outbox byte limit.

Install with `pip install -e 'scaling-controller[smf]'`. A runnable application is in [native_app.py](../examples/payments/native_app.py).

## The topic decides the key

Start from [the complete native configuration](../examples/measured/payments-native.yaml), configure the fleet inventory and compile your matching performance model. Both `serve` and `run` must use the same configuration and persistent assignment database, on the same host. See [automatic scaling](automatic-scaling.md) for the startup commands and inventory setup.

```yaml
assignment:
  store: ./native-assignment.db
  routing: partitioned
  strategy: rendezvous
  partitions: 128
messaging:
  enabled: true
  routes:
    - pattern: payments/*/*
      shard: payments
      key_levels: [1]
```

Levels start at zero. For `payments/account-42/created`, level 1 is `account-42`. Events `created` and `settled` for that account use the same fixed partition and recorded broker owner. The application never supplies a partition or queue name. Topics must match exactly one route; unmatched or overlapping routes are rejected at publication time. One extremely busy account cannot be split without relaxing its ordering boundary.

| Choice | Meaning |
|---|---|
| `messaging.enabled` | Opt into managed native topic dispatch; default is off for existing integrations. |
| `routes[].pattern` | Topic family to route. Supported wildcards: whole-level `*` and terminal `>`. `>` requires at least one remaining level. |
| `routes[].shard` | The configured fleet shard that owns this topic family. |
| `routes[].key_levels` | Ordered list of topic levels forming the business key. |
| `assignment.partitions` | Fixed number of independently movable partitions per shard. |
| `assignment.strategy` / `broker_weights` | Initial placement policy; the automatic controller subsequently moves existing partitions by load. |
| `automation.migration_grace` / `empty_settle` | Minimum grace and confirmed empty period; elapsed time never permits discarding backlog. |

Use a fresh fleet/state for native mode. The controller refuses to enable it over existing legacy queue assignments or silently change the durable routing contract. Do not delete an existing state database to bypass this check.

## Solace performs the fanout

The shim publishes **one native topic message**. The controller creates durable exclusive queues for each subscription group and partition, then adds native Solace topic subscriptions. The broker does the matching and fanout:

- `ledger` subscribing to `payments/>` receives all payment events.
- `audit` subscribing to `payments/*/created` receives created events only.
- Two workers using `ledger` with the same patterns share its durable queues. Solace selects an active receiver per exclusive queue, with standby receivers for failover; each worker does not receive its own copy.
- Different groups get independent copies. Overlapping subscriptions on the same queue do not create application-side copies.

The physical topic has an internal fleet, broker-owner and partition prefix. The shim restores the original business topic for the handler. The owner prefix keeps a prepared destination from matching old-owner publications. This is native SMF and native queue subscription routing, but it is **not transparent compatibility with arbitrary existing unprefixed publishers, queues or event meshes**. DMR interoperability has not been qualified.

Registration is durable. The controller marks a group ready only after provisioning its queues and subscriptions. Publications require ready matching groups. Closing a subscriber leaves its queues and backlog intact. Group names and patterns are immutable after registration; replicas must use the same definition. Adding a new group while a migration is pending is rejected and can be retried afterward. There is no automatic group deletion.

The controller configures its managed application user's client profile to reject guaranteed publications with no matching subscription. Use a dedicated managed user: this changes its profile. All workers need access to that user's queues on every eligible broker.

## During a burst or a move

1. The controller identifies sustained overload and chooses a partition that fits spare capacity.
2. It creates destination group queues and subscriptions. Shims discover and bind destination receivers.
3. It fences ingress to **every source group queue**. Rejected or uncertain publications stay in the publisher's persistent outbox.
4. It waits for every source group to drain, including unacknowledged work, plus the grace and empty-settle periods.
5. It records the new owner, opens destination ingress, and shims retry there. Other partitions continue independently.

An offline or stuck group can prevent safe migration. The controller waits or recovers; it does not abandon that group's payments. There is no backlog copying. An ambiguous acknowledgement can cause redelivery, so deduplication remains essential. Ordering applies to sequential publications from one outbox and partition; concurrent independent publishers do not establish a global business order.

## Current scope and validation

Real two-broker tests cover native wildcard matching, multiple groups, overlapping subscriptions, exclusive replicas, source fencing, durable publisher retry and controller restart during handover. CI runs the native and legacy migration tests. Unit tests cover matching, authentication, readiness and immutable registry contracts.

This first API supports JSON payloads and a deliberately limited SMF wildcard subset, with a 128-byte logical topic limit. Broker metadata, binary payloads and every advanced SDK option are not exposed. Routes, groups and local outbox state must remain durable. A new client requires the controller to start; a controller outage retains accepted work locally, and this reference worker retries control-plane refresh before further delivery. The implementation uses one receiver per group/partition/location and needs workload and connection-count sizing before production. It is not throughput-qualified against the supplied performance profiles. Multi-host controller HA and automatic scale-in/hibernation remain outside this implementation.

## Dispatch and scaling policies in YAML

Three implemented choices let you control what can spread across brokers:

| `dispatch` | Unit kept together | Example |
|---|---|---|
| `by-key` (default) | Values extracted by `key_levels` | All events for account 42 stay together; different accounts can spread. |
| `by-topic` | The complete concrete topic | Each sensor topic stays together; different sensor topics can spread. |
| `single` | Everything matching this one route | Keep a control-command family on one partition. It can move as a whole. |

Only `by-key` accepts `key_levels`, which it requires. All three use durable hashed partitions;
multiple keys/topics can share a partition, so this does not promise one queue or broker per topic.
Changing dispatch/key extraction requires explicit state migration, not a live config edit.
Patterns must match exactly one route; overlaps are rejected at publication time.

For example, these are alternatives to the payment route in the complete configuration:

```yaml
messaging:
  enabled: true
  routes:
    - pattern: sensors/*/temperature
      shard: telemetry
      dispatch: by-topic
    - pattern: commands/>
      shard: control
      dispatch: single
  allow_dynamic_groups: false
  subscriptions:
    - group: analytics
      topics: ["sensors/*/temperature"]
    - group: command-worker
      topics: ["commands/>"]
```

Declare `telemetry` and `control` as separate shards in your broker inventory. Workers can now call
`client.subscribe(group="analytics", handler=process)`; the shim obtains the topic patterns from
YAML. Applications cannot create undeclared groups when `allow_dynamic_groups` is false. The
compatibility default is true. Removing a group from YAML does **not** delete its existing queues,
subscriptions or backlog. Changing an existing group's patterns is refused.

Each shard can inherit or override automatic balancing preferences:

```yaml
automation:
  shards:
    telemetry:
      enabled: true
      trigger_utilization: 0.85
      target_utilization: 0.65
      scale_up_window: 60s
      cooldown: 10m
    control:
      enabled: false
```

`enabled: false` stops new automatic partition moves for that shard; existing moves still finish
safely. It does not pin a named broker, disable message delivery, delete capacity, or disable the
separate Cloud warm-pool provisioner. Thresholds are fractions of measured capacity, not raw CPU
percentages. Omitted values inherit the global settings. A target must be below its trigger;
an explicit sustained window must cover at least three metric scrape intervals. Unknown
settings, unsupported dispatch names and uninventoried scaling shards are rejected.

The existing global YAML controls still set broker ceilings, warm reserve, Cloud creation,
move/hour limits, grace, empty-settle time, migration timeout and the kill switch. Topic families
requiring independent scaling rules must use distinct shards: moving a partition moves all its
messages. This release does not provide per-topic priorities or broker allowlists within one shard.

Configuration is read at process startup. Restart `serve` and `run` with the same file for policy
updates; this is not a hot-reload facility. Routing changes remain guarded by the persisted contract.
Worker state directories, handlers and credential providers remain application-specific.

## Useful next steps

These are proposals, not accepted YAML options yet:

1. **Explain and preview:** given a topic and traffic forecast, show its rule, key, partition,
   current owner and the moves a proposed policy would cause, before applying it.
2. **Hot-key diagnosis:** distinguish a busy broker from one indivisible account or a slow
   subscriber. Explain when another broker will not help.
3. **Cost and tenant budgets:** per-shard spending ceilings, reserved capacity and tenant fairness,
   with explicit backpressure when capacity or budget is exhausted.
4. **Policy rollout:** validate a new policy against recorded traffic, then apply a versioned
   change with audit history and a safe rollback boundary.
