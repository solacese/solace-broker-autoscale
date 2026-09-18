# Topics in, managed delivery out

Your application publishes business events. It does not call a subscriber or wait for a reply.

`client.publish(...)` returns after saving the event in a bounded local durable buffer. A separate delivery worker sends it to Solace. The broker's asynchronous receipt lets the shim remove its saved copy. The subscriber processes the event independently and acknowledges its queue message after its handler succeeds.

Those are transport confirmations, not request/response APIs. Removing broker receipts would prevent the shim from knowing whether it can safely delete a payment. No-receipt, best-effort publishing is not the managed guaranteed-delivery mode.

## The application policy

Use [payments.yaml](../examples/simple/payments.yaml):

```yaml
version: 1
connection: connection.yaml
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

This means: keep one account's payments on the same partition; spread independent accounts across available brokers; give ledger every payment and audit only created payments. The controller uses sustained measured load to move eligible partitions. Adding a broker does not randomly redistribute every message.

| Choice | Meaning |
|---|---|
| `keep_together: account` | Extract the named topic level as the ordering key. |
| `keep_together: [region, account]` | Use several named levels together. |
| `keep_together: topic` | Keep each complete topic together; different topics may spread. |
| `keep_together: all` | Keep the whole workload on one partition; it cannot spread across brokers. |
| `mode: paused` | Stop new moves and Cloud provisioning; finish existing handovers safely. |
| `max_brokers` | Active plus warm service ceiling **per workload**, not a fleet-wide budget. |
| `warm_brokers` | Desired ready reserve per workload, within that ceiling. |

An operator configures [connection.yaml](../examples/simple/connection.yaml) once: inventory, measured capacity file, persistent controller storage and client identity. Paths resolve relative to that file. Credentials remain environment references in the inventory. The example uses placeholder endpoints and requires a locally compiled matching measured profile. Cloud creation is off; raising a limit alone does not create brokers.

Advanced tuning belongs in the operator profile. The short policy overrides routing, guaranteed delivery, scaling mode, broker limits and subscriber definitions. Unknown fields and overlapping routes fail validation.

## Inspect before running

```bash
solace-autoscale explain --config examples/simple/payments.yaml --topic payments/account-42/created
```

This reads local files only. It shows the effective rules, partition and limits; it does not claim a current broker owner or create resources.

Run both services with the **same policy and local persistent assignment database**:

```bash
solace-autoscale serve --config examples/simple/payments.yaml
solace-autoscale run --config examples/simple/payments.yaml
```

They are separate long-running processes. Use the existing deployment and API authentication instructions in [native messaging](native-messaging.md). The simple policy enables automatic movement in the configured managed fleet when you start `run`.

Application calls remain short:

```python
client.subscribe(group="ledger", handler=commit_payment)
client.publish("payments/account-42/created", {"amount": 25}, event_id="payment-123")
```

See [the complete application](../examples/payments/native_app.py) for client construction and credentials. Subscriber definitions come from YAML. Queue readiness is required, but a subscriber does not need to be online when publishing to its prepared durable queue.

## Bursts, outages and migrations

- Delivery runs independently of policy polling. Independent partitions can have concurrent outstanding publications, with one unconfirmed head per partition to preserve retry order. Native sends request an immediate asynchronous broker receipt to avoid the idle acknowledgement batching timer; this trades extra receipt traffic for ordered-stream latency.
- During fencing or an outage, unconfirmed messages stay on disk. Receipts, including negative receipts, wake the delivery worker without waiting for the next controller poll.
- A transient controller outage can use known ownership for up to five minutes after its last successful fetch. After that, delivery waits and the buffer remains durable. Authorization or incompatible-policy errors pause delivery.
- The buffer is bounded. Full storage raises backpressure rather than discarding accepted events. It survives process restart on the same disk, not loss of that disk.
- Each consumer group receives its own copy through native Solace subscriptions. Handler execution uses a shared pool of eight workers; handlers must be bounded and must atomically deduplicate event IDs with their business transaction.
- Existing backlog drains on the original broker. A slow consumer or one indivisible hot account is not solved by adding brokers. Grace periods and spare capacity make short bursts manageable; Cloud creation is not instantaneous.

Delivery is at least once. A lost receipt can cause a retry and duplicate; this is why event IDs and idempotent handlers matter. `flush()` is an optional wait for broker acceptance, useful during tests or controlled shutdown, not something to call after every event. `close()` preserves unsent data.

## Add a use case

Add a non-overlapping workload, its inventory capacity and subscriber definitions. Restart the controller and assignment service together with the updated policy; wait for queue readiness. Existing route identities and partition count remain unchanged. Existing clients can continue their original routes, but restart a client to use the new route.

Additions are refused during an active migration. Removing or changing existing routes is still refused; do not delete state to bypass that check. Subscriber definitions are immutable once registered. This is controlled additive onboarding, not arbitrary live reconfiguration.

## Qualification

The asynchronous path has passed local two-broker fanout and migration tests. These are functional tests, not a production throughput certificate. Same-partition throughput is bounded by receipt latency; SQLite persistence, many queues and management polling also require workload-specific measurement.

The controller's queue-copy counters provide conservative capacity input, not an exact count of original publications for overlapping subscriptions. Full fanout calibration, management polling at fleet scale, production Cloud provisioning and multi-host controller availability remain qualification work. Automatic scale-in and copying existing backlog are outside the supported path.

Solace references: [asynchronous persistent publishing](https://docs.solace.com/API/API-Developer-Guide-Python/Python-PM-Publish.htm) and [broker acknowledgement timing](https://docs.solace.com/API/API-Developer-Guide/Acknowledging-Published-.htm).
