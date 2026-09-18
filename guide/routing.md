# How messages reach the right broker

Adding a broker creates room. It does not move a queue, split a busy publisher, or change an existing connection. Applications need a routing rule as well as a capacity recommendation.

## What the original resolver did

It chose the active broker with the fewest recorded client placements. This was **per-client assignment**, not per-message round robin. Guaranteed placements stayed on their original broker.

That leaves two problems. A client sending one message a second counts the same as one sending a million. Also, a publisher and consumer with different client IDs can be assigned different brokers even though they need the same queue.

## Recommended: stable business keys and fixed partitions

Choose the smallest key whose messages must stay together: an order ID, account ID, device ID, or another business identifier. A tenant ID is convenient, but one huge tenant can become a bottleneck.

The new `KeyRouter` follows this path:

**Business key → fixed partition → recorded broker owner → your existing messaging connection.**

The publisher and consumer resolve the same partition. They then use the same partition-specific topic/queue convention on that broker. In generic routing mode the application creates queues and subscriptions. In managed automatic mode the controller creates queues and the supplied SMF adapters follow them; see [automatic scaling](automatic-scaling.md).

```yaml
assignment:
  store: ./assignment.db
  routing: partitioned
  strategy: rendezvous
  partitions: 128
  lease_seconds: 300
  broker_weights: {}
```

This block is read by `solace-autoscale serve --config your-config.yaml`. It is runnable configuration, not a design sketch. The application passes the business key; a YAML string cannot inspect messages inside an existing client library.

| YAML setting | Available values | Meaning |
|---|---|---|
| `routing` | `client`, `partitioned` | Route by client identity, or require a shared business key/partition. |
| `strategy` | `least-placements`, `rendezvous` | Choose the fewest unexpired/direct or durable placements, or a stable hash winner. Applied when creating a placement, and when an eligible direct placement expires. |
| `partitions` | 1–65,536; default 128 | Fixed number of partitions **per shard**. More partitions give finer potential migration units. The managed controller creates these queues; generic routing alone does not. |
| `lease_seconds` | 1–86,400; default 300 | How long clients cache a location. This never expires guaranteed queue ownership. |
| `broker_weights` | Positive numbers keyed by broker ID | Relative tested capacity. `{broker-a: 1, broker-b: 2}` gives B approximately twice the key share over many keys; it does not guarantee twice the traffic. |
| `store` | Local SQLite path | Durable placement state. Keep it on persistent local storage and back it up. |

Defaults preserve existing behavior: `routing: client`, `strategy: least-placements`. Use the recommended block for a new partition-aware application. Round robin is not offered.

### Which option should I choose?

| Choice | Good fit | Main tradeoff |
|---|---|---|
| Client routing + least placements | Existing clients with similar traffic; a simple adoption path | Counts clients/placements, not message rate. A large publisher can dominate a broker. |
| Partitioned routing + rendezvous | Ordered business keys, repeatable routing, adding capacity gradually | Requires application routing and partition queues. A single hot key remains indivisible. |
| Partitioned routing + weighted rendezvous | Deliberately unequal, tested broker capacity | Weights are operator supplied. Do not use changing queue depth as a weight. |
| Partitioned routing + least placements | Evenly sized partitions with persistent ownership | Balances placement count; ignores key traffic skew. |

Rendezvous hashing gives each partition a repeatable preferred broker. Adding broker D changes only the hash winners that prefer D; it does not reshuffle keys between A, B, and C. **The persisted owner takes precedence over that hash result.**

## What happens when a new broker joins?

1. The broker must be configured, reachable and ready, then explicitly added to the assignment inventory as `ACTIVE`.
2. Previously unassigned partitions can choose it. Direct partitions can choose it after their lease expires and their clients reconnect.
3. Existing guaranteed partitions stay on their current broker, even after restart or lease expiry.
4. If every guaranteed partition is already assigned, adding a broker alone moves **zero** existing traffic. A migration is needed to use that capacity.

With `automation.enabled: true`, `solace-autoscale run` automates this handover for managed guaranteed queues. It selects a fitting partition using measured load, prepares a destination consumer, fences source ingress, waits for drained/acknowledged state and a grace period, then changes ownership. Publishers buffer and retry. See [the complete workflow and YAML](automatic-scaling.md). Without that controller, guaranteed owners remain unchanged. A timed lease alone is not a fence.

Strict ordering during direct handover is also not guaranteed. Publishers with cached leases can briefly use different owners. Use guaranteed sticky partitions and a controlled cutover when ordering matters. Key locality by itself does not establish a total order across concurrent publishers.

Changing `partitions` changes the key-to-partition mapping. The service persists the routing contract and rejects such a configuration change on the existing store. Do not delete that database to bypass the check: its records identify where durable data lives.

## Python publisher and consumer

```python
from solace_autoscale_client import KeyRouter, Resolver

resolver = Resolver("https://assign.example.com", api_key=assignment_api_key)
router = KeyRouter(resolver, shard="orders", client_id="publisher-1",
                   partitions=128, mode="guaranteed", protocol="smf")

location = router.resolve_key("order-123")
# Reuse a normal protocol connection from your application's pool:
connection = connections[location.broker_id]
# Application convention; create matching queues/subscriptions before publishing.
connection.publish(topic=f"orders/p/{location.partition_id}/created", message=message)
```

The local key hash and lease cache keep the resolver out of the per-message network path. The example's `connections` and `publish` method are application pseudocode: use your own SMF, AMQP, MQTT, or REST library.

Consumers use their assigned partition numbers:

```python
consumer_router = KeyRouter(resolver, "orders", "worker-1", partitions=128,
                            mode="guaranteed", protocol="smf")
location = consumer_router.resolve_partition(17)
# Connect to location.endpoint("smf") and consume the queue for orders/p/17/>.
```

The HTTP interface accepts either `routing_key=order-123` or `partition=17`, together with `shard`, `client_id`, `mode`, and optionally `protocol`. It returns `partition_id`, `partition_count`, broker ID, VPN and protocol endpoints. All participants in a guaranteed partition use `mode=guaranteed`, including publishers.

The Python `KeyRouter` supports the new partition contract. Java's existing adapter remains a client-mode adapter; partition support is not claimed for it. Applications can also call the HTTP contract directly.

## Hot keys, fanout and backlogs

- **One hot key:** split it further only if the business ordering rule allows it, or use a larger broker. The engine holds rather than recommending ineffective scale-up when `key_subdividable=false`.
- **Uneven partitions:** inspect per-broker telemetry from `monitor-fleet`; aggregate utilization can hide a hot broker. The automatic controller measures managed partition traffic and chooses fitting moves to relieve a hot broker.
- **Broadcast/fanout:** key routing is not cross-broker subscription discovery. DMR/mesh links need deliberate configuration and consume bandwidth. The capacity model measures fanout within the benchmark environment; it does not prove arbitrary mesh throughput.
- **A slow consumer:** adding brokers does not accelerate an existing queue automatically. Scale consumers if their processing is the bottleneck; use broker sizing when the broker itself is constrained.

## Assignment service operations

Set `SOLACE_ASSIGNMENT_API_KEY` when serving beyond loopback; terminate TLS at your ingress and keep the service private to authorized applications. The token is service-wide authentication, not tenant-specific authorization. Health endpoints remain available for probes.

SQLite supports threads/processes on one host sharing one local database. It is not a multi-host HA state service. Transactions cover selection and persistence; guaranteed placements survive restart. Production deployments needing multi-host availability still require a shared durable backend. The implemented migration controller resumes after restart on its original persistent host.
