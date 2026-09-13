# Event spine (control events on the product's own brokers)

The autoscaler and the smart shim talk through **events**, not a request/response API. A scale event
reaches the shim as a push, at the speed of the change, and a partition key's stream survives the
reassignment it triggers. This is the design [ADR 0009](adr/0009-event-spine-ordering-first-reassignment.md)
records; this page is how it works.

The old seam was a poll: the shim asked `GET /assignment` (and later `GET /topology`) on an interval
and cached the answer. That bounded the reassignment window by the poll interval rather than by how
fast the fleet actually changed, and made every shim poll whether or not anything had moved. The
control signal is inherently a push, so it now rides the bus.

## The topic tree

Control events publish to a **reserved** `_autoscale/*` topic tree on the **same Solace brokers that
carry client data**. There is no separate message bus to run. This does not put anything on the data
path (ADR 0002): the spine carries control, never a client message.

```
_autoscale/
  shard/
    <shard>/
      topology   last-value snapshot: the full, self-describing state of one shard.
                 A cold subscriber gets current state from the one retained message.
      activity   append stream of domain events (a broker went DRAINING, a handoff
                 started). Observability only - never required for correctness.
```

A shim following many shards subscribes once per broker over the single-level wildcard
`_autoscale/shard/*/topology` and receives every shard's snapshots on one control receiver.

## Event-carried state

Each topology event is a **full snapshot for one shard**, self-describing, stamped with a monotonic
per-shard **generation** (`gen`). It is not a delta and it does not require an event log:

```json
{
  "version": 1,
  "shard": "orders",
  "gen": 2,
  "emitted_at": "2026-01-01T00:00:02Z",
  "brokers": [
    {"broker_id": "broker-a", "state": "active",   "endpoints": {"amqp": "amqp://broker-a:5672"}},
    {"broker_id": "broker-d", "state": "active",   "endpoints": {"amqp": "amqp://broker-d:5672"}},
    {"broker_id": "broker-old","state": "draining", "endpoints": {"amqp": "amqp://broker-old:5672"}}
  ],
  "handoffs": [
    {"from_broker": "broker-a", "to_broker": "broker-d", "effective_gen": 2, "reason": "scale-up"}
  ]
}
```

Applying a snapshot is **idempotent**. The shim keeps the highest generation it has applied and
**generation-win** discards any event at or below it, so a duplicate, late, or reordered control event
is harmless. That is what lets the spine ride best-effort control topics: at-least-once delivery plus
generation-win is enough - no exactly-once, no replay.

## Who publishes

The decision core stays **pure**: it returns events as data. A single publisher (`SpinePublisher`) is
the **only** component that touches the bus, so the I/O choke point is one file. The publisher stamps
the generation, enforces that it is monotonic per shard, and spends a generation only on a real
membership change (dense generations - a quiet shard emits nothing).

## Ownership: rendezvous hashing

The owner of a partition key is the ACTIVE broker with the greatest `sha256(broker_id + 0x00 + key)`,
ties broken on the greater `broker_id`. The Python control plane and the Go shim compute this
**identically**, proven byte-for-byte by a committed golden vector (CI fails if they diverge).

Rendezvous is chosen for one property: scaling from N to N+1 brokers moves only about `1/(N+1)` of
keys. The fewer keys move, the fewer ever face a handoff - so the ordering machinery below runs for
as small a set as the math allows.

## The ordering guarantee: drain-before-cutover

Moving a key from one broker to another is where per-key order is most at risk. The spine makes a
reassignment **ordering-first**: a moved key drains on its old broker before any message crosses to
the new one.

A topology event names the moves in `handoffs[]`. For each moved key:

**Publisher side (fencer).** During a bounded grace window the publisher keeps sending the key to its
**old** broker, stamped below the cutover generation; when the window elapses it cuts to the new owner
at the new generation. A key is never split across two brokers at once.

**Listener side (reorderer).** The listener releases a key's messages in nondecreasing `(gen, seq)`
order. It holds a post-cutover (higher-gen) message until the in-flight pre-cutover (lower-gen) stream
for that key has drained, and drops a straggler whose generation is below what the key has already
released.

```
key "order-eu-1", handoff broker-a -> broker-d at gen 2:

  gen 1 (old broker, broker-a)        gen 2 (new broker, broker-d)
  |----m1----m2----m3--|                        |----m4----|
                        \___ grace window ___/  cutover
  listener releases:  m1  m2  m3            then  m4      (never m4 before m3)
```

Both the fencer and the reorderer are **pure state machines with an injected clock** - no lock across
brokers, no global coordination. Deterministic-clock tests in `shim/spine` prove no reorder on
scale-up, drain-before-cutover, straggler drop, and per-key independence.

The data message carries the generation as the `saas_gen` application property alongside the partition
key (`saas_partition_key`, also the AMQP group-id), so the listener can fence on generation without
decoding the body. See [rule-spec.md](rule-spec.md).

## Fail-open: the bus going quiet never takes routing down

HTTP and DNS are **not removed** - they are demoted to cold-start and fallback (ADR 0002's steering
tiers still stand):

- On boot the shim fetches `GET /topology` once to seed its cache.
- Live events update the cache from then on; **events win by generation**.
- If the bus drops, the shim keeps serving the **last applied snapshot**.
- Only a cold boot with no cache **and** an unreachable resolver is an error.

Elasticity is additive to availability, never a precondition for it.

## See it run

```
shim demo --scale
```

Runs the whole path offline with no broker: a topology snapshot is pushed over the spine, moves a
key, holds it on its old broker through the grace window, then cuts over - while the listener releases
the key's stream in generation order.

## Related

- [ADR 0009](adr/0009-event-spine-ordering-first-reassignment.md) - the decision and its reasoning.
- [ADR 0002](adr/0002-no-proxy-in-data-path.md) - no proxy in the data path; the spine is control, not data.
- [client-integration.md](client-integration.md) - the steering tiers and the smart shim.
- [rule-spec.md](rule-spec.md) - the portable rule spec and the wire properties.
