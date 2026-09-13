# ADR 0009: Event spine on Solace, ordering-first reassignment

## Status
Accepted. Builds on [ADR 0002](0002-no-proxy-in-data-path.md) (no proxy in the data path) and refines
the steering tiers in [ADR 0003](0003-shard-first-not-full-mesh.md) and §9.

## Context
Before this decision, the shim learned where a key belonged by asking: it polled the assignment
service (`GET /assignment`, `GET /topology`) and cached the answer. That is a pull seam on a control
signal that is inherently a push. A scale event - a broker going ACTIVE or DRAINING - only reaches
routing on the next poll, so the reassignment window is bounded by the poll interval, not by how fast
the fleet actually changed. It also invites every shim to poll every interval whether or not anything
changed.

Two things had to be true at once. First, the control signal should propagate as an event, at the
speed of the change, without a new piece of infrastructure to run. Second - and this was the binding
constraint - **a reassignment must not reorder a partition key's stream.** A messaging system whose
value is per-key ordering cannot trade ordering for elasticity; moving a key from one broker to
another is exactly where ordering is most at risk.

## Decision
Introduce an **event spine**: control events ride the product's own Solace brokers, on a reserved
topic tree, and the shim reacts to them instead of polling. Reassignment is **ordering-first**:
a moved key drains on its old broker before any message crosses to the new one.

Concretely:

- **The bus is Solace itself.** Control events publish to a reserved `_autoscale/*` topic tree on the
  same brokers that carry client data. No separate message bus, no new dependency to operate. This
  does **not** violate ADR 0002: the spine carries *control* (topology snapshots), never a client
  message, and the data path is still client-to-broker-direct.
- **Event-carried state.** Each topology event is a **self-describing, last-value snapshot** for one
  shard on `_autoscale/shard/<shard>/topology`: the full set of brokers, their states, endpoints, and
  the in-flight handoffs, stamped with a monotonic per-shard **generation** (`gen` / `saas_gen`).
  Applying a snapshot is idempotent; a duplicate or late event is harmless because generation-win
  discards anything at or below the applied generation. A cold subscriber reconstructs current state
  from the one retained snapshot.
- **The pure core returns events; only the spine publishes.** The decision core stays pure - it emits
  events as data. A single publisher (`SpinePublisher`) is the one component that touches the bus, so
  the I/O choke point is one file, testable and mockable.
- **Ownership is rendezvous hashing**, computed identically in the Python control plane and the Go
  shim (proven byte-for-byte by a golden vector). Rendezvous moves only ~1/(N+1) of keys on a
  scale-up N to N+1, which keeps the set of keys that ever face a handoff - and therefore the ordering
  risk - as small as the math allows.
- **Drain-before-cutover is the ordering guarantee.** A topology event carries `handoffs[]`
  (`from_broker`, `to_broker`, `effective_gen`). For a moved key the publisher keeps sending to the
  old broker for a bounded grace window (stamped below the cutover generation), then cuts to the new
  owner at the new generation - so a key is never split across two brokers at once. The listener
  releases a key's messages in generation order, holding a post-cutover (higher-gen) message until the
  in-flight pre-cutover (lower-gen) stream for that key drains, and dropping stragglers below what the
  key has already released.
- **HTTP and DNS are demoted, not removed.** `GET /topology` and the resolver become **cold-start and
  fallback** only: fetch once on boot, let live events update the cache, events win by generation, and
  fail open to the last snapshot if the bus drops. Polling is no longer the primary path; it is the
  floor under a silent bus.

## Reasoning
- **Reuse the brokers we already run.** A messaging product that stood up a *second* message bus to
  coordinate the first would be hard to justify. The reserved topic tree gives event delivery,
  last-value retention, and wildcard fan-in for free on infrastructure already in production.
- **Self-describing snapshots make the seam robust.** Because every event is a full snapshot keyed by
  generation, the shim needs no event log, no replay, and no exactly-once delivery: at-least-once plus
  generation-win is enough. Lost, duplicated, and reordered *control* events are all survivable, which
  is what lets the spine ride best-effort control topics.
- **Rendezvous plus fencing is the minimal ordering machinery.** Rendezvous minimises how many keys
  move; fencing (grace window on the publisher, generation-ordered release on the listener) protects
  the few that do. Neither needs global coordination or a lock across brokers - both are pure state
  machines with an injected clock, so they are unit-testable and deterministic.
- **Fail-open keeps elasticity from becoming a liability.** The spine going quiet must never take an
  application's routing down. The last applied snapshot keeps serving; the HTTP floor covers a cold
  boot with no cache. Elasticity is additive to availability, never a precondition for it.

## Consequences
- A scale event reaches the shim as a push, at the speed of the change, not the poll interval. Shims
  that follow many shards open one control receiver per broker over the topology wildcard.
- Per-key order survives a scale-up: the deterministic-clock tests prove no reorder across a handoff
  (`shim/spine`), and `shim demo --scale` shows the whole path end to end with no broker.
- The generation is now load-bearing in two roles - dedup/version and ordering fence - so it must be
  monotonic per shard and stamped on both the topology event and the data message (`saas_gen`). The
  publisher enforces monotonicity and spends a generation only on a real membership change (dense
  generations).
- The control plane and the shim must compute ownership identically forever; the golden vector is the
  contract and CI fails if the two diverge.
- ADR 0002 still holds unchanged: no proxy, direct data path, control plane carries no client message.
  The spine is a control channel that happens to share the brokers, not a data intermediary.
