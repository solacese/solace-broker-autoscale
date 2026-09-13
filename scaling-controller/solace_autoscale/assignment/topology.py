"""Shard topology and the ordering-first key-to-broker model (event spine, ADR 0009).

This module is **pure**: no I/O, no clock, no logging. It defines the self-describing topology
snapshot the control plane publishes on the event spine, the deterministic way a partition key maps
to a broker within a shard, and the minimal set of handoffs a topology change produces.

Why it lives here and stays pure:
  - The spine publisher (the only component that touches the bus) turns a ``ShardTopology`` into the
    JSON event; the Go shim parses the same JSON and computes the same owner for a key. A committed
    golden vector proves the two agree, exactly like the rule spec.
  - Ordering is the priority. Keys map to brokers by **rendezvous (highest-random-weight) hashing**
    so a scale from N to N+1 brokers moves only about 1/(N+1) of the keys instead of reshuffling all
    of them. Fewer moved keys means fewer per-key handoffs, which means less opportunity to reorder a
    key's stream. ``diff`` reports exactly which keys' owners changed as ``Handoff`` records, each
    fenced by the generation at which the cutover becomes effective.

The generation (``gen``) is monotonic per shard. It is both the dedup/version for idempotent event
application (a consumer ignores any snapshot with ``gen`` less than or equal to the one it applied)
and the fence for ordering (a message cut over to a new broker is stamped with the gen so the
listener can hold it until the old broker's stream for that key has drained).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from .store import Broker, BrokerState

#: Wire property name carrying the generation a message was published under. Stamped alongside the
#: partition key so a listener can preserve per-key order across a broker handoff.
GENERATION_PROPERTY = "saas_gen"

#: Topology event schema version. Bump when the event shape changes in a way consumers must gate on.
TOPOLOGY_VERSION = 1

#: A broker takes NEW keyed traffic only in this state (mirrors _ASSIGNABLE_STATES in the store).
_OWNABLE_STATES = {BrokerState.ACTIVE}


@dataclass(frozen=True)
class BrokerRef:
    """A broker as it appears in a topology snapshot: id, state, and its endpoint map (locations,
    never credentials)."""

    broker_id: str
    state: BrokerState
    endpoints: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_broker(cls, b: Broker) -> BrokerRef:
        return cls(broker_id=b.broker_id, state=b.state, endpoints=dict(b.endpoints))


@dataclass(frozen=True)
class Handoff:
    """One key-ownership move produced by a topology change. The shim publishes the moved key to
    ``from_broker`` until it is quiesced, then cuts it to ``to_broker`` stamped with
    ``effective_gen`` so the listener keeps the key's order across the cutover."""

    from_broker: str
    to_broker: str
    effective_gen: int
    reason: str = "topology-change"


@dataclass(frozen=True)
class ShardTopology:
    """Self-describing snapshot of one shard's brokers at a given generation.

    ``handoffs`` is empty in steady state and populated only across a change so a shim knows which
    cutovers to run drain-before-cutover for. Applying a snapshot needs no callback: it carries
    everything a shim needs to route and to hand keys off safely.
    """

    shard: str
    gen: int
    brokers: tuple[BrokerRef, ...] = ()
    handoffs: tuple[Handoff, ...] = ()

    def ownable_brokers(self) -> list[BrokerRef]:
        """Brokers that may own a key (ACTIVE), in deterministic broker_id order."""
        return sorted((b for b in self.brokers if b.state in _OWNABLE_STATES),
                      key=lambda b: b.broker_id)

    def owner(self, key: str) -> str | None:
        """The broker that owns ``key`` in this topology, by rendezvous hashing over the ownable
        brokers. Returns None if the shard has no ownable broker. Deterministic and identical to the
        Go shim's Owner."""
        return _rendezvous_owner(key, [b.broker_id for b in self.ownable_brokers()])

    # ---- event round-trip (the wire form the spine publishes) ------------------------------------

    def to_event(self, *, emitted_at: str) -> dict:
        """Render the portable topology event. ``emitted_at`` is an ISO-8601 timestamp supplied by
        the caller (this module never reads a clock)."""
        return {
            "version": TOPOLOGY_VERSION,
            "shard": self.shard,
            "gen": self.gen,
            "emitted_at": emitted_at,
            "brokers": [
                {"broker_id": b.broker_id, "state": b.state.value, "endpoints": dict(b.endpoints)}
                for b in self.brokers
            ],
            "handoffs": [
                {"from_broker": h.from_broker, "to_broker": h.to_broker,
                 "effective_gen": h.effective_gen, "reason": h.reason}
                for h in self.handoffs
            ],
        }

    @classmethod
    def from_event(cls, event: dict) -> ShardTopology:
        version = event.get("version")
        if version != TOPOLOGY_VERSION:
            raise ValueError(f"unsupported topology version {version!r} (expected {TOPOLOGY_VERSION})")
        brokers = tuple(
            BrokerRef(
                broker_id=b["broker_id"],
                state=BrokerState(b["state"]),
                endpoints=dict(b.get("endpoints", {})),
            )
            for b in event.get("brokers", [])
        )
        handoffs = tuple(
            Handoff(
                from_broker=h["from_broker"],
                to_broker=h["to_broker"],
                effective_gen=int(h["effective_gen"]),
                reason=h.get("reason", "topology-change"),
            )
            for h in event.get("handoffs", [])
        )
        return cls(shard=event["shard"], gen=int(event["gen"]), brokers=brokers, handoffs=handoffs)


def _rendezvous_owner(key: str, broker_ids: list[str]) -> str | None:
    """Highest-random-weight (rendezvous) hashing: the owner is the broker with the greatest
    ``hash(broker_id + "\\x00" + key)``. Ties break on broker_id so the result is total and
    deterministic. Moving from N to N+1 brokers reassigns a key only if the new broker outscores the
    incumbent, so about 1/(N+1) of keys move and the rest stay put."""
    if not broker_ids:
        return None
    best_id: str | None = None
    best_score = b""
    for bid in broker_ids:
        score = hashlib.sha256(f"{bid}\x00{key}".encode()).digest()
        if best_id is None or score > best_score or (score == best_score and bid > best_id):
            best_id, best_score = bid, score
    return best_id


def diff(old: ShardTopology, new: ShardTopology, keys: list[str]) -> list[Handoff]:
    """Handoffs implied by moving from ``old`` to ``new`` for a known set of partition ``keys``.

    A handoff is emitted for every key whose owner changes, deduplicated to one per
    (from_broker, to_broker) pair since the cutover protocol is per broker pair, not per key. The
    cutover is fenced at ``new.gen``. Keys that do not move produce nothing, which is the common case
    under rendezvous hashing.
    """
    seen: set[tuple[str, str]] = set()
    handoffs: list[Handoff] = []
    for key in keys:
        src = old.owner(key)
        dst = new.owner(key)
        if src is None or dst is None or src == dst:
            continue
        pair = (src, dst)
        if pair in seen:
            continue
        seen.add(pair)
        handoffs.append(Handoff(from_broker=src, to_broker=dst, effective_gen=new.gen))
    return handoffs
