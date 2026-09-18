"""Placement logic (§9.1). Pure decisions over the store; the store does persistence.

Rules:
- **Guaranteed** placement is sticky and durable. If a placement exists and its broker can still
  serve (ACTIVE/DRAINING/DRAINED), the client returns to the SAME broker - even after lease expiry,
  as long as the queue (broker) still exists. A queue lives on one broker; sending the consumer
  elsewhere makes its messages unreachable.
- **Direct** clients (and publishers) may be (re)assigned freely; a DRAINING broker takes no new
  ones. Direct placements are convenience/stickiness only and can move.
- New assignments go only to ACTIVE brokers. Selection is deterministic (least-loaded by current
  placement count, then broker_id) so the same inputs give the same answer.
- Leases expire so a vanished client does not pin a broker forever; guaranteed placements survive
  lease expiry (the queue outlives the connection).
"""

from __future__ import annotations

from dataclasses import dataclass

from .routing import rendezvous_broker
from .store import _SERVING_STATES, AssignmentStore, Broker, Placement


class NoBrokerAvailable(Exception):
    pass


@dataclass(frozen=True)
class Assignment:
    broker: Broker
    lease_seconds: int
    reused_existing: bool


class ProtocolUnavailable(NoBrokerAvailable):
    """No eligible broker supports the requested protocol."""


def _pick_broker(store: AssignmentStore, shard: str, now: float, protocol: str | None,
                 key: str, strategy: str, weights: dict[str, float]) -> Broker:
    candidates = store.assignable_brokers(shard)  # ACTIVE only
    if not candidates:
        raise NoBrokerAvailable(f"no ACTIVE broker for shard {shard!r}")
    if protocol is not None:
        candidates = [b for b in candidates if protocol in b.endpoints]
        if not candidates:
            raise ProtocolUnavailable(f"protocol {protocol!r} unavailable on ACTIVE brokers for {shard!r}")
    if strategy == "rendezvous":
        return rendezvous_broker(candidates, shard, key, weights)
    # Counts are placements, NOT traffic. Expired direct placements do not skew new assignments.
    def load(b: Broker) -> tuple[float, str]:
        return (sum(p.mode == "guaranteed" or p.lease_expires_at > now
                    for p in store.placements_on_broker(b.broker_id)) / weights.get(b.broker_id, 1),
                b.broker_id)
    return min(candidates, key=load)


def assign(
    store: AssignmentStore,
    shard: str,
    client_id: str,
    mode: str,
    now: float,
    lease_seconds: int,
    protocol: str | None = None,
    strategy: str = "least-placements",
    broker_weights: dict[str, float] | None = None,
) -> Assignment:
    """Atomically select and persist one placement, including across local worker processes."""
    if mode not in ("direct", "guaranteed") or lease_seconds <= 0 or not shard or not client_id:
        raise ValueError("nonempty shard/client_id, valid mode and positive lease are required")
    if strategy not in ("least-placements", "rendezvous"):
        raise ValueError("unknown placement strategy")
    with store.transaction():
        return _assign(store, shard, client_id, mode, now, lease_seconds, protocol,
                       strategy, broker_weights or {})


def _assign(store: AssignmentStore, shard: str, client_id: str, mode: str, now: float,
            lease_seconds: int, protocol: str | None, strategy: str,
            weights: dict[str, float]) -> Assignment:
    """Return the broker this client should use, creating/renewing the placement as needed."""
    existing = store.get_placement(shard, client_id)

    if existing is not None and existing.mode != mode:
        raise ValueError("placement mode cannot change without an explicit migration")
    if existing is not None and protocol is not None:
        home = store.get_broker(existing.broker_id)
        if home is not None and protocol not in home.endpoints:
            raise ProtocolUnavailable(f"protocol {protocol!r} unavailable on existing placement")

    if mode == "guaranteed":
        if existing is not None:
            broker = store.get_broker(existing.broker_id)
            # sticky: return to the same broker as long as it can still serve (even if lease expired)
            if broker is not None and broker.state in _SERVING_STATES:
                store.renew_lease(shard, client_id, now + lease_seconds)
                return Assignment(broker=broker, lease_seconds=lease_seconds, reused_existing=True)
            raise NoBrokerAvailable(
                f"guaranteed placement remains on unavailable broker {existing.broker_id!r}; "
                "restore that broker or complete an explicit queue migration before reassignment"
            )
        broker = _pick_broker(store, shard, now, protocol, client_id, strategy, weights)
        version = existing.version if existing is not None else 1
        store.put_placement(Placement(shard, client_id, broker.broker_id, "guaranteed",
                                      now + lease_seconds, version=version))
        return Assignment(broker=broker, lease_seconds=lease_seconds, reused_existing=False)

    # direct (and publishers): reuse an existing placement if its broker still serves and isn't
    # draining; otherwise (re)assign to a fresh ACTIVE broker.
    if existing is not None:
        broker = store.get_broker(existing.broker_id)
        if (broker is not None and broker.state == broker.state.ACTIVE
                and existing.lease_expires_at > now):
            store.renew_lease(shard, client_id, now + lease_seconds)
            return Assignment(broker=broker, lease_seconds=lease_seconds, reused_existing=True)
    broker = _pick_broker(store, shard, now, protocol, client_id, strategy, weights)
    version = existing.version if existing is not None else 1
    store.put_placement(Placement(shard, client_id, broker.broker_id, "direct",
                                  now + lease_seconds, version=version))
    return Assignment(broker=broker, lease_seconds=lease_seconds, reused_existing=False)
