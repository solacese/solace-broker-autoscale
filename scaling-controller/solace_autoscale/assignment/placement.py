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

from .store import _SERVING_STATES, AssignmentStore, Broker, Placement


class NoBrokerAvailable(Exception):
    pass


@dataclass(frozen=True)
class Assignment:
    broker: Broker
    lease_seconds: int
    reused_existing: bool


def _pick_broker(store: AssignmentStore, shard: str) -> Broker:
    candidates = store.assignable_brokers(shard)  # ACTIVE only
    if not candidates:
        raise NoBrokerAvailable(f"no ACTIVE broker for shard {shard!r}")
    # least-loaded by placement count, then broker_id for determinism
    def load(b: Broker) -> tuple[int, str]:
        return (len(store.placements_on_broker(b.broker_id)), b.broker_id)
    return min(candidates, key=load)


def assign(
    store: AssignmentStore,
    shard: str,
    client_id: str,
    mode: str,
    now: float,
    lease_seconds: int,
) -> Assignment:
    """Return the broker this client should use, creating/renewing the placement as needed."""
    existing = store.get_placement(shard, client_id)

    if mode == "guaranteed":
        if existing is not None:
            broker = store.get_broker(existing.broker_id)
            # sticky: return to the same broker as long as it can still serve (even if lease expired)
            if broker is not None and broker.state in _SERVING_STATES:
                store.renew_lease(shard, client_id, now + lease_seconds)
                return Assignment(broker=broker, lease_seconds=lease_seconds, reused_existing=True)
            # broker gone → the durable queue is unreachable; a new placement is a real migration,
            # which for guaranteed consumers must be explicit, not silent. We place on a new ACTIVE
            # broker but flag reuse=False so callers can surface it.
        broker = _pick_broker(store, shard)
        version = existing.version if existing is not None else 1
        store.put_placement(Placement(shard, client_id, broker.broker_id, "guaranteed",
                                      now + lease_seconds, version=version))
        return Assignment(broker=broker, lease_seconds=lease_seconds, reused_existing=False)

    # direct (and publishers): reuse an existing placement if its broker still serves and isn't
    # draining; otherwise (re)assign to a fresh ACTIVE broker.
    if existing is not None:
        broker = store.get_broker(existing.broker_id)
        if broker is not None and broker.state == broker.state.ACTIVE:
            store.renew_lease(shard, client_id, now + lease_seconds)
            return Assignment(broker=broker, lease_seconds=lease_seconds, reused_existing=True)
    broker = _pick_broker(store, shard)
    version = existing.version if existing is not None else 1
    store.put_placement(Placement(shard, client_id, broker.broker_id, "direct",
                                  now + lease_seconds, version=version))
    return Assignment(broker=broker, lease_seconds=lease_seconds, reused_existing=False)
