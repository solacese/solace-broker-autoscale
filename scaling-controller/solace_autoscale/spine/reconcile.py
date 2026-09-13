"""Build spine events from observed broker inventory. PURE apart from the injected generation source.

The spine daemon (spine.py) reads the current broker set for a shard from the assignment store, then
calls :func:`plan_events` to compute what to publish. This function holds the "what changed" logic
and returns events as data; it does no bus I/O and reads no clock (``emitted_at`` is passed in). The
generation is obtained from an injected :class:`GenerationCounter`, which is the one stateful
dependency and is trivially fakeable.

A snapshot is emitted only when the shard's topology actually changed (a new broker set or new
handoffs), so a steady state produces no bus traffic. When it did change, a new generation is minted
and the topology is fenced at it; the resulting :class:`TopologySnapshotEvent` is self-describing and
idempotent to apply, and an accompanying :class:`ActivityEvent` records the change for observability.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from ..assignment.store import Broker
from ..assignment.topology import (
    BrokerRef,
    Handoff,
    ShardTopology,
    diff,
)
from .events import ActivityEvent, SpineEvent, TopologySnapshotEvent


def build_topology(shard: str, gen: int, brokers: Iterable[Broker]) -> ShardTopology:
    """A :class:`ShardTopology` (no handoffs) for the given broker set at a generation."""
    refs = tuple(sorted((BrokerRef.from_broker(b) for b in brokers), key=lambda r: r.broker_id))
    return ShardTopology(shard=shard, gen=gen, brokers=refs)


def plan_events(
    *,
    shard: str,
    brokers: Iterable[Broker],
    previous: ShardTopology | None,
    next_gen: int,
    emitted_at: str,
    sample_keys: Sequence[str],
) -> list[SpineEvent]:
    """Compute the events to publish for a shard's current broker inventory.

    ``previous`` is the last topology published for this shard (``None`` on cold start). ``next_gen``
    is the generation to stamp if a change is detected - the caller mints it from a
    :class:`GenerationCounter` only when it decides to publish, so generations stay dense. Returns an
    empty list when nothing changed, so a steady shard produces no traffic.
    """
    # Ownable set at the previous generation, if any, to detect a real change without minting a gen.
    candidate = build_topology(shard, next_gen, brokers)

    if previous is not None and _same_membership(previous, candidate):
        return []

    handoffs: tuple[Handoff, ...] = ()
    if previous is not None:
        handoffs = tuple(diff(previous, candidate, list(sample_keys)))
    fenced = ShardTopology(
        shard=shard, gen=next_gen, brokers=candidate.brokers, handoffs=handoffs
    )

    events: list[SpineEvent] = [
        TopologySnapshotEvent(topology=fenced, emitted_at=emitted_at)
    ]
    kind = "topology-init" if previous is None else "topology-change"
    events.append(
        ActivityEvent(
            shard=shard,
            gen=next_gen,
            kind=kind,
            emitted_at=emitted_at,
            detail={
                "brokers": [r.broker_id for r in fenced.ownable_brokers()],
                "handoffs": [
                    {"from": h.from_broker, "to": h.to_broker} for h in handoffs
                ],
            },
        )
    )
    return events


def _same_membership(a: ShardTopology, b: ShardTopology) -> bool:
    """True iff the two topologies own the identical broker set (ignoring generation)."""
    return [r.broker_id for r in a.ownable_brokers()] == [
        r.broker_id for r in b.ownable_brokers()
    ]
