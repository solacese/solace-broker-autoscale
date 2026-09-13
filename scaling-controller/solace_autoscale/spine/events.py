"""Spine events. PURE. No I/O, no clock reads, no logging.

The decision engine and actuator return these as data; the publisher (publisher.py) is the only
thing that turns them into bus messages. Two event kinds live on the reserved ``_autoscale`` tree:

- **topology** (``_autoscale/shard/<shard>/topology``): a self-describing, last-value snapshot of a
  shard's broker set and any in-flight handoffs. Event-carried state: applying one needs no callback
  and no prior event; a late or duplicate snapshot is harmless because the generation orders them.
- **activity** (``_autoscale/shard/<shard>/activity``): an append stream of domain events (a broker
  went DRAINING, a handoff completed) for observability. NOT required for correctness - a listener
  that only reads topology snapshots still routes correctly.

The reserved prefix keeps control off any customer topic. It is a single source of truth shared by
the Python control plane and the Go shim, so the topic strings are defined here once.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..assignment.topology import TOPOLOGY_VERSION, ShardTopology

#: Reserved topic root. Control events only; never a customer data topic.
RESERVED_ROOT = "_autoscale"


def topology_topic(shard: str) -> str:
    """Last-value topic carrying the current :class:`TopologySnapshotEvent` for a shard."""
    return f"{RESERVED_ROOT}/shard/{shard}/topology"


def activity_topic(shard: str) -> str:
    """Append-stream topic carrying :class:`ActivityEvent`s for a shard."""
    return f"{RESERVED_ROOT}/shard/{shard}/activity"


class SpineEvent:
    """Marker base: something the spine can publish. Subclasses are pure and self-describing."""

    #: The topic this event publishes to. Pure - derived from the event's own fields.
    def topic(self) -> str:  # pragma: no cover - overridden
        raise NotImplementedError

    #: The wire body as a JSON-serializable dict. Pure.
    def to_payload(self) -> dict[str, Any]:  # pragma: no cover - overridden
        raise NotImplementedError

    #: True for a last-value (retained) publish; False for an append-stream event.
    @property
    def last_value(self) -> bool:  # pragma: no cover - overridden
        raise NotImplementedError


@dataclass(frozen=True)
class TopologySnapshotEvent(SpineEvent):
    """A full topology snapshot for one shard, carried as state (ADR 0009).

    Wraps a :class:`ShardTopology`. Idempotent to apply: a listener keeps the snapshot with the
    highest ``gen`` and ignores any snapshot whose ``gen`` is not greater than the one it holds.
    """

    topology: ShardTopology
    emitted_at: str  # ISO-8601 timestamp, supplied by the publisher's clock (not read here)

    def topic(self) -> str:
        return topology_topic(self.topology.shard)

    def to_payload(self) -> dict[str, Any]:
        return self.topology.to_event(emitted_at=self.emitted_at)

    @property
    def last_value(self) -> bool:
        return True

    @property
    def gen(self) -> int:
        return self.topology.gen


@dataclass(frozen=True)
class ActivityEvent(SpineEvent):
    """An observability event on the append stream. Not required for routing correctness."""

    shard: str
    gen: int
    kind: str  # e.g. "broker-draining", "handoff-complete", "scale-up"
    emitted_at: str  # ISO-8601 timestamp
    detail: dict[str, Any] | None = None

    def topic(self) -> str:
        return activity_topic(self.shard)

    def to_payload(self) -> dict[str, Any]:
        return {
            "version": TOPOLOGY_VERSION,
            "shard": self.shard,
            "gen": self.gen,
            "kind": self.kind,
            "emitted_at": self.emitted_at,
            "detail": dict(self.detail) if self.detail else {},
        }

    @property
    def last_value(self) -> bool:
        return False
