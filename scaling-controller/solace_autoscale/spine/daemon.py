"""The spine daemon: reconcile broker inventory into published events, once per tick.

This is the runtime that wires the pieces together. It is small on purpose - all the decisions live
in the pure reconciler and topology model; the daemon only reads inventory, calls the reconciler,
mints a generation when there is a change, and hands events to the publisher.

The clock is injected (``now``), the generation source is injected, and the store is any object with
``brokers_for_shard``. So the whole tick is deterministic and testable against an
:class:`~solace_autoscale.spine.publisher.InMemoryBus` with a fake clock and counter.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol

from ..assignment.generation import GenerationCounter
from ..assignment.store import Broker
from ..assignment.topology import ShardTopology
from .events import TopologySnapshotEvent
from .publisher import SpinePublisher
from .reconcile import plan_events


class InventorySource(Protocol):
    """The read surface the daemon needs from a store: the brokers currently on a shard."""

    def brokers_for_shard(self, shard: str) -> list[Broker]: ...


class SpineDaemon:
    """Reconciles one or more shards onto the spine. Call :meth:`tick` on a schedule.

    Holds the last topology published per shard so it can diff without re-reading the bus. On a
    change it mints the next generation from ``counter`` (dense generations: a gen is only spent when
    something actually changed) and publishes a fenced snapshot plus an activity event.
    """

    def __init__(
        self,
        *,
        inventory: InventorySource,
        publisher: SpinePublisher,
        counter: GenerationCounter,
        sample_keys: Sequence[str],
        now: Callable[[], str],
    ) -> None:
        self._inventory = inventory
        self._publisher = publisher
        self._counter = counter
        self._sample_keys = tuple(sample_keys)
        self._now = now
        self._last: dict[str, ShardTopology] = {}

    def tick(self, shard: str) -> list[str]:
        """Reconcile one shard. Returns the topics published this tick (empty if no change)."""
        brokers = self._inventory.brokers_for_shard(shard)
        previous = self._last.get(shard)
        next_gen = self._counter.next(shard) if self._would_change(shard, brokers, previous) else 0
        if next_gen == 0:
            return []

        events = plan_events(
            shard=shard,
            brokers=brokers,
            previous=previous,
            next_gen=next_gen,
            emitted_at=self._now(),
            sample_keys=self._sample_keys,
        )
        published: list[str] = []
        for event in events:
            if self._publisher.publish(event):
                published.append(event.topic())
            if isinstance(event, TopologySnapshotEvent):
                self._last[shard] = event.topology
        return published

    def _would_change(
        self, shard: str, brokers: list[Broker], previous: ShardTopology | None
    ) -> bool:
        # Peek without minting a generation: build at the current gen and compare membership.
        peek_gen = self._counter.current(shard)
        return bool(
            plan_events(
                shard=shard,
                brokers=brokers,
                previous=previous,
                next_gen=peek_gen + 1,
                emitted_at="",
                sample_keys=self._sample_keys,
            )
        )
