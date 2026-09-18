"""Load-aware bin packing of fixed partitions; hot keys stay indivisible."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PartitionLoad:
    shard: str
    partition: int
    broker_id: str
    # Fractions of one broker's measured per-axis capacity.
    messages: float
    bytes: float
    connections: float = 0
    spool: float = 0

    @property
    def pressure(self) -> float:
        return max(self.messages, self.bytes, self.connections, self.spool)


def propose_move(
    loads: list[PartitionLoad],
    brokers: list[str],
    *,
    trigger: float,
    target: float,
    excluded: set[tuple[str, int]],
) -> tuple[PartitionLoad, str] | None:
    """Move a useful partition off an overloaded broker without overloading its destination.

    Prefer the smallest move that relieves the overload. If none fully relieves it, move the
    largest useful fitting partition. Repeat after fresh telemetry; never assume migration
    relieved source load before it actually completes.
    """
    totals = {b: [0.0] * 4 for b in brokers}
    for load in loads:
        if load.broker_id in totals:
            for i, value in enumerate((load.messages, load.bytes, load.connections, load.spool)):
                totals[load.broker_id][i] += value
    options = []
    for load in loads:
        if (load.shard, load.partition) in excluded or load.broker_id not in totals or load.pressure <= 0:
            continue
        source = totals[load.broker_id]
        if max(source) <= trigger:
            continue
        # Backlog stays on the source until consumers drain it; migration does not move stored messages.
        vector = (load.messages, load.bytes, load.connections, 0)
        after_source = max(a - b for a, b in zip(source, vector, strict=True))
        for broker, total in totals.items():
            if broker == load.broker_id:
                continue
            after_target = max(a + b for a, b in zip(total, vector, strict=True))
            if after_target > target or max(after_source, after_target) >= max(source):
                continue
            relieves = after_source <= target
            options.append(
                (
                    (
                        not relieves,
                        load.pressure if relieves else -load.pressure,
                        after_target,
                        broker,
                        load.partition,
                    ),
                    load,
                    broker,
                )
            )
    if not options:
        return None
    _, partition, destination = min(options, key=lambda item: item[0])
    return partition, destination
