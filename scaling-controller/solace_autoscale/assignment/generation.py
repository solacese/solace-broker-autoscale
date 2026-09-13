"""Monotonic per-shard generation numbers for the event spine (ADR 0009).

A topology snapshot is stamped with a generation (``gen``) that must strictly increase per shard.
``gen`` is both the version that makes event application idempotent (a consumer ignores any snapshot
with ``gen`` less than or equal to the one it has applied) and the fence for ordering across a broker
handoff. It must therefore never go backwards or repeat, even across control-plane restarts or with
multiple control-plane instances.

The allocator is a tiny protocol so the pure model and tests do not depend on a database. The
in-memory implementation here is used by tests and single-process runs; the multi-instance daemon
backs it with the store's optimistic-locking table (a compare-and-set bump), the same mechanism used
for placement writes.
"""

from __future__ import annotations

from typing import Protocol


class GenerationCounter(Protocol):
    """Allocates strictly increasing generation numbers per shard."""

    def current(self, shard: str) -> int:
        """The generation last allocated for ``shard``; 0 if none has been allocated yet."""
        ...

    def next(self, shard: str) -> int:
        """Allocate and return the next generation for ``shard`` (strictly greater than
        ``current(shard)``). Must be safe against losing an update: two racing callers must not
        receive the same number."""
        ...


class InMemoryGenerationCounter:
    """Process-local generation counter. Deterministic and offline; the default for single-process
    runs and every test. Not shared across processes; multi-instance uses the store-backed counter."""

    def __init__(self) -> None:
        self._by_shard: dict[str, int] = {}

    def current(self, shard: str) -> int:
        return self._by_shard.get(shard, 0)

    def next(self, shard: str) -> int:
        nxt = self._by_shard.get(shard, 0) + 1
        self._by_shard[shard] = nxt
        return nxt
