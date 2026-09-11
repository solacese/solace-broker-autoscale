"""Rolling per-shard sample history for the continuous monitor.

The one-shot CLI can only see the window it's handed. The monitor accumulates samples over time so
derived headroom (§5.7, needs growth history) and the evaluation window (§5.8) have real data, and
so the accuracy recorder gets a stream to join. Bounded by a retention horizon so memory stays flat.

Pure in-memory bookkeeping; persistence (if wanted across restarts) is a thin add via the accuracy
store, kept separate to avoid coupling the hot loop to disk.
"""

from __future__ import annotations

from collections import defaultdict, deque

from ..decision.types import MetricSample


class RollingHistory:
    def __init__(self, retention_seconds: float) -> None:
        self._retention = retention_seconds
        self._by_shard: dict[str, deque[MetricSample]] = defaultdict(deque)

    def add(self, shard: str, sample: MetricSample) -> None:
        dq = self._by_shard[shard]
        dq.append(sample)
        self._evict(shard, sample.timestamp)

    def _evict(self, shard: str, now: float) -> None:
        dq = self._by_shard[shard]
        horizon = now - self._retention
        while dq and dq[0].timestamp < horizon:
            dq.popleft()

    def window(self, shard: str) -> list[MetricSample]:
        return list(self._by_shard[shard])

    def shards(self) -> list[str]:
        return list(self._by_shard.keys())
