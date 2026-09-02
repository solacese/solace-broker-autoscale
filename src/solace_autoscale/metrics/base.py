"""Metrics collector interface.

A collector turns a broker metrics source into normalised ``MetricSample`` windows per shard. The
decision engine never sees raw broker fields - collectors do the mapping (see docs/metrics.md).

Collectors may do I/O; the engine does not. ``current_brokers`` comes from the fleet inventory, not
the broker, so ``collect`` takes a callable that supplies it per shard.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..decision.types import MetricSample


class MetricsCollector(ABC):
    """Produce the latest ``MetricSample`` for a shard's VPN, mapped to the normalised schema."""

    @abstractmethod
    def collect(
        self,
        shard_name: str,
        msg_vpn: str,
        now: float,
        current_brokers: int,
    ) -> MetricSample:
        """Return one MetricSample. ``now`` and ``current_brokers`` are supplied by the caller so the
        collector performs no clock read and no fleet lookup of its own."""
        raise NotImplementedError


class CollectorError(Exception):
    pass
