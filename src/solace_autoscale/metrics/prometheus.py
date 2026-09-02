"""PubSub+ Prometheus exporter collector - documented stub.

No exporter dump was supplied. Per §1/§14 we do NOT invent metric names. This collector raises
``NotImplementedError`` listing the normalised fields it must map, to be completed against a real
exporter sample.
"""

from __future__ import annotations

from ..decision.types import MetricSample
from .base import MetricsCollector

REQUIRED_FIELDS = (
    "ingress_msg_rate",
    "egress_msg_rate",
    "ingress_byte_rate",
    "egress_byte_rate",
    "connection_count",
    "spool_used (bytes)",
)


class PrometheusCollector(MetricsCollector):
    def __init__(self, *_: object, **__: object) -> None:
        pass

    def collect(self, shard_name: str, msg_vpn: str, now: float, current_brokers: int) -> MetricSample:
        raise NotImplementedError(
            "prometheus metrics collector is not implemented: no PubSub+ Prometheus exporter dump "
            "was supplied, and metric names must not be guessed (§1, §14). Provide a scrape sample "
            "to map: " + ", ".join(REQUIRED_FIELDS) + ". Use metrics.source: semp or static instead."
        )
