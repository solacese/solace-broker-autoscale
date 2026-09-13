"""Solace Cloud monitoring API collector - documented stub.

No sample payload of the Cloud managed monitoring API was supplied. Per §1/§14 we do NOT invent
field names. This collector raises ``NotImplementedError`` listing exactly the fields it needs, so
the mapping can be filled in the moment a real sample is available.
"""

from __future__ import annotations

from ..decision.types import MetricSample
from .base import MetricsCollector

# Fields this collector must map, once a real Cloud monitoring payload is provided.
REQUIRED_FIELDS = (
    "ingress_msg_rate",
    "egress_msg_rate",
    "ingress_byte_rate",
    "egress_byte_rate",
    "connection_count",
    "spool_used (bytes)",
)


class CloudApiCollector(MetricsCollector):
    def __init__(self, *_: object, **__: object) -> None:
        pass

    def collect(self, shard_name: str, msg_vpn: str, now: float, current_brokers: int) -> MetricSample:
        raise NotImplementedError(
            "cloud-api metrics collector is not implemented: no sample of the Solace Cloud "
            "monitoring API payload was supplied, and field names must not be guessed (§1, §14). "
            "Provide a captured sample to map these fields: " + ", ".join(REQUIRED_FIELDS) + ". "
            "In the meantime use metrics.source: semp (verified) or static."
        )
