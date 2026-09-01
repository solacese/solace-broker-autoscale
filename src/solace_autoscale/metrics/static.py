"""Static collector: reads pre-captured MetricSample windows from JSON. No field-name guessing.

File format (``metrics.static_path``):

    {
      "shards": {
        "domain-a": {
          "msg_vpn": "acme-prod",
          "samples": [
            {"timestamp": 1000000, "ingress_msg_rate": 1000, "egress_msg_rate": 1000,
             "ingress_byte_rate": 1000000, "egress_byte_rate": 1000000, "avg_msg_size": 1000,
             "connection_count": 100, "spool_used": 1000000, "current_brokers": 1},
            ...
          ]
        }
      }
    }

Used by tests, offline analysis, and CI (needs no live broker).
"""

from __future__ import annotations

import json
from pathlib import Path

from ..decision.types import MetricSample
from .base import CollectorError, MetricsCollector


class StaticCollector(MetricsCollector):
    def __init__(self, path: str | Path) -> None:
        self._doc = json.loads(Path(path).read_text())
        if "shards" not in self._doc:
            raise CollectorError("static metrics file missing top-level 'shards'")

    def window(self, shard_name: str) -> list[MetricSample]:
        shard = self._doc["shards"].get(shard_name)
        if shard is None:
            raise CollectorError(f"no samples for shard {shard_name!r} in static file")
        return [MetricSample(**s) for s in shard["samples"]]

    def msg_vpn(self, shard_name: str) -> str:
        return self._doc["shards"][shard_name].get("msg_vpn", shard_name)

    def shard_names(self) -> list[str]:
        return list(self._doc["shards"].keys())

    def subscribing_brokers(self, shard_name: str) -> int:
        """Brokers subscribing to this shard (mesh link math, §5.3). Defaults to 1."""
        return int(self._doc["shards"][shard_name].get("subscribing_brokers", 1))

    def key_subdividable(self, shard_name: str) -> bool:
        """Whether the shard key can subdivide further (§5.6 hot-shard). Defaults to True."""
        return bool(self._doc["shards"][shard_name].get("key_subdividable", True))

    def collect(self, shard_name: str, msg_vpn: str, now: float, current_brokers: int) -> MetricSample:
        samples = self.window(shard_name)
        if not samples:
            raise CollectorError(f"empty sample window for shard {shard_name!r}")
        return max(samples, key=lambda s: s.timestamp)
