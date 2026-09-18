"""SEMPv2 monitor collector. Field names verified against a live broker (docs/metrics.md).

Reads ``GET /SEMP/v2/monitor/msgVpns/{vpn}`` for rates + spool, and the clients collection
``meta.count`` for the live connection count.
"""

from __future__ import annotations

import math
from typing import Any
from urllib.parse import quote

import httpx

from ..decision.types import MetricSample
from .base import CollectorError, MetricsCollector


def map_vpn_monitor(
    vpn_data: dict[str, Any],
    connection_count: int,
    now: float,
    current_brokers: int,
) -> MetricSample:
    """Pure mapping from a SEMPv2 msgVpn monitor object + connection count to a MetricSample.

    Kept pure so it can be unit-tested against the captured fixture with no network.
    """
    required = ("averageRxMsgRate", "averageTxMsgRate", "averageRxByteRate",
                "averageTxByteRate", "msgSpoolUsage")
    try:
        values = [vpn_data[k] for k in required]
        if any(isinstance(v, bool) or not isinstance(v, (int, float))
               or not math.isfinite(v) or v < 0 for v in values):
            raise ValueError("invalid counters")
        rx_msg, tx_msg, rx_byte, tx_byte, spool_bytes = map(float, values)
    except (KeyError, TypeError, ValueError) as e:
        raise CollectorError("SEMP VPN metrics missing or invalid; refusing to substitute zero load") from e

    if rx_msg > 0:
        avg_size = rx_byte / rx_msg
    elif tx_msg > 0:
        avg_size = tx_byte / tx_msg
    else:
        avg_size = 0.0

    return MetricSample(
        timestamp=now,
        ingress_msg_rate=rx_msg,
        egress_msg_rate=tx_msg,
        ingress_byte_rate=rx_byte,
        egress_byte_rate=tx_byte,
        avg_msg_size=avg_size,
        connection_count=connection_count,
        spool_used=spool_bytes,  # monitor msgSpoolUsage is bytes; maxMsgSpoolUsage is MB
        current_brokers=current_brokers,
    )


class SempCollector(MetricsCollector):
    def __init__(self, base_url: str, username: str, password: str, *, verify: bool = True,
                 timeout: float = 10.0) -> None:
        self._base = base_url.rstrip("/")
        self._client = httpx.Client(
            auth=(username, password), verify=verify, timeout=timeout,
        )

    def _get(self, path: str) -> dict[str, Any]:
        try:
            resp = self._client.get(f"{self._base}{path}")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise CollectorError(f"SEMP request failed: {path}: {e}") from e
        return resp.json()

    def connection_count(self, msg_vpn: str) -> int:
        # meta.count on the clients collection is the live connection count (no scalar VPN field).
        body = self._get(f"/SEMP/v2/monitor/msgVpns/{msg_vpn}/clients?count=1")
        count = body.get("meta", {}).get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise CollectorError("SEMP clients response missing a valid total meta.count")
        return count

    def collect(self, shard_name: str, msg_vpn: str, now: float, current_brokers: int) -> MetricSample:
        if current_brokers != 1:
            raise CollectorError("one SEMP endpoint observes one broker; aggregate fleet metrics explicitly")
        msg_vpn = quote(msg_vpn, safe="")
        vpn = self._get(f"/SEMP/v2/monitor/msgVpns/{msg_vpn}")["data"]
        conns = self.connection_count(msg_vpn)
        return map_vpn_monitor(vpn, conns, now, current_brokers)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> SempCollector:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
