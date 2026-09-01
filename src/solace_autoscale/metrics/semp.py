"""SEMPv2 monitor collector. Field names verified against a live broker (docs/metrics.md).

Reads ``GET /SEMP/v2/monitor/msgVpns/{vpn}`` for rates + spool, and the clients collection
``meta.count`` for the live connection count.
"""

from __future__ import annotations

from typing import Any

import httpx

from ..decision.types import MetricSample
from .base import CollectorError, MetricsCollector

MB = 1_048_576


def map_vpn_monitor(
    vpn_data: dict[str, Any],
    connection_count: int,
    now: float,
    current_brokers: int,
) -> MetricSample:
    """Pure mapping from a SEMPv2 msgVpn monitor object + connection count to a MetricSample.

    Kept pure so it can be unit-tested against the captured fixture with no network.
    """
    rx_msg = float(vpn_data.get("averageRxMsgRate", 0.0))
    tx_msg = float(vpn_data.get("averageTxMsgRate", 0.0))
    rx_byte = float(vpn_data.get("averageRxByteRate", 0.0))
    tx_byte = float(vpn_data.get("averageTxByteRate", 0.0))
    spool_mb = float(vpn_data.get("msgSpoolUsage", 0.0))

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
        spool_used=spool_mb * MB,  # SEMP reports MB → bytes
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
        return int(body.get("meta", {}).get("count", len(body.get("data", []))))

    def collect(self, shard_name: str, msg_vpn: str, now: float, current_brokers: int) -> MetricSample:
        vpn = self._get(f"/SEMP/v2/monitor/msgVpns/{msg_vpn}")["data"]
        conns = self.connection_count(msg_vpn)
        return map_vpn_monitor(vpn, conns, now, current_brokers)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> SempCollector:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
