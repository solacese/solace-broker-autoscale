"""Idempotent, scoped queue preparation and broker-enforced ingress fencing over SEMP."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from threading import Event
from typing import Any
from urllib.parse import quote

import httpx

from ..actuator.solace_cloud import SempConnection
from ..assignment.topics import TopicRegistry, group_queue, topic_prefix
from ..decision.types import MetricSample
from ..metrics.semp import map_vpn_monitor
from .store import queue_name


@dataclass(frozen=True)
class QueueStatus:
    messages: int
    unacked: int
    consumers: int
    spool_bytes: int
    ingress_enabled: bool
    spooled_messages: int
    spooled_bytes: int
    all_ingress_enabled: bool | None = None

    @property
    def drained(self) -> bool:
        """Consumer-bound messages are outstanding until acknowledged, even at zero ready depth."""
        return self.messages == self.unacked == self.spool_bytes == 0

    @property
    def fully_enabled(self) -> bool:
        """Require every group queue to accept ingress during normal ownership."""
        return self.ingress_enabled if self.all_ingress_enabled is None else self.all_ingress_enabled


class QueueManager:
    """Only operates generated queue names in the explicitly configured fleet namespace."""

    def __init__(
        self,
        fleet_id: str,
        connections: dict[str, SempConnection],
        vpns: dict[str, str],
        spool_mb: int = 1000,
    ) -> None:
        self.fleet_id, self.connections, self.vpns, self.spool_mb = fleet_id, connections, vpns, spool_mb
        self.registry: TopicRegistry | None = None
        self.client_username = "autoscale-app"
        self._native_configured: set[str] = set()
        self.refresh_requested = Event()
        self.client = httpx.Client(timeout=10, verify=True)

    def request_refresh(self) -> None:
        """Coalesce broker-event wakeups; SEMP remains the source of observations."""
        self.refresh_requested.set()

    @staticmethod
    def _already_exists(response: httpx.Response) -> bool:
        return response.status_code == 409 or (
            response.status_code == 400
            and response.json().get("meta", {}).get("error", {}).get("status") == "ALREADY_EXISTS"
        )

    def _request(self, broker: str, method: str, path: str, body: dict | None = None) -> httpx.Response:
        """Keep SEMP credentials on their configured endpoint; never follow redirects."""
        connection = self.connections[broker]
        result = self.client.request(
            method,
            connection.base_url.rstrip("/") + path,
            auth=(connection.username, connection.password),
            json=body,
        )
        return result

    def _path(
        self, broker: str, shard: str, partition: int, api: str = "config", group: str | None = None
    ) -> str:
        return f"/SEMP/v2/{api}/msgVpns/{quote(self.vpns[broker], safe='')}/queues/" + quote(
            self._name(shard, partition, group), safe=""
        )

    def _prepare_one(
        self,
        broker: str,
        shard: str,
        partition: int,
        *,
        enabled: bool = False,
        group: str | None = None,
        patterns: list[str] | None = None,
    ) -> None:
        """Create a durable exclusive queue with NACK-on-shutdown; never adopt unsafe existing settings."""
        path = self._path(broker, shard, partition, group=group)
        result = self._request(broker, "GET", path)
        missing = result.status_code == 404 or (
            result.status_code == 400
            and result.json().get("meta", {}).get("error", {}).get("status") == "NOT_FOUND"
        )
        if missing:
            body = {
                "queueName": self._name(shard, partition, group),
                "accessType": "exclusive",
                "permission": "consume",
                "ingressEnabled": enabled,
                "egressEnabled": True,
                "rejectMsgToSenderOnDiscardBehavior": "always",
                "maxMsgSpoolUsage": self.spool_mb,
            }
            result = self._request(broker, "POST", path.rsplit("/", 1)[0], body)
            if not self._already_exists(result):
                result.raise_for_status()
            result = self._request(broker, "GET", path)
        result.raise_for_status()
        data = result.json()["data"]
        if (
            data.get("rejectMsgToSenderOnDiscardBehavior") != "always"
            or data.get("accessType") != "exclusive"
            or not data.get("egressEnabled")
        ):
            raise ValueError("managed queue has incompatible delivery/fencing settings")
        if data.get("ingressEnabled") is not enabled:
            raise ValueError("managed queue ingress differs from its expected ownership state")

        for pattern in patterns or []:
            subscription = topic_prefix(self.fleet_id, broker, shard, partition) + pattern
            result = self._request(
                broker, "POST", path + "/subscriptions", {"subscriptionTopic": subscription}
            )
            if not self._already_exists(result):
                result.raise_for_status()

    def _ingress_one(
        self, broker: str, shard: str, partition: int, enabled: bool, group: str | None = None
    ) -> None:
        """Fencing is enforced by the broker, including publishers with stale cached assignments."""
        path = self._path(broker, shard, partition, group=group)
        r = self._request(
            broker, "PATCH", path, {"rejectMsgToSenderOnDiscardBehavior": "always", "ingressEnabled": enabled}
        )
        r.raise_for_status()
        data = self._request(broker, "GET", path)
        data.raise_for_status()
        if (
            data.json()["data"].get("ingressEnabled") is not enabled
            or data.json()["data"].get("rejectMsgToSenderOnDiscardBehavior") != "always"
        ):
            raise ValueError("broker did not confirm the ingress fence")

    @staticmethod
    def _counter(value: Any) -> int:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
            or value != int(value)
        ):
            raise ValueError("missing/invalid managed queue telemetry")
        return int(value)

    def _status_one(self, broker: str, shard: str, partition: int, group: str | None = None) -> QueueStatus:
        """Read fresh depth, unacked messages and bound consumers; unknown values are never zero."""
        path = self._path(broker, shard, partition, "monitor", group=group)
        response = self._request(broker, "GET", path)
        response.raise_for_status()
        data = response.json()["data"]
        children = []
        for collection in ("msgs", "txFlows"):
            result = self._request(broker, "GET", path + "/" + collection + "?count=1")
            result.raise_for_status()
            children.append(self._counter(result.json().get("meta", {}).get("count")))
        if not isinstance(data.get("ingressEnabled"), bool):
            raise ValueError("missing ingress state")
        return QueueStatus(
            children[0],
            self._counter(data.get("txUnackedMsgCount")),
            children[1],
            self._counter(data.get("msgSpoolUsage")),
            data["ingressEnabled"],
            self._counter(data.get("spooledMsgCount")),
            self._counter(data.get("spooledByteCount")),
        )

    def _name(self, shard: str, partition: int, group: str | None) -> str:
        base = queue_name(self.fleet_id, shard, partition)
        return group_queue(base, group) if group is not None else base

    def _groups(self, shard: str) -> dict[str | None, list[str]]:
        if self.registry is None:
            return {None: []}
        return {group: patterns for group, patterns in self.registry.groups(shard).items()}

    def configure_native(self, broker: str) -> None:
        """Give the managed application a scoped profile that NACKs unroutable guaranteed topics."""
        if broker in self._native_configured:
            return
        base = "/SEMP/v2/config/msgVpns/" + quote(self.vpns[broker], safe="")
        profile = "autoscale-" + self.fleet_id
        if len(profile) > 32:
            profile = "autoscale-" + hashlib.sha256(self.fleet_id.encode()).hexdigest()[:22]
        body = {
            "clientProfileName": profile,
            "allowGuaranteedMsgSendEnabled": True,
            "allowGuaranteedMsgReceiveEnabled": True,
            "rejectMsgToSenderOnNoSubscriptionMatchEnabled": True,
        }
        result = self._request(broker, "POST", base + "/clientProfiles", body)
        if self._already_exists(result):
            result = self._request(broker, "PATCH", base + "/clientProfiles/" + quote(profile, safe=""), body)
        result.raise_for_status()
        result = self._request(
            broker,
            "PATCH",
            base + "/clientUsernames/" + quote(self.client_username, safe=""),
            {"clientProfileName": profile},
        )
        result.raise_for_status()
        self._native_configured.add(broker)

    def prepare(self, broker: str, shard: str, partition: int, *, enabled: bool = False) -> None:
        if self.registry is not None:
            self.configure_native(broker)
        for group, patterns in self._groups(shard).items():
            self._prepare_one(broker, shard, partition, enabled=enabled, group=group, patterns=patterns)

    def ingress(self, broker: str, shard: str, partition: int, enabled: bool) -> None:
        for group in self._groups(shard):
            self._ingress_one(broker, shard, partition, enabled, group=group)

    def broker_metrics(self, broker: str, shard: str, now: float) -> MetricSample:
        """Read complete VPN totals for residual traffic and dynamic connection pressure."""
        vpn = quote(self.vpns[broker], safe="")
        response = self._request(broker, "GET", f"/SEMP/v2/monitor/msgVpns/{vpn}")
        response.raise_for_status()
        clients = self._request(
            broker, "GET", f"/SEMP/v2/monitor/msgVpns/{vpn}/clients?count=1"
        )
        clients.raise_for_status()
        count = clients.json().get("meta", {}).get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("missing/invalid broker connection telemetry")
        return map_vpn_monitor(response.json()["data"], count, now, 1)

    def status(self, broker: str, shard: str, partition: int) -> QueueStatus:
        states = [self._status_one(broker, shard, partition, group=g) for g in self._groups(shard)]
        if not states:
            return QueueStatus(0, 0, 0, 0, True, 0, 0)
        return QueueStatus(
            sum(s.messages for s in states),
            sum(s.unacked for s in states),
            min(s.consumers for s in states),
            sum(s.spool_bytes for s in states),
            any(s.ingress_enabled for s in states),
            sum(s.spooled_messages for s in states),
            sum(s.spooled_bytes for s in states),
            all(s.ingress_enabled for s in states),
        )

    def close(self) -> None:
        self.client.close()
