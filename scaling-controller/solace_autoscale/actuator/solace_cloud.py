"""Solace Cloud (Mission Control) REST client - the ONLY component permitted to call the API (§2).

Endpoints (verified against the Mission Control OpenAPI, version 2.0.0):
  POST   /api/v2/missionControl/eventBrokerServices                     createService  → 202 Operation
  DELETE /api/v2/missionControl/eventBrokerServices/{id}                deleteService  → 202 Operation
  PATCH  /api/v2/missionControl/eventBrokerServices/{serviceId}/messageSpool  updateMessageSpool
  GET    /api/v2/missionControl/eventBrokerServices/{id}                getService
  GET    /api/v2/missionControl/eventBrokerServices/{serviceId}/operations/{operationId}  (preferred)
  GET    /api/v2/missionControl/eventBrokerServices/multiResourceOperations/{operationId}  (id-only)
  GET    /api/v2/missionControl/eventBrokerServices/{serviceId}/brokerState
  GET    {configured broker SEMP endpoint}/SEMP/v2/monitor/...       (pre-delete observation)

create/delete are async: they return 202 with an OperationResponse whose data.id is the operation to
poll. Mutation calls carry an idempotency key header; duplicate prevention also requires server support
and caller-side reconciliation after timeouts. The exact header name is configurable because it is
not pinned in the spec; default
``Idempotency-Key``.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlparse

import httpx

from ..cloud import OperationStatus

if TYPE_CHECKING:
    from ..config import Config

DEFAULT_BASE = "https://api.solace.cloud"


@dataclass(frozen=True)
class SempConnection:
    """Management connection for one service; credentials are never included in repr/logs."""

    base_url: str
    username: str = field(repr=False)
    password: str = field(repr=False)


class SolaceCloudClient:
    def __init__(self, api_token: str, base_url: str = DEFAULT_BASE, *,
                 idempotency_header: str = "Idempotency-Key", timeout: float = 30.0,
                 semp_connections: dict[str, SempConnection] | None = None) -> None:
        self._base = base_url.rstrip("/")
        self._semp_connections = dict(semp_connections or {})
        # Separate authentication domains: never send the Cloud bearer token to a broker.
        self._semp_client = httpx.Client(timeout=timeout, verify=True)
        self._idem_header = idempotency_header
        self._client = httpx.Client(
            headers={"Authorization": f"Bearer {api_token}",
                     "Content-Type": "application/json"},
            timeout=timeout,
        )

    @classmethod
    def from_config(cls, config: Config, api_token: str) -> SolaceCloudClient:
        """Build a client from ``cloud:`` config. The token stays out of config (env/secret)."""
        c = config.cloud
        return cls(
            api_token,
            base_url=c.effective_base_url(),
            idempotency_header=c.idempotency_header,
            timeout=c.timeout,
        )

    def _post(self, path: str, body: dict[str, Any], idem: str) -> dict[str, Any]:
        r = self._client.post(f"{self._base}{path}", json=body,
                              headers={self._idem_header: idem})
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str, idem: str) -> dict[str, Any]:
        r = self._client.request("DELETE", f"{self._base}{path}",
                                 headers={self._idem_header: idem})
        r.raise_for_status()
        return r.json()

    def _patch(self, path: str, body: dict[str, Any], idem: str) -> dict[str, Any]:
        r = self._client.patch(f"{self._base}{path}", json=body,
                               headers={self._idem_header: idem})
        r.raise_for_status()
        return r.json()

    def _get(self, path: str) -> dict[str, Any]:
        r = self._client.get(f"{self._base}{path}")
        r.raise_for_status()
        return r.json()

    # ---- CloudClient protocol ----------------------------------------------------------------

    def create_service(self, body: dict[str, Any], idempotency_key: str) -> str:
        resp = self._post("/api/v2/missionControl/eventBrokerServices", body, idempotency_key)
        return str(resp["data"]["id"])  # operation id (202 OperationResponse)

    def delete_service(self, service_id: str, idempotency_key: str) -> str:
        resp = self._delete(f"/api/v2/missionControl/eventBrokerServices/{service_id}",
                            idempotency_key)
        return str(resp["data"]["id"])

    def update_message_spool(self, service_id: str, size_gb: int, idempotency_key: str) -> str:
        resp = self._patch(
            f"/api/v2/missionControl/eventBrokerServices/{service_id}/messageSpool",
            {"messageSpoolSizeInGB": size_gb}, idempotency_key,
        )
        return str(resp.get("data", {}).get("id", ""))

    def get_service(self, service_id: str) -> dict[str, Any]:
        """GET the full Service (getService).

        Returns the Service record: ``serviceClassId``, ``creationState`` (see
        ``ServiceCreationState``), ``adminState`` (``ServiceAdminState``), ``msgVpnName``,
        ``serviceConnectionEndpoints`` (hosts/ports/protocols), ``messageSpoolDetails``, and
        ``ongoingOperationIds``. This is the spec-first way to read endpoints and lifecycle state.
        """
        return self._get(f"/api/v2/missionControl/eventBrokerServices/{service_id}")

    def get_service_operation(self, service_id: str, operation_id: str) -> dict[str, Any]:
        """GET a service-scoped operation (spec's first-documented operation path).

        Prefer this when the serviceId is known (create/delete/update all target a known service);
        ``status`` is one of ``OperationStatus``. Use ``get_operation`` only when just the op id is
        available.
        """
        return self._get(
            f"/api/v2/missionControl/eventBrokerServices/{service_id}/operations/{operation_id}"
        )

    def get_operation(self, operation_id: str) -> dict[str, Any]:
        """GET an operation by id alone, via the multi-resource operation endpoint.

        This is the id-only fallback for callers that do not have the serviceId. When the serviceId
        is known, ``get_service_operation`` is the more specific path. ``status`` is one of
        ``OperationStatus``.
        """
        return self._get(
            f"/api/v2/missionControl/eventBrokerServices/multiResourceOperations/{operation_id}"
        )

    @staticmethod
    def operation_status(operation_response: dict[str, Any]) -> OperationStatus:
        """Parse ``data.status`` of an Operation/MultiResourceOperation into the typed enum.

        Callers compare against ``OperationStatus.SUCCEEDED`` / ``.is_terminal()`` rather than a
        bare string. Raises ``ValueError`` (via the enum) on an unexpected value, surfacing a
        spec drift instead of silently treating it as not-done.
        """
        raw = str(operation_response.get("data", {}).get("status", ""))
        return OperationStatus(raw)

    def get_broker_state(self, service_id: str) -> dict[str, Any]:
        return self._get(
            f"/api/v2/missionControl/eventBrokerServices/{service_id}/brokerState"
        )

    def list_services(self, page_size: int = 100) -> list[dict[str, Any]]:
        """Return all event broker services in the org (paged).

        Mission Control has no first-class tag/label field on a service, so a "scaling cluster" is
        identified by a shared name prefix (see ``find_by_name_prefix``). This is the read side of
        that convention. The API has no server-side name filter, so filtering is done client-side.
        """
        out: list[dict[str, Any]] = []
        offset = 1
        while True:
            resp = self._get(
                "/api/v2/missionControl/eventBrokerServices"
                f"?pageSize={page_size}&pageNumber={offset}"
            )
            batch = resp.get("data", [])
            out.extend(batch)
            meta = resp.get("meta", {}).get("pagination", {})
            total_pages = meta.get("totalPages")
            if total_pages is None or offset >= int(total_pages) or not batch:
                break
            offset += 1
        return out

    def find_by_name_prefix(self, prefix: str) -> list[dict[str, Any]]:
        """Return services whose name starts with ``prefix`` (the scaling-cluster convention).

        This is how the tool bundles and finds the brokers it scaled out together, given that
        Mission Control exposes no tag field. Matching is exact-prefix on the service ``name``.
        """
        return [s for s in self.list_services() if str(s.get("name", "")).startswith(prefix)]

    @staticmethod
    def _counter(value: Any) -> float:
        """Missing/invalid counters are unknown, never evidence that a broker is empty."""
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value < 0):
            raise ValueError("missing or invalid SEMP counter")
        return float(value)

    def _endpoint_counts(self, path: str, get: Callable[[str], dict[str, Any]]) -> tuple[float, float]:
        """Read messages and consumer flows from ALL pages of queues/topic endpoints.

        SEMP returns child counts in collections[i], aligned with data[i]. Its nextPageUri
        can name a host: retain only the verified collection path/query and send subsequent
        requests to the explicitly configured SEMP endpoint, never a host from pagination.
        """
        next_path = path + "?count=100"
        seen: set[str] = set()
        messages = flows = 0.0
        received = 0
        expected: float | None = None
        while next_path:
            if next_path in seen or len(seen) >= 10000:
                raise ValueError("incomplete SEMP pagination: repeated or excessive pages")
            seen.add(next_path)
            body = get(next_path)
            data, collections = body.get("data"), body.get("collections")
            if not isinstance(data, list) or not isinstance(collections, list):
                raise ValueError("incomplete SEMP endpoint data/collections")
            if len(data) != len(collections):
                raise ValueError("incomplete SEMP endpoint child counts")
            count = self._counter(body.get("meta", {}).get("count"))
            if expected is not None and count != expected:
                raise ValueError("endpoint collection changed during deletion observation; retry")
            expected = count
            received += len(data)
            for endpoint, children in zip(data, collections, strict=True):
                messages += self._counter(children.get("msgs", {}).get("count"))
                flow_count = children.get("txFlows", {}).get("count")
                if flow_count is None:
                    # Brokers may omit child flow counts in the parent response. The
                    # txFlows collection itself provides a total in meta.count.
                    key = "queueName" if path.endswith("/queues") else "topicEndpointName"
                    name = endpoint.get(key)
                    if not isinstance(name, str) or not name:
                        raise ValueError("incomplete SEMP endpoint identity")
                    flow_body = get(path + "/" + quote(name, safe="") + "/txFlows?count=1")
                    flow_count = flow_body.get("meta", {}).get("count")
                flows += self._counter(flow_count)
            uri = body.get("meta", {}).get("paging", {}).get("nextPageUri")
            if not uri:
                if received != expected:
                    raise ValueError("incomplete SEMP pagination: endpoint count mismatch")
                break
            parsed = urlparse(uri)
            semp_path = path[path.index("/SEMP/"):]
            if parsed.path not in (path, semp_path) or not parsed.query:
                raise ValueError("unexpected SEMP pagination target")
            next_path = path + "?" + parsed.query
        return messages, flows

    def queue_state(self, service_id: str, msg_vpn: str) -> dict[str, Any]:
        """Fail-closed pre-delete observation, including topic endpoints and live clients.

        A connected producer can refill a queue after a check, so even an otherwise empty
        service is refused while clients remain. Callers must quiesce publishing first.
        """
        connection = self._semp_connections.get(service_id)
        if connection is None:
            raise ValueError("no direct SEMP management connection configured for service; "
                             "Cloud bearer authentication is not a SEMP monitoring proxy")
        def get(path: str) -> dict[str, Any]:
            response = self._semp_client.get(
                connection.base_url.rstrip("/") + path,
                auth=(connection.username, connection.password),
            )
            response.raise_for_status()
            return response.json()

        base = f"/SEMP/v2/monitor/msgVpns/{quote(msg_vpn, safe='')}"
        vpn = get(base).get("data")
        if not isinstance(vpn, dict):
            raise ValueError("missing SEMP VPN data")
        messages = self._counter(vpn.get("msgSpoolMsgCount"))
        spooled = self._counter(vpn.get("msgSpoolUsage"))
        clients = get(base + "/clients?count=1")
        connected = self._counter(clients.get("meta", {}).get("count"))
        endpoint_messages = flows = 0.0
        for collection in ("queues", "topicEndpoints"):
            count, active = self._endpoint_counts(base + "/" + collection, get)
            endpoint_messages += count
            flows += active
        return {
            "total_msgs_spooled": max(messages, endpoint_messages),
            "bound_consumers": flows,
            "active_flows": max(flows, connected),
            "spooled_bytes": spooled,
        }

    def close(self) -> None:
        self._client.close()
        self._semp_client.close()
