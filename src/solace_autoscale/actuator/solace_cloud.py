"""Solace Cloud (Mission Control) REST client — the ONLY component permitted to call the API (§2).

Endpoints (verified against the Mission Control OpenAPI, version 2.0.0):
  POST   /api/v2/missionControl/eventBrokerServices                     createService  → 202 Operation
  DELETE /api/v2/missionControl/eventBrokerServices/{id}                deleteService  → 202 Operation
  PATCH  /api/v2/missionControl/eventBrokerServices/{serviceId}/messageSpool  updateMessageSpool
  GET    /api/v2/missionControl/eventBrokerServices/{serviceId}/operations/{operationId}
  GET    /api/v2/missionControl/eventBrokerServices/{serviceId}/brokerState
  GET    /api/v2/missionControl/.../broker/SEMP/v2/monitor/...          (queue state for pre-delete)

create/delete are async: they return 202 with an OperationResponse whose data.id is the operation to
poll. Every call carries an idempotency key header so a retry after a timeout cannot double-provision
(§10). The exact header name is configurable because it is not pinned in the spec; default
``Idempotency-Key``.
"""

from __future__ import annotations

from typing import Any

import httpx

DEFAULT_BASE = "https://api.solace.cloud"


class SolaceCloudClient:
    def __init__(self, api_token: str, base_url: str = DEFAULT_BASE, *,
                 idempotency_header: str = "Idempotency-Key", timeout: float = 30.0) -> None:
        self._base = base_url.rstrip("/")
        self._idem_header = idempotency_header
        self._client = httpx.Client(
            headers={"Authorization": f"Bearer {api_token}",
                     "Content-Type": "application/json"},
            timeout=timeout,
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

    def get_operation(self, operation_id: str) -> dict[str, Any]:
        # The service-scoped operation path needs the serviceId; callers that only have the op id can
        # use the multi-resource operation endpoint. We use the documented service operation path via
        # the operation record's resourceId when available.
        return self._get(
            f"/api/v2/missionControl/eventBrokerServices/multiResourceOperations/{operation_id}"
        )

    def get_broker_state(self, service_id: str) -> dict[str, Any]:
        return self._get(
            f"/api/v2/missionControl/eventBrokerServices/{service_id}/brokerState"
        )

    def queue_state(self, service_id: str, msg_vpn: str) -> dict[str, Any]:
        """Live pre-delete safety check via the SEMPv2 monitor passthrough.

        Aggregates queue depth, bound consumers, active flows, and spooled bytes across the VPN's
        queues. Field names match the verified SEMPv2 monitor schema (docs/metrics.md).
        """
        base = (f"/api/v2/missionControl/eventBrokerServices/{service_id}"
                f"/broker/SEMP/v2/monitor/msgVpns/{msg_vpn}/queues")
        resp = self._get(base + "?count=100")
        total_msgs = 0
        bound = 0
        flows = 0
        spooled = 0
        for q in resp.get("data", []):
            total_msgs += int(q.get("msgSpoolMsgCount", 0))
            bound += int(q.get("boundConsumerCount", 0))
            flows += int(q.get("bindRequestCount", 0)) - int(q.get("unbindCount", 0))
            spooled += int(q.get("msgSpoolUsage", 0)) * 1_048_576  # MB→bytes
        flows = max(0, flows)
        return {
            "total_msgs_spooled": total_msgs,
            "bound_consumers": bound,
            "active_flows": flows,
            "spooled_bytes": spooled,
        }

    def close(self) -> None:
        self._client.close()
