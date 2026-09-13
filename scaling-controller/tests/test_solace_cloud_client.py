"""SolaceCloudClient endpoint + enum-parsing tests.

Uses an httpx MockTransport so no network is touched. Asserts the exact request URLs, methods, and
idempotency header against the Mission Control OpenAPI v2.0.0, and that operation status parses
into the typed OperationStatus enum. Payload shapes are taken from the spec schema; no field values
are invented beyond what the spec defines.
"""

from __future__ import annotations

import httpx
import pytest

from solace_autoscale.actuator.solace_cloud import SolaceCloudClient
from solace_autoscale.cloud import OperationStatus
from solace_autoscale.config import Config


def _client_recording() -> tuple[SolaceCloudClient, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        # Return a shape appropriate to the path.
        if request.url.path.endswith("/messageSpool"):
            return httpx.Response(200, json={"data": {"id": "op-spool-1"}})
        if "operations/" in request.url.path or "multiResourceOperations/" in request.url.path:
            return httpx.Response(200, json={"data": {"id": "op-1", "status": "INPROGRESS"}})
        if request.method in ("POST", "DELETE"):
            return httpx.Response(202, json={"data": {"id": "op-1"}})
        return httpx.Response(200, json={"data": {"id": "svc-1", "creationState": "COMPLETED"}})

    c = SolaceCloudClient("tok", base_url="https://api.solace.cloud", idempotency_header="Idempotency-Key")
    c._client = httpx.Client(transport=httpx.MockTransport(handler),
                             headers={"Authorization": "Bearer tok"})
    return c, seen


def test_create_service_path_and_idempotency_header():
    c, seen = _client_recording()
    op_id = c.create_service({"name": "s", "serviceClassId": "ENTERPRISE_10K_STANDALONE",
                              "datacenterId": "dc"}, "idem-123")
    assert op_id == "op-1"
    req = seen[-1]
    assert req.method == "POST"
    assert req.url.path == "/api/v2/missionControl/eventBrokerServices"
    assert req.headers["Idempotency-Key"] == "idem-123"


def test_update_message_spool_body_and_path():
    c, seen = _client_recording()
    c.update_message_spool("svc-1", 200, "idem-x")
    req = seen[-1]
    assert req.method == "PATCH"
    assert req.url.path == "/api/v2/missionControl/eventBrokerServices/svc-1/messageSpool"
    assert b'"messageSpoolSizeInGB":200' in req.content.replace(b" ", b"")


def test_get_service_path():
    c, seen = _client_recording()
    data = c.get_service("svc-1")
    assert seen[-1].url.path == "/api/v2/missionControl/eventBrokerServices/svc-1"
    assert data["data"]["creationState"] == "COMPLETED"


def test_service_scoped_operation_path_preferred():
    c, seen = _client_recording()
    c.get_service_operation("svc-1", "op-1")
    assert seen[-1].url.path == "/api/v2/missionControl/eventBrokerServices/svc-1/operations/op-1"


def test_multiresource_operation_is_id_only_fallback():
    c, seen = _client_recording()
    c.get_operation("op-1")
    assert seen[-1].url.path == (
        "/api/v2/missionControl/eventBrokerServices/multiResourceOperations/op-1"
    )


def test_operation_status_parses_to_enum():
    status = SolaceCloudClient.operation_status({"data": {"status": "SUCCEEDED"}})
    assert status is OperationStatus.SUCCEEDED
    assert status.is_terminal() and status.is_success()

    inprog = SolaceCloudClient.operation_status({"data": {"status": "INPROGRESS"}})
    assert not inprog.is_terminal()


def test_operation_status_unknown_value_raises():
    # Spec drift surfaces as an error, not a silent "not done".
    with pytest.raises(ValueError):
        SolaceCloudClient.operation_status({"data": {"status": "WAT"}})


def test_from_config_uses_region_base_and_header():
    cfg = Config.model_validate({
        "cloud": {"region": "eu", "idempotency_header": "X-Idem", "timeout": "10s"},
    })
    c = SolaceCloudClient.from_config(cfg, "tok")
    assert c._base == "https://api.solacecloud.eu"
    assert c._idem_header == "X-Idem"
    c.close()
