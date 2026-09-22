"""Bounded exact-ID lifecycle for isolated Solace Cloud qualification services."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..actuator.solace_cloud import SolaceCloudClient

SERVICE_PATH = "/api/v2/missionControl/eventBrokerServices"


class ServiceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    name: str = Field(min_length=1, max_length=50)
    service_class_id: str = Field(alias="serviceClassId")
    datacenter_id: str = Field(alias="datacenterId")
    event_broker_version: str = Field(alias="eventBrokerVersion")
    msg_vpn_name: str = Field(alias="msgVpnName")
    redundancy_group_ssl_enabled: bool = Field(alias="redundancyGroupSslEnabled")
    locked: bool = False


class QualificationPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: str
    region: str
    service_class: str
    broker_version: str
    budget_eur: float = Field(gt=0)
    billing_basis: str = "internal-non-billable-user-attested"
    max_runtime_minutes: int = Field(default=180, ge=15, le=480)
    cleanup_timeout_minutes: int = Field(default=60, ge=15, le=240)
    delete_test_services_after_run: bool
    service_count: int = Field(ge=1, le=2)
    services_created: list[dict[str, Any]] = Field(default_factory=list)
    price_eur_per_hour: float | None = Field(default=None, ge=0)
    purpose: str
    requests: list[ServiceRequest]

    @model_validator(mode="after")
    def bounded_scope(self) -> QualificationPlan:
        if not self.delete_test_services_after_run:
            raise ValueError("qualification requires cleanup")
        if self.billing_basis != "internal-non-billable-user-attested":
            if self.price_eur_per_hour is None:
                raise ValueError("a verified EUR rate is required for a billable account")
        if self.service_count != len(self.requests) or len(self.requests) > 2:
            raise ValueError("service_count must match at most two requests")
        names = [request.name for request in self.requests]
        if len(set(names)) != len(names):
            raise ValueError("qualification service names must be unique")
        for request in self.requests:
            if request.service_class_id != self.service_class:
                raise ValueError("every request must use the authorized service class")
            if request.datacenter_id != self.region or request.event_broker_version != self.broker_version:
                raise ValueError("every request must use the authorized region and broker version")
            if request.locked or not request.redundancy_group_ssl_enabled:
                raise ValueError("test services must be deletable HA services with mate-link encryption")
        if self.price_eur_per_hour is not None:
            projected = self.price_eur_per_hour * self.service_count * self.max_runtime_minutes / 60
            if projected > self.budget_eur:
                raise ValueError("bounded qualification would exceed the EUR spending ceiling")
        return self


@dataclass(frozen=True)
class CleanupResult:
    attempted: int
    deleted: int
    remaining: tuple[str, ...]


class RunJournal:
    """Private crash-recovery journal; writes are atomic and owner-readable only."""

    def __init__(
        self, path: str | Path, plan: QualificationPlan, *, now: Callable[[], float] = time.time
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            self.data = json.loads(self.path.read_text())
        else:
            self.data = {
                "schema_version": "1",
                "started_at": _utc(),
                "deadline_epoch": now() + plan.max_runtime_minutes * 60,
                "billing": {
                    "basis": plan.billing_basis,
                    "budget_eur": plan.budget_eur,
                    "price_eur_per_hour": plan.price_eur_per_hour,
                },
                "services": [],
                "events": [],
            }
            self.save()

    def save(self) -> None:
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump(self.data, stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
        finally:
            if temporary.exists():
                temporary.unlink()

    def event(self, name: str, **detail: Any) -> None:
        self.data["events"].append({"at": _utc(), "event": name, **detail})
        self.save()

    def service(self, name: str) -> dict[str, Any]:
        for item in self.data["services"]:
            if item["name"] == name:
                return item
        item = {"name": name, "idempotency_key": "qualify-" + hashlib.sha256(name.encode()).hexdigest()[:24]}
        self.data["services"].append(item)
        self.save()
        return item

    def created_ids(self) -> tuple[str, ...]:
        return tuple(item["service_id"] for item in self.data["services"] if item.get("service_id"))


class QualificationRunner:
    def __init__(
        self,
        plan: QualificationPlan,
        journal: RunJournal,
        cloud: SolaceCloudClient,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.plan, self.journal, self.cloud = plan, journal, cloud
        self.sleep, self.clock = sleep, clock

    def _bounded(self) -> None:
        if self.clock() >= float(self.journal.data["deadline_epoch"]):
            raise TimeoutError("qualification deadline reached")

    def preflight(self) -> None:
        self._bounded()
        names = {request.name for request in self.plan.requests}
        existing = [service for service in self.cloud.list_services() if service.get("name") in names]
        known = set(self.journal.created_ids())
        for service in existing:
            service_id = str(service.get("id", ""))
            record = next(
                (item for item in self.journal.data["services"]
                 if item["name"] == service.get("name")),
                None,
            )
            if service_id in known:
                continue
            if record and record.get("create_attempted_at") and service_id:
                record["service_id"] = service_id
                record["create_outcome"] = "reconciled-after-uncertain-response"
                self.journal.save()
                known.add(service_id)
                continue
            raise ValueError("a requested qualification service name already exists")
        datacenter = self.cloud.get_datacenter(self.plan.region)["data"]
        if not datacenter.get("available") or self.plan.service_class not in datacenter.get(
            "supportedServiceClasses", []
        ):
            raise ValueError("authorized datacenter is unavailable or lacks the requested class")
        versions = self.cloud.list_service_versions(self.plan.region)["data"]
        matches = [
            version for version in versions
            if version.get("version") == self.plan.broker_version
            and version.get("recommended") is True
            and self.plan.service_class in version.get("supportedServiceClasses", [])
        ]
        if len(matches) != 1:
            raise ValueError("authorized broker version is not uniquely available and recommended")
        self.journal.event("preflight-passed", existing_services=len(existing))

    def _wait_operation(
        self, operation_id: str, service_id: str | None = None, *, deadline: float | None = None
    ) -> dict[str, Any]:
        while True:
            if deadline is None:
                self._bounded()
            elif time.monotonic() >= deadline:
                raise TimeoutError("cleanup deadline reached")
            try:
                body = (
                    self.cloud.get_service_operation(service_id, operation_id)
                    if service_id else self.cloud.get_operation(operation_id)
                )
            except httpx.HTTPStatusError as exc:
                if service_id and exc.response.status_code == 404:
                    return {"id": operation_id, "resourceId": service_id, "status": "SUCCEEDED"}
                raise
            status = self.cloud.operation_status(body)
            self.journal.event("operation", operation_id=operation_id, status=status.value)
            if status.is_terminal():
                if not status.is_success():
                    raise RuntimeError(f"Cloud operation failed: {operation_id}")
                return body["data"]
            self.sleep(10)

    def create_all(self) -> tuple[str, ...]:
        self.preflight()
        for request in self.plan.requests:
            record = self.journal.service(request.name)
            if record.get("service_id"):
                service = self.cloud.get_service(record["service_id"])["data"]
                if service.get("creationState") != "COMPLETED" or service.get("adminState") != "START":
                    operation_id = record.get("create_operation_id")
                    if not operation_id:
                        raise ValueError("journaled service is not ready and has no create operation")
                    self._wait_operation(operation_id, record["service_id"])
                self._verify_service(request, record)
                continue
            payload = request.model_dump(by_alias=True)
            payload["serviceConnectionEndpoints"] = [{
                "name": "public",
                "accessType": "PUBLIC",
                "ports": [
                    {"protocol": "serviceManagementTlsListenPort", "port": 943},
                    {"protocol": "serviceSmfTlsListenPort", "port": 55443},
                    {"protocol": "serviceAmqpTlsListenPort", "port": 5671},
                ],
            }]
            record["create_attempted_at"] = _utc()
            self.journal.save()
            try:
                response = self.cloud.create_service_request(payload, record["idempotency_key"])
                operation = response["data"]
                operation_id = str(operation["id"])
                service_id = str(operation.get("resourceId", ""))
                record["create_operation_id"] = operation_id
                if service_id:
                    record["service_id"] = service_id
                self.journal.save()
                operation = self._wait_operation(operation_id, service_id or None)
                service_id = str(operation.get("resourceId", service_id))
            except httpx.HTTPError as exc:
                error_body: Any = {}
                if isinstance(exc, httpx.HTTPStatusError):
                    try:
                        error_body = redact(exc.response.json())
                    except ValueError:
                        error_body = {"status": exc.response.status_code}
                record["create_error"] = error_body
                self.journal.save()
                matches = [s for s in self.cloud.list_services() if s.get("name") == request.name]
                if len(matches) != 1:
                    raise RuntimeError("Cloud create request failed; inspect redacted journal") from exc
                service_id = str(matches[0]["id"])
                record["create_outcome"] = "reconciled-after-uncertain-response"
            if not service_id:
                raise ValueError("create operation did not identify its service")
            record["service_id"] = service_id
            self.journal.save()
            self._verify_service(request, record)
        return self.journal.created_ids()

    def _verify_service(self, request: ServiceRequest, record: dict[str, Any]) -> None:
        service = self.cloud.get_service(record["service_id"])["data"]
        expected = {
            "name": request.name,
            "serviceClassId": request.service_class_id,
            "datacenterId": request.datacenter_id,
            "eventBrokerServiceVersion": request.event_broker_version,
            "locked": False,
        }
        if any(service.get(key) != value for key, value in expected.items()):
            raise ValueError("created service identity differs from the authorized request")
        record["ready"] = (
            service.get("creationState") == "COMPLETED"
            and service.get("adminState") == "START"
        )
        self.journal.save()
        if not record["ready"]:
            raise RuntimeError("created service is not ready after successful operation")

    def connection_bundle(self) -> dict[str, Any]:
        """Resolve private per-service SEMP/SMF/AMQP details for the workload process."""
        brokers = []
        requests = {request.name: request for request in self.plan.requests}
        for index, record in enumerate(self.journal.data["services"]):
            service_id = record.get("service_id")
            if not service_id or record["name"] not in requests:
                continue
            detail = self.cloud.get_service(service_id, expand=True)["data"]
            vpn = next(
                item for item in detail["broker"]["msgVpns"]
                if item["msgVpnName"] == requests[record["name"]].msg_vpn_name
            )
            endpoint = next(
                item for item in detail["serviceConnectionEndpoints"]
                if item.get("accessType") == "PUBLIC" and item.get("hostNames")
            )
            ports = {item["protocol"]: item["port"] for item in endpoint["ports"] if item.get("port")}
            admin = vpn["managementAdminLoginCredential"]
            client = vpn["serviceLoginCredential"]
            host = endpoint["hostNames"][0]
            brokers.append({
                "broker_id": chr(ord("a") + index),
                "service_id": service_id,
                "msg_vpn": vpn["msgVpnName"],
                "semp": f"https://{host}:{ports['serviceManagementTlsListenPort']}",
                "smf": f"tcps://{host}:{ports['serviceSmfTlsListenPort']}",
                "amqp": f"amqps://{host}:{ports['serviceAmqpTlsListenPort']}",
                "admin_username": admin["username"],
                "admin_password": admin["password"],
                "client_username": client["username"],
                "client_password": client["password"],
            })
        if len(brokers) != self.plan.service_count:
            raise ValueError("not every created service has a complete private connection bundle")
        return {
            "topology": "independent-ha-services",
            "service_class": self.plan.service_class,
            "broker_version": self.plan.broker_version,
            "brokers": brokers,
        }

    def cleanup(self) -> CleanupResult:
        remaining = []
        deleted = 0
        ids = self.journal.created_ids()
        cleanup_deadline = time.monotonic() + self.plan.cleanup_timeout_minutes * 60
        for service_id in ids:
            record = next(
                item for item in self.journal.data["services"]
                if item.get("service_id") == service_id
            )
            try:
                operation_id = record.get("delete_operation_id")
                if not operation_id:
                    operation_id = self.cloud.delete_service(
                        service_id, "delete-" + record["idempotency_key"]
                    )
                    record["delete_operation_id"] = operation_id
                    self.journal.save()
                self._wait_operation(operation_id, service_id, deadline=cleanup_deadline)
                try:
                    self.cloud.get_service(service_id)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        record["deleted"] = True
                        deleted += 1
                        self.journal.save()
                        continue
                    raise
                remaining.append(service_id)
            except Exception as exc:
                record["cleanup_error_type"] = type(exc).__name__
                self.journal.save()
                remaining.append(service_id)
        self.journal.event("cleanup-complete", deleted=deleted, remaining=len(remaining))
        return CleanupResult(len(ids), deleted, tuple(remaining))


def load_plan(path: str | Path) -> QualificationPlan:
    return QualificationPlan.model_validate_json(Path(path).read_text())


def read_dotenv_secret(path: str | Path, key: str) -> str:
    """Read one exact key without copying unrelated environment contents."""
    for raw in Path(path).read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            if name.strip() == key:
                result = value.strip().strip('"').strip("'")
                if result:
                    return result
    raise ValueError(f"missing {key} in credential file")


_REDACT_KEYS = {"serviceid", "resourceid", "hostname", "hostnames", "username", "password",
                "token", "authorization", "payload", "idempotencykey"}

def redact(value: Any, key: str = "") -> Any:
    """Recursively redact secrets, payloads, hostnames, and stable resource identifiers."""
    if key.lower().replace("_", "") in _REDACT_KEYS:
        if key.lower().replace("_", "") in {"serviceid", "resourceid"} and isinstance(value, str):
            return redacted_id(value)
        return "[REDACTED]"
    if isinstance(value, dict):
        return {name: redact(item, str(name)) for name, item in value.items()}
    if isinstance(value, list):
        return [redact(item, key) for item in value]
    return value


def redacted_id(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()[:16]


def _utc() -> str:
    return datetime.now(UTC).isoformat()


def install_signal_cleanup(cleanup: Callable[[], None]) -> None:
    def stop(signum: int, frame: Any) -> None:
        del frame
        cleanup()
        raise KeyboardInterrupt(f"qualification interrupted by signal {signum}")

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
