"""Durable, name-reconciled Solace Cloud warm-capacity replenishment.

No service deletion. Every create intent is persisted before the Cloud call. Retries reuse
one unique service name (the Cloud API requires names to be unique), and reconcile by that
exact name after errors/restarts. SEMP secrets are re-read in memory, never saved to state.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from ..actuator.base import Operation, OperationType
from ..actuator.safety import AuditLog, FleetState, SafetyGate
from ..actuator.solace_cloud import SempConnection, SolaceCloudClient
from ..assignment.store import Broker, BrokerState
from ..metrics.fleet import BrokerEndpoint
from ..metrics.semp import SempCollector
from .runtime import Controller


class CloudProvisioner:
    """One pending creation at a time; persisted names bound duplicate and spending risk."""

    def __init__(self, controller: Controller, cloud: SolaceCloudClient, audit: AuditLog) -> None:
        self.controller, self.cloud = controller, cloud
        self.audit = audit
        self.gate = SafetyGate(controller.config, controller.model, audit, cloud)
        cfg = controller.config
        if not cfg.provisioning.enabled:
            raise ValueError("provisioning is not enabled")
        requested = cfg.provisioning.broker_version or ""
        if requested.split("-")[0] != controller.model.provenance.broker_version or "-" not in requested:
            raise ValueError(
                "pin exact Cloud broker version including revision, matching the measured profile"
            )
        assignments = controller.assignments
        with assignments.transaction():
            assignments._conn.execute("""CREATE TABLE IF NOT EXISTS cloud_capacity (
                name TEXT PRIMARY KEY, shard TEXT NOT NULL, created_at REAL NOT NULL,
                status TEXT NOT NULL, service_id TEXT, operation_id TEXT)""")
            row = assignments._conn.execute(
                "SELECT value FROM routing_settings WHERE name='controller_id'"
            ).fetchone()
            if row is None:
                value = uuid.uuid4().hex[:12]
                assignments._conn.execute("INSERT INTO routing_settings VALUES ('controller_id',?)", (value,))
            else:
                value = row["value"]
        self.prefix = f"autoscale-{cfg.automation.fleet_id[:16]}-{value}-"

    def _rows(self) -> list[dict[str, Any]]:
        with self.controller.assignments.transaction():
            return [
                dict(row)
                for row in self.controller.assignments._conn.execute(
                    "SELECT * FROM cloud_capacity ORDER BY created_at"
                ).fetchall()
            ]

    def _set(
        self, name: str, status: str, service_id: str | None = None, operation_id: str | None = None
    ) -> None:
        with self.controller.assignments.transaction():
            self.controller.assignments._conn.execute(
                "UPDATE cloud_capacity SET status=?,service_id=COALESCE(?,service_id),"
                "operation_id=COALESCE(?,operation_id) WHERE name=?",
                (status, service_id, operation_id, name),
            )

    def _attach_ready(self, row: dict, service: dict) -> bool:
        c, cfg = self.controller, self.controller.config
        if service.get("creationState") != "COMPLETED" or service.get("adminState") != "START":
            return False
        sid = str(service["id"])
        detail = self.cloud._get(
            f"/api/v2/missionControl/eventBrokerServices/{sid}?expand=broker,serviceConnectionEndpoints"
        )["data"]
        expected = {
            "id": sid, "name": row["name"],
            "datacenterId": cfg.provisioning.datacenter_id,
            "serviceClassId": c.model.service_classes[cfg.fleet.service_class].service_class_id,
            "eventBrokerServiceVersion": cfg.provisioning.broker_version,
        }
        if any(detail.get(key) != value for key, value in expected.items()):
            raise ValueError("provisioned service identity, region, class or version differs from its intent")
        broker = detail.get("broker", {})
        if (broker.get("version") != c.model.provenance.broker_version
                or broker.get("redundancyGroupSslEnabled") is not True):
            raise ValueError("provisioned broker generation or mate-link encryption differs from the profile")
        vpn = detail["broker"]["msgVpns"][0]
        admin = vpn["managementAdminLoginCredential"]
        endpoint = detail["serviceConnectionEndpoints"][cfg.provisioning.endpoint_index]
        host = endpoint["hostNames"][0]
        ports = {p["protocol"]: p["port"] for p in endpoint["ports"]
                 if isinstance(p.get("port"), int) and 0 < p["port"] <= 65535}
        if not all(key in ports for key in ("serviceManagementTlsListenPort", "serviceSmfTlsListenPort")):
            raise ValueError("new service needs enabled TLS management and managed SMF ports")
        base = f"https://{host}:{ports['serviceManagementTlsListenPort']}"
        connection = SempConnection(base, admin["username"], admin["password"])
        password = os.environ.get(cfg.provisioning.client_password_env)
        if not password:
            raise ValueError("managed application credential environment variable is missing")
        # Configure the application's own credential on this newly created, exact-name service.
        # Secrets never enter the controller event log or assignment responses.
        with httpx.Client(base_url=base, auth=(admin["username"], admin["password"]), timeout=10) as semp:
            vpn_path = "/SEMP/v2/config/msgVpns/" + quote(vpn["msgVpnName"], safe="")
            path = vpn_path + "/clientUsernames/" + quote(cfg.provisioning.client_username, safe="")
            response = semp.get(path)
            missing = response.status_code == 404 or (
                response.status_code == 400
                and response.json().get("meta", {}).get("error", {}).get("status") == "NOT_FOUND"
            )
            body = {"enabled": True, "password": password}
            if missing:
                response = semp.post(
                    vpn_path + "/clientUsernames",
                    json={**body, "clientUsername": cfg.provisioning.client_username},
                )
            else:
                response.raise_for_status()
                response = semp.patch(path, json=body)
            response.raise_for_status()
        # A Cloud "created" state is not sufficient: verify the actual management endpoint.
        with SempCollector(base, admin["username"], admin["password"]) as collector:
            collector.collect(row["shard"], vpn["msgVpnName"], 0, 1)
        protocol_ports = {
            "smf": ("serviceSmfTlsListenPort", "tcps"),
            "amqp": ("serviceAmqpTlsListenPort", "amqps"),
            "mqtt": ("serviceMqttTlsListenPort", "mqtts"),
            "rest": ("serviceRestIncomingTlsListenPort", "https"),
        }
        endpoints = {
            protocol: f"{scheme}://{host}:{ports[key]}"
            for protocol, (key, scheme) in protocol_ports.items()
            if key in ports
        }
        if "smf" not in endpoints:
            raise ValueError("new service does not expose the managed SMF transport")
        c.queues.connections[sid] = connection
        c.queues.vpns[sid] = vpn["msgVpnName"]
        if cfg.messaging.enabled:
            c.queues.configure_native(sid)
        if not any(b.broker_id == sid for b in c.inventory.brokers):
            c.inventory.brokers.append(
                BrokerEndpoint(
                    broker_id=sid,
                    shard=row["shard"],
                    msg_vpn=vpn["msgVpnName"],
                    base_url=base,
                    username_env="CLOUD_MANAGED",
                    password_env="CLOUD_MANAGED",
                    endpoints=endpoints,
                    role="warm",
                )
            )
        if c.assignments.get_broker(sid) is None:
            c.assignments.upsert_broker(
                Broker(sid, row["shard"], vpn["msgVpnName"], BrokerState.WARM, endpoints)
            )
        self._set(row["name"], "ready", sid)
        return True

    def reconcile(self, now: float, *, metrics_fresh: bool) -> str:
        """Recover known services first, then replenish the configured number of warm spares."""
        c, cfg = self.controller, self.controller.config
        if Path(cfg.actuation.kill_switch_file).exists():
            return "provisioning refused: kill switch present"
        rows = self._rows()
        matches = {s["name"]: s for s in self.cloud.find_by_name_prefix(self.prefix)}
        for row in rows:
            service = matches.get(row["name"])
            if row["status"] == "ready":
                if row["service_id"] not in c.queues.connections:
                    if service is None or not self._attach_ready(row, service):
                        return "previously provisioned broker is not ready; no new creates"
                continue
            if service is not None:
                self._set(row["name"], "creating", str(service["id"]))
                if self._attach_ready(row, service):
                    return "warm service ready"
                if now - row["created_at"] > cfg.provisioning.readiness_timeout:
                    return "ALERT: Cloud service readiness timeout; inspect the pending service"
                return "waiting for Cloud and SEMP readiness"
            if not metrics_fresh:
                return "creation deferred: no complete fresh workload observation"
            return self._issue(row, now)
        if not metrics_fresh:
            return "warm replenishment waiting for fresh workload telemetry"
        for shard in sorted({b.shard for b in c.inventory.brokers}):
            brokers = c.assignments.brokers_for_shard(shard)
            warm = sum(b.state == BrokerState.WARM for b in brokers)
            if warm >= cfg.policy.warm_pool or len(brokers) >= cfg.fleet.max_brokers:
                continue
            digest = hashlib.sha256(shard.encode()).hexdigest()[:6]
            name = f"{self.prefix}{digest}-{len(rows) + 1:04d}"
            with c.assignments.transaction():
                c.assignments._conn.execute(
                    "INSERT INTO cloud_capacity VALUES (?,?,?,?,?,?)",
                    (name, shard, now, "planned", None, None),
                )
                c.store.event(now, "cloud-capacity-intent", {"name": name, "shard": shard})
            return self._issue(self._rows()[-1], now)
        return "warm pool satisfied or total capacity ceiling reached"

    def _issue(self, row: dict, now: float) -> str:
        c, cfg = self.controller, self.controller.config
        # The spending ceiling includes both active and warm services.
        live = c.assignments.brokers_for_shard(row["shard"])
        if len(live) >= cfg.fleet.max_brokers:
            return "total provisioned capacity ceiling reached"
        op = Operation(
            OperationType.CREATE_SERVICE,
            row["shard"],
            row["name"],
            c.model.model_version,
            cfg.config_hash(),
            {
                "name": row["name"],
                "serviceClassId": c.model.service_classes[cfg.fleet.service_class].service_class_id,
                "datacenterId": cfg.provisioning.datacenter_id,
                "eventBrokerVersion": cfg.provisioning.broker_version,
                "msgVpnName": "autoscale",
                "redundancyGroupSslEnabled": True,
            },
            idempotency_key=row["name"],
        )
        # Warm services consume a slot in the same spending ceiling as active services.
        with c.assignments.transaction():
            attempts = c.assignments._conn.execute(
                "SELECT ts FROM controller_events WHERE event='cloud-create-attempt' AND ts>=?", (now - 3600,)
            ).fetchall()
        if attempts and now - max(r["ts"] for r in attempts) < 60:
            return "Cloud create retry backoff"
        state = FleetState(len(live), 0, len(attempts), 0)
        self._set(row["name"], "issuing")
        c.store.event(now, "cloud-create-attempt", {"name": row["name"]})
        try:
            result = self.gate.approve_and_issue(op, state, now)
        except httpx.HTTPError:
            # Unique name is retained. Next tick lists services before retrying that SAME name.
            return "create result uncertain; reconciling exact unique service name before retry"
        if not result.issued:
            return f"create refused: {result.refused_reason or 'dry run'}"
        self._set(row["name"], "creating", operation_id=result.cloud_operation_id)
        return "Cloud creation requested; awaiting readiness"
