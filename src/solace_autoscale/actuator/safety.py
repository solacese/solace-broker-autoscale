"""Safety guardrails, kill switch, and audit log (§10).

Every guardrail is enforced HERE, before any operation is issued. These rules are not optional and
are not configurable away.

Guardrails:
  1. dry_run: true (default) logs the operation it would issue and returns without issuing.
  2. Refuse all actuation when the capacity model is synthetic.
  3. Refuse all actuation when metrics are stale beyond staleness_limit.
  4. Refuse to delete a broker with non-zero queue depth, bound consumers, active flows, or spooled
     messages. Checked immediately before the call, not from cached state.
  5. Never scale below min_brokers or above max_brokers.
  6. Honour max_ops_in_flight and max_ops_per_hour.
  7. Check kill_switch_file before every operation. If present, halt and log.
  8. Write the audit record BEFORE issuing the call, including decision id, model version, config
     hash, and the full request body.
  9. Idempotency keys on every Cloud API call (constructed by the caller; safety verifies presence).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..capacity.schema import CapacityModel
from ..config import Config
from .base import CloudClient, Operation, OperationType


class ActuationRefused(Exception):
    """Raised when a guardrail refuses an operation. Carries the reason."""


@dataclass
class AuditRecord:
    ts: float
    decision_id: str
    model_version: str
    config_hash: str
    op_type: str
    shard: str
    idempotency_key: str
    request_body: dict[str, Any]
    phase: str  # "intent" (before issuing) | "issued" | "refused"
    detail: str | None = None


class AuditLog:
    """Append-only JSONL audit log. The intent record is written BEFORE the call (§10)."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, record: AuditRecord) -> None:
        with self._path.open("a") as f:
            f.write(json.dumps(record.__dict__, default=str) + "\n")

    def read_all(self) -> list[dict[str, Any]]:
        if not self._path.exists():
            return []
        return [json.loads(line) for line in self._path.read_text().splitlines() if line.strip()]


@dataclass
class FleetState:
    """What the safety layer needs to know about the fleet right now."""

    current_brokers: int
    ops_in_flight: int
    ops_in_last_hour: int
    newest_metric_age_seconds: float


class SafetyGate:
    """Approve or refuse an operation. The ONLY path to issuing a Cloud call."""

    def __init__(self, config: Config, model: CapacityModel, audit: AuditLog,
                 cloud: CloudClient) -> None:
        self._cfg = config
        self._model = model
        self._audit = audit
        self._cloud = cloud

    # ---- individual guardrails (each pure/deterministic given inputs) ------------------------

    def _check_model_not_synthetic(self) -> None:
        if self._model.synthetic:
            raise ActuationRefused("capacity model is synthetic; actuation hard-blocked (§10)")

    def _check_metrics_fresh(self, state: FleetState) -> None:
        if state.newest_metric_age_seconds > self._cfg.metrics.staleness_limit:
            raise ActuationRefused(
                f"metrics stale: newest sample {state.newest_metric_age_seconds:.0f}s old > "
                f"staleness_limit {self._cfg.metrics.staleness_limit:.0f}s"
            )

    def _check_kill_switch(self) -> None:
        if Path(self._cfg.actuation.kill_switch_file).exists():
            raise ActuationRefused(
                f"kill switch present ({self._cfg.actuation.kill_switch_file}); halting"
            )

    def _check_rate_limits(self, state: FleetState) -> None:
        if state.ops_in_flight >= self._cfg.actuation.max_ops_in_flight:
            raise ActuationRefused(
                f"max_ops_in_flight reached ({state.ops_in_flight} >= "
                f"{self._cfg.actuation.max_ops_in_flight})"
            )
        if state.ops_in_last_hour >= self._cfg.actuation.max_ops_per_hour:
            raise ActuationRefused(
                f"max_ops_per_hour reached ({state.ops_in_last_hour} >= "
                f"{self._cfg.actuation.max_ops_per_hour})"
            )

    def _check_bounds(self, op: Operation, state: FleetState) -> None:
        if op.op_type is OperationType.CREATE_SERVICE:
            if state.current_brokers + 1 > self._cfg.fleet.max_brokers:
                raise ActuationRefused(
                    f"create would exceed max_brokers ({self._cfg.fleet.max_brokers})"
                )
        elif op.op_type is OperationType.DELETE_SERVICE:
            if state.current_brokers - 1 < self._cfg.fleet.min_brokers:
                raise ActuationRefused(
                    f"delete would drop below min_brokers ({self._cfg.fleet.min_brokers})"
                )

    def _check_mode_allows(self, op: Operation) -> None:
        mode = self._cfg.actuation.mode
        if mode == "recommend":
            # Should never reach here — the actuator isn't constructed in recommend mode.
            raise ActuationRefused("actuation.mode is recommend; actuator must not be constructed")
        if mode == "scale-up-only" and op.op_type is OperationType.DELETE_SERVICE:
            raise ActuationRefused("actuation.mode is scale-up-only; delete refused")

    def _check_idempotency_key(self, op: Operation) -> None:
        if not op.idempotency_key:
            raise ActuationRefused("operation has no idempotency key (§10)")

    def _check_safe_to_delete(self, op: Operation) -> None:
        """Refuse to delete a broker with non-zero queue depth, bound consumers, active flows, or
        spooled messages. Checked immediately before the call (live), not from cached state."""
        if op.op_type is not OperationType.DELETE_SERVICE:
            return
        sid = op.target_service_id
        if sid is None:
            raise ActuationRefused("delete operation missing target_service_id")
        msg_vpn = op.metadata.get("msg_vpn", "default")
        live = self._cloud.queue_state(sid, msg_vpn)  # live SEMP check
        depth = live.get("total_msgs_spooled", 0)
        bound = live.get("bound_consumers", 0)
        flows = live.get("active_flows", 0)
        spooled = live.get("spooled_bytes", 0)
        if depth or bound or flows or spooled:
            raise ActuationRefused(
                f"refusing to delete {sid}: not empty (msgs={depth}, consumers={bound}, "
                f"flows={flows}, spooled_bytes={spooled}); drain must complete first"
            )

    # ---- the gate ----------------------------------------------------------------------------

    def approve_and_issue(self, op: Operation, state: FleetState, now: float) -> OperationResultT:
        from .base import OperationResult

        # 8. audit intent BEFORE anything else touches the API
        self._audit.write(AuditRecord(
            ts=now, decision_id=op.decision_id, model_version=op.model_version,
            config_hash=op.config_hash, op_type=op.op_type.value, shard=op.shard,
            idempotency_key=op.idempotency_key, request_body=op.request_body, phase="intent",
        ))

        try:
            self._check_mode_allows(op)
            self._check_idempotency_key(op)
            self._check_kill_switch()
            self._check_model_not_synthetic()
            self._check_metrics_fresh(state)
            self._check_rate_limits(state)
            self._check_bounds(op, state)
            self._check_safe_to_delete(op)
        except ActuationRefused as e:
            self._audit.write(AuditRecord(
                ts=now, decision_id=op.decision_id, model_version=op.model_version,
                config_hash=op.config_hash, op_type=op.op_type.value, shard=op.shard,
                idempotency_key=op.idempotency_key, request_body=op.request_body,
                phase="refused", detail=str(e),
            ))
            return OperationResult(operation=op, issued=False, refused_reason=str(e))

        # dry-run: log what we WOULD do, do not issue
        if self._cfg.actuation.dry_run:
            self._audit.write(AuditRecord(
                ts=now, decision_id=op.decision_id, model_version=op.model_version,
                config_hash=op.config_hash, op_type=op.op_type.value, shard=op.shard,
                idempotency_key=op.idempotency_key, request_body=op.request_body,
                phase="issued", detail="dry-run: not issued",
            ))
            return OperationResult(operation=op, issued=False, dry_run=True)

        # issue for real
        cloud_op_id = self._issue(op)
        self._audit.write(AuditRecord(
            ts=now, decision_id=op.decision_id, model_version=op.model_version,
            config_hash=op.config_hash, op_type=op.op_type.value, shard=op.shard,
            idempotency_key=op.idempotency_key, request_body=op.request_body,
            phase="issued", detail=f"cloud_operation_id={cloud_op_id}",
        ))
        return OperationResult(operation=op, issued=True, cloud_operation_id=cloud_op_id)

    def _issue(self, op: Operation) -> str:
        if op.op_type is OperationType.CREATE_SERVICE:
            return self._cloud.create_service(op.request_body, op.idempotency_key)
        if op.op_type is OperationType.DELETE_SERVICE:
            assert op.target_service_id
            return self._cloud.delete_service(op.target_service_id, op.idempotency_key)
        if op.op_type is OperationType.UPDATE_SPOOL:
            assert op.target_service_id
            size = int(op.request_body["messageSpoolSizeInGB"])
            return self._cloud.update_message_spool(op.target_service_id, size, op.idempotency_key)
        raise ActuationRefused(f"unknown operation type {op.op_type}")


# Late import type alias to avoid a cycle in annotations above.
from .base import OperationResult as OperationResultT  # noqa: E402
