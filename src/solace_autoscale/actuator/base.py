"""Actuator abstractions (§10).

An ``Operation`` is a proposed change (create/delete/spool). The ``Actuator`` protocol issues them,
but ONLY after ``safety.SafetyGate`` approves. In ``recommend`` mode no actuator is constructed at
all (ADR 0004) — see ``build_actuator``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol


class OperationType(StrEnum):
    CREATE_SERVICE = "create-service"
    DELETE_SERVICE = "delete-service"
    UPDATE_SPOOL = "update-spool"


@dataclass
class Operation:
    """A proposed change. ``request_body`` is the exact payload that would be sent."""

    op_type: OperationType
    shard: str
    decision_id: str
    model_version: str
    config_hash: str
    request_body: dict[str, Any]
    #: idempotency key so a retry after a timeout cannot double-provision (§10)
    idempotency_key: str
    #: for delete: the broker/service id being removed
    target_service_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    #: Operator confirmation for THIS operation. When ``actuation.require_confirmation`` is set, the
    #: gate refuses unless this is True. It is collected by the caller (e.g. the CLI prompts the
    #: operator) and passed in explicitly — the gate never prompts, so it stays deterministic and
    #: testable. Ignored when ``require_confirmation`` is false.
    approved: bool = False


@dataclass
class OperationResult:
    operation: Operation
    issued: bool  # False when dry-run or refused
    refused_reason: str | None = None
    #: async operation id returned by the Cloud API (202 OperationResponse), when issued
    cloud_operation_id: str | None = None
    dry_run: bool = False


class CloudClient(Protocol):
    """The only thing permitted to call the Solace Cloud REST API (§2)."""

    def create_service(self, body: dict[str, Any], idempotency_key: str) -> str: ...
    def delete_service(self, service_id: str, idempotency_key: str) -> str: ...
    def update_message_spool(self, service_id: str, size_gb: int, idempotency_key: str) -> str: ...
    def get_operation(self, operation_id: str) -> dict[str, Any]: ...
    def get_broker_state(self, service_id: str) -> dict[str, Any]: ...
    #: SEMPv2 monitor passthrough used for pre-delete safety checks
    def queue_state(self, service_id: str, msg_vpn: str) -> dict[str, Any]: ...
