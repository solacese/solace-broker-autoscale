"""Actuator safety tests (§10, §13 Phase 4 gate).

Every guardrail has a test proving the operation is REFUSED. No test issues a real Cloud API call —
a FakeCloud records calls instead. Also tests the recommend-mode-not-constructed rule and the audit
'before the call' ordering.
"""

from __future__ import annotations

from typing import Any

import pytest

from solace_autoscale.actuator.base import Operation, OperationType
from solace_autoscale.actuator.factory import build_actuator
from solace_autoscale.actuator.safety import AuditLog, FleetState, SafetyGate

from .conftest import default_config, make_test_model


class FakeCloud:
    """Records calls; NEVER hits the network. queue_state is configurable per test."""

    def __init__(self, queue_state: dict[str, Any] | None = None) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self._queue_state = queue_state or {
            "total_msgs_spooled": 0, "bound_consumers": 0, "active_flows": 0, "spooled_bytes": 0,
        }

    def create_service(self, body, idempotency_key):
        self.calls.append(("create", (body, idempotency_key)))
        return "op-create-1"

    def delete_service(self, service_id, idempotency_key):
        self.calls.append(("delete", (service_id, idempotency_key)))
        return "op-delete-1"

    def update_message_spool(self, service_id, size_gb, idempotency_key):
        self.calls.append(("spool", (service_id, size_gb, idempotency_key)))
        return "op-spool-1"

    def get_operation(self, operation_id):
        return {"data": {"status": "SUCCEEDED"}}

    def get_broker_state(self, service_id):
        return {"data": {"status": "ACTIVE"}}

    def queue_state(self, service_id, msg_vpn):
        return self._queue_state


def _cfg(**over):
    # default_config already deep-merges overrides onto its base (elastic billing, enterprise-10k).
    return default_config(**over)


def _op(op_type=OperationType.CREATE_SERVICE, **kw):
    defaults = dict(
        op_type=op_type, shard="shard-a", decision_id="dec-1", model_version="test-v1",
        config_hash="cfg1", request_body={"name": "svc", "serviceClassId": "ENTERPRISE_10K_HIGHAVAILABILITY"},
        idempotency_key="idem-123",
    )
    defaults.update(kw)
    return Operation(**defaults)


def _gate(cfg, cloud, tmp_path, synthetic=False):
    model = make_test_model(synthetic=synthetic)
    return SafetyGate(cfg, model, AuditLog(tmp_path / "audit.jsonl"), cloud)


def _state(current=2, in_flight=0, last_hour=0, age=10.0):
    return FleetState(current_brokers=current, ops_in_flight=in_flight,
                      ops_in_last_hour=last_hour, newest_metric_age_seconds=age)


# ---- recommend mode: actuator not constructed (ADR 0004) -------------------------------------

def test_recommend_mode_actuator_not_constructed():
    cfg = default_config(actuation={"mode": "recommend"})
    model = make_test_model()
    act = build_actuator(cfg, model, cloud=FakeCloud())
    assert act is None  # never constructed


def test_full_mode_actuator_constructed():
    cfg = default_config(actuation={"mode": "full", "dry_run": True})
    model = make_test_model()
    act = build_actuator(cfg, model, cloud=FakeCloud())
    assert act is not None


def test_synthetic_model_blocks_construction_in_active_mode():
    cfg = default_config(actuation={"mode": "full"})
    model = make_test_model(synthetic=True)
    with pytest.raises(ValueError, match="synthetic"):
        build_actuator(cfg, model, cloud=FakeCloud())


def test_active_mode_without_cloud_client_refuses():
    cfg = default_config(actuation={"mode": "full"})
    model = make_test_model()
    with pytest.raises(ValueError, match="no Solace Cloud client"):
        build_actuator(cfg, model, cloud=None)


# ---- dry-run (default) -----------------------------------------------------------------------

def test_dry_run_does_not_issue(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": True})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    r = gate.approve_and_issue(_op(), _state(), now=100.0)
    assert r.dry_run is True and r.issued is False
    assert cloud.calls == []  # nothing issued


# ---- guardrail refusals ----------------------------------------------------------------------

def test_refuse_when_model_synthetic(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path, synthetic=True)
    r = gate.approve_and_issue(_op(), _state(), now=100.0)
    assert not r.issued and "synthetic" in r.refused_reason
    assert cloud.calls == []


def test_refuse_when_metrics_stale(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False}, metrics={"staleness_limit": "3m"})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    r = gate.approve_and_issue(_op(), _state(age=600.0), now=100.0)
    assert not r.issued and "stale" in r.refused_reason
    assert cloud.calls == []


def test_refuse_on_kill_switch(tmp_path):
    kill = tmp_path / "halt"
    kill.write_text("stop")
    cfg = _cfg(actuation={"mode": "full", "dry_run": False, "kill_switch_file": str(kill)})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    r = gate.approve_and_issue(_op(), _state(), now=100.0)
    assert not r.issued and "kill switch" in r.refused_reason
    assert cloud.calls == []


def test_refuse_above_max_brokers(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False},
               fleet={"service_class": "enterprise-10k", "min_brokers": 1, "max_brokers": 3})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    r = gate.approve_and_issue(_op(OperationType.CREATE_SERVICE), _state(current=3), now=100.0)
    assert not r.issued and "max_brokers" in r.refused_reason


def test_refuse_below_min_brokers(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False},
               fleet={"service_class": "enterprise-10k", "min_brokers": 2, "max_brokers": 8})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    op = _op(OperationType.DELETE_SERVICE, target_service_id="svc-9")
    r = gate.approve_and_issue(op, _state(current=2), now=100.0)
    assert not r.issued and "min_brokers" in r.refused_reason


def test_refuse_max_ops_in_flight(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False, "max_ops_in_flight": 1})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    r = gate.approve_and_issue(_op(), _state(in_flight=1), now=100.0)
    assert not r.issued and "in_flight" in r.refused_reason


def test_refuse_max_ops_per_hour(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False, "max_ops_per_hour": 4})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    r = gate.approve_and_issue(_op(), _state(last_hour=4), now=100.0)
    assert not r.issued and "per_hour" in r.refused_reason


def test_refuse_delete_nonempty_broker(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False})
    cloud = FakeCloud(queue_state={"total_msgs_spooled": 5, "bound_consumers": 1,
                                   "active_flows": 1, "spooled_bytes": 1000})
    gate = _gate(cfg, cloud, tmp_path)
    op = _op(OperationType.DELETE_SERVICE, target_service_id="svc-9", metadata={"msg_vpn": "vpn"})
    r = gate.approve_and_issue(op, _state(current=3), now=100.0)
    assert not r.issued and "not empty" in r.refused_reason
    assert cloud.calls == []  # never issued delete


def test_scale_up_only_refuses_delete(tmp_path):
    cfg = _cfg(actuation={"mode": "scale-up-only", "dry_run": False})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    op = _op(OperationType.DELETE_SERVICE, target_service_id="svc-9")
    r = gate.approve_and_issue(op, _state(current=3), now=100.0)
    assert not r.issued and "scale-up-only" in r.refused_reason


def test_refuse_missing_idempotency_key(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    r = gate.approve_and_issue(_op(idempotency_key=""), _state(), now=100.0)
    assert not r.issued and "idempotency" in r.refused_reason


# ---- happy path issues (against FakeCloud, still no network) ---------------------------------

def test_create_issues_with_idempotency_key(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False})
    cloud = FakeCloud()
    gate = _gate(cfg, cloud, tmp_path)
    r = gate.approve_and_issue(_op(), _state(current=2), now=100.0)
    assert r.issued and r.cloud_operation_id == "op-create-1"
    assert cloud.calls[0][0] == "create"
    assert cloud.calls[0][1][1] == "idem-123"  # idempotency key passed through


def test_delete_empty_broker_issues(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False})
    cloud = FakeCloud()  # default queue_state is empty
    gate = _gate(cfg, cloud, tmp_path)
    op = _op(OperationType.DELETE_SERVICE, target_service_id="svc-9", metadata={"msg_vpn": "vpn"})
    r = gate.approve_and_issue(op, _state(current=3), now=100.0)
    assert r.issued and cloud.calls[0][0] == "delete"


# ---- audit BEFORE the call (§10) -------------------------------------------------------------

def test_audit_intent_written_before_issue(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False})
    cloud = FakeCloud()
    audit_path = tmp_path / "audit.jsonl"
    gate = SafetyGate(cfg, make_test_model(), AuditLog(audit_path), cloud)
    gate.approve_and_issue(_op(), _state(current=2), now=100.0)
    records = AuditLog(audit_path).read_all()
    phases = [r["phase"] for r in records]
    assert phases[0] == "intent"  # intent first, before issue
    assert "issued" in phases
    # intent record carries decision id, model version, config hash, and full request body (§10)
    intent = records[0]
    assert intent["decision_id"] == "dec-1"
    assert intent["model_version"] == "test-v1"
    assert intent["config_hash"] == "cfg1"
    assert intent["request_body"]["serviceClassId"] == "ENTERPRISE_10K_HIGHAVAILABILITY"


def test_audit_written_even_when_refused(tmp_path):
    cfg = _cfg(actuation={"mode": "full", "dry_run": False})
    audit_path = tmp_path / "audit.jsonl"
    gate = SafetyGate(cfg, make_test_model(synthetic=True), AuditLog(audit_path), FakeCloud())
    gate.approve_and_issue(_op(), _state(), now=100.0)
    records = AuditLog(audit_path).read_all()
    assert records[0]["phase"] == "intent"
    assert any(r["phase"] == "refused" for r in records)
