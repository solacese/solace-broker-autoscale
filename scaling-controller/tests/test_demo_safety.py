"""The local demo cannot turn invented measurements into real cloud provisioning."""
import importlib.util
import json
import stat
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parent


def load_script(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reduced_demo_model_is_synthetic():
    path = ROOT / 'demo' / 'capacity-model-demo.json'
    if not path.exists():
        pytest.skip('optional local demo not present')
    assert json.loads(path.read_text())['synthetic'] is True


def test_signal_runner_refuses_live_actuation_before_loading_credentials():
    path = ROOT / 'demo' / 'autoscale_runner.py'
    if not path.exists():
        pytest.skip('optional local demo not present')
    module = load_script("runner_under_test", path)
    with pytest.raises(SystemExit, match='simulated telemetry'):
        module.cmd_watch(live=True)


def test_workload_reconciliation_catches_duplicate_masking_loss():
    module = load_script("cloud_workload_reconcile", REPO / "scripts/cloud-workload.py")
    processed = [
        {"group": "ledger", "event_id": "one"},
        {"group": "ledger", "event_id": "one"},
        {"group": "audit", "event_id": "one"},
        {"group": "audit", "event_id": "two"},
    ]
    _, loss, duplicates = module.reconcile_deliveries(
        {"one", "two"}, ("ledger", "audit"), processed
    )
    assert loss == {"ledger": 1, "audit": 0}
    assert duplicates == 1


def test_workload_phase_metrics_have_explicit_completion_windows():
    module = load_script("cloud_workload_phases", REPO / "scripts/cloud-workload.py")
    metrics = module.phase_metrics(100, {
        "started": 10.0,
        "last_injected": 10.5,
        "all_accepted": 11.0,
        "all_flushed": 12.0,
        "all_processed": 14.0,
    })
    assert metrics == {
        "offer_injection_seconds": .5,
        "offer_injection_messages_per_second": 200.0,
        "durable_accept_completion_seconds": 1.0,
        "durable_accept_completion_messages_per_second": 100.0,
        "broker_flush_completion_seconds": 2.0,
        "broker_flush_completion_messages_per_second": 50.0,
        "broker_flush_after_accept_seconds": 1.0,
        "business_handler_completion_seconds": 4.0,
        "business_handler_completion_messages_per_second": 25.0,
    }


def test_workload_phase_metrics_reject_nonmonotonic_flush():
    module = load_script("cloud_workload_bad_phases", REPO / "scripts/cloud-workload.py")
    with pytest.raises(ValueError, match="flush completed before durable acceptance"):
        module.phase_metrics(1, {
            "started": 1.0,
            "last_injected": 2.0,
            "all_accepted": 4.0,
            "all_flushed": 3.0,
            "all_processed": 5.0,
        })


def test_broker_counter_delta_is_cumulative_and_nonnegative():
    module = load_script("cloud_workload_counters", REPO / "scripts/cloud-workload.py")
    assert module.extract_cumulative_counters({"counter": {
        "dataRxMsgCount": 10, "dataTxMsgCount": 5,
        "dataRxByteCount": 100, "dataTxByteCount": 50,
    }}) == {
        "dataRxMsgCount": 10, "dataTxMsgCount": 5,
        "dataRxByteCount": 100, "dataTxByteCount": 50,
    }
    assert module.extract_cumulative_counters({"dataRxMsgCount": 10}) is None
    before = {
        "dataRxMsgCount": 10, "dataTxMsgCount": 5,
        "dataRxByteCount": 100, "dataTxByteCount": 50,
    }
    after = {
        "dataRxMsgCount": 13, "dataTxMsgCount": 9,
        "dataRxByteCount": 180, "dataTxByteCount": 90,
    }
    assert module.counter_delta(before, after) == {
        "dataRxMsgCount": 3, "dataTxMsgCount": 4,
        "dataRxByteCount": 80, "dataTxByteCount": 40,
    }
    assert module.counter_delta(after, before) is None
    assert module.counter_delta(None, after) is None


def test_receiver_readiness_requires_every_partition_group(monkeypatch):
    module = load_script("cloud_workload_readiness", REPO / "scripts/cloud-workload.py")

    class Status:
        def __init__(self, consumers):
            self.consumers = consumers

    class Queues:
        def __init__(self):
            self.calls = 0

        def status(self, broker, shard, partition):
            assert (broker, shard) == ("broker-a", "qualification")
            self.calls += 1
            return Status(1 if self.calls > 2 else 0)

    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    queues = Queues()
    module.wait_for_receivers(queues, "broker-a", 2, timeout=1, poll_seconds=0)
    assert queues.calls == 4


def test_cloud_workflow_fits_hosted_limit_and_installs_go():
    workflow = (REPO / ".github/workflows/cloud-qualification.yml").read_text()
    assert "timeout-minutes: 360" in workflow
    assert "actions/setup-go@v5" in workflow
    assert 'go-version: "1.26.x"' in workflow
    assert "--result cloud-qualification-result.json" in workflow


def test_cloud_lifecycle_creates_private_files_without_chmod_window(tmp_path):
    lifecycle = load_script("cloud_lifecycle_under_test", REPO / "scripts/cloud-qualification.py")
    target = tmp_path / "private.json"
    lifecycle.private_write(target, '{"secret":"value"}\n')
    assert target.stat().st_mode & 0o777 == 0o600
    assert json.loads(target.read_text()) == {"secret": "value"}


def test_workload_output_is_private_and_captured(tmp_path):
    lifecycle = load_script("cloud_lifecycle_capture", REPO / "scripts/cloud-qualification.py")
    log = tmp_path / "workload.log"
    code, status = lifecycle.run_workload(
        [sys.executable, "-c", "print('private workload output')"], log, 5, []
    )
    assert (code, status) == (0, "completed")
    assert stat.S_IMODE(log.stat().st_mode) == 0o600
    assert log.read_text().strip() == "private workload output"


def test_workload_timeout_kills_process_group(tmp_path):
    lifecycle = load_script("cloud_lifecycle_timeout", REPO / "scripts/cloud-qualification.py")
    log = tmp_path / "workload.log"
    code, status = lifecycle.run_workload(
        [sys.executable, "-c", "import subprocess,time; subprocess.Popen(['sleep','60']); time.sleep(60)"],
        log,
        1,
        [],
    )
    assert (code, status) == (1, "timed-out")


def test_cloud_workload_keeps_a_counter_settle_window():
    source = (REPO / "scripts/cloud-workload.py").read_text()
    assert 'case.get("quiet_seconds", 1.0)' in source
    assert "_native_configured" not in source


def test_corrected_workload_has_real_local_smoke_evidence():
    path = REPO / "examples/qualification-evidence/2026-09-23/local-workload-v2-smoke.json"
    evidence = json.loads(path.read_text())
    case = evidence["case"]
    windows = case["measurement_windows"]
    assert evidence["schema_version"] == "local-workload-v2-smoke-v1"
    assert case["accepted"] == case["messages"] == 40
    assert sum(case["processed"].values()) == case["messages"] * case["fanout"] == 80
    assert all(missing == 0 for missing in case["loss"].values())
    assert case["duplicates"] == case["ordering_violations"] == 0
    assert (
        0 < windows["offer_injection_seconds"]
        <= windows["durable_accept_completion_seconds"]
        <= windows["broker_flush_completion_seconds"]
    )
    assert windows["business_handler_completion_seconds"] > 0
    counters = case["broker_cumulative_counters"]
    assert counters["supported"] is True
    assert counters["delta"]["dataRxMsgCount"] == 40
    assert counters["delta"]["dataTxMsgCount"] == 80


def test_cloud_workload_passes_declared_groups_and_delay_to_go_client():
    module = load_script("cloud_workload_under_test", REPO / "scripts/cloud-workload.py")
    command = module.client_command(
        Path("/tmp/payments-demo"),
        "http://127.0.0.1:8099",
        Path("/tmp/state"),
        ("case-0", "case-1"),
        20,
    )
    assert command[-5:] == ["--subscribe", "--groups", "case-0,case-1", "--handler-delay", "20ms"]
