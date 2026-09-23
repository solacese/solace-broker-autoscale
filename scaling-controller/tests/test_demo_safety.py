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
