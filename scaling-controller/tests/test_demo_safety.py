"""The local demo cannot turn invented measurements into real cloud provisioning."""
import importlib.util
import json
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
