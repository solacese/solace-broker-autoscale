"""The local demo cannot turn invented measurements into real cloud provisioning."""
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_reduced_demo_model_is_synthetic():
    path = ROOT / 'demo' / 'capacity-model-demo.json'
    if not path.exists():
        pytest.skip('optional local demo not present')
    assert json.loads(path.read_text())['synthetic'] is True


def test_signal_runner_refuses_live_actuation_before_loading_credentials():
    path = ROOT / 'demo' / 'autoscale_runner.py'
    if not path.exists():
        pytest.skip('optional local demo not present')
    spec = importlib.util.spec_from_file_location('runner_under_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with pytest.raises(SystemExit, match='simulated telemetry'):
        module.cmd_watch(live=True)
