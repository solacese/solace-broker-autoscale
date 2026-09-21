"""Real Go SIGKILL recovery, native AMQP fanout and managed broker migration."""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def test_go_payment_burst_kill_restart_and_migration(tmp_path):
    go = shutil.which("go")
    if not go:
        pytest.skip("Go toolchain is required for managed Go integration")
    root = Path(__file__).parents[2]
    binary = tmp_path / "payments-demo"
    subprocess.run([go, "build", "-race", "-o", str(binary), "./cmd/payments-demo"],
                   cwd=root / "shim", check=True, timeout=120)
    output = tmp_path / "run"
    result = subprocess.run([
        sys.executable, str(root / "examples/manager-demo/run.py"),
        "--binary", str(binary), "--output", str(output),
    ], capture_output=True, text=True, timeout=150,
        env={**os.environ, "PYTHONPATH": str(root / "scaling-controller")})
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((output / "report.json").read_text())
    assert report["passed"]
    assert report["accepted"] == report["ledger"] == report["audit"] == 112
    assert report["events"][-1]["pending"] == 0
    assert "b" in report["events"][-1]["owners"]


def test_go_amqp_and_python_smf_share_native_envelopes(tmp_path):
    import importlib.util

    from solace_autoscale_client import MessagingClient

    go = shutil.which("go")
    if not go:
        pytest.skip("Go toolchain required")
    root = Path(__file__).parents[2]
    spec = importlib.util.spec_from_file_location("manager_demo", root / "examples/manager-demo/run.py")
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)
    binary = tmp_path / "payments-demo"
    subprocess.run([go, "build", "-race", "-o", str(binary), "./cmd/payments-demo"],
                   cwd=root / "shim", check=True, timeout=120)
    lab = demo.Lab(tmp_path, partitions=1)
    publisher = subscriber = None
    received = []
    try:
        with demo.api(lab.app) as url, MessagingClient(
            url, state_dir=tmp_path / "python", credentials=lambda _: ("default", "default"),
            poll_interval=0.05,
        ) as client:
            client.subscribe(group="ledger", handler=lambda m: received.append(m.event_id))
            publisher = demo.Process(binary, url, tmp_path / "go-publisher")
            publisher.request({"topic": "payments/account/created", "event_id": "from-go", "payload": {"amount": 7}})
            publisher.request({"op": "flush"})
            demo.eventually(lambda: "from-go" in received)
            subscriber = demo.Process(binary, url, tmp_path / "go-subscriber", True)
            client.publish("payments/account/created", {"amount": 8}, event_id="from-python")
            assert client.flush(10)
            demo.eventually(lambda: {x["event_id"] for x in subscriber.all if x.get("group") == "audit"}
                            == {"from-go", "from-python"})
            assert "from-python" in received
    finally:
        if publisher:
            publisher.close()
        if subscriber:
            subscriber.close()
        lab.close()
