"""Bounded lifecycle checks for the customer startup script."""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

from .conftest import REPO


def test_alive_unhealthy_api_is_stopped_without_starting_controller(tmp_path: Path) -> None:
    root = tmp_path / "checkout"
    venv = root / ".venv/bin"
    venv.mkdir(parents=True)
    (root / "run-controller.sh").write_bytes((REPO / "run-controller.sh").read_bytes())
    (root / "run-controller.sh").chmod(0o755)
    config = root / "config.yaml"
    inventory = root / "inventory.yaml"
    config.touch()
    inventory.touch()
    marker = root / "controller-started"
    process_file = root / "api.pid"
    (venv / "solace-autoscale").write_text(
        "#!/usr/bin/env bash\n"
        "if [ \"$1\" = serve ]; then\n"
        f"  printf '%s' \"$$\" > {process_file!s}\n"
        "  trap 'exit 0' TERM INT HUP\n"
        "  while :; do sleep 1; done\n"
        "fi\n"
        f"touch {marker!s}\n"
    )
    (venv / "solace-autoscale").chmod(0o755)
    (venv / "python").write_text("#!/usr/bin/env bash\nexit 1\n")
    (venv / "python").chmod(0o755)

    started = time.monotonic()
    result = subprocess.run(
        [str(root / "run-controller.sh"), str(config), str(inventory)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert time.monotonic() - started < 8
    assert result.returncode != 0
    assert "Assignment API failed to start" in result.stderr
    assert not marker.exists()
    pid = int(process_file.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
