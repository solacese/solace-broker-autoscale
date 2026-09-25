"""Wheel builds must contain only current source packages."""

from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

from .conftest import CONTROLLER


def test_wheel_ignores_stale_build_tree(tmp_path: Path) -> None:
    source = tmp_path / "source"
    shutil.copytree(
        CONTROLLER,
        source,
        ignore=shutil.ignore_patterns("build", "dist", "*.egg-info", "__pycache__"),
    )
    stale = source / "build/lib/solace_autoscale/__stale_build_probe__.py"
    stale.parent.mkdir(parents=True)
    stale.write_text("STALE = True\n")
    wheel_dir = tmp_path / "wheel"
    wheel_dir.mkdir()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--wheel-dir",
            str(wheel_dir),
            str(source),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    wheel = next(wheel_dir.glob("*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        assert not any("__stale_build_probe__" in name for name in archive.namelist())
