#!/usr/bin/env python3
"""Run the bounded Solace Cloud service lifecycle; workload phases resume from its journal."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path

from solace_autoscale.actuator.solace_cloud import SolaceCloudClient
from solace_autoscale.qualification.cloud import (
    QualificationRunner,
    RunJournal,
    install_signal_cleanup,
    load_cleanup_plan,
    load_plan,
    read_dotenv_secret,
    redact,
)

ROOT = Path(__file__).resolve().parents[1]


def private_write(path: Path, value: str) -> None:
    """Create or replace a private file without a world-readable pre-chmod window."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def terminate_process_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def run_workload(
    command: list[str], log_path: Path, timeout: int, active: list[subprocess.Popen[str]]
) -> tuple[int, str]:
    """Capture private child output and stop the complete process group on timeout."""
    fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            text=True,
        )
        active.append(process)
        try:
            return process.wait(timeout=timeout), "completed"
        except subprocess.TimeoutExpired:
            terminate_process_group(process)
            return 1, "timed-out"
        finally:
            active.remove(process)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, default=ROOT / "state/cloud-qualification-20260922/plan.json")
    parser.add_argument("--journal", type=Path, default=ROOT / "state/cloud-qualification-20260922/run.json")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--create", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--result", type=Path)
    args = parser.parse_args()
    plan = load_cleanup_plan(args.plan) if args.cleanup else load_plan(args.plan)
    token = os.environ.get("SOLACE_CLOUD_TOKEN") or read_dotenv_secret(ROOT / ".env", "bearer")
    cloud = SolaceCloudClient(token)
    journal = RunJournal(args.journal, plan, cleanup_only=args.cleanup)
    runner = QualificationRunner(plan, journal, cloud)
    active_workloads: list[subprocess.Popen[str]] = []
    report: dict[str, object] = {
        "schema_version": "cloud-qualification-v2",
        "run_id": journal.data.get("run_id", "legacy-cleanup"),
        "plan_sha256": journal.data.get("plan_sha256", "legacy-cleanup"),
        "preflight": "not-run",
        "workload": "not-run",
    }

    def cleanup() -> None:
        for process in active_workloads:
            terminate_process_group(process)
        result = runner.cleanup()
        report["cleanup"] = redact(result.__dict__)

    install_signal_cleanup(cleanup)
    exit_code = 0
    try:
        if args.cleanup:
            cleanup()
            exit_code = int(bool(report["cleanup"]["remaining"] or report["cleanup"]["unresolved_create_intents"]))  # type: ignore[index]
        else:
            runner.preflight()
            report["preflight"] = "passed"
            report["billing"] = redact(journal.data["billing"])
            if args.create:
                bundle = args.journal.with_suffix(".connections.json")
                try:
                    ids = runner.create_all()
                    report["created"] = {
                        "count": len(ids),
                        "ids": [redact(item, "service_id") for item in ids],
                    }
                    private_write(bundle, json.dumps(runner.connection_bundle(), indent=2) + "\n")
                    command = [part.format(connections=str(bundle)) for part in plan.workload_command]
                    remaining = max(1, int(float(journal.data["deadline_epoch"]) - time.time()))
                    log_path = args.journal.with_suffix(".workload.log")
                    exit_code, status = run_workload(
                        command, log_path, remaining, active_workloads
                    )
                    report["workload"] = "passed" if exit_code == 0 else status if status != "completed" else "failed"
                    report["workload_log"] = "private-local-artifact"
                finally:
                    if bundle.exists():
                        bundle.unlink()
                    cleanup()
                    if report["cleanup"]["remaining"] or report["cleanup"]["unresolved_create_intents"]:  # type: ignore[index]
                        exit_code = 1
    except Exception as exc:  # noqa: BLE001 - every workload failure must enter exact-ID cleanup
        report["error_type"] = type(exc).__name__
        exit_code = 1
        if args.create:
            cleanup()
    finally:
        cloud.close()
        journal.close()
    output = json.dumps(redact(report), indent=2) + "\n"
    if args.result:
        private_write(args.result, output)
    print(output, end="")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
