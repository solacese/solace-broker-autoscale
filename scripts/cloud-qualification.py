#!/usr/bin/env python3
"""Run the bounded Solace Cloud service lifecycle; workload phases resume from its journal."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from solace_autoscale.actuator.solace_cloud import SolaceCloudClient
from solace_autoscale.qualification.cloud import (
    QualificationRunner,
    RunJournal,
    install_signal_cleanup,
    load_plan,
    read_dotenv_secret,
    redact,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, default=ROOT / "state/cloud-qualification-20260922/plan.json")
    parser.add_argument("--journal", type=Path, default=ROOT / "state/cloud-qualification-20260922/run.json")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--create", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    plan = load_plan(args.plan)
    token = os.environ.get("SOLACE_CLOUD_TOKEN") or read_dotenv_secret(ROOT / ".env", "bearer")
    cloud = SolaceCloudClient(token)
    journal = RunJournal(args.journal, plan)
    runner = QualificationRunner(plan, journal, cloud)

    def cleanup() -> None:
        result = runner.cleanup()
        print(json.dumps({"cleanup": redact(result.__dict__)}, sort_keys=True), file=sys.stderr)

    install_signal_cleanup(cleanup)
    try:
        if args.cleanup:
            result = runner.cleanup()
            print(json.dumps({"cleanup": redact(result.__dict__)}, indent=2))
            return int(bool(result.remaining))
        runner.preflight()
        print(json.dumps({"preflight": "passed", "billing": redact(journal.data["billing"])}, indent=2))
        if args.preflight_only or not args.create:
            return 0
        error = None
        try:
            ids = runner.create_all()
            print(json.dumps({"created": len(ids), "ids": [redact(i, "service_id") for i in ids]}))
            # Workload execution is added as an explicit bounded phase; never leave services behind.
            error = "Cloud workload phase is not yet configured; refusing to leave services running"
        finally:
            result = runner.cleanup()
            print(json.dumps({"cleanup": redact(result.__dict__)}, indent=2))
        if result.remaining:
            raise RuntimeError("Cloud cleanup incomplete")
        if error:
            raise RuntimeError(error)
    finally:
        cloud.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
