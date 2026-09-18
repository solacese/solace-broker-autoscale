"""Read-only fleet monitor with complete snapshots and per-broker visibility."""
from __future__ import annotations

import json
import time
from dataclasses import asdict
from pathlib import Path

import click

from ..capacity.model import load_model
from ..config import load_config
from ..decision.engine import DecisionRequest, decide
from ..decision.types import ShardInput
from ..report.json import build_report
from .base import CollectorError
from .fleet import FleetCollector, load_inventory
from .history import RollingHistory


@click.command("monitor-fleet")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--inventory", "inventory_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--once", is_flag=True, help="One read-only snapshot; exits nonzero if any endpoint fails")
@click.option("--iterations", type=click.IntRange(min=1), default=None)
def monitor_fleet(config_path: Path, inventory_path: Path, once: bool, iterations: int | None) -> None:
    """Observe every active service in a homogeneous fleet; emit JSON lines, never actuate."""
    try:
        cfg = load_config(config_path)
        model = load_model(cfg.capacity.model)
        inventory = load_inventory(inventory_path)
        inventory.validate_profile(model, cfg.fleet.service_class)
        if cfg.actuation.mode != "recommend":
            raise ValueError("monitor-fleet is read-only; set actuation.mode: recommend")
        collector = FleetCollector(inventory)
    except (ValueError, OSError, CollectorError) as exc:
        raise click.ClickException(str(exc)) from exc
    history = RollingHistory(max(cfg.policy.scale_down_window, 3600) + cfg.metrics.scrape_interval)
    tick = 0
    try:
        while True:
            start = time.time()
            tick += 1
            failed = False
            try:
                snapshot = collector.collect(start)
                for shard, sample in snapshot.shards.items():
                    history.add(shard, sample)
                decisions = [decide(DecisionRequest(cfg, model, ShardInput(name, history.window(name)),
                                                    now=time.time())) for name in snapshot.shards]
                report = build_report(cfg, model, decisions)
                report.update({"status": "complete", "read_only": True, "tick": tick,
                               "brokers": {key: asdict(s) for key, s in snapshot.brokers.items()},
                               "scope": "Targets and min/max bounds apply per shard; review broker skew "
                                        "and queue ownership before applying recommendations."})
            except CollectorError as exc:
                failed = True
                report = {"status": "no-decision", "read_only": True, "tick": tick, "reason": str(exc)}
            click.echo(json.dumps(report))
            if once or (iterations and tick >= iterations):
                if failed:
                    raise click.ClickException("no complete fleet snapshot")
                break
            time.sleep(max(0, cfg.metrics.scrape_interval - (time.time() - start)))
    finally:
        collector.close()
