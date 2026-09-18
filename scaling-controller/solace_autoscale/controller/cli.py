"""Unattended control command; no confirmation prompts inside the running loop."""

from __future__ import annotations

import fcntl
import json
import os
import time
from dataclasses import asdict
from pathlib import Path

import click

from ..actuator.safety import AuditLog
from ..actuator.solace_cloud import SempConnection, SolaceCloudClient
from ..assignment.store import AssignmentStore
from ..capacity.model import load_model
from ..config import load_config
from ..metrics.fleet import load_inventory
from .provisioning import CloudProvisioner
from .runtime import Controller
from .semp import QueueManager


@click.command("run")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--inventory", "inventory_path", required=False, type=click.Path(exists=True, path_type=Path))
@click.option("--once", is_flag=True)
def run_controller(config_path: Path, inventory_path: Path | None, once: bool) -> None:
    """Run autonomous managed-partition scaling and recover pending handovers after restart."""
    cfg = load_config(config_path)
    inventory_path = inventory_path or (Path(cfg.inventory) if cfg.inventory else None)
    if inventory_path is None:
        raise click.ClickException("set inventory in the connection profile or pass --inventory")
    inventory = load_inventory(inventory_path)
    model = load_model(cfg.capacity.model)
    path = Path(cfg.assignment.store)
    path.parent.mkdir(parents=True, exist_ok=True)
    # A process lock (not an expiring lease) prevents two controller writers on this host.
    with path.with_suffix(path.suffix + ".controller.lock").open("a") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise click.ClickException("another controller owns this local state database") from exc
        connections = {}
        for endpoint in inventory.brokers:
            user, password = os.environ.get(endpoint.username_env), os.environ.get(endpoint.password_env)
            if not user or not password:
                raise click.ClickException(f"missing SEMP credential variables for {endpoint.broker_id}")
            connections[endpoint.broker_id] = SempConnection(endpoint.base_url, user, password)
        store = AssignmentStore(path)
        queues = QueueManager(
            cfg.automation.fleet_id,
            connections,
            {b.broker_id: b.msg_vpn for b in inventory.brokers},
            cfg.automation.queue_spool_mb,
        )
        try:
            controller = Controller(cfg, model, inventory, store, queues)
            cloud = None
            provisioner = None
            if cfg.provisioning.enabled:
                token = os.environ.get(cfg.provisioning.token_env)
                if not token or not os.environ.get(cfg.provisioning.client_password_env):
                    raise click.ClickException(
                        "Cloud token and managed client password variables are required"
                    )
                cloud = SolaceCloudClient(token, base_url=cfg.provisioning.api_base_url)
                provisioner = CloudProvisioner(controller, cloud, AuditLog(path.with_suffix(".audit.jsonl")))
            initialized = False
            while True:
                start = time.time()
                try:
                    if not initialized:
                        if provisioner:
                            provisioner.reconcile(start, metrics_fresh=False)
                        controller.bootstrap(start)
                        initialized = not Path(cfg.actuation.kill_switch_file).exists()
                    result = controller.tick(start)
                    report = {"timestamp": start, **asdict(result)}
                    if provisioner:
                        fresh = result.state in ("observing", "migration-planned", "capacity-shortfall")
                        report["provisioning"] = provisioner.reconcile(start, metrics_fresh=fresh)
                    click.echo(json.dumps(report))
                except Exception as exc:
                    controller.store.event(start, "loop-retry", {"error_type": type(exc).__name__})
                    if once:
                        raise click.ClickException(f"controller cycle failed: {type(exc).__name__}") from exc
                    click.echo(
                        json.dumps({"timestamp": start, "state": "retry", "error_type": type(exc).__name__})
                    )
                if once:
                    break
                time.sleep(max(0, cfg.automation.poll_interval - (time.time() - start)))
        except (ValueError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc
        finally:
            if "cloud" in locals() and cloud is not None:
                cloud.close()
            queues.close()
            store.close()
