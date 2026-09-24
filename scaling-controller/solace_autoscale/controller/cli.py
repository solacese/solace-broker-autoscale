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
from .events import ManagedControlBus, ReconcileSchedule
from .features import contract_digest, feature_contract, placement_contract
from .provisioning import CloudProvisioner
from .runtime import Controller
from .semp import QueueManager
from .store import ControllerStore


def _wait_for_reconcile(event_bus: ManagedControlBus | None, timeout: float) -> bool:
    """Block until the deadline even when optional events are disabled."""
    if event_bus is not None:
        return event_bus.wait(timeout)
    time.sleep(timeout)
    return False


@click.command("adopt-feature-contract")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--inventory", "inventory_path", required=False, type=click.Path(exists=True, path_type=Path))
@click.option("--yes", is_flag=True, help="Confirm that the displayed contract describes existing state.")
def adopt_feature_contract(config_path: Path, inventory_path: Path | None, yes: bool) -> None:
    """Explicitly attest feature requirements for a pre-contract assignment database."""
    cfg = load_config(config_path)
    inventory_path = inventory_path or (Path(cfg.inventory) if cfg.inventory else None)
    if inventory_path is None:
        raise click.ClickException("set inventory in the connection profile or pass --inventory")
    inventory = load_inventory(inventory_path)
    shards = sorted({b.shard for b in inventory.brokers if b.role in ("active", "warm")})
    contract = feature_contract(cfg, shards, deployment_mode=inventory.deployment_mode)
    broker_contract = placement_contract(inventory)
    digest = contract_digest({"features": contract, "placement": broker_contract})
    click.echo(json.dumps({
        "digest": digest, "feature_contract": contract, "placement_contract": broker_contract
    }, indent=2))
    if not yes:
        raise click.ClickException("review the contract, then rerun with --yes to attest existing state")
    store = AssignmentStore(cfg.assignment.store)
    try:
        controller_store = ControllerStore(store)
        if controller_store.pending():
            raise click.ClickException("finish pending migrations before adopting a feature contract")
        with store.transaction():
            if store.feature_contract() is not None or store.placement_contract() is not None:
                raise click.ClickException(
                    "placement contracts already exist; adoption never overwrites them"
                )
            store.ensure_feature_contract(contract, adopt_existing=True)
            store.ensure_placement_contract(broker_contract, adopt_existing=True)
            controller_store.event(
                time.time(), "feature-contract-adopted", {"digest": digest, "shards": shards}
            )
    finally:
        store.close()
    click.echo(f"adopted feature contract {digest}")


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
        scaling_brokers = [b for b in inventory.brokers if b.role in ("active", "warm")]
        for endpoint in scaling_brokers:
            assert endpoint.username_env and endpoint.password_env and endpoint.base_url
            user, password = os.environ.get(endpoint.username_env), os.environ.get(endpoint.password_env)
            if not user or not password:
                raise click.ClickException(f"missing SEMP credential variables for {endpoint.broker_id}")
            connections[endpoint.broker_id] = SempConnection(endpoint.base_url, user, password)
        store = AssignmentStore(path)
        queues = QueueManager(
            cfg.automation.fleet_id,
            connections,
            {b.broker_id: b.msg_vpn for b in scaling_brokers},
            cfg.automation.queue_spool_mb,
        )
        try:
            controller = Controller(cfg, model, inventory, store, queues)
            event_bus = None
            if cfg.automation.events.enabled and not once:
                event_credentials = {}
                for endpoint in scaling_brokers:
                    password = os.environ.get(cfg.automation.events.password_env)
                    if not password:
                        raise click.ClickException(
                            f"missing managed event credential variable "
                            f"{cfg.automation.events.password_env}"
                        )
                    event_credentials[endpoint.broker_id] = (
                        cfg.automation.events.client_username,
                        password,
                    )
                event_bus = ManagedControlBus(
                    cfg.automation.fleet_id,
                    cfg.automation.events,
                    scaling_brokers,
                    event_credentials,
                    controller.store,
                    sample_due=queues.request_refresh,
                )
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
            schedule = ReconcileSchedule.start(cfg.automation.poll_interval, time.monotonic())
            event_wakeup = False
            while True:
                monotonic_now = time.monotonic()
                if not once and not schedule.should_run(monotonic_now, event_wakeup):
                    timeout = schedule.due_in(monotonic_now)
                    event_wakeup = _wait_for_reconcile(event_bus, timeout)
                    continue
                event_wakeup = False
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
        except (ValueError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc
        finally:
            if "event_bus" in locals() and event_bus is not None:
                event_bus.close()
            if "cloud" in locals() and cloud is not None:
                cloud.close()
            queues.close()
            store.close()
