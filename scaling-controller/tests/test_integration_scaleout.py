"""Real unattended 1 -> 2 -> 3 -> 4 local broker scale-out qualification.

The broker workload, queue/VPN telemetry, publisher routing, subscriber fanout, and migration
steps are real. The deliberately small capacity envelope is invented test-only policy data: it
makes a bounded functional run trigger at low load and MUST NOT train or validate capacity.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path

import pytest
import uvicorn

from solace_autoscale.actuator.solace_cloud import SempConnection
from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore, BrokerState
from solace_autoscale.capacity.benchmarks import BenchmarkPoint, BenchmarkSheet, BenchmarkWorkbook
from solace_autoscale.capacity.profile_model import compile_profile
from solace_autoscale.config import Config
from solace_autoscale.controller import runtime as controller_runtime
from solace_autoscale.controller.runtime import Controller
from solace_autoscale.controller.semp import QueueManager
from solace_autoscale.metrics.fleet import FleetInventory
from solace_autoscale_client import KeyRouter, MessagingClient, Resolver

pytestmark = pytest.mark.integration


@contextmanager
def live_api(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


def eventually(condition, *, timeout=30, detail="condition did not become true"):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, detail
        time.sleep(0.05)


def _invented_functional_profile():
    """Compile a tiny non-production envelope solely to exercise automatic controller decisions.

    Controller intentionally rejects models flagged synthetic. This fixture therefore uses the same
    measured-profile compiler as production but labels every source/provenance field as invented.
    Its 30 msg/s fanout-adjusted envelope is a test trigger, not a PubSub+ Standard capacity statement.
    """
    points = [
        BenchmarkPoint(
            scenario=scenario,
            fanout=fanout,
            msg_size_bytes=size,
            ingress_msg_rate=120,
            egress_msg_rate=240,
            ingress_cell=f"SYNTHETIC-{scenario}-{fanout}-{size}-IN",
            egress_cell=f"SYNTHETIC-{scenario}-{fanout}-{size}-OUT",
        )
        for scenario in ("direct", "streaming")
        for fanout in (1, 2)
        for size in (256, 4096)
    ]
    workbook = BenchmarkWorkbook(
        source_filename="SYNTHETIC-TEST-ONLY-NOT-CAPACITY.xlsx",
        source_sha256="0" * 64,
        provider="aws",
        broker_version="TEST-ONLY-NOT-A-BROKER-VERSION",
        sheets=[
            BenchmarkSheet(
                sheet="synthetic-functional-trigger",
                service_class="enterprise-1k",
                metadata={"evidence": "invented reduced-threshold functional test only"},
                observations=points,
            )
        ],
        notes=[
            "INVENTED TEST-ONLY CAPACITY ENVELOPE; never use for planning, training, or product claims."
        ],
    )
    return compile_profile(
        workbook,
        {
            "enterprise-1k": {
                "service_class_id": "ENTERPRISE_1K_HIGHAVAILABILITY",
                "connections_max": 10000,
                "spool_bytes_max": 1_000_000_000,
            }
        },
        compiled_at="TEST-ONLY",
        limits_source="INVENTED TEST-ONLY LIMITS; not production data",
    )


def _config(partitions: int, fleet_id: str) -> Config:
    return Config.model_validate(
        {
            "fleet": {"service_class": "enterprise-1k", "max_brokers": 4},
            "workload": {"delivery": "guaranteed"},
            "metrics": {"source": "semp", "scrape_interval": 1, "staleness_limit": 30},
            "policy": {"scale_up_window": 3, "cooldown": 0, "warm_pool": 3},
            "actuation": {
                "mode": "scale-up-only",
                "dry_run": False,
                "require_confirmation": False,
                "kill_switch_file": "/tmp/solace-autoscale-test-only-kill-switch",
            },
            "capacity": {"scenario": "streaming", "fanout": 2, "message_size_hint": 1024},
            "assignment": {
                "routing": "partitioned",
                "strategy": "least-placements",
                "partitions": partitions,
                "lease_seconds": 1,
            },
            "automation": {
                "enabled": True,
                "fleet_id": fleet_id,
                "poll_interval": 5,
                "migration_grace": 0,
                "empty_settle": 0.1,
                "migration_timeout": 30,
                "max_migrations_per_hour": 12,
                "target_utilization": 0.15,
                "trigger_utilization": 0.20,
                "queue_spool_mb": 100,
            },
            "provisioning": {"client_username": "default"},
            "messaging": {
                "enabled": True,
                "allow_dynamic_groups": False,
                "routes": [
                    {"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}
                ],
                "subscriptions": [
                    {"group": "ledger", "topics": ["payments/*/created"]},
                    {"group": "audit", "topics": ["payments/*/created"]},
                ],
            },
        }
    )


def _inventory(ports: list[dict[str, int]]) -> FleetInventory:
    return FleetInventory.model_validate(
        {
            "provider": "aws",
            "broker_version": "TEST-ONLY-NOT-A-BROKER-VERSION",
            "service_class": "enterprise-1k",
            "deployment_mode": "HA",
            "brokers": [
                {
                    "broker_id": chr(ord("a") + index),
                    "shard": "payments",
                    "msg_vpn": "default",
                    "base_url": f"http://127.0.0.1:{item['semp']}",
                    "username_env": "TEST_ONLY_SEMP_USERNAME",
                    "password_env": "TEST_ONLY_SEMP_PASSWORD",
                    "endpoints": {"smf": f"tcp://127.0.0.1:{item['smf']}"},
                    "role": "active" if index == 0 else "warm",
                }
                for index, item in enumerate(ports)
            ],
        }
    )


def _owners(db: AssignmentStore, partitions: int) -> list[str]:
    return [
        db.get_placement("payments", f"partition:{partition}").broker_id
        for partition in range(partitions)
    ]


def _events(db: AssignmentStore, migration_id: str) -> list[dict]:
    rows = db._conn.execute(
        "SELECT event,detail FROM controller_events WHERE migration_id=? ORDER BY id",
        (migration_id,),
    ).fetchall()
    return [{"event": row["event"], "detail": json.loads(row["detail"])} for row in rows]


def test_real_unattended_sequential_scaleout_preserves_affinity_fanout_and_order(
    tmp_path, monkeypatch
):
    ports = json.loads(os.environ.get("SOLACE_TOPOLOGY_PORTS", "[]"))
    if len(ports) < 4:
        pytest.skip("set SOLACE_TOPOLOGY_PORTS to four owned local Standard broker port pairs")
    ports = ports[:4]
    assert all(
        isinstance(item, dict)
        and isinstance(item.get("semp"), int)
        and isinstance(item.get("smf"), int)
        for item in ports
    )

    partitions = 12
    fleet_id = "scaleout-" + str(int(time.time() * 1000))
    cfg = _config(partitions, fleet_id)
    inventory = _inventory(ports)
    db = AssignmentStore(tmp_path / "scaleout.db")
    queues = QueueManager(
        fleet_id,
        {
            endpoint.broker_id: SempConnection(endpoint.base_url, "admin", "admin")
            for endpoint in inventory.brokers
        },
        {endpoint.broker_id: endpoint.msg_vpn for endpoint in inventory.brokers},
        cfg.automation.queue_spool_mb,
    )
    controller = Controller(cfg, _invented_functional_profile(), inventory, db, queues)
    app = create_app(
        db,
        policy=cfg.assignment,
        fleet_id=fleet_id,
        messaging=cfg.messaging,
    )
    controller.bootstrap(0)
    assert set(_owners(db, partitions)) == {"a"}
    assert [db.get_broker(name).state for name in "abcd"] == [
        BrokerState.ACTIVE,
        BrokerState.WARM,
        BrokerState.WARM,
        BrokerState.WARM,
    ]

    account_router = KeyRouter(Resolver("http://unused"), "payments", "qualification", partitions=partitions)
    accounts: dict[int, str] = {}
    candidate = 0
    while len(accounts) < partitions:
        account = f"account-{candidate}"
        partition = account_router.partition_for(json.dumps([account], separators=(",", ":")))
        accounts.setdefault(partition, account)
        candidate += 1
    assert set(accounts) == set(range(partitions))

    accepted: list[str] = []
    deliveries: dict[str, list[str]] = {"ledger": [], "audit": []}
    event_partition: dict[str, int] = {}
    expected_by_partition: dict[int, list[str]] = {partition: [] for partition in range(partitions)}
    stages: list[dict] = []
    migrations: list[dict] = []
    planner_observations: list[dict] = []
    now = 0.0

    original_plan_next_move = controller_runtime.plan_next_move

    def observe_plan(problem):
        outcome = original_plan_next_move(problem)
        planner_observations.append(
            {
                "bundles": [
                    {
                        "bundle": bundle.bundle_id,
                        "owner": bundle.current_broker,
                        "movable": bundle.movable.values,
                        "relief": (bundle.relief or bundle.movable).values,
                    }
                    for bundle in problem.bundles
                ],
                "brokers": [
                    {
                        "broker": broker.broker_id,
                        "role": broker.role,
                        "state": broker.state,
                        "fixed_load": broker.fixed_load.values,
                    }
                    for broker in problem.brokers
                ],
                "trigger": problem.constraints.trigger,
                "target": problem.constraints.target,
                "proposal": asdict(outcome.proposal) if outcome.proposal else None,
                "reason": outcome.reason,
                "rejections": dict(outcome.rejection_counts),
            }
        )
        return outcome

    monkeypatch.setattr(controller_runtime, "plan_next_move", observe_plan)

    def handler(group: str):
        return lambda message: deliveries[group].append(message.event_id)

    with live_api(app) as url, MessagingClient(
        url,
        state_dir=tmp_path / "client",
        credentials=lambda _: ("default", "default"),
        poll_interval=0.05,
        max_inflight=128,
    ) as client:
        client.subscribe(group="ledger", handler=handler("ledger"), timeout=30)
        client.subscribe(group="audit", handler=handler("audit"), timeout=30)

        def snapshot() -> dict[int, tuple[str, int, int]]:
            result = {}
            for partition, owner in enumerate(_owners(db, partitions)):
                status = queues.status(owner, "payments", partition)
                result[partition] = (owner, status.spooled_messages, status.spooled_bytes)
            return result

        def publish_interval(stage: int, count: int, interval: int, *, phase="pre") -> list[str]:
            ids = []
            for sequence in range(count):
                for partition in range(partitions):
                    event_id = f"s{stage}-{phase}-i{interval}-p{partition}-n{sequence}"
                    client.publish(
                        f"payments/{accounts[partition]}/created",
                        {"stage": stage, "sequence": sequence, "padding": "x" * 700},
                        event_id=event_id,
                    )
                    accepted.append(event_id)
                    ids.append(event_id)
                    event_partition[event_id] = partition
                    expected_by_partition[partition].append(event_id)
            assert client.flush(30), client.status()
            eventually(
                lambda: all(len(deliveries[group]) >= len(accepted) for group in deliveries),
                timeout=30,
                detail=f"stage {stage} did not reach both subscriber groups",
            )
            return ids

        def finish_migration() -> None:
            nonlocal now
            deadline = time.monotonic() + 30
            while controller.store.pending():
                assert time.monotonic() < deadline, "automatic migration did not complete"
                time.sleep(0.5)
                now = time.monotonic()
                controller.tick(now)

        # Each stage uses actual monotonic time for every controller sample. A no-decision response can
        # occur when a full live SEMP scrape plus publication processing exceeds the declared continuity
        # window; that sample resets history and is retried rather than converted into an invented rate.
        # Counts grow 15 -> 25 -> 30 per partition/interval. With the invented 30 msg/s fanout-adjusted
        # envelope, existing one-partition owners cannot accept another stage-2/3 bundle, forcing a new
        # warm destination rather than manually selecting or moving an owner.
        for stage_number, per_partition_interval in enumerate((15, 25, 30), start=1):
            before = snapshot()
            now = time.monotonic()
            baseline = controller.tick(now)
            assert baseline.state in ("observing", "no-decision")
            stage_ids = []
            result = baseline
            deadline = time.monotonic() + 90
            interval = 0
            while result.state != "migration-planned":
                assert time.monotonic() < deadline, result
                interval += 1
                stage_ids += publish_interval(stage_number, per_partition_interval, interval)
                time.sleep(5)
                now = time.monotonic()
                result = controller.tick(now)
                assert result.state in ("observing", "no-decision", "migration-planned"), result
            planned = result
            assert planned.state == "migration-planned", planned
            pending = controller.store.pending()
            assert len(pending) == 1
            move = pending[0]
            owners_before = _owners(db, partitions)
            assert move.source == owners_before[move.partition]
            assert db.get_broker(move.target).state == BrokerState.ACTIVE
            finish_migration()
            owners_after = _owners(db, partitions)
            assert owners_after[move.partition] == move.target
            assert len(set(owners_after)) == stage_number + 1
            assert set(owners_after) == set("abcd"[: stage_number + 1])

            after = snapshot()
            expected_copies = per_partition_interval * interval * len(deliveries)
            counter_deltas = {}
            for partition in range(partitions):
                before_owner, before_messages, before_bytes = before[partition]
                current_owner = owners_before[partition]
                assert current_owner == before_owner
                # All stage publications happened before the migration, on that stage's owner.
                status = queues.status(current_owner, "payments", partition)
                message_delta = status.spooled_messages - before_messages
                byte_delta = status.spooled_bytes - before_bytes
                assert message_delta == expected_copies
                assert byte_delta > 0
                counter_deltas[str(partition)] = {
                    "owner": current_owner,
                    "spooled_message_delta": message_delta,
                    "spooled_byte_delta": byte_delta,
                }

            source = queues.status(move.source, "payments", move.partition)
            target = queues.status(move.target, "payments", move.partition)
            assert source.drained and not source.ingress_enabled
            assert target.drained and target.ingress_enabled
            assert after[move.partition][0] == move.target

            # Publish once through every partition after cutover. Counters on each durable owner prove
            # that the newly activated target receives real traffic, including broker d at the final step.
            post_before = snapshot()
            post_ids = publish_interval(stage_number, 2, 1, phase="post")
            post_after = snapshot()
            post_counter_deltas = {}
            for partition, owner in enumerate(owners_after):
                before_owner, before_messages, before_bytes = post_before[partition]
                after_owner, after_messages, after_bytes = post_after[partition]
                assert before_owner == after_owner == owner
                message_delta = after_messages - before_messages
                byte_delta = after_bytes - before_bytes
                assert message_delta == 2 * len(deliveries)
                assert byte_delta > 0
                post_counter_deltas[str(partition)] = {
                    "owner": owner,
                    "spooled_message_delta": message_delta,
                    "spooled_byte_delta": byte_delta,
                }
            assert post_counter_deltas[str(move.partition)]["owner"] == move.target

            transition_events = _events(db, move.id)
            transitions = [
                event["detail"]["to"]
                for event in transition_events
                if event["event"] == "transition"
            ]
            assert transitions == ["fencing", "draining", "activating", "complete"]
            migrations.append(
                {
                    "stage": stage_number,
                    "partition": move.partition,
                    "source": move.source,
                    "target": move.target,
                    "transitions": transitions,
                    "source_drained": source.drained,
                    "source_ingress_enabled": source.ingress_enabled,
                    "target_ingress_enabled": target.ingress_enabled,
                }
            )
            stages.append(
                {
                    "active_owners": stage_number + 1,
                    "pre_cutover_events": len(stage_ids),
                    "post_cutover_events": len(post_ids),
                    "owners": owners_after,
                    "pre_cutover_queue_counter_deltas": counter_deltas,
                    "post_cutover_queue_counter_deltas": post_counter_deltas,
                    "planner": planned.explanation,
                }
            )

        eventually(
            lambda: all(len(deliveries[group]) == len(accepted) for group in deliveries),
            timeout=30,
            detail="final accepted IDs did not reach both groups",
        )

    expected = set(accepted)
    for group, ids in deliveries.items():
        assert set(ids) == expected, group
        assert len(ids) == len(set(ids)) == len(accepted), group
        for partition in range(partitions):
            assert [event for event in ids if event_partition[event] == partition] == expected_by_partition[
                partition
            ]

    # Historical source queues remain as fenced audit/drain artifacts. Exactly one queue per group and
    # partition may accept ingress, and it must be the durable owner selected by Controller.tick().
    for partition, owner in enumerate(_owners(db, partitions)):
        for group in deliveries:
            enabled = []
            for broker in "abcd":
                response = queues._request(
                    broker,
                    "GET",
                    queues._path(broker, "payments", partition, group=group),
                )
                if response.is_success:
                    if response.json()["data"]["ingressEnabled"]:
                        enabled.append(broker)
                else:
                    assert response.status_code in (400, 404)
            assert enabled == [owner]

    report = {
        "schema_version": "qualification-evidence-v1",
        "measurement_scope": (
            "bounded reduced-threshold functional automatic 1-to-4 scale-out; "
            "not capacity, HA failover, Cloud provisioning, or production qualification"
        ),
        "capacity_policy": {
            "kind": "INVENTED TEST-ONLY",
            "purpose": "trigger low-volume functional control decisions",
            "must_not_train_or_support_capacity_claims": True,
        },
        "case": {
            "name": "automatic-1-to-4",
            "partitions": partitions,
            "original_events": len(accepted),
            "subscriber_groups": len(deliveries),
            "expected_deliveries": len(accepted) * len(deliveries),
            "scale_steps": stages,
            "migrations": migrations,
            "planner_observations": planner_observations,
            "missing_ids": sum(len(expected - set(ids)) for ids in deliveries.values()),
            "duplicate_ids": sum(len(ids) - len(set(ids)) for ids in deliveries.values()),
            "ordering_violations": 0,
            "active_owner_counts": [len(set(stage["owners"])) for stage in stages],
        },
    }
    output = os.environ.get("AUTOSCALE_SCALEOUT_REPORT")
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2) + "\n")

    queues.close()
    db.close()
