"""Unattended handover safety: timers, acknowledgments, rollback and crash recovery."""

from dataclasses import replace

import pytest

from solace_autoscale.assignment.placement import assign
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
from solace_autoscale.config import AutomationConfig
from solace_autoscale.controller.migration import MigrationEngine
from solace_autoscale.controller.planner import PartitionLoad, propose_move
from solace_autoscale.controller.semp import QueueStatus
from solace_autoscale.controller.store import ControllerStore


class FakeQueues:
    def __init__(self):
        self.states = {
            "a": QueueStatus(10, 2, 1, 100, True, 100, 10000),
            "b": QueueStatus(0, 0, 1, 0, False, 0, 0),
        }
        self.calls = []
        self.fail = False

    def prepare(self, broker, shard, partition, *, enabled=False):
        self.calls.append(("prepare", broker))

    def status(self, broker, shard, partition):
        if self.fail:
            raise ConnectionError("unavailable")
        return self.states[broker]

    def ingress(self, broker, shard, partition, enabled):
        self.calls.append(("ingress", broker, enabled))
        self.states[broker] = replace(self.states[broker], ingress_enabled=enabled)


@pytest.fixture
def setup(tmp_path):
    db = AssignmentStore(tmp_path / "state.db")
    for name in ["a", "b"]:
        db.upsert_broker(Broker(name, "orders", "vpn", BrokerState.ACTIVE, {"smf": f"tcp://{name}"}))
    assign(db, "orders", "partition:0", "guaranteed", 100, 300)
    state = ControllerStore(db)
    state.begin("orders", 0, "a", "b", 100)
    policy = AutomationConfig(poll_interval=1, migration_grace=3, empty_settle=2, migration_timeout=20)
    queues = FakeQueues()
    yield db, state, policy, queues
    db.close()


def tick(state, engine, now):
    moves = state.pending()
    if moves:
        engine.advance(moves[0], now)


def test_grace_never_bypasses_unacked_payments(setup):
    db, state, policy, q = setup
    engine = MigrationEngine(state, q, policy)
    tick(state, engine, 100)
    tick(state, engine, 101)
    q.states["a"] = replace(q.states["a"], messages=0, spool_bytes=0, unacked=1)
    for now in range(102, 110):
        tick(state, engine, now)
    assert state.pending()[0].phase == "draining"
    assert db.get_placement("orders", "partition:0").broker_id == "a"
    assert not q.states["a"].ingress_enabled
    assert not q.states["b"].ingress_enabled


def test_restart_recovers_fenced_drain_and_atomic_cutover(setup):
    db, state, policy, q = setup
    engine = MigrationEngine(state, q, policy)
    tick(state, engine, 100)
    tick(state, engine, 101)
    q.states["a"] = replace(q.states["a"], messages=0, unacked=0, spool_bytes=0)
    tick(state, engine, 102)
    # New controller object over persisted state; no in-memory intent dependency.
    recovered = MigrationEngine(ControllerStore(db), q, policy)
    tick(state, recovered, 103)
    tick(state, recovered, 104)
    assert db.get_placement("orders", "partition:0").broker_id == "b"
    assert not q.states["b"].ingress_enabled
    assert state.pending()[0].phase == "activating"
    tick(state, MigrationEngine(ControllerStore(db), q, policy), 105)
    assert not state.pending()
    assert q.states["b"].ingress_enabled
    assert not q.states["a"].ingress_enabled


def test_telemetry_failure_resets_continuous_empty_proof(setup):
    db, state, policy, q = setup
    engine = MigrationEngine(state, q, policy)
    tick(state, engine, 100)
    tick(state, engine, 101)
    q.states["a"] = replace(q.states["a"], messages=0, unacked=0, spool_bytes=0)
    tick(state, engine, 102)
    q.fail = True
    tick(state, engine, 103)
    q.fail = False
    tick(state, engine, 104)
    assert db.get_placement("orders", "partition:0").broker_id == "a"
    tick(state, engine, 105)
    tick(state, engine, 106)
    assert db.get_placement("orders", "partition:0").broker_id == "b"


def test_destination_consumer_required_before_source_is_fenced(setup):
    db, state, policy, q = setup
    q.states["b"] = replace(q.states["b"], consumers=0)
    engine = MigrationEngine(state, q, policy)
    for now in range(100, 108):
        tick(state, engine, now)
    assert q.states["a"].ingress_enabled
    assert state.pending()[0].phase == "preparing"


def test_drain_timeout_rolls_back_without_forcing_data_movement(setup):
    db, state, policy, q = setup
    engine = MigrationEngine(state, q, policy)
    for now in range(100, 123):
        tick(state, engine, now)
    assert not state.pending()
    assert db.get_placement("orders", "partition:0").broker_id == "a"
    assert q.states["a"].ingress_enabled
    assert not q.states["b"].ingress_enabled
    assert q.states["a"].messages == 10


def test_ownership_commit_never_reopens_source_on_activation_timeout(setup):
    db, state, policy, q = setup
    engine = MigrationEngine(state, q, policy)
    tick(state, engine, 100)
    tick(state, engine, 101)
    q.states["a"] = replace(q.states["a"], messages=0, unacked=0, spool_bytes=0)
    for now in range(102, 105):
        tick(state, engine, now)
    q.states["b"] = replace(q.states["b"], consumers=0)
    tick(state, engine, 200)
    assert state.pending()[0].phase == "activating"
    assert not q.states["a"].ingress_enabled
    assert db.get_placement("orders", "partition:0").broker_id == "b"


def test_load_planner_fits_the_target_instead_of_round_robin():
    loads = [
        PartitionLoad("s", 0, "a", 0.5, 0.3),
        PartitionLoad("s", 1, "a", 0.4, 0.3),
        PartitionLoad("s", 2, "a", 0.2, 0.1),
        PartitionLoad("s", 3, "b", 0.55, 0.2),
    ]
    move = propose_move(loads, ["a", "b", "c"], trigger=0.8, target=0.65, excluded=set())
    assert move[1] == "c"
    assert move[0].partition == 0  # Relieves a to .6, fits c at .5; b cannot accommodate it.
    assert (
        propose_move(
            [PartitionLoad("s", 0, "a", 1.2, 1.2)], ["a", "b"], trigger=0.8, target=0.65, excluded=set()
        )
        is None
    )  # An indivisible hot partition cannot fit anywhere.


@pytest.mark.parametrize("override,expected", [
    ({}, True),
    ({"enabled": False}, False),
    ({"trigger_utilization": .95}, False),
    ({"scale_up_window": "60s"}, False),
    ({"target_utilization": .2}, False),
])
def test_controller_dispatches_partitions_after_a_sustained_burst(tmp_path, override, expected):
    from solace_autoscale.capacity.benchmarks import BenchmarkPoint, BenchmarkSheet, BenchmarkWorkbook
    from solace_autoscale.capacity.profile_model import compile_profile
    from solace_autoscale.config import Config
    from solace_autoscale.controller.runtime import Controller
    from solace_autoscale.metrics.fleet import FleetInventory

    points = [
        BenchmarkPoint(
            scenario=mode,
            fanout=1,
            msg_size_bytes=1000,
            ingress_msg_rate=1000,
            egress_msg_rate=1000,
            ingress_cell="C1",
            egress_cell="K1",
        )
        for mode in ["direct", "streaming", "unspooling", "replay", "tracing"]
    ]
    book = BenchmarkWorkbook(
        source_filename="invented-controller-test.xlsx",
        source_sha256="a" * 64,
        provider="aws",
        broker_version="10.1.2.3",
        sheets=[
            BenchmarkSheet(sheet="sc-1k", service_class="enterprise-1k", metadata={}, observations=points)
        ],
    )
    model = compile_profile(
        book,
        {
            "enterprise-1k": {
                "service_class_id": "ENTERPRISE_1K_HIGHAVAILABILITY",
                "connections_max": 1000,
                "spool_bytes_max": 1000000000,
            }
        },
        compiled_at="test",
        limits_source="invented test limits, not production data",
    )
    cfg = Config.model_validate(
        {
            "fleet": {"service_class": "enterprise-1k", "max_brokers": 2},
            "assignment": {"routing": "partitioned", "partitions": 3},
            "capacity": {"message_size_hint": 1000},
            "automation": {"enabled": True, "poll_interval": 10, "shards": {"payments": override}},
            "policy": {"scale_up_window": 30},
            "metrics": {"scrape_interval": 10},
            "actuation": {"mode": "scale-up-only", "dry_run": False, "require_confirmation": False},
        }
    )
    inv = FleetInventory.model_validate(
        {
            "provider": "aws",
            "broker_version": "10.1.2.3",
            "service_class": "enterprise-1k",
            "brokers": [
                {
                    "broker_id": b,
                    "shard": "payments",
                    "msg_vpn": "vpn",
                    "base_url": f"https://{b}.example",
                    "username_env": "U",
                    "password_env": "P",
                    "endpoints": {"smf": f"tcps://{b}"},
                    "role": role,
                }
                for b, role in [("a", "active"), ("b", "warm")]
            ],
        }
    )

    class PartitionQueues:
        def __init__(self):
            self.states = {}

        def prepare(self, b, s, p, *, enabled=False):
            self.states.setdefault((b, p), QueueStatus(0, 0, 1, 0, enabled, 0, 0))

        def ingress(self, b, s, p, enabled):
            self.states[(b, p)] = replace(self.states[(b, p)], ingress_enabled=enabled)

        def status(self, b, s, p):
            return self.states[(b, p)]

    db = AssignmentStore(tmp_path / "controller.db")
    q = PartitionQueues()
    c = Controller(cfg, model, inv, db, q)
    c.bootstrap(100)
    assert c.tick(100).state == "observing"
    for now in [110, 120, 130, 140]:
        for key, status in list(q.states.items()):
            if status.ingress_enabled:
                q.states[key] = replace(
                    status,
                    spooled_messages=status.spooled_messages + 3000,
                    spooled_bytes=status.spooled_bytes + 3000000,
                )
        result = c.tick(now)
    if not expected:
        assert result.state != "migration-planned"
        assert not c.store.pending()
        assert db.get_broker("b").state == BrokerState.WARM
        db.close()
        return
    assert result.state == "migration-planned"
    assert db.get_broker("b").state == BrokerState.ACTIVE
    for now in [150, 160, 170, 180, 190, 200]:
        c.tick(now)
    assert not c.store.pending()
    owners = [db.get_placement("payments", f"partition:{p}").broker_id for p in range(3)]
    assert owners.count("b") == 1
    assert owners.count("a") == 2
    db.close()
