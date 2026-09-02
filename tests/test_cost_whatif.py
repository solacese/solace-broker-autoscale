"""Cost modelling, what-if projection, rolling history, and static-collector extensions."""

from __future__ import annotations

from solace_autoscale.decision.engine import DecisionRequest, decide
from solace_autoscale.decision.types import Action, MetricSample, ShardInput
from solace_autoscale.metrics.history import RollingHistory
from solace_autoscale.report.cost import fleet_cost_summary, shard_cost, warm_pool_monthly
from solace_autoscale.simulator.what_if import project

from .conftest import default_config, make_test_model, window


def _decide(cfg, model, rate=30000, size=1000, brokers=1):
    shard = ShardInput("orders", window(n=20, ingress_msg_rate=rate, avg_msg_size=size,
                                        current_brokers=brokers))
    now = max(s.timestamp for s in shard.samples) + 1
    return decide(DecisionRequest(config=cfg, model=model, shard=shard, now=now)), shard, now


# ---- cost ------------------------------------------------------------------------------------

def test_cost_priced():
    cfg = default_config(
        workload={"delivery": "direct", "bottleneck": "messages"},
        policy={"headroom": {"mode": "fixed"}},
        billing={"model": "elastic", "per_broker_monthly": {"enterprise-10k": 4200}},
    )
    model = make_test_model()
    d, _, _ = _decide(cfg, model, rate=30000)  # needs 4 brokers
    c = shard_cost(cfg, d)
    assert c.per_broker_monthly == 4200
    assert c.current_monthly == 4200 * d.current_brokers
    assert c.recommended_monthly == 4200 * d.recommended_brokers
    assert c.delta_monthly == 4200 * (d.recommended_brokers - d.current_brokers)


def test_cost_unpriced_is_count_only():
    cfg = default_config(workload={"delivery": "direct"}, policy={"headroom": {"mode": "fixed"}})
    model = make_test_model()
    d, _, _ = _decide(cfg, model)
    summary = fleet_cost_summary(cfg, [d])
    assert summary["priced"] is False
    assert summary["total_current_monthly"] is None


def test_warm_pool_monthly_committed():
    cfg = default_config(
        billing={"model": "committed", "per_broker_monthly": {"enterprise-10k": 4200}},
        policy={"warm_pool": 2},
    )
    assert warm_pool_monthly(cfg) == 8400
    summary = fleet_cost_summary(cfg, [])
    assert "no offsetting saving" in (summary["warm_pool_note"] or "")


def test_warm_pool_monthly_none_without_price():
    cfg = default_config(policy={"warm_pool": 2})  # no price table
    assert warm_pool_monthly(cfg) is None


# ---- what-if ---------------------------------------------------------------------------------

def test_whatif_multiplier_1_matches_recommendation():
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"},
                         policy={"headroom": {"mode": "fixed"}})
    model = make_test_model()
    d, shard, now = _decide(cfg, model, rate=30000)
    projs = project(cfg, model, shard, now, multipliers=(1.0,))
    # projection at 1.0 must equal the real recommendation (same pure engine)
    assert projs[0].recommended_brokers == d.recommended_brokers


def test_whatif_monotonic_and_ceiling():
    cfg = default_config(
        fleet={"service_class": "enterprise-10k", "min_brokers": 1, "max_brokers": 4},
        workload={"delivery": "direct", "bottleneck": "messages"},
        policy={"headroom": {"mode": "fixed"}},
    )
    model = make_test_model()
    _, shard, now = _decide(cfg, model, rate=10000)
    projs = project(cfg, model, shard, now, multipliers=(1.0, 2.0, 10.0))
    recs = [p.recommended_brokers for p in projs]
    assert recs == sorted(recs)  # non-decreasing with load
    assert projs[-1].hit_ceiling  # 10x hits the 4-broker ceiling


# ---- rolling history -------------------------------------------------------------------------

def test_rolling_history_evicts_old():
    h = RollingHistory(retention_seconds=100)
    for t in range(0, 200, 10):
        h.add("s", MetricSample(t, 1, 1, 1, 1, 100, 1, 0, 1))
    w = h.window("s")
    # newest ts is 190; retention 100 → only ts >= 90 kept
    assert all(s.timestamp >= 90 for s in w)
    assert w[-1].timestamp == 190


def test_rolling_history_accumulates_across_shards():
    h = RollingHistory(retention_seconds=1000)
    h.add("a", MetricSample(1, 1, 1, 1, 1, 1, 1, 0, 1))
    h.add("b", MetricSample(1, 1, 1, 1, 1, 1, 1, 0, 1))
    assert set(h.shards()) == {"a", "b"}


# ---- static collector extensions -------------------------------------------------------------

def test_static_collector_subscribing_and_subdividable(tmp_path):
    import json

    from solace_autoscale.metrics.static import StaticCollector
    doc = {"shards": {"telemetry": {
        "msg_vpn": "acme", "subscribing_brokers": 4, "key_subdividable": False,
        "samples": [{"timestamp": 1, "ingress_msg_rate": 1, "egress_msg_rate": 1,
                     "ingress_byte_rate": 1, "egress_byte_rate": 1, "avg_msg_size": 100,
                     "connection_count": 1, "spool_used": 0, "current_brokers": 1}],
    }}}
    p = tmp_path / "m.json"
    p.write_text(json.dumps(doc))
    c = StaticCollector(p)
    assert c.shard_names() == ["telemetry"]
    assert c.subscribing_brokers("telemetry") == 4
    assert c.key_subdividable("telemetry") is False
    # defaults when absent
    assert StaticCollector(p).subscribing_brokers("telemetry") == 4


def test_mesh_amplification_reaches_engine_via_shardinput():
    # a shard with 4 subscribing brokers under mesh must show higher bytes demand than sharded
    model = make_test_model()
    samples = window(n=20, ingress_msg_rate=2000, avg_msg_size=1000)
    shard = ShardInput("s", samples, subscribing_brokers=4)
    mesh = default_config(topology={"mode": "mesh"},
                          workload={"delivery": "direct", "bottleneck": "bytes"},
                          policy={"headroom": {"mode": "fixed"}})
    sharded = default_config(topology={"mode": "sharded"},
                             workload={"delivery": "direct", "bottleneck": "bytes"},
                             policy={"headroom": {"mode": "fixed"}})
    now = max(s.timestamp for s in samples) + 1
    dm = decide(DecisionRequest(config=mesh, model=model, shard=shard, now=now))
    ds = decide(DecisionRequest(config=sharded, model=model, shard=shard, now=now))
    assert dm.axes["bytes"].demand_ratio > ds.axes["bytes"].demand_ratio
    assert dm.action in (Action.scale_up, Action.hold)


# ---- monitor loop (no live broker: drive with a fake collector via CliRunner is heavy; test the
#      loop's building blocks instead - history + decide + record - which is what monitor wires) ---

def test_monitor_building_blocks_accumulate(tmp_path):
    """The monitor loop = collect → history.add → decide → record. Verify that composition with a
    scripted sequence of samples grows the window and records each tick."""
    from solace_autoscale.accuracy.recorder import AccuracyRecorder

    model = make_test_model()
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"})
    hist = RollingHistory(retention_seconds=10_000)
    rec = AccuracyRecorder(tmp_path / "acc.db")
    for i in range(5):
        ts = 1000 + i * 30
        s = MetricSample(ts, 30000, 30000, 30000000, 30000000, 1000, 100, 0, 1)
        hist.add("orders", s)
        shard = ShardInput("orders", hist.window("orders"))
        d = decide(DecisionRequest(config=cfg, model=model, shard=shard, now=ts + 1))
        rec.record_recommendation(d, cfg.config_hash(), ts=ts + 1)
    assert len(hist.window("orders")) == 5
    import sqlite3
    n = sqlite3.connect(str(tmp_path / "acc.db")).execute(
        "select count(*) from recommendations").fetchone()[0]
    assert n == 5
    rec.close()
