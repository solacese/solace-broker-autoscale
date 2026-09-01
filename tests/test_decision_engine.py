"""Exhaustive unit tests for the pure decision engine (§5, §13).

Covers: capacity lookup + interpolation, demand ratios, required-count + clamp, binding axis by
ratio-to-threshold, hysteresis/gating (up window, down window, cooldown, committed suppression),
staleness refusal, every §5.6 warning, and both branches of derived headroom (§5.7).
"""

from __future__ import annotations

import pytest

from solace_autoscale.decision.engine import DecisionRequest, decide
from solace_autoscale.decision.types import Action, Axis, MetricSample, ShardInput, WarningCode

from .conftest import default_config, make_test_model, window


def codes(decision) -> set[str]:
    return {w.code.value for w in decision.warnings}


def run(cfg, model, shard, now=None, last=None, mtc=None):
    if now is None:
        now = max(s.timestamp for s in shard.samples) + 1
    return decide(DecisionRequest(config=cfg, model=model, shard=shard,
                                  now=now, last_decision_at=last, minutes_to_capacity=mtc))


# ---- 5.2 capacity lookup + interpolation -----------------------------------------------------

def test_exact_bucket_not_interpolated():
    cfg = default_config(workload={"delivery": "direct"})
    model = make_test_model()
    shard = ShardInput("s", window(ingress_msg_rate=100, avg_msg_size=1000))
    d = run(cfg, model, shard)
    assert not d.interpolated
    assert WarningCode.INTERPOLATED_CAPACITY.value not in codes(d)


def test_interpolation_flagged():
    cfg = default_config(workload={"delivery": "direct"})
    model = make_test_model()
    shard = ShardInput("s", window(ingress_msg_rate=100, avg_msg_size=5500))  # between 1000 & 10000
    d = run(cfg, model, shard)
    assert d.interpolated
    assert WarningCode.INTERPOLATED_CAPACITY.value in codes(d)


# ---- 5.3 / 5.4 demand ratios, required count, clamp ------------------------------------------

def test_required_count_arithmetic():
    # direct @1KB: msg_rate cap = 10000/broker. threshold messages 0.75 (fixed mode).
    # ingress 30000 msg/s → required = ceil(30000 / (0.75*10000)) = ceil(4.0) = 4
    cfg = default_config(
        workload={"delivery": "direct", "bottleneck": "messages"},
        policy={"headroom": {"mode": "fixed"}},
    )
    model = make_test_model()
    shard = ShardInput("s", window(ingress_msg_rate=30000, avg_msg_size=1000, current_brokers=1))
    d = run(cfg, model, shard)
    assert d.binding_axis is Axis.messages
    assert d.recommended_brokers == 4
    assert d.action is Action.scale_up


def test_clamp_to_max_and_hit_ceiling_warning():
    cfg = default_config(
        fleet={"service_class": "enterprise-10k", "min_brokers": 1, "max_brokers": 3},
        workload={"delivery": "direct", "bottleneck": "messages"},
        policy={"headroom": {"mode": "fixed"}},
    )
    model = make_test_model()
    shard = ShardInput("s", window(ingress_msg_rate=1_000_000, avg_msg_size=1000))
    d = run(cfg, model, shard)
    assert d.recommended_brokers == 3
    assert WarningCode.HIT_CEILING.value in codes(d)


def test_clamp_to_min():
    cfg = default_config(
        fleet={"service_class": "enterprise-10k", "min_brokers": 2, "max_brokers": 8},
        workload={"delivery": "direct"},
        policy={"headroom": {"mode": "fixed"}},
    )
    model = make_test_model()
    shard = ShardInput("s", window(ingress_msg_rate=1, avg_msg_size=1000, current_brokers=2))
    d = run(cfg, model, shard)
    assert d.recommended_brokers == 2


def test_binding_axis_by_ratio_to_threshold():
    # Construct so spool (0.60 thr) binds over messages (0.75 thr) despite a lower raw ratio.
    # direct @1KB msg cap=10000, bytes cap=10e6, spool cap=500e9.
    # ingress 3000 msg/s @1KB → messages ratio 0.30/0.75 = 0.40;
    #   bytes = (3000+3000)*1000 = 6e6 → ratio 0.60/0.75 = 0.80
    # spool used 350e9 → ratio 0.70/0.60 = 1.167 → spool binds (highest ratio-to-threshold)
    cfg = default_config(workload={"delivery": "direct"}, policy={"headroom": {"mode": "fixed"}})
    model = make_test_model(spool_bytes_max=500_000_000_000)
    shard = ShardInput("s", window(ingress_msg_rate=3000, avg_msg_size=1000,
                                   spool_used=350_000_000_000))
    d = run(cfg, model, shard)
    assert d.binding_axis is Axis.spool


# ---- 5.5 hysteresis / gating -----------------------------------------------------------------

def test_scale_up_requires_full_window():
    # Mean demand requires >1 broker, but the binding ratio did NOT stay above threshold for the
    # whole window (the oldest sample dipped below), so scale-up is gated to hold.
    # direct @1KB msg cap=10000, fixed threshold 0.75 → threshold rate = 7500 msg/s.
    # explicit 300s window over 30s cadence → last ~11 samples must all be hot.
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"},
                         policy={"headroom": {"mode": "fixed"}, "scale_up_window": "300s"})
    model = make_test_model()
    samples = window(n=15, ingress_msg_rate=20000, avg_msg_size=1000)  # hot enough to want scale-up
    # Dip the most recent sample below the 7500 threshold so the ratio was NOT held for the window.
    dipped = []
    for i, s in enumerate(samples):
        rate = 1000 if i == len(samples) - 1 else 20000
        dipped.append(MetricSample(s.timestamp, rate, rate, rate*1000, rate*1000, 1000,
                                   s.connection_count, s.spool_used, 1))
    shard = ShardInput("s", dipped)
    d = run(cfg, model, shard)
    assert d.recommended_brokers == 1  # held, not scaled
    assert d.action is Action.hold
    assert "window" in (d.reason or "")


def test_scale_up_when_held_whole_window():
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"},
                         policy={"headroom": {"mode": "fixed"}, "scale_up_window": "auto"})
    model = make_test_model()
    shard = ShardInput("s", window(n=20, ingress_msg_rate=30000, avg_msg_size=1000))
    d = run(cfg, model, shard)
    assert d.action is Action.scale_up


def test_scale_down_requires_down_window_and_elastic():
    cfg = default_config(
        billing={"model": "elastic"},
        workload={"delivery": "direct", "bottleneck": "messages"},
        policy={"headroom": {"mode": "fixed"}, "scale_down_at": 0.40, "scale_down_window": "10m"},
    )
    model = make_test_model()
    # current 4 brokers, tiny load → required 1, below 0.40 for whole 10m window
    shard = ShardInput("s", window(n=40, cadence=30, ingress_msg_rate=100, avg_msg_size=1000,
                                   current_brokers=4))
    d = run(cfg, model, shard)
    assert d.action is Action.scale_down
    assert d.recommended_brokers == 1


def test_scale_down_suppressed_under_committed():
    cfg = default_config(
        billing={"model": "committed"},
        workload={"delivery": "direct", "bottleneck": "messages"},
        policy={"headroom": {"mode": "fixed"}, "scale_down_window": "10m"},
    )
    model = make_test_model()
    shard = ShardInput("s", window(n=40, ingress_msg_rate=100, avg_msg_size=1000, current_brokers=4))
    d = run(cfg, model, shard)
    assert d.action is Action.hold
    assert WarningCode.COMMITTED_NO_SCALEDOWN.value in codes(d)


def test_cooldown_suppression():
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"},
                         policy={"headroom": {"mode": "fixed"}, "cooldown": "15m"})
    model = make_test_model()
    shard = ShardInput("s", window(n=20, ingress_msg_rate=30000, avg_msg_size=1000))
    now = max(s.timestamp for s in shard.samples) + 1
    d = run(cfg, model, shard, now=now, last=now - 60)  # last decision 60s ago, cooldown 900s
    assert d.action is Action.hold
    assert "cooldown" in (d.reason or "")


# ---- staleness refusal (5.5 / 10) ------------------------------------------------------------

def test_stale_metrics_refuses_decision():
    cfg = default_config(metrics={"scrape_interval": "30s", "staleness_limit": "3m"})
    model = make_test_model()
    shard = ShardInput("s", window(ingress_msg_rate=30000, avg_msg_size=1000))
    latest = max(s.timestamp for s in shard.samples)
    d = run(cfg, model, shard, now=latest + 600)  # 10 min old
    assert d.action is Action.no_decision
    assert WarningCode.STALE_METRICS.value in codes(d)


# ---- 5.6 warnings ----------------------------------------------------------------------------

def test_hot_shard_warning():
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"},
                         policy={"headroom": {"mode": "fixed"}})
    model = make_test_model()
    shard = ShardInput("s", window(n=20, ingress_msg_rate=30000, avg_msg_size=1000),
                       key_subdividable=False)
    d = run(cfg, model, shard)
    assert WarningCode.HOT_SHARD.value in codes(d)


def test_byte_bound_large_message_warning():
    # avg_msg_size beyond largest bucket (10000) and bytes-binding → claim-check advice
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "bytes"},
                         policy={"headroom": {"mode": "fixed"}})
    model = make_test_model()
    shard = ShardInput("s", window(n=20, ingress_msg_rate=2000, avg_msg_size=50000))
    d = run(cfg, model, shard)
    assert WarningCode.BYTE_BOUND_LARGE_MSG.value in codes(d)
    assert WarningCode.MODEL_EXTRAPOLATION.value in codes(d)


def test_model_extrapolation_warning_small():
    cfg = default_config(workload={"delivery": "direct"}, policy={"headroom": {"mode": "fixed"}})
    model = make_test_model()
    shard = ShardInput("s", window(ingress_msg_rate=100, avg_msg_size=200))  # below min bucket 1000
    d = run(cfg, model, shard)
    assert WarningCode.MODEL_EXTRAPOLATION.value in codes(d)


def test_insufficient_window_warning():
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"},
                         policy={"headroom": {"mode": "fixed"}, "scale_up_window": "auto"})
    model = make_test_model()
    # only 3 samples spanning 60s, window needs 5*30=150s
    shard = ShardInput("s", window(n=3, ingress_msg_rate=30000, avg_msg_size=1000))
    d = run(cfg, model, shard)
    assert WarningCode.INSUFFICIENT_WINDOW.value in codes(d)
    assert d.action is Action.hold


def test_no_samples_is_no_decision():
    cfg = default_config()
    model = make_test_model()
    d = run_no_samples(cfg, model)
    assert d.action is Action.no_decision
    assert WarningCode.INSUFFICIENT_WINDOW.value in codes(d)


def run_no_samples(cfg, model):
    shard = ShardInput("s", [])
    return decide(DecisionRequest(config=cfg, model=model, shard=shard, now=1.0))


# ---- 5.7 derived headroom, both branches -----------------------------------------------------

def test_derived_headroom_lowers_threshold_and_flags_unsafe():
    # steep growth → derived safe headroom < configured → UNSAFE_HEADROOM + effective lowered
    cfg = default_config(
        workload={"delivery": "direct", "bottleneck": "messages"},
        policy={"headroom": {"mode": "derived", "messages": 0.75, "safety_factor": 1.5},
                "warm_pool": 0},
    )
    model = make_test_model()
    # 10% growth per 30s sample ≈ big per-minute growth
    shard = ShardInput("s", window(n=20, ingress_msg_rate=2000, avg_msg_size=1000,
                                   growth_per_sample=0.10))
    d = run(cfg, model, shard)
    ar = d.axes["messages"]
    assert ar.derived_threshold is not None
    assert ar.effective_threshold < ar.configured_threshold
    assert WarningCode.UNSAFE_HEADROOM.value in codes(d)
    # minutes_to_capacity was an assumption (no Phase 4 value)
    assert any("assumption" in w.message for w in d.warnings if w.code == WarningCode.UNSAFE_HEADROOM)


def test_derived_headroom_flat_load_keeps_configured():
    cfg = default_config(
        workload={"delivery": "direct", "bottleneck": "messages"},
        policy={"headroom": {"mode": "derived", "messages": 0.75}},
    )
    model = make_test_model()
    shard = ShardInput("s", window(n=20, ingress_msg_rate=2000, avg_msg_size=1000,
                                   growth_per_sample=0.0))
    d = run(cfg, model, shard)
    ar = d.axes["messages"]
    # flat load → safe_headroom == 1.0 → effective == configured (0.75)
    assert ar.derived_threshold == pytest.approx(1.0)
    assert ar.effective_threshold == pytest.approx(0.75)
    assert WarningCode.UNSAFE_HEADROOM.value not in codes(d)


def test_fixed_mode_no_derived_threshold():
    cfg = default_config(workload={"delivery": "direct"},
                         policy={"headroom": {"mode": "fixed"}})
    model = make_test_model()
    shard = ShardInput("s", window(ingress_msg_rate=2000, avg_msg_size=1000))
    d = run(cfg, model, shard)
    for ar in d.axes.values():
        assert ar.derived_threshold is None


def test_warm_pool_assumption_is_more_optimistic():
    # warm_pool>0 → minutes_to_capacity=1.0; warm_pool=0 → 12.0. Same growth → warm has higher safe.
    model = make_test_model()
    samples = window(n=20, ingress_msg_rate=2000, avg_msg_size=1000, growth_per_sample=0.02)
    cold = run(default_config(policy={"headroom": {"mode": "derived"}, "warm_pool": 0},
                              workload={"delivery": "direct", "bottleneck": "messages"}),
               model, ShardInput("s", samples))
    warm = run(default_config(policy={"headroom": {"mode": "derived"}, "warm_pool": 1},
                              workload={"delivery": "direct", "bottleneck": "messages"}),
               model, ShardInput("s", samples))
    assert warm.axes["messages"].derived_threshold >= cold.axes["messages"].derived_threshold


# ---- 5.3 mesh amplification ------------------------------------------------------------------

def test_mesh_adds_link_bytes_and_warns():
    cfg_shard = default_config(topology={"mode": "sharded"},
                               workload={"delivery": "direct", "bottleneck": "bytes"},
                               policy={"headroom": {"mode": "fixed"}})
    cfg_mesh = default_config(topology={"mode": "mesh"},
                              workload={"delivery": "direct", "bottleneck": "bytes"},
                              policy={"headroom": {"mode": "fixed"}})
    model = make_test_model()
    samples = window(n=20, ingress_msg_rate=2000, avg_msg_size=1000)
    shard_a = ShardInput("s", samples, subscribing_brokers=4)
    d_shard = run(cfg_shard, model, shard_a)
    d_mesh = run(cfg_mesh, model, shard_a)
    assert d_mesh.axes["bytes"].demand_ratio > d_shard.axes["bytes"].demand_ratio
    assert WarningCode.MESH_AMPLIFICATION.value in codes(d_mesh)


def test_synthetic_model_flag_present():
    cfg = default_config(workload={"delivery": "direct"}, policy={"headroom": {"mode": "fixed"}})
    model = make_test_model(synthetic=True)
    assert model.synthetic is True


def test_determinism_same_input_same_output():
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"})
    model = make_test_model()
    shard = ShardInput("s", window(n=20, ingress_msg_rate=30000, avg_msg_size=1000))
    d1 = run(cfg, model, shard)
    d2 = run(cfg, model, shard)
    assert d1 == d2
