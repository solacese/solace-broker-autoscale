"""Regressions for realistic scrape timing, fleet load and incomplete telemetry."""
from dataclasses import replace

import pytest

from solace_autoscale.decision.engine import DecisionRequest, decide
from solace_autoscale.decision.types import Action, ShardInput

from .conftest import default_config, make_test_model, window


def evaluate(samples, **overrides):
    cfg = default_config(
        workload={"delivery": "direct", "bottleneck": "messages"},
        policy={"headroom": {"mode": "fixed"}, "scale_up_window": 90, "scale_down_window": 300},
        **overrides,
    )
    return decide(DecisionRequest(cfg, make_test_model(), ShardInput("orders", samples),
                                  now=max(s.timestamp for s in samples)))


def test_scale_up_accepts_jitter_without_exact_window_boundary():
    samples = window(n=5, cadence=30.2, ingress_msg_rate=9000, egress_msg_rate=0)
    assert evaluate(samples).action == Action.scale_up


def test_large_gap_does_not_prove_sustained_load():
    samples = window(n=4, cadence=30, ingress_msg_rate=9000, egress_msg_rate=0)
    assert evaluate([samples[0], samples[-1]]).action == Action.hold


def test_old_idle_history_does_not_hide_current_overload():
    old = window(n=100, ingress_msg_rate=10, egress_msg_rate=0)
    recent = window(n=5, t0=old[-1].timestamp + 30, ingress_msg_rate=9000, egress_msg_rate=0)
    decision = evaluate(old + recent)
    assert decision.action == Action.scale_up
    assert decision.recommended_brokers == 2


def test_scale_up_requires_whole_fleet_overload_for_whole_window():
    samples = window(n=5, ingress_msg_rate=12000, egress_msg_rate=0, current_brokers=2)
    samples[-1] = replace(samples[-1], ingress_msg_rate=30000, ingress_byte_rate=30000000)
    assert evaluate(samples).action == Action.hold


def test_scale_down_uses_per_broker_utilization():
    # 6,000 / (4 * 10,000) = 15%; one broker fits below the 75% target.
    samples = window(n=12, ingress_msg_rate=6000, egress_msg_rate=0, current_brokers=4)
    decision = evaluate(samples)
    assert decision.action == Action.scale_down
    assert decision.recommended_brokers == 1


def test_scale_down_checks_other_axes_even_with_forced_bottleneck():
    samples = window(n=12, ingress_msg_rate=100, egress_msg_rate=0, current_brokers=4,
                     connection_count=35000)
    assert evaluate(samples).action == Action.hold


@pytest.mark.parametrize('field,value', [('ingress_msg_rate', float('nan')),
                                        ('spool_used', -1), ('current_brokers', 0)])
def test_invalid_metrics_refuse_decision(field, value):
    samples = window(n=5)
    samples[-1] = replace(samples[-1], **{field: value})
    assert evaluate(samples).action == Action.no_decision


def test_mesh_does_not_add_bytes_per_second_to_stored_bytes():
    samples = window(n=5, ingress_msg_rate=100, egress_msg_rate=0, spool_used=1000)
    cfg = default_config(topology={"mode": "mesh"}, workload={"delivery": "guaranteed"},
                         policy={"headroom": {"mode": "fixed"}, "scale_up_window": 90})
    d = decide(DecisionRequest(cfg, make_test_model(), ShardInput("s", samples, subscribing_brokers=4),
                               now=samples[-1].timestamp))
    assert d.axes['spool'].demand_ratio == pytest.approx(1000 / 500_000_000_000)


def test_exact_interior_bucket_is_not_reported_as_interpolated(synthetic_model):
    from solace_autoscale.capacity.model import lookup
    point = lookup(synthetic_model, 'enterprise-250', 1000, 'direct')
    assert point.msg_rate == 10000
    assert not point.interpolated
