"""Warm pool tests (§10): provisioning-duration feedback into minutes_to_capacity + cost note."""

from __future__ import annotations

from solace_autoscale.actuator.warm_pool import WarmPool


def test_shortfall():
    wp = WarmPool(target=2)
    assert wp.shortfall(current_idle=0) == 2
    assert wp.shortfall(current_idle=1) == 1
    assert wp.shortfall(current_idle=2) == 0
    assert wp.shortfall(current_idle=3) == 0


def test_observed_minutes_to_capacity_none_without_samples():
    assert WarmPool(target=1).observed_minutes_to_capacity() is None


def test_observed_minutes_to_capacity_median():
    wp = WarmPool(target=1)
    wp.record_provisioning(ts=1, duration_seconds=60)   # 1 min
    wp.record_provisioning(ts=2, duration_seconds=120)  # 2 min
    wp.record_provisioning(ts=3, duration_seconds=180)  # 3 min
    assert wp.observed_minutes_to_capacity() == 2.0  # median of 1,2,3 min


def test_cost_note():
    wp = WarmPool(target=2)
    note = wp.cost_note(per_broker_monthly=1000.0)
    assert note["warm_pool_target"] == 2
    assert note["billed_idle"] is True
    assert note["estimated_monthly_cost"] == 2000.0


def test_cost_note_zero_target_not_billed():
    note = WarmPool(target=0).cost_note()
    assert note["billed_idle"] is False
