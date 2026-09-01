"""Drain state machine tests (§10, §13): including the stall→STUCK path that never auto-deletes."""

from __future__ import annotations

import pytest

from solace_autoscale.actuator.drain import DrainMachine, DrainObservation, DrainState


def _m(settle=60.0, stall=600.0):
    return DrainMachine(broker_id="b0", settle_seconds=settle, stall_timeout_seconds=stall)


def test_happy_path_active_to_gone():
    m = _m(settle=60, stall=600)
    assert m.state is DrainState.ACTIVE
    m.begin_drain(now=0)
    assert m.state is DrainState.DRAINING
    # empty at t=10, stays empty; settle 60s → DRAINED at t>=70
    m.observe(DrainObservation(ts=10, queue_depth=0, bound_consumers=0))
    assert m.state is DrainState.DRAINING  # settle not met
    m.observe(DrainObservation(ts=80, queue_depth=0, bound_consumers=0))
    assert m.state is DrainState.DRAINED
    m.mark_deleting(now=90)
    assert m.state is DrainState.DELETING
    m.mark_gone(now=100)
    assert m.state is DrainState.GONE


def test_settle_resets_when_refilled():
    m = _m(settle=60, stall=100000)
    m.begin_drain(now=0)
    m.observe(DrainObservation(ts=10, queue_depth=0, bound_consumers=0))  # empty starts
    m.observe(DrainObservation(ts=40, queue_depth=5, bound_consumers=0))  # refilled → reset
    m.observe(DrainObservation(ts=80, queue_depth=0, bound_consumers=0))  # empty again from t=80
    assert m.state is DrainState.DRAINING  # only 0s settled since 80
    m.observe(DrainObservation(ts=145, queue_depth=0, bound_consumers=0))  # 65s settled
    assert m.state is DrainState.DRAINED


def test_drained_refill_falls_back_to_draining():
    m = _m(settle=30, stall=100000)
    m.begin_drain(now=0)
    m.observe(DrainObservation(ts=10, queue_depth=0, bound_consumers=0))
    m.observe(DrainObservation(ts=50, queue_depth=0, bound_consumers=0))  # DRAINED
    assert m.state is DrainState.DRAINED
    m.observe(DrainObservation(ts=60, queue_depth=0, bound_consumers=2))  # consumer bound → refill
    assert m.state is DrainState.DRAINING


def test_bound_consumers_block_drained():
    m = _m(settle=30, stall=100000)
    m.begin_drain(now=0)
    # queue empty but a consumer is still bound → NOT empty → never DRAINED
    for t in range(10, 200, 10):
        m.observe(DrainObservation(ts=t, queue_depth=0, bound_consumers=1))
    assert m.state is DrainState.DRAINING


def test_stall_goes_to_stuck():
    m = _m(settle=60, stall=300)
    m.begin_drain(now=0)
    # never empties; at t>=300 since draining_since → STUCK
    m.observe(DrainObservation(ts=100, queue_depth=100, bound_consumers=1))
    assert m.state is DrainState.DRAINING
    m.observe(DrainObservation(ts=350, queue_depth=100, bound_consumers=1))
    assert m.state is DrainState.STUCK
    assert m.requires_operator()


def test_stuck_never_auto_deletes():
    m = _m(settle=60, stall=300)
    m.begin_drain(now=0)
    m.observe(DrainObservation(ts=350, queue_depth=100, bound_consumers=1))
    assert m.state is DrainState.STUCK
    # mark_deleting must refuse from STUCK
    with pytest.raises(ValueError, match="only DRAINED may delete"):
        m.mark_deleting(now=400)


def test_cannot_delete_from_draining():
    m = _m()
    m.begin_drain(now=0)
    with pytest.raises(ValueError, match="only DRAINED may delete"):
        m.mark_deleting(now=10)


def test_large_message_slow_drain_is_not_stuck_if_progressing():
    # A long but progressing drain (still within stall timeout) stays DRAINING, not STUCK.
    m = _m(settle=60, stall=3600)
    m.begin_drain(now=0)
    m.observe(DrainObservation(ts=1800, queue_depth=10, bound_consumers=0, spooled_bytes=5_000_000))
    assert m.state is DrainState.DRAINING  # slow but not stuck (correct behaviour, §10)
