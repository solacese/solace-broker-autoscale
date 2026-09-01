"""Derived headroom and derived window (§5.7, §5.8). Pure functions.

ADR 0006: a threshold is not a preference; it must leave room to absorb load growth that occurs
while capacity is being added.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .types import MetricSample

# §5.7 default before Phase 4 supplies a measured value.
DEFAULT_MINUTES_TO_CAPACITY_WARM = 1.0
DEFAULT_MINUTES_TO_CAPACITY_COLD = 12.0


def default_minutes_to_capacity(warm_pool: int) -> float:
    """§5.7: warm pool present → 1.0 min, else 12.0 min (labelled an assumption by callers)."""
    return DEFAULT_MINUTES_TO_CAPACITY_WARM if warm_pool > 0 else DEFAULT_MINUTES_TO_CAPACITY_COLD


def _percentile(sorted_vals: list[float], p: float) -> float:
    """Linear-interpolation percentile (p in [0,1]) over an already-sorted list."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = p * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def peak_growth_rate_per_min(
    samples: list[MetricSample],
    axis_value: Callable[[MetricSample], float],
) -> float:
    """95th percentile of one-minute-over-one-minute fractional growth in the binding axis (§5.7).

    ``axis_value(sample)`` extracts the axis's raw demand from a sample. Growth is measured between
    samples ~60s apart. Returns a per-minute fractional rate (e.g. 0.05 = 5%/min). Non-positive
    growth contributes 0.
    """
    if len(samples) < 2:
        return 0.0
    ordered = sorted(samples, key=lambda s: s.timestamp)
    growths: list[float] = []
    for i, later in enumerate(ordered):
        # find the sample closest to 60s before `later`
        target = later.timestamp - 60.0
        best = None
        best_dt = None
        for earlier in ordered[:i]:
            dt = abs(earlier.timestamp - target)
            if best_dt is None or dt < best_dt:
                best, best_dt = earlier, dt
        if best is None:
            continue
        span_min = (later.timestamp - best.timestamp) / 60.0
        if span_min <= 0:
            continue
        v0 = axis_value(best)
        v1 = axis_value(later)
        if v0 <= 0:
            continue
        frac_growth = (v1 - v0) / v0
        per_min = frac_growth / span_min
        growths.append(max(0.0, per_min))
    if not growths:
        return 0.0
    growths.sort()
    return _percentile(growths, 0.95)


@dataclass(frozen=True)
class DerivedHeadroom:
    safe_headroom: float
    peak_growth_rate_per_min: float
    minutes_to_capacity: float
    safety_factor: float
    minutes_to_capacity_is_assumption: bool


def derive_headroom(
    peak_growth: float,
    minutes_to_capacity: float,
    safety_factor: float,
    minutes_is_assumption: bool,
) -> DerivedHeadroom:
    """safe_headroom = 1 - (peak_growth * minutes_to_capacity * safety_factor), clamped to (0,1]."""
    raw = 1.0 - (peak_growth * minutes_to_capacity * safety_factor)
    safe = min(1.0, max(0.01, raw))
    return DerivedHeadroom(
        safe_headroom=safe,
        peak_growth_rate_per_min=peak_growth,
        minutes_to_capacity=minutes_to_capacity,
        safety_factor=safety_factor,
        minutes_to_capacity_is_assumption=minutes_is_assumption,
    )


def derive_scale_up_window(scrape_interval: float, minutes_to_capacity: float) -> float:
    """§5.8: scale_up_window = max(5 * scrape_interval, minutes_to_capacity). Seconds in, seconds out."""
    return max(5.0 * scrape_interval, minutes_to_capacity * 60.0)
