"""The decision engine (§5). PURE. No I/O, no network, no clock reads, no logging.

``decide(request) -> ShardDecision`` maps observed state + policy + capacity to a target state.
Everything time-related is passed in: ``now`` (epoch seconds), the previous decision time, and the
sample timestamps. The engine never calls a clock.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from ..capacity.model import CapacityPoint, lookup
from ..capacity.schema import CapacityModel
from ..config import Config
from . import headroom as hr
from .types import (
    AXES,
    Action,
    Axis,
    AxisResult,
    MetricSample,
    ShardDecision,
    ShardInput,
    Warning,
    WarningCode,
)


@dataclass(frozen=True)
class DecisionRequest:
    """One engine invocation for one shard."""

    config: Config
    model: CapacityModel
    shard: ShardInput
    now: float  # epoch seconds, supplied by caller
    #: epoch seconds of the previous recommendation for this shard, or None
    last_decision_at: float | None = None
    #: measured minutes_to_capacity from the actuator audit log (Phase 4), or None → §5.7 default
    minutes_to_capacity: float | None = None


# ---- axis demand extractors (raw demand, pre-capacity) --------------------------------------

def _axis_raw_demand(axis: Axis, s: MetricSample, subscribing_brokers: int, mesh: bool) -> float:
    if axis is Axis.messages:
        return s.ingress_msg_rate
    if axis is Axis.bytes:
        base = s.ingress_byte_rate + s.egress_byte_rate
        if mesh:
            base += _link_bytes(s, subscribing_brokers)
        return base
    if axis is Axis.connections:
        return float(s.connection_count)
    if axis is Axis.spool:
        base = s.spool_used
        if mesh:
            # guaranteed: message spooled at each hop → add link pressure to spool too (§5.3)
            base += _link_bytes(s, subscribing_brokers)
        return base
    raise ValueError(axis)


def _link_bytes(s: MetricSample, subscribing_brokers: int) -> float:
    return s.ingress_byte_rate * max(0, subscribing_brokers - 1)


def _axis_capacity(axis: Axis, cap: CapacityPoint) -> float:
    return {
        Axis.messages: cap.msg_rate,
        Axis.bytes: cap.byte_rate,
        Axis.connections: float(cap.connections),
        Axis.spool: float(cap.spool_bytes),
    }[axis]


def _mean(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def decide(req: DecisionRequest) -> ShardDecision:
    cfg = req.config
    shard = req.shard
    warnings: list[Warning] = []
    mesh = cfg.topology.mode in ("mesh", "hybrid")

    if not shard.samples:
        return _no_decision(req, "no samples for shard", [
            Warning(WarningCode.INSUFFICIENT_WINDOW, "no metric samples provided for this shard")
        ])

    latest = max(shard.samples, key=lambda s: s.timestamp)

    # §5.5 / §10: refuse to decide on stale data.
    age = req.now - latest.timestamp
    if age > cfg.metrics.staleness_limit:
        return _no_decision(req, f"newest sample is {age:.0f}s old", [
            Warning(
                WarningCode.STALE_METRICS,
                f"newest metric sample is {age:.0f}s old, older than staleness_limit "
                f"{cfg.metrics.staleness_limit:.0f}s; refusing to decide",
            )
        ])

    avg_msg_size = _mean([s.avg_msg_size for s in shard.samples if s.avg_msg_size > 0]) or latest.avg_msg_size
    delivery = cfg.workload.delivery

    # §5.2 capacity lookup
    try:
        cap = lookup(req.model, cfg.fleet.service_class, avg_msg_size, delivery)
    except KeyError as e:
        return _no_decision(req, f"capacity lookup failed: {e}", [
            Warning(WarningCode.MODEL_EXTRAPOLATION, f"capacity model has no data: {e}")
        ])

    if cap.interpolated:
        warnings.append(Warning(
            WarningCode.INTERPOLATED_CAPACITY,
            f"capacity interpolated between measured size buckets at avg_msg_size={avg_msg_size:.0f}B",
        ))
    if cap.extrapolated:
        lo, hi = cap.measured_size_range
        warnings.append(Warning(
            WarningCode.MODEL_EXTRAPOLATION,
            f"avg_msg_size={avg_msg_size:.0f}B is outside the measured range [{lo}, {hi}]B; "
            "capacity clamped to the nearest bucket rather than extrapolated",
        ))

    # per-broker capacity is what a single broker can take; demand is fleet-wide observed load.
    # Build per-axis results.
    axis_results: dict[str, AxisResult] = {}
    minutes_to_cap = (
        req.minutes_to_capacity
        if req.minutes_to_capacity is not None
        else hr.default_minutes_to_capacity(cfg.policy.warm_pool)
    )
    minutes_is_assumption = req.minutes_to_capacity is None

    unsafe_flags: list[str] = []
    for axis_name in AXES:
        axis = Axis(axis_name)
        per_broker_cap = _axis_capacity(axis, cap)
        demand = _mean([
            _axis_raw_demand(axis, s, shard.subscribing_brokers, mesh) for s in shard.samples
        ])
        demand_ratio = demand / per_broker_cap if per_broker_cap > 0 else math.inf
        configured = getattr(cfg.policy.headroom, axis_name)

        derived_val: float | None = None
        derived_inputs: dict[str, float] = {}
        effective = configured
        if cfg.policy.headroom.mode == "derived":
            growth = hr.peak_growth_rate_per_min(
                shard.samples,
                lambda s, a=axis: _axis_raw_demand(a, s, shard.subscribing_brokers, mesh),
            )
            dh = hr.derive_headroom(
                growth, minutes_to_cap, cfg.policy.headroom.safety_factor, minutes_is_assumption
            )
            derived_val = dh.safe_headroom
            derived_inputs = {
                "peak_growth_rate_per_min": dh.peak_growth_rate_per_min,
                "minutes_to_capacity": dh.minutes_to_capacity,
                "safety_factor": dh.safety_factor,
            }
            # §5.7: never raise configured; effective = min(configured, safe_headroom)
            effective = min(configured, derived_val)
            if derived_val < configured:
                unsafe_flags.append(
                    f"{axis_name}: configured {configured:.2f} exceeds derived-safe {derived_val:.2f}"
                )

        pressure = demand_ratio / effective if effective > 0 else math.inf
        axis_results[axis_name] = AxisResult(
            axis=axis,
            demand_ratio=demand_ratio,
            effective_threshold=effective,
            configured_threshold=configured,
            derived_threshold=derived_val,
            pressure=pressure,
            derived_inputs=derived_inputs,
        )

    if unsafe_flags:
        warnings.append(Warning(
            WarningCode.UNSAFE_HEADROOM,
            "configured headroom exceeds derived-safe value on: " + "; ".join(unsafe_flags)
            + (" (minutes_to_capacity is an assumption; measured value not yet available)"
               if minutes_is_assumption else ""),
        ))

    # §5.4 binding axis: highest ratio-to-its-own-threshold (pressure).
    if cfg.workload.bottleneck == "auto":
        binding_name = max(AXES, key=lambda a: axis_results[a].pressure)
    else:
        binding_name = cfg.workload.bottleneck
    binding = axis_results[binding_name]
    binding_axis = Axis(binding_name)

    # required = ceil(total_demand_on_binding_axis / (threshold * per_broker_capacity))
    per_broker_cap = _axis_capacity(binding_axis, cap)
    total_demand = _mean([
        _axis_raw_demand(binding_axis, s, shard.subscribing_brokers, mesh) for s in shard.samples
    ])
    denom = binding.effective_threshold * per_broker_cap
    if denom <= 0:
        raw_required = cfg.fleet.max_brokers
    else:
        raw_required = math.ceil(total_demand / denom)
    required = max(cfg.fleet.min_brokers, min(cfg.fleet.max_brokers, raw_required))

    # §5.6 warnings that flag rather than silently resolve
    if raw_required > cfg.fleet.max_brokers:
        warnings.append(Warning(
            WarningCode.HIT_CEILING,
            f"required {raw_required} brokers exceeds max_brokers {cfg.fleet.max_brokers}; "
            "the tool cannot solve this workload by adding brokers within the configured ceiling",
        ))
    if required > 1 and not shard.key_subdividable:
        warnings.append(Warning(
            WarningCode.HOT_SHARD,
            f"shard {shard.shard_name!r} needs {required} brokers but its shard key cannot "
            "subdivide; adding brokers will not help. Use a different shard key or a partitioned "
            "queue",
        ))
    if binding_axis is Axis.bytes and avg_msg_size > cap.measured_size_range[1]:
        warnings.append(Warning(
            WarningCode.BYTE_BOUND_LARGE_MSG,
            f"binding axis is bytes and avg_msg_size {avg_msg_size:.0f}B exceeds the largest "
            f"measured bucket {cap.measured_size_range[1]}B; evaluate the claim-check pattern "
            "(payload to object storage, publish a reference) before adding brokers — often the "
            "cheaper fix",
        ))
    if mesh:
        warnings.append(Warning(
            WarningCode.MESH_AMPLIFICATION,
            f"mesh topology adds inter-broker link traffic (subscribing_brokers="
            f"{shard.subscribing_brokers}) to the bytes"
            + (" and spool" if delivery in ("guaranteed", "mixed") else "")
            + " axes; this cost grows with payload size",
        ))

    # §5.5 insufficient window check
    window_secs = _effective_scale_up_window(cfg, minutes_to_cap)
    span = latest.timestamp - min(s.timestamp for s in shard.samples)
    if span < window_secs:
        warnings.append(Warning(
            WarningCode.INSUFFICIENT_WINDOW,
            f"samples span {span:.0f}s but the scale-up window needs {window_secs:.0f}s; "
            "reporting the gap and recommending no change",
        ))
        return ShardDecision(
            shard_name=shard.shard_name,
            action=Action.hold,
            current_brokers=latest.current_brokers,
            recommended_brokers=latest.current_brokers,
            binding_axis=binding_axis,
            axes=axis_results,
            warnings=warnings,
            model_version=req.model.model_version,
            interpolated=cap.interpolated,
            reason="insufficient window",
            fanout_ratio=latest.fanout_ratio,
            avg_msg_size=avg_msg_size,
        )

    # decide action with hysteresis + gating (§5.5)
    binding_per_broker_cap = _axis_capacity(binding_axis, cap)
    action, reason = _gate(
        req, binding, required, latest.current_brokers, window_secs, warnings,
        binding_axis, mesh, binding_per_broker_cap,
    )

    return ShardDecision(
        shard_name=shard.shard_name,
        action=action,
        current_brokers=latest.current_brokers,
        recommended_brokers=required if action is not Action.hold else latest.current_brokers,
        binding_axis=binding_axis,
        axes=axis_results,
        warnings=warnings,
        model_version=req.model.model_version,
        interpolated=cap.interpolated,
        reason=reason,
        fanout_ratio=latest.fanout_ratio,
        avg_msg_size=avg_msg_size,
    )


def _effective_scale_up_window(cfg: Config, minutes_to_cap: float) -> float:
    if cfg.policy.scale_up_window == "auto":
        return hr.derive_scale_up_window(cfg.metrics.scrape_interval, minutes_to_cap)
    return float(cfg.policy.scale_up_window)


def _gate(
    req: DecisionRequest,
    binding: AxisResult,
    required: int,
    current: int,
    window_secs: float,
    warnings: list[Warning],
    binding_axis: Axis,
    mesh: bool,
    per_broker_cap: float,
) -> tuple[Action, str | None]:
    cfg = req.config

    # cooldown suppression (§5.5)
    if req.last_decision_at is not None:
        since = req.now - req.last_decision_at
        if since < cfg.policy.cooldown:
            return Action.hold, f"within cooldown ({since:.0f}s < {cfg.policy.cooldown:.0f}s)"

    if required > current:
        # scale up only when the binding ratio exceeded its threshold for the whole window (§5.5).
        if _held_condition(req, binding_axis, mesh, window_secs, per_broker_cap,
                           lambda ratio: ratio > binding.effective_threshold):
            return Action.scale_up, None
        return Action.hold, "up condition not held for full scale_up_window"

    if required < current:
        # §5.5 suppress all scale-down under committed billing
        if cfg.billing.model == "committed":
            warnings.append(Warning(
                WarningCode.COMMITTED_NO_SCALEDOWN,
                "billing.model=committed: scale-down suppressed. A warm pool is billed idle "
                "capacity with no offsetting saving under committed billing",
            ))
            return Action.hold, "scale-down suppressed under committed billing"
        # scale down only when below scale_down_at for the whole scale_down_window (§5.5).
        if _held_condition(req, binding_axis, mesh, cfg.policy.scale_down_window, per_broker_cap,
                           lambda ratio: ratio < cfg.policy.scale_down_at):
            return Action.scale_down, None
        return Action.hold, "down condition not held for full scale_down_window"

    return Action.hold, "at target broker count"


def _held_condition(
    req: DecisionRequest,
    axis: Axis,
    mesh: bool,
    window_secs: float,
    per_broker_cap: float,
    predicate,
) -> bool:
    """True iff ``predicate(demand_ratio)`` holds for every sample within ``window_secs`` of latest.

    ``demand_ratio`` is per-sample raw demand / per-broker capacity (capacity is constant across the
    window, so it is passed in rather than re-looked-up). If the window is not actually covered by
    samples, this returns False — the condition cannot be shown to have *held* for the full window.
    """
    samples = sorted(req.shard.samples, key=lambda s: s.timestamp)
    if not samples:
        return False
    latest_ts = samples[-1].timestamp
    window_start = latest_ts - window_secs
    windowed = [s for s in samples if s.timestamp >= window_start - 1e-9]
    if not windowed:
        return False
    # The window must be covered: the oldest windowed sample must reach back the full window.
    if (latest_ts - windowed[0].timestamp) < window_secs - 1e-9:
        return False
    for s in windowed:
        demand = _axis_raw_demand(axis, s, req.shard.subscribing_brokers, mesh)
        ratio = demand / per_broker_cap if per_broker_cap > 0 else math.inf
        if not predicate(ratio):
            return False
    return True


def _no_decision(req: DecisionRequest, reason: str, warnings: list[Warning]) -> ShardDecision:
    latest_brokers = 0
    if req.shard.samples:
        latest_brokers = max(req.shard.samples, key=lambda s: s.timestamp).current_brokers
    return ShardDecision(
        shard_name=req.shard.shard_name,
        action=Action.no_decision,
        current_brokers=latest_brokers,
        recommended_brokers=latest_brokers,
        binding_axis=None,
        axes={},
        warnings=warnings,
        model_version=req.model.model_version,
        interpolated=False,
        reason=reason,
    )
