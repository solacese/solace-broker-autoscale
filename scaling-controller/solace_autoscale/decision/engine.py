"""The decision engine (§5). PURE. No I/O, no network, no clock reads, no logging.

``decide(request) -> ShardDecision`` maps observed state + policy + capacity to a target state.
Everything time-related is passed in: ``now`` (epoch seconds), the previous decision time, and the
sample timestamps. The engine never calls a clock.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace

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

def _axis_raw_demand(axis: Axis, s: MetricSample, subscribing_brokers: int, mesh: bool,
                     cap: CapacityPoint | None = None) -> float:
    if axis is Axis.messages:
        return s.ingress_msg_rate
    if axis is Axis.bytes:
        if cap is not None and cap.ingress_byte_rate is not None and cap.egress_byte_rate is not None:
            egress = s.egress_byte_rate + (_link_bytes(s, subscribing_brokers) if mesh else 0)
            # Keep direction constraints independent; spare ingress cannot pay for saturated egress.
            return max(s.ingress_byte_rate / cap.ingress_byte_rate,
                       egress / cap.egress_byte_rate) * cap.byte_rate
        base = s.ingress_byte_rate + s.egress_byte_rate
        if mesh:
            base += _link_bytes(s, subscribing_brokers)
        return base
    if axis is Axis.connections:
        return float(s.connection_count)
    if axis is Axis.spool:
        # Stored bytes already include observed retained copies. A byte RATE cannot be
        # added to a byte COUNT without a retention-time model.
        return s.spool_used
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

    numeric_fields = (
        "timestamp", "ingress_msg_rate", "egress_msg_rate", "ingress_byte_rate",
        "egress_byte_rate", "avg_msg_size", "connection_count", "spool_used", "current_brokers",
    )
    if (not math.isfinite(req.now) or any(
        any(not math.isfinite(getattr(s, f)) or getattr(s, f) < 0 for f in numeric_fields)
        or s.current_brokers < 1 or s.timestamp > req.now
        for s in shard.samples
    )):
        return _no_decision(req, "invalid metric values or future timestamps", [
            Warning(WarningCode.INVALID_METRICS, "metrics must be finite, nonnegative, "
                    "not future-dated, and describe at least one broker")
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

    minutes_to_cap = (
        req.minutes_to_capacity if req.minutes_to_capacity is not None
        else hr.default_minutes_to_capacity(cfg.policy.warm_pool)
    )
    window_secs = _effective_scale_up_window(cfg, minutes_to_cap)
    recent = _window_samples(shard.samples, window_secs)
    avg_msg_size = _mean([s.avg_msg_size for s in recent if s.avg_msg_size > 0]) or latest.avg_msg_size
    if req.model.schema_version == "2" and avg_msg_size <= 0:
        avg_msg_size = float(cfg.capacity.message_size_hint or 0)
    delivery = cfg.workload.delivery

    # §5.2 capacity lookup
    try:
        cap = lookup(req.model, cfg.fleet.service_class, avg_msg_size, delivery,
                     fanout=max(cfg.capacity.fanout, max(s.fanout_ratio for s in recent)),
                     scenario=cfg.capacity.scenario)
        # Validate the full recent workload, not just an average that could hide an unsupported size.
        if req.model.schema_version == "2":
            sample_caps = [cap]
            for sample in recent:
                idle = not (sample.ingress_msg_rate or sample.egress_msg_rate or
                            sample.ingress_byte_rate or sample.egress_byte_rate)
                sample_caps.append(lookup(
                    req.model, cfg.fleet.service_class,
                    avg_msg_size if idle else sample.avg_msg_size, delivery,
                    fanout=max(cfg.capacity.fanout, sample.fanout_ratio), scenario=cfg.capacity.scenario,
                ))
            # The mean size can hide a slower large-message interval. Preserve the most
            # conservative observed direction and message limits throughout this decision window.
            cap = replace(
                cap, msg_rate=min(c.msg_rate for c in sample_caps),
                byte_rate=min(c.byte_rate for c in sample_caps),
                ingress_byte_rate=min(c.ingress_byte_rate for c in sample_caps
                                      if c.ingress_byte_rate is not None),
                egress_byte_rate=min(c.egress_byte_rate for c in sample_caps
                                     if c.egress_byte_rate is not None),
                interpolated=any(c.interpolated for c in sample_caps),
                source_cells=sorted({ref for c in sample_caps for ref in c.source_cells}),
            )
    except (KeyError, ValueError) as e:
        return _no_decision(req, f"capacity lookup failed: {e}", [
            Warning(WarningCode.MODEL_EXTRAPOLATION, f"capacity model has no data: {e}")
        ])

    if cap.interpolated:
        warnings.append(Warning(
            WarningCode.INTERPOLATED_CAPACITY,
            f"capacity estimated between measured size/fanout points at avg_msg_size={avg_msg_size:.0f}B",
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
    minutes_is_assumption = req.minutes_to_capacity is None

    unsafe_flags: list[str] = []
    for axis_name in AXES:
        axis = Axis(axis_name)
        per_broker_cap = _axis_capacity(axis, cap)
        demand = _mean([
            _axis_raw_demand(axis, s, shard.subscribing_brokers, mesh, cap) for s in recent
        ])
        demand_ratio = demand / per_broker_cap if per_broker_cap > 0 else math.inf
        configured = getattr(cfg.policy.headroom, axis_name)

        derived_val: float | None = None
        derived_inputs: dict[str, float] = {}
        effective = configured
        if cfg.policy.headroom.mode == "derived":
            def _axis_demand(s: MetricSample, a: Axis = axis) -> float:
                return _axis_raw_demand(a, s, shard.subscribing_brokers, mesh, cap)

            growth = hr.peak_growth_rate_per_min(recent, _axis_demand)
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
        _axis_raw_demand(binding_axis, s, shard.subscribing_brokers, mesh, cap) for s in recent
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
            "(payload to object storage, publish a reference) before adding brokers - often the "
            "cheaper fix",
        ))
    if mesh:
        warnings.append(Warning(
            WarningCode.MESH_AMPLIFICATION,
            f"mesh topology adds inter-broker link traffic (subscribing_brokers="
            f"{shard.subscribing_brokers}) to the bytes"
            + " axis; retained spool copies must be included in observed spool_used",
        ))

    # §5.5 insufficient window check
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
            capacity_source_cells=cap.source_cells,
        )

    # decide action with hysteresis + gating (§5.5)
    binding_per_broker_cap = _axis_capacity(binding_axis, cap)
    action, reason = _gate(
        req, binding, required, latest.current_brokers, window_secs, warnings,
        binding_axis, mesh, binding_per_broker_cap, cap, axis_results,
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
        capacity_source_cells=cap.source_cells,
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
    cap: CapacityPoint,
    axis_results: dict[str, AxisResult],
) -> tuple[Action, str | None]:
    cfg = req.config

    # cooldown suppression (§5.5)
    if req.last_decision_at is not None:
        since = req.now - req.last_decision_at
        if since < cfg.policy.cooldown:
            return Action.hold, f"within cooldown ({since:.0f}s < {cfg.policy.cooldown:.0f}s)"

    if required > current:
        if not req.shard.key_subdividable:
            return Action.hold, "hot shard cannot subdivide; adding brokers cannot relieve its load"
        # scale up only when the binding ratio exceeded its threshold for the whole window (§5.5).
        samples = _covered_window(req, window_secs)
        if samples and all(
            s.current_brokers == current and
            _axis_raw_demand(binding_axis, s, req.shard.subscribing_brokers, mesh, cap)
            / (per_broker_cap * current) > binding.effective_threshold
            for s in samples
        ):
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
        samples = _covered_window(req, cfg.policy.scale_down_window)
        # Every axis must stay low on the current fleet AND fit on the proposed fleet.
        # Also check capacity at each observed message size, not only the current average.
        if samples and all(s.current_brokers == current for s in samples):
            for s in samples:
                try:
                    sample_cap = lookup(req.model, cfg.fleet.service_class,
                                        s.avg_msg_size or cfg.capacity.message_size_hint or 0,
                                        cfg.workload.delivery,
                                        fanout=max(cfg.capacity.fanout, s.fanout_ratio),
                                        scenario=cfg.capacity.scenario)
                except (KeyError, ValueError):
                    return Action.hold, "down window contains workload outside measured benchmark"
                for axis_name in AXES:
                    axis = Axis(axis_name)
                    capacity = min(_axis_capacity(axis, cap), _axis_capacity(axis, sample_cap))
                    demand = _axis_raw_demand(axis, s, req.shard.subscribing_brokers, mesh, cap)
                    if axis is Axis.bytes and sample_cap.ingress_byte_rate is not None:
                        sample_ratio = _axis_raw_demand(axis, s, req.shard.subscribing_brokers,
                                                        mesh, sample_cap) / sample_cap.byte_rate
                        demand = max(demand, sample_ratio * capacity)
                    if (demand / (capacity * current) >= cfg.policy.scale_down_at or
                            demand / (capacity * required) > axis_results[axis_name].effective_threshold):
                        return Action.hold, "down condition not held for all axes on current and target fleet"
            return Action.scale_down, None
        return Action.hold, "down condition not held for full scale_down_window"

    return Action.hold, "at target broker count"


def _window_samples(samples: list[MetricSample], window_secs: float) -> list[MetricSample]:
    """Recent samples plus the boundary observation immediately before the window.

    Scrapes need not land exactly on the window boundary. Keeping the preceding observation
    brackets it conservatively, without manufacturing historical samples.
    """
    ordered = sorted(samples, key=lambda s: s.timestamp)
    if not ordered:
        return []
    start = ordered[-1].timestamp - window_secs
    before = [s for s in ordered if s.timestamp <= start]
    return before[-1:] + [s for s in ordered if s.timestamp > start]


def _covered_window(req: DecisionRequest, seconds: float) -> list[MetricSample]:
    """Require coverage, distinct timestamps and no gap exceeding two scrape intervals."""
    samples = _window_samples(req.shard.samples, seconds)
    if not samples or samples[-1].timestamp - samples[0].timestamp < seconds - 1e-9:
        return []
    max_gap = 2 * req.config.metrics.scrape_interval
    if any(not 0 < b.timestamp - a.timestamp <= max_gap for a, b in zip(samples, samples[1:], strict=False)):
        return []
    return samples


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
