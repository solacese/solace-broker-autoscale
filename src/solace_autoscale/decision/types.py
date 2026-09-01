"""Types for the pure decision engine (§5).

These are plain dataclasses / enums with no behaviour beyond derivation of trivially-derived fields.
The engine (engine.py) is a pure function ``decide(inputs) -> ShardDecision``. Nothing here does
I/O, reads a clock, or logs.

All time in this module is expressed as a monotonic count of seconds/minutes supplied by the caller;
the engine never calls ``time`` itself. Sample timestamps are epoch seconds passed in by the
collector.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

# The four capacity axes, in a fixed order for deterministic reporting.
AXES = ("messages", "bytes", "connections", "spool")


class Axis(StrEnum):
    messages = "messages"
    bytes = "bytes"
    connections = "connections"
    spool = "spool"


class Action(StrEnum):
    scale_up = "scale-up"
    scale_down = "scale-down"
    hold = "hold"
    no_decision = "no-decision"  # refused (stale data / insufficient window)


class WarningCode(StrEnum):
    HOT_SHARD = "hot-shard"
    HIT_CEILING = "hit-ceiling"
    BYTE_BOUND_LARGE_MSG = "byte-bound-large-msg"
    MODEL_EXTRAPOLATION = "model-extrapolation"
    INSUFFICIENT_WINDOW = "insufficient-window"
    UNSAFE_HEADROOM = "unsafe-headroom"
    STALE_METRICS = "stale-metrics"
    INTERPOLATED_CAPACITY = "interpolated-capacity"
    COMMITTED_NO_SCALEDOWN = "committed-billing-no-scaledown"
    MESH_AMPLIFICATION = "mesh-amplification"


@dataclass(frozen=True)
class Warning:
    code: WarningCode
    message: str


@dataclass(frozen=True)
class MetricSample:
    """One point of observed load for a shard (§5.1). Rates already averaged over the sample."""

    timestamp: float  # epoch seconds, supplied by the collector
    ingress_msg_rate: float
    egress_msg_rate: float
    ingress_byte_rate: float
    egress_byte_rate: float
    avg_msg_size: float
    connection_count: int
    spool_used: float  # bytes
    current_brokers: int

    @property
    def fanout_ratio(self) -> float:
        if self.ingress_msg_rate <= 0:
            return 0.0
        return self.egress_msg_rate / self.ingress_msg_rate


@dataclass(frozen=True)
class ShardInput:
    """Everything the engine needs about one shard over the evaluation window."""

    shard_name: str
    #: Chronological samples over the window, oldest first. May be short (→ insufficient window).
    samples: list[MetricSample]
    #: Whether this shard's key can subdivide further. False → hot-shard warning if required>1.
    key_subdividable: bool = True
    #: Number of brokers currently subscribing to this shard (for mesh link math). Default 1.
    subscribing_brokers: int = 1


@dataclass(frozen=True)
class AxisResult:
    axis: Axis
    demand_ratio: float  # demand / per-broker capacity (fraction), pre-threshold
    effective_threshold: float
    configured_threshold: float
    derived_threshold: float | None  # None when headroom.mode == fixed
    #: demand_ratio / effective_threshold — the value used to pick the binding axis
    pressure: float
    #: inputs that produced the derived threshold, for the report
    derived_inputs: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ShardDecision:
    """Engine output for one shard. Fully self-describing for the report and the actuator."""

    shard_name: str
    action: Action
    current_brokers: int
    recommended_brokers: int
    binding_axis: Axis | None
    axes: dict[str, AxisResult]
    warnings: list[Warning]
    model_version: str
    #: True when capacity lookup interpolated between measured buckets.
    interpolated: bool
    #: Explains a no-decision / hold when relevant (e.g. "cooldown", "window not met").
    reason: str | None = None
    fanout_ratio: float = 0.0
    avg_msg_size: float = 0.0
