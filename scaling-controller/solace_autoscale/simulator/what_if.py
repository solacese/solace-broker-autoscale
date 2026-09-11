"""What-if projection (pure): how many brokers would this shard need if load multiplied?

Re-runs the SAME pure decision engine with each sample's rates scaled by a multiplier. No new
capacity maths - it reuses ``decide`` - so a projection can never disagree with a real
recommendation at multiplier 1.0. Answers "what happens if messages double / quadruple?" which is
the question that follows every capacity recommendation.

Scales the rate axes (messages, bytes) and spool; connection_count is left unscaled by default
because connections rarely scale with message volume (configurable via ``scale_connections``).
"""

from __future__ import annotations

from dataclasses import dataclass

from ..capacity.schema import CapacityModel
from ..config import Config
from ..decision.engine import DecisionRequest, decide
from ..decision.types import MetricSample, ShardInput


@dataclass(frozen=True)
class Projection:
    multiplier: float
    recommended_brokers: int
    binding_axis: str | None
    action: str
    hit_ceiling: bool


def _scale_sample(s: MetricSample, m: float, scale_connections: bool) -> MetricSample:
    return MetricSample(
        timestamp=s.timestamp,
        ingress_msg_rate=s.ingress_msg_rate * m,
        egress_msg_rate=s.egress_msg_rate * m,
        ingress_byte_rate=s.ingress_byte_rate * m,
        egress_byte_rate=s.egress_byte_rate * m,
        avg_msg_size=s.avg_msg_size,  # size unchanged; volume scales
        connection_count=int(s.connection_count * m) if scale_connections else s.connection_count,
        spool_used=s.spool_used * m,
        current_brokers=s.current_brokers,
    )


def project(
    config: Config,
    model: CapacityModel,
    shard: ShardInput,
    now: float,
    multipliers: tuple[float, ...] = (1.0, 2.0, 4.0),
    scale_connections: bool = False,
) -> list[Projection]:
    out: list[Projection] = []
    for m in multipliers:
        scaled = ShardInput(
            shard_name=shard.shard_name,
            samples=[_scale_sample(s, m, scale_connections) for s in shard.samples],
            key_subdividable=shard.key_subdividable,
            subscribing_brokers=shard.subscribing_brokers,
        )
        d = decide(DecisionRequest(config=config, model=model, shard=scaled, now=now))
        hit = any(w.code.value == "hit-ceiling" for w in d.warnings)
        out.append(Projection(
            multiplier=m,
            recommended_brokers=d.recommended_brokers,
            binding_axis=d.binding_axis.value if d.binding_axis else None,
            action=d.action.value,
            hit_ceiling=hit,
        ))
    return out
