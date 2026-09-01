"""JSON report (§11): machine consumption and the actuator's input.

One data structure; markdown.py renders the same content for humans. Carries model version and a
synthetic banner where applicable — a recommendation without provenance is not reproducible.
"""

from __future__ import annotations

from typing import Any

from ..capacity.schema import CapacityModel
from ..config import Config
from ..decision.types import ShardDecision


def decision_to_dict(d: ShardDecision) -> dict[str, Any]:
    return {
        "shard": d.shard_name,
        "action": d.action.value,
        "current_brokers": d.current_brokers,
        "recommended_brokers": d.recommended_brokers,
        "binding_axis": d.binding_axis.value if d.binding_axis else None,
        "reason": d.reason,
        "fanout_ratio": round(d.fanout_ratio, 4),
        "avg_msg_size_bytes": round(d.avg_msg_size, 2),
        "interpolated": d.interpolated,
        "axes": {
            name: {
                "demand_ratio": round(ar.demand_ratio, 4),
                "effective_threshold": round(ar.effective_threshold, 4),
                "configured_threshold": round(ar.configured_threshold, 4),
                "derived_threshold": (round(ar.derived_threshold, 4)
                                      if ar.derived_threshold is not None else None),
                "pressure": round(ar.pressure, 4),
                "derived_inputs": {k: round(v, 6) for k, v in ar.derived_inputs.items()},
            }
            for name, ar in d.axes.items()
        },
        "warnings": [{"code": w.code.value, "message": w.message} for w in d.warnings],
        "model_version": d.model_version,
    }


def warm_pool_cost_note(config: Config) -> dict[str, Any] | None:
    if config.policy.warm_pool <= 0:
        return None
    committed = config.billing.model == "committed"
    return {
        "warm_pool_brokers": config.policy.warm_pool,
        "billed_idle": True,
        "note": (
            f"{config.policy.warm_pool} warm broker(s) are pre-provisioned and billed as idle "
            "capacity. "
            + ("Under committed billing there is no offsetting saving from scaling down."
               if committed else "")
        ),
    }


def build_report(
    config: Config,
    model: CapacityModel,
    decisions: list[ShardDecision],
) -> dict[str, Any]:
    return {
        "model_version": model.model_version,
        "synthetic_model": model.synthetic,
        "synthetic_warning": model.warning if model.synthetic else None,
        "provenance": model.provenance.model_dump(),
        "config_hash": config.config_hash(),
        "billing_model": config.billing.model,
        "topology_mode": config.topology.mode,
        "warm_pool_cost": warm_pool_cost_note(config),
        "shards": [decision_to_dict(d) for d in decisions],
    }
