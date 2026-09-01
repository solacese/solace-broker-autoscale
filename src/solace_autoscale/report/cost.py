"""Cost modelling (pure).

Turns broker COUNTS into money when the operator supplies a price table (``billing.per_broker_monthly``).
No pricing is shipped — the operator provides their own rates — so with no table the functions return
count-only structures and the report omits dollar figures.

Two things this answers that a raw broker count does not:
  1. What does the current vs recommended fleet cost per month, and what is the delta?
  2. Warm-pool idle cost (billed capacity with no offsetting saving under committed billing, §4).

Kept out of the decision engine (which stays pure and money-free); this is report-layer analysis.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..config import Config
from ..decision.types import ShardDecision


@dataclass(frozen=True)
class ShardCost:
    shard: str
    service_class: str
    currency: str
    per_broker_monthly: float | None
    current_brokers: int
    recommended_brokers: int
    current_monthly: float | None
    recommended_monthly: float | None
    delta_monthly: float | None

    def as_dict(self) -> dict:
        return {
            "shard": self.shard,
            "service_class": self.service_class,
            "currency": self.currency,
            "per_broker_monthly": self.per_broker_monthly,
            "current_brokers": self.current_brokers,
            "recommended_brokers": self.recommended_brokers,
            "current_monthly": self.current_monthly,
            "recommended_monthly": self.recommended_monthly,
            "delta_monthly": self.delta_monthly,
            "priced": self.per_broker_monthly is not None,
        }


def price_for(config: Config) -> float | None:
    return config.billing.per_broker_monthly.get(config.fleet.service_class)


def shard_cost(config: Config, decision: ShardDecision) -> ShardCost:
    price = price_for(config)
    cur = decision.current_brokers
    rec = decision.recommended_brokers
    cur_m = price * cur if price is not None else None
    rec_m = price * rec if price is not None else None
    delta = (rec_m - cur_m) if (cur_m is not None and rec_m is not None) else None
    return ShardCost(
        shard=decision.shard_name,
        service_class=config.fleet.service_class,
        currency=config.billing.currency,
        per_broker_monthly=price,
        current_brokers=cur,
        recommended_brokers=rec,
        current_monthly=cur_m,
        recommended_monthly=rec_m,
        delta_monthly=delta,
    )


def warm_pool_monthly(config: Config) -> float | None:
    price = price_for(config)
    if price is None or config.policy.warm_pool <= 0:
        return None
    return price * config.policy.warm_pool


def fleet_cost_summary(config: Config, decisions: list[ShardDecision]) -> dict:
    """Aggregate cost across shards + warm-pool idle cost. Count-only when unpriced."""
    per_shard = [shard_cost(config, d) for d in decisions]
    priced = price_for(config) is not None
    total_current = sum((c.current_monthly or 0) for c in per_shard) if priced else None
    total_recommended = sum((c.recommended_monthly or 0) for c in per_shard) if priced else None
    wp = warm_pool_monthly(config)
    return {
        "currency": config.billing.currency,
        "priced": priced,
        "per_shard": [c.as_dict() for c in per_shard],
        "total_current_monthly": total_current,
        "total_recommended_monthly": total_recommended,
        "total_delta_monthly": (
            (total_recommended - total_current)
            if (total_current is not None and total_recommended is not None) else None
        ),
        "warm_pool_monthly": wp,
        "warm_pool_note": (
            "warm pool is billed idle capacity with no offsetting saving under committed billing"
            if (wp is not None and config.billing.model == "committed") else None
        ),
    }
