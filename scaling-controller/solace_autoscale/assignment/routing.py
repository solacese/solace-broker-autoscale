"""Stable key partitioning and weighted rendezvous hashing. No per-message round robin."""
from __future__ import annotations

import hashlib
import json
import math

from .store import Broker


def partition_for(shard: str, routing_key: str, count: int) -> int:
    """Same UTF-8 shard/key always yields the same partition for a fixed partition count."""
    if not shard or not routing_key or count < 1:
        raise ValueError("shard, routing_key and positive partition count are required")
    payload = json.dumps([shard, routing_key], ensure_ascii=False, separators=(",", ":")).encode()
    return int.from_bytes(hashlib.sha256(payload).digest(), "big") % count


def rendezvous_broker(candidates: list[Broker], shard: str, key: str,
                      weights: dict[str, float]) -> Broker:
    """Choose a stable owner; adding a candidate changes only keys that prefer that candidate.

    Exponential-race weighted rendezvous: -log(U)/weight. Weights represent relative capacity.
    This is used for NEW placements; durable placements always override a new hash winner.
    """
    def score(broker: Broker) -> tuple[float, str]:
        weight = weights.get(broker.broker_id, 1.0)
        if not math.isfinite(weight) or weight <= 0:
            raise ValueError("broker weights must be finite and positive")
        payload = json.dumps([shard, key, broker.broker_id], separators=(",", ":")).encode()
        value = int.from_bytes(hashlib.sha256(payload).digest()[:6], "big")
        uniform = (value + 1) / (2**48 + 1)
        return (-math.log(uniform) / weight, broker.broker_id)
    if not candidates:
        raise ValueError("rendezvous requires eligible brokers")
    return min(candidates, key=score)
