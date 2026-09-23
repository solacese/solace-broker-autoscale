"""Fail-closed broker-feature compatibility before load-aware placement.

Measured capacity and migration correctness are separate evidence. A profile can quantify replay or
tracing throughput without proving that this controller can safely move the feature's broker state.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from ..config import Config
from ..metrics.fleet import FleetInventory


@dataclass(frozen=True)
class FeatureRequirements:
    """Features that can couple one ordering partition to broker-local state."""

    transactions: str = "none"
    replication: str = "none"
    replay: bool = False
    tracing: bool = False


@dataclass(frozen=True)
class CompatibilityDecision:
    allowed: bool
    reason: str


def requirements_for(config: Config, shard: str) -> FeatureRequirements:
    """Combine explicit operator requirements with features implied by the measured scenario."""
    features = config.automation.shards.get(shard)
    declared = features.features if features is not None else None
    scenario = config.capacity.scenario
    return FeatureRequirements(
        transactions=declared.transactions if declared else "none",
        replication=declared.replication if declared else "none",
        replay=(declared.replay if declared else False) or scenario == "replay",
        tracing=(declared.tracing if declared else False) or scenario == "tracing",
    )


def feature_contract(config: Config, shards: list[str], *, deployment_mode: str) -> dict[str, Any]:
    """Return the stable migration-safety contract persisted beside routing ownership."""
    return {
        "schema": "feature-placement-v1",
        "bundle_scope": "partition-with-subscriber-groups-v1",
        "cross_partition_transactions": "pinned",
        "deployment_mode": deployment_mode,
        "shards": {
            shard: asdict(requirements_for(config, shard))
            for shard in sorted(shards)
        },
    }


def contract_digest(contract: dict[str, Any]) -> str:
    """Identify the exact canonical contract without exposing environment secrets."""
    encoded = json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def placement_contract(inventory: FleetInventory) -> dict[str, Any]:
    """Persist non-secret broker capabilities and failure-domain assertions."""
    return {
        "schema": "placement-constraints-v1",
        "brokers": {
            broker.broker_id: {
                "shard": broker.shard,
                "role": broker.role,
                "capabilities": sorted(broker.capabilities),
                "failure_domains": dict(sorted(broker.failure_domains.items())),
                "fixed_load": broker.fixed_load.model_dump(mode="json"),
            }
            for broker in sorted(inventory.brokers, key=lambda item: item.broker_id)
        },
        "placement": inventory.placement.model_dump(mode="json"),
    }


def assess_migration(
    requirements: FeatureRequirements, *, deployment_mode: str, target_role: str
) -> CompatibilityDecision:
    """Admit only combinations proven safe by the existing queue handover implementation."""
    if deployment_mode != "HA":
        return CompatibilityDecision(False, "deployment-mode-unsupported")
    if target_role not in ("active", "warm"):
        return CompatibilityDecision(False, "destination-not-scaling-capacity")
    if requirements.transactions != "none":
        return CompatibilityDecision(False, "transaction-boundary-unverified")
    if requirements.replication != "none":
        return CompatibilityDecision(False, "replication-migration-unverified")
    if requirements.replay and requirements.tracing:
        return CompatibilityDecision(False, "unsupported-feature-combination")
    if requirements.replay:
        return CompatibilityDecision(False, "replay-state-migration-unverified")
    if requirements.tracing:
        return CompatibilityDecision(False, "tracing-migration-unverified")
    return CompatibilityDecision(True, "supported")
