"""Complete, concurrent SEMP snapshots of an explicitly inventoried homogeneous HA fleet.

One inventory entry is one active service endpoint, not an HA standby. A failed endpoint
invalidates the entire snapshot: losing a broker must never look like disappearing demand.
"""
from __future__ import annotations

import os
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..capacity.schema import CapacityModel
from ..decision.types import MetricSample
from .base import CollectorError
from .semp import SempCollector

_LABEL = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"


class NormalizedLoad(BaseModel):
    """Operator-attributed broker load that cannot safely be assigned to one partition."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    messages: float = Field(default=0, ge=0)
    bytes: float = Field(default=0, ge=0)
    connections: float = Field(default=0, ge=0)
    spool: float = Field(default=0, ge=0)


class BrokerEndpoint(BaseModel):
    """Location and environment references for scaling capacity or a named DR peer."""

    model_config = ConfigDict(extra="forbid")
    broker_id: str = Field(min_length=1)
    shard: str = Field(min_length=1)
    msg_vpn: str = Field(min_length=1)
    base_url: str | None = None
    username_env: str | None = None
    password_env: str | None = None
    endpoints: dict[str, str] = Field(default_factory=dict)
    role: Literal["active", "warm", "dr"] = "active"
    capabilities: frozenset[str] = Field(default_factory=frozenset)
    failure_domains: dict[str, str] = Field(default_factory=dict)
    fixed_load: NormalizedLoad = Field(default_factory=NormalizedLoad)

    @field_validator("capabilities")
    @classmethod
    def valid_capabilities(cls, values: frozenset[str]) -> frozenset[str]:
        if any(not re.fullmatch(_LABEL, value) for value in values):
            raise ValueError("capabilities must use stable nonempty identifiers")
        return values

    @field_validator("failure_domains")
    @classmethod
    def valid_failure_domains(cls, values: dict[str, str]) -> dict[str, str]:
        if any(not re.fullmatch(_LABEL, key) or not re.fullmatch(_LABEL, value)
               for key, value in values.items()):
            raise ValueError("failure-domain keys and values must use stable nonempty identifiers")
        return values

    @model_validator(mode="after")
    def validate_url(self) -> BrokerEndpoint:
        """Require complete connection metadata for active/warm scaling capacity."""
        if self.role == "dr" and self.base_url is None:
            if self.username_env or self.password_env or self.endpoints:
                raise ValueError("a DR endpoint must provide complete connection metadata or none")
            return self
        if self.role == "dr" and (not self.username_env or not self.password_env):
            raise ValueError("a connected DR endpoint needs both credential references")
        if not self.base_url or not self.username_env or not self.password_env:
            raise ValueError("active/warm brokers require SEMP URL and credential references")
        url = urlsplit(self.base_url)
        if (url.scheme not in ("http", "https") or not url.hostname or url.username or url.password
                or url.query or url.fragment or url.path not in ("", "/")):
            raise ValueError("base_url must be an HTTP(S) origin without credentials, path or query")
        if url.scheme != "https" and url.hostname not in ("localhost", "127.0.0.1", "::1"):
            raise ValueError("non-loopback SEMP endpoints require HTTPS")
        return self


class ShardPlacementConstraints(BaseModel):
    """Operator-owned hard constraints and soft spread preferences for one shard."""

    model_config = ConfigDict(extra="forbid")
    required_capabilities: frozenset[str] = Field(default_factory=frozenset)
    allowed_failure_domains: dict[str, frozenset[str]] = Field(default_factory=dict)
    max_partitions_per_broker: int | None = Field(default=None, ge=1)
    max_partitions_per_domain: dict[str, int] = Field(default_factory=dict)
    spread_by: tuple[str, ...] = ()
    pinned_partitions: frozenset[int] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def valid_constraints(self) -> ShardPlacementConstraints:
        names = [
            *self.required_capabilities,
            *self.allowed_failure_domains,
            *(value for values in self.allowed_failure_domains.values() for value in values),
            *self.spread_by,
            *self.max_partitions_per_domain,
        ]
        if any(not re.fullmatch(_LABEL, value) for value in names):
            raise ValueError("placement capabilities and domain keys must use stable identifiers")
        if any(not values for values in self.allowed_failure_domains.values()):
            raise ValueError("allowed failure-domain sets must be nonempty")
        if any(value < 1 for value in self.max_partitions_per_domain.values()):
            raise ValueError("failure-domain partition limits must be positive")
        if any(partition < 0 for partition in self.pinned_partitions):
            raise ValueError("pinned partitions must be nonnegative")
        return self


class InventoryPlacement(BaseModel):
    model_config = ConfigDict(extra="forbid")
    shards: dict[str, ShardPlacementConstraints] = Field(default_factory=dict)


class FleetInventory(BaseModel):
    """Operator-declared environment; a single tier/provider/generation per monitor."""

    model_config = ConfigDict(extra="forbid")
    provider: Literal["aws", "azure", "gcp"]
    broker_version: str
    service_class: str
    deployment_mode: Literal["HA"] = "HA"
    brokers: list[BrokerEndpoint] = Field(min_length=1, max_length=256)
    placement: InventoryPlacement = Field(default_factory=InventoryPlacement)

    @model_validator(mode="after")
    def unique_services(self) -> FleetInventory:
        """Prevent duplicate counting and reject constraints with incomplete topology facts."""
        ids = [b.broker_id for b in self.brokers]
        urls = [b.base_url.rstrip("/").lower() for b in self.brokers if b.base_url]
        if len(set(ids)) != len(ids) or len(set(urls)) != len(urls):
            raise ValueError("each broker id and service endpoint must appear exactly once")
        scaling_shards = {b.shard for b in self.brokers if b.role in ("active", "warm")}
        if set(self.placement.shards) - scaling_shards:
            raise ValueError("placement constraints must name scaling-capacity shards")
        for shard, constraints in self.placement.shards.items():
            brokers = [b for b in self.brokers if b.shard == shard and b.role in ("active", "warm")]
            if any(partition > 65535 for partition in constraints.pinned_partitions):
                raise ValueError(f"{shard}: pinned partition exceeds the supported maximum")
            domain_keys = {
                *constraints.allowed_failure_domains,
                *constraints.max_partitions_per_domain,
                *constraints.spread_by,
            }
            if any(key not in broker.failure_domains for key in domain_keys for broker in brokers):
                raise ValueError(f"{shard}: every scaling broker needs each constrained failure domain")
        return self

    def validate_profile(self, model: CapacityModel, service_class: str) -> None:
        """Refuse an accidental cross-provider/version/tier recommendation."""
        if (model.schema_version != "2" or self.provider != model.provenance.platform
                or self.broker_version != model.provenance.broker_version
                or self.service_class != service_class or service_class not in model.service_classes):
            raise ValueError("inventory provider/version/tier must match the measured model and config")


def load_inventory(path: str | Path) -> FleetInventory:
    """Read a non-secret YAML inventory with strict field validation."""
    return FleetInventory.model_validate(yaml.safe_load(Path(path).read_text()))


@dataclass(frozen=True)
class FleetSnapshot:
    shards: dict[str, MetricSample]
    brokers: dict[str, MetricSample]


def aggregate(samples: list[MetricSample]) -> MetricSample:
    """Sum counts/rates, and derive message size using rate weighting rather than broker averages."""
    if not samples or any(s.current_brokers != 1 for s in samples):
        raise CollectorError("aggregate requires one complete sample per active service")
    rx = sum(s.ingress_msg_rate for s in samples)
    tx = sum(s.egress_msg_rate for s in samples)
    rb = sum(s.ingress_byte_rate for s in samples)
    tb = sum(s.egress_byte_rate for s in samples)
    return MetricSample(min(s.timestamp for s in samples), rx, tx, rb, tb,
                        rb / rx if rx else tb / tx if tx else 0,
                        sum(s.connection_count for s in samples), sum(s.spool_used for s in samples),
                        len(samples))


class FleetCollector:
    """Bounded parallel collection; credentials are loaded once and never written into reports."""

    def __init__(self, inventory: FleetInventory, *, timeout: float = 10.0) -> None:
        self.inventory = inventory.model_copy(
            update={"brokers": [b for b in inventory.brokers if b.role == "active"]}
        )
        if not self.inventory.brokers:
            raise CollectorError("inventory must contain at least one active broker")
        self.collectors: dict[str, SempCollector] = {}
        try:
            for broker in self.inventory.brokers:
                assert broker.username_env and broker.password_env and broker.base_url
                username, password = os.environ.get(broker.username_env), os.environ.get(broker.password_env)
                if not username or not password:
                    raise CollectorError(f"missing credential environment variables for {broker.broker_id}")
                self.collectors[broker.broker_id] = SempCollector(broker.base_url, username, password,
                                                                timeout=timeout)
        except Exception:
            self.close()
            raise

    def collect(self, now: float) -> FleetSnapshot:
        """Return only complete snapshots; no stale cache or zeros for failed brokers."""
        with ThreadPoolExecutor(max_workers=min(16, len(self.collectors))) as pool:
            futures = {b.broker_id: pool.submit(self.collectors[b.broker_id].collect,
                                              b.shard, b.msg_vpn, now, 1) for b in self.inventory.brokers}
            results = {}
            failed = []
            for broker_id, future in futures.items():
                try:
                    results[broker_id] = future.result()
                except Exception:
                    failed.append(broker_id)
            if failed:
                raise CollectorError("incomplete fleet snapshot; failed brokers: " + ", ".join(failed))
        by_shard: dict[str, list[MetricSample]] = defaultdict(list)
        for broker in self.inventory.brokers:
            by_shard[broker.shard].append(results[broker.broker_id])
        return FleetSnapshot({shard: aggregate(samples) for shard, samples in by_shard.items()}, results)

    def close(self) -> None:
        """Release every SEMP transport, including after partial initialization."""
        for collector in self.collectors.values():
            collector.close()
