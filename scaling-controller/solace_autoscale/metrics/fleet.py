"""Complete, concurrent SEMP snapshots of an explicitly inventoried homogeneous HA fleet.

One inventory entry is one active service endpoint, not an HA standby. A failed endpoint
invalidates the entire snapshot: losing a broker must never look like disappearing demand.
"""
from __future__ import annotations

import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..capacity.schema import CapacityModel
from ..decision.types import MetricSample
from .base import CollectorError
from .semp import SempCollector


class BrokerEndpoint(BaseModel):
    """Location and environment-variable references; no inline credentials."""

    model_config = ConfigDict(extra="forbid")
    broker_id: str = Field(min_length=1)
    shard: str = Field(min_length=1)
    msg_vpn: str = Field(min_length=1)
    base_url: str
    username_env: str = Field(min_length=1)
    password_env: str = Field(min_length=1)
    endpoints: dict[str, str] = Field(default_factory=dict)
    role: Literal["active", "warm"] = "active"

    @model_validator(mode="after")
    def validate_url(self) -> BrokerEndpoint:
        """Require TLS except for loopback integration tests, and forbid embedded secrets."""
        url = urlsplit(self.base_url)
        if (url.scheme not in ("http", "https") or not url.hostname or url.username or url.password
                or url.query or url.fragment or url.path not in ("", "/")):
            raise ValueError("base_url must be an HTTP(S) origin without credentials, path or query")
        if url.scheme != "https" and url.hostname not in ("localhost", "127.0.0.1", "::1"):
            raise ValueError("non-loopback SEMP endpoints require HTTPS")
        return self


class FleetInventory(BaseModel):
    """Operator-declared environment; a single tier/provider/generation per monitor."""

    model_config = ConfigDict(extra="forbid")
    provider: Literal["aws", "azure", "gcp"]
    broker_version: str
    service_class: str
    deployment_mode: Literal["HA"] = "HA"
    brokers: list[BrokerEndpoint] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def unique_services(self) -> FleetInventory:
        """Prevent duplicate counting, including multiple VPNs on the same service endpoint."""
        ids = [b.broker_id for b in self.brokers]
        urls = [b.base_url.rstrip("/").lower() for b in self.brokers]
        if len(set(ids)) != len(ids) or len(set(urls)) != len(urls):
            raise ValueError("each broker id and service endpoint must appear exactly once")
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
