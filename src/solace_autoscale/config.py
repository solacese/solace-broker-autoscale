"""Configuration model (§4).

Loaded from YAML, validated with Pydantic, fails loudly on unknown keys (``extra='forbid'``
everywhere). Every unresolved design question is a setting with a conservative default.

Durations accept ``30s``, ``3m``, ``45m``, ``1h`` style strings and normalise to seconds.
``scale_up_window`` and ``bottleneck`` accept the literal ``auto``.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$", re.IGNORECASE)
_UNIT_SECONDS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value: str | int | float) -> float:
    """Parse a duration to seconds. Accepts a bare number (seconds) or ``<n><unit>``."""
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    # a bare number means seconds
    try:
        return float(s)
    except ValueError:
        pass
    m = _DURATION_RE.match(s)
    if not m:
        raise ValueError(f"invalid duration {value!r}; use forms like '30s', '3m', '45m', '1h'")
    return float(m.group(1)) * _UNIT_SECONDS[m.group(2).lower()]


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FleetConfig(_Base):
    provider: Literal["solace-cloud"] = "solace-cloud"
    service_class: str = "enterprise-10k"
    min_brokers: int = Field(default=1, ge=1)
    max_brokers: int = Field(default=8, ge=1)

    @model_validator(mode="after")
    def _check(self) -> FleetConfig:
        if self.max_brokers < self.min_brokers:
            raise ValueError("fleet.max_brokers must be >= fleet.min_brokers")
        return self


class ShardSpec(_Base):
    name: str
    match: str


class TopologyConfig(_Base):
    mode: Literal["sharded", "mesh", "hybrid"] = "sharded"
    shard_key: str = "{domain}"
    shards: list[ShardSpec] = Field(default_factory=list)


class WorkloadConfig(_Base):
    delivery: Literal["direct", "guaranteed", "mixed"] = "guaranteed"
    bottleneck: Literal["auto", "bytes", "messages", "spool", "connections"] = "auto"


class ProtocolSpec(_Base):
    enabled: bool = False
    port: int | None = None  # read from broker config; not hardcoded


class ProtocolsConfig(_Base):
    smf: ProtocolSpec = Field(default_factory=lambda: ProtocolSpec(enabled=True))
    amqp: ProtocolSpec = Field(default_factory=lambda: ProtocolSpec(enabled=True))
    mqtt: ProtocolSpec = Field(default_factory=lambda: ProtocolSpec(enabled=False))
    rest: ProtocolSpec = Field(default_factory=lambda: ProtocolSpec(enabled=True))
    jms: ProtocolSpec = Field(default_factory=lambda: ProtocolSpec(enabled=False))
    web: ProtocolSpec = Field(default_factory=lambda: ProtocolSpec(enabled=False))

    def enabled_protocols(self) -> list[str]:
        return [name for name in ("smf", "amqp", "mqtt", "rest", "jms", "web")
                if getattr(self, name).enabled]


class DnsConfig(_Base):
    enabled: bool = False
    zone: str = "brokers.example.com"
    ttl: float = Field(default=30.0)

    @field_validator("ttl", mode="before")
    @classmethod
    def _ttl(cls, v: Any) -> float:
        return parse_duration(v)


class IntegrationConfig(_Base):
    dns: DnsConfig = Field(default_factory=DnsConfig)


class MetricsConfig(_Base):
    source: Literal["prometheus", "cloud-api", "semp", "static"] = "prometheus"
    scrape_interval: float = Field(default=30.0)
    staleness_limit: float = Field(default=180.0)
    endpoint: str | None = None  # collector-specific; parsed by the collector, not the engine
    static_path: str | None = None

    @field_validator("scrape_interval", "staleness_limit", mode="before")
    @classmethod
    def _dur(cls, v: Any) -> float:
        return parse_duration(v)


class HeadroomConfig(_Base):
    mode: Literal["derived", "fixed"] = "derived"
    messages: float = Field(default=0.75, gt=0, le=1)
    bytes: float = Field(default=0.75, gt=0, le=1)
    spool: float = Field(default=0.60, gt=0, le=1)
    connections: float = Field(default=0.85, gt=0, le=1)
    safety_factor: float = Field(default=1.5, gt=0)


class PolicyConfig(_Base):
    headroom: HeadroomConfig = Field(default_factory=HeadroomConfig)
    scale_down_at: float = Field(default=0.40, gt=0, lt=1)
    scale_up_window: Literal["auto"] | float = "auto"
    scale_down_window: float = Field(default=2700.0)
    cooldown: float = Field(default=900.0)
    warm_pool: int = Field(default=1, ge=0)

    @field_validator("scale_down_window", "cooldown", mode="before")
    @classmethod
    def _dur(cls, v: Any) -> float:
        return parse_duration(v)

    @field_validator("scale_up_window", mode="before")
    @classmethod
    def _win(cls, v: Any) -> Any:
        if isinstance(v, str) and v.strip().lower() == "auto":
            return "auto"
        return parse_duration(v)


class BillingConfig(_Base):
    model: Literal["committed", "elastic"] = "committed"
    #: Optional monthly price per broker, keyed by service_class (e.g. {"enterprise-10k": 4200}).
    #: No secrets and no Solace-published pricing is shipped; the operator supplies their own rates.
    #: When absent, the report shows broker COUNTS only and omits dollar figures.
    per_broker_monthly: dict[str, float] = Field(default_factory=dict)
    currency: str = "USD"


class ActuationConfig(_Base):
    mode: Literal["recommend", "scale-up-only", "full"] = "recommend"
    dry_run: bool = True
    require_confirmation: bool = True
    max_ops_in_flight: int = Field(default=1, ge=1)
    max_ops_per_hour: int = Field(default=4, ge=0)
    kill_switch_file: str = "/var/run/solace-autoscale.halt"


class CapacityConfigBlock(_Base):
    model: str = "models/synthetic-v0.json"


class AccuracyConfig(_Base):
    record: bool = True
    store: str = "./accuracy.db"


class Config(_Base):
    fleet: FleetConfig = Field(default_factory=FleetConfig)
    topology: TopologyConfig = Field(default_factory=TopologyConfig)
    workload: WorkloadConfig = Field(default_factory=WorkloadConfig)
    protocols: ProtocolsConfig = Field(default_factory=ProtocolsConfig)
    integration: IntegrationConfig = Field(default_factory=IntegrationConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    billing: BillingConfig = Field(default_factory=BillingConfig)
    actuation: ActuationConfig = Field(default_factory=ActuationConfig)
    capacity: CapacityConfigBlock = Field(default_factory=CapacityConfigBlock)
    accuracy: AccuracyConfig = Field(default_factory=AccuracyConfig)

    @model_validator(mode="after")
    def _validate_windows(self) -> Config:
        # §5.8: reject a configured scale_up_window below 3 * scrape_interval.
        if self.policy.scale_up_window != "auto":
            floor = 3 * self.metrics.scrape_interval
            if float(self.policy.scale_up_window) < floor:
                raise ValueError(
                    f"policy.scale_up_window ({self.policy.scale_up_window}s) must be >= "
                    f"3 * metrics.scrape_interval ({floor}s); a shorter window measures noise, "
                    "not trend"
                )
        return self

    def config_hash(self) -> str:
        """Stable hash of the effective config, recorded on every decision (§7)."""
        payload = self.model_dump(mode="json")
        blob = repr(sorted(_flatten(payload).items())).encode()
        return hashlib.sha256(blob).hexdigest()[:16]


def _flatten(d: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(d, dict):
        for k, v in d.items():
            out.update(_flatten(v, f"{prefix}.{k}" if prefix else str(k)))
    elif isinstance(d, list):
        for i, v in enumerate(d):
            out.update(_flatten(v, f"{prefix}[{i}]"))
    else:
        out[prefix] = d
    return out


def load_config(path: str | Path) -> Config:
    """Load and validate YAML config. Raises pydantic.ValidationError on unknown keys / bad values."""
    raw = yaml.safe_load(Path(path).read_text()) or {}
    if not isinstance(raw, dict):
        raise ValueError("config root must be a mapping")
    return Config.model_validate(raw)
