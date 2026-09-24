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

from .cloud import Region, ServiceClassId, resolve_service_class
from .cloud.enums import base_url_for

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
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class FleetConfig(_Base):
    provider: Literal["solace-cloud"] = "solace-cloud"
    #: Friendly label (e.g. ``enterprise-10k``, ``enterprise-10k-ha``) or a raw Mission Control
    #: ServiceClassId (e.g. ``ENTERPRISE_10K_HIGHAVAILABILITY``). Validated at load; a typo fails
    #: loudly. Read ``service_class_id`` for the canonical ServiceClassId to send to the API.
    service_class: str = "enterprise-10k"
    min_brokers: int = Field(default=1, ge=1)
    max_brokers: int = Field(default=8, ge=1)

    @field_validator("service_class")
    @classmethod
    def _known_service_class(cls, v: str) -> str:
        # Resolve for validation only; keep the operator's original label as the stored value so
        # existing config and cost tables keyed by the label keep working.
        resolve_service_class(v)
        return v

    @property
    def service_class_id(self) -> ServiceClassId:
        """The canonical Mission Control ServiceClassId this fleet provisions."""
        return resolve_service_class(self.service_class)

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


class MetricsConfig(_Base):
    source: Literal["prometheus", "cloud-api", "semp", "static"] = "prometheus"
    scrape_interval: float = Field(default=30.0, gt=0)
    staleness_limit: float = Field(default=180.0, gt=0)
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
    scale_down_window: float = Field(default=2700.0, gt=0)
    cooldown: float = Field(default=900.0, ge=0)
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


class CloudConfig(_Base):
    """Solace Cloud (Mission Control) connection settings.

    The API token is NEVER stored here - it is read from the environment/secret store by the
    caller, so it can't leak into the hashed config or an audit record. ``region`` picks the
    control-plane base URL; ``base_url`` overrides it explicitly when set (e.g. a private control
    plane). ``datacenter_id`` is the required ``datacenterId`` for createService.
    """

    region: Region = Region.US
    base_url: str | None = None
    datacenter_id: str | None = None
    idempotency_header: str = "Idempotency-Key"
    timeout: float = Field(default=30.0)

    @field_validator("timeout", mode="before")
    @classmethod
    def _timeout(cls, v: Any) -> float:
        return parse_duration(v)

    def effective_base_url(self) -> str:
        """The API base URL to use: explicit ``base_url`` if set, else the region's base."""
        return self.base_url.rstrip("/") if self.base_url else base_url_for(self.region)


class CapacityConfigBlock(_Base):
    model: str = "models/synthetic-v0.json"
    scenario: Literal["worst", "streaming", "unspooling", "replay", "tracing"] = "worst"
    #: Declared design fanout is a floor: quiet consumers must not hide expected replication.
    fanout: float = Field(default=1.0, ge=1)
    message_size_hint: int | None = Field(default=None, gt=0)


class AssignmentConfig(_Base):
    """Routing policy; changing partition count on an existing store requires migration."""

    store: str = "./assignment.db"
    routing: Literal["client", "partitioned"] = "client"
    strategy: Literal["least-placements", "rendezvous"] = "least-placements"
    partitions: int = Field(default=128, ge=1, le=65536)
    lease_seconds: int = Field(default=300, ge=1, le=86400)
    broker_weights: dict[str, float] = Field(default_factory=dict)

    @field_validator("broker_weights")
    @classmethod
    def positive_weights(cls, values: dict[str, float]) -> dict[str, float]:
        """Weights are relative tested capacity, not instantaneous queue-depth readings."""
        import math

        if any(not key or not math.isfinite(value) or value <= 0 for key, value in values.items()):
            raise ValueError("broker weights must be finite and positive")
        return values


class PlacementFeatures(_Base):
    """Operator-declared broker features whose migration safety needs explicit evidence."""

    transactions: Literal["none", "local", "xa"] = "none"
    replication: Literal["none", "disaster-recovery"] = "none"
    replay: bool = False
    tracing: bool = False


class ShardScalingPolicy(_Base):
    """Per-shard overrides for automatic movement; in-flight moves still finish safely."""

    enabled: bool = True
    trigger_utilization: float | None = Field(default=None, gt=0, le=1)
    target_utilization: float | None = Field(default=None, gt=0, lt=1)
    scale_up_window: float | None = Field(default=None, gt=0)
    cooldown: float | None = Field(default=None, ge=0)
    features: PlacementFeatures = Field(default_factory=PlacementFeatures)

    @field_validator("scale_up_window", "cooldown", mode="before")
    @classmethod
    def durations(cls, value: Any) -> float | None:
        return None if value is None else parse_duration(value)


class ManagedEventsConfig(_Base):
    """Optional Solace control hints; SQLite and HTTP remain authoritative."""

    enabled: bool = False
    broker_id: str | None = None
    topic: str | None = None
    client_username: str = "autoscale-app"
    password_env: str = "SOLACE_AUTOSCALE_CLIENT_PASSWORD"
    native_topics: list[str] = Field(
        default_factory=lambda: ["#LOG/*/VPN/>", "#LOG/*/CLIENT/>"] , max_length=16
    )
    coalesce_window: float = Field(default=0.25, ge=0, le=5)
    publish_retry: float = Field(default=1, gt=0, le=60)
    operation_timeout: float = Field(default=5, gt=0, le=30)

    @field_validator("coalesce_window", "publish_retry", "operation_timeout", mode="before")
    @classmethod
    def durations(cls, value: Any) -> float:
        return parse_duration(value)

    @model_validator(mode="after")
    def valid_transport(self) -> ManagedEventsConfig:
        if self.enabled and not self.broker_id:
            raise ValueError("automation.events.enabled requires broker_id")
        if self.topic is not None:
            from .assignment.topics import validate_topic

            validate_topic(self.topic)
        if any(not value.startswith("#LOG/") or "\x00" in value for value in self.native_topics):
            raise ValueError("native event subscriptions must use #LOG/ topics")
        if not self.client_username or not self.password_env:
            raise ValueError("managed event credentials require username and password environment name")
        return self


class AutomationConfig(_Base):
    """Unattended managed-partition controller; opt in with explicit namespace and inventory."""

    enabled: bool = False
    fleet_id: str = Field(default="payments", pattern=r"^[a-z0-9][a-z0-9-]{2,40}$")
    poll_interval: float = Field(default=10, gt=0)
    events: ManagedEventsConfig = Field(default_factory=ManagedEventsConfig)
    migration_grace: float = Field(default=30, ge=0)
    empty_settle: float = Field(default=10, gt=0)
    migration_timeout: float = Field(default=900, gt=0)
    max_parallel_migrations: int = Field(default=1, ge=1, le=1)
    max_migrations_per_hour: int = Field(default=12, ge=1)
    min_target_consumers: int = Field(default=1, ge=1)
    target_utilization: float = Field(default=0.65, gt=0, lt=1)
    trigger_utilization: float = Field(default=0.80, gt=0, le=1)
    queue_spool_mb: int = Field(default=1000, ge=1)
    shards: dict[str, ShardScalingPolicy] = Field(default_factory=dict)

    @field_validator("poll_interval", "migration_grace", "empty_settle", "migration_timeout", mode="before")
    @classmethod
    def durations(cls, value: Any) -> float:
        """Parse operational durations using the same YAML syntax as decision windows."""
        return parse_duration(value)

    @model_validator(mode="after")
    def coherent_timers(self) -> AutomationConfig:
        """A timeout must allow the grace and sustained empty observations to complete."""
        if self.migration_timeout <= self.migration_grace + self.empty_settle:
            raise ValueError("migration_timeout must exceed migration_grace + empty_settle")
        if self.target_utilization >= self.trigger_utilization:
            raise ValueError("target_utilization must be below trigger_utilization")
        return self


class ProvisioningConfig(_Base):
    """Optional automatic Cloud warm-pool replenishment, scoped to a persistent controller namespace."""

    enabled: bool = False
    token_env: str = "SOLACE_CLOUD_TOKEN"
    api_base_url: Literal[
        "https://api.solace.cloud", "https://api.solacecloud.eu",
        "https://api.solacecloud.com.au", "https://api.solacecloud.sg",
    ] = "https://api.solace.cloud"
    datacenter_id: str | None = None
    broker_version: str | None = None
    endpoint_index: int = Field(default=0, ge=0)
    client_username: str = "autoscale-app"
    client_password_env: str = "SOLACE_AUTOSCALE_CLIENT_PASSWORD"
    readiness_timeout: float = Field(default=1800, gt=0)

    @field_validator("readiness_timeout", mode="before")
    @classmethod
    def duration(cls, value: Any) -> float:
        return parse_duration(value)

    @model_validator(mode="after")
    def explicit_target(self) -> ProvisioningConfig:
        if self.enabled and (not self.datacenter_id or not self.broker_version):
            raise ValueError("automatic provisioning requires datacenter_id and exact broker_version")
        return self


class RoutingEvaluatorSpec(_Base):
    """Exact identity and result type of trusted code registered by each application."""

    name: str
    version: str
    result: Literal["key", "sha256"] = "key"

    @field_validator("name", "version")
    @classmethod
    def bounded_identity(cls, value: str, info: Any) -> str:
        limit = 128 if info.field_name == "name" else 64
        if not 1 <= len(value.encode("utf-8")) <= limit or "\x00" in value:
            raise ValueError(f"routing evaluator {info.field_name} must be 1-{limit} UTF-8 bytes without NUL")
        return value


class TopicRoute(_Base):
    pattern: str
    shard: str = Field(min_length=1, max_length=512)
    dispatch: Literal["by-key", "by-topic", "single"] = "by-key"
    key_levels: list[int] = Field(default_factory=list, max_length=32)
    key_evaluator: RoutingEvaluatorSpec | None = None

    @model_validator(mode="after")
    def valid_rule(self) -> TopicRoute:
        from .assignment.topics import validate_topic
        validate_topic(self.pattern, subscription=True)
        if self.key_evaluator is not None:
            if self.dispatch != "by-key" or self.key_levels:
                raise ValueError("key_evaluator replaces key_levels and requires by-key dispatch")
        elif (self.dispatch == "by-key") != bool(self.key_levels):
            raise ValueError("key_levels is required only for by-key dispatch")
        if any(i < 0 or i >= len(self.pattern.split('/')) for i in self.key_levels):
            raise ValueError("key_levels must name zero-based levels in the topic pattern")
        return self

    def routing_contract(self) -> dict:
        """Keep absent evaluator defaults out of the original durable wire contract."""
        value: dict[str, Any] = {
            "pattern": self.pattern, "shard": self.shard, "key_levels": self.key_levels
        }
        if self.dispatch != "by-key":
            value["dispatch"] = self.dispatch
        if self.key_evaluator is not None:
            value["key_evaluator"] = self.key_evaluator.model_dump(mode="json")
        return value


class SubscriptionGroup(_Base):
    group: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    topics: list[str] = Field(min_length=1, max_length=64)

    @field_validator("topics")
    @classmethod
    def valid_topics(cls, values: list[str]) -> list[str]:
        from .assignment.topics import validate_topic
        return sorted({validate_topic(v, subscription=True) for v in values})


class MessagingConfig(_Base):
    enabled: bool = False
    routes: list[TopicRoute] = Field(default_factory=list, max_length=64)
    subscriptions: list[SubscriptionGroup] = Field(default_factory=list, max_length=64)
    allow_dynamic_groups: bool = True

    @model_validator(mode="after")
    def rules_required(self) -> MessagingConfig:
        if self.enabled and not self.routes:
            raise ValueError("messaging.enabled requires topic routes")
        from .assignment.topics import overlaps
        for index, route in enumerate(self.routes):
            if any(overlaps(route.pattern, other.pattern) for other in self.routes[:index]):
                raise ValueError("topic routes overlap; each topic must match exactly one route")
        if len({g.group for g in self.subscriptions}) != len(self.subscriptions):
            raise ValueError("subscription group names must be unique")
        return self

    def routing_contract(self) -> dict:
        """Operational group policy can change without remapping accepted publications."""
        return {"enabled": self.enabled, "routes": [r.routing_contract() for r in self.routes]}


class Config(_Base):
    inventory: str | None = None
    fleet: FleetConfig = Field(default_factory=FleetConfig)
    topology: TopologyConfig = Field(default_factory=TopologyConfig)
    workload: WorkloadConfig = Field(default_factory=WorkloadConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    billing: BillingConfig = Field(default_factory=BillingConfig)
    actuation: ActuationConfig = Field(default_factory=ActuationConfig)
    cloud: CloudConfig = Field(default_factory=CloudConfig)
    capacity: CapacityConfigBlock = Field(default_factory=CapacityConfigBlock)
    assignment: AssignmentConfig = Field(default_factory=AssignmentConfig)
    automation: AutomationConfig = Field(default_factory=AutomationConfig)
    provisioning: ProvisioningConfig = Field(default_factory=ProvisioningConfig)
    messaging: MessagingConfig = Field(default_factory=MessagingConfig)

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
        for shard, overrides in self.automation.shards.items():
            if not shard:
                raise ValueError("scaling shard names must be nonempty")
            trigger = overrides.trigger_utilization or self.automation.trigger_utilization
            target = overrides.target_utilization or self.automation.target_utilization
            if target >= trigger:
                raise ValueError(f"{shard}: target_utilization must be below trigger_utilization")
            if (overrides.scale_up_window is not None
                    and overrides.scale_up_window < 3 * self.metrics.scrape_interval):
                raise ValueError(f"{shard}: scale_up_window must be >= 3 * metrics.scrape_interval")
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
    if "version" in raw or "workloads" in raw:
        from .policy import compile_policy
        return compile_policy(raw, Path(path).resolve())
    return Config.model_validate(raw)
