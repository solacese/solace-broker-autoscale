"""Small business policy compiled into the existing validated controller configuration."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, model_validator

from .config import Config, _Base


class Scaling(_Base):
    mode: Literal["automatic", "paused"] = "automatic"
    max_brokers: int = Field(default=4, ge=1)
    warm_brokers: int = Field(default=1, ge=0)

    @model_validator(mode="after")
    def capacity(self) -> Scaling:
        if self.warm_brokers >= self.max_brokers:
            raise ValueError("reserve room for at least one active broker")
        return self


class Workload(_Base):
    topic: str
    keep_together: str | list[str]
    subscribers: dict[str, str | list[str]] = Field(min_length=1)


class Policy(_Base):
    version: Literal[1]
    connection: str
    scaling: Scaling = Field(default_factory=Scaling)
    workloads: dict[str, Workload] = Field(min_length=1, max_length=64)


def compile_policy(raw: dict, path: Path) -> Config:
    """Load one operator profile, then compile named topic levels without guessing credentials."""
    policy = Policy.model_validate(raw)
    profile_path = (path.parent / policy.connection).resolve()
    profile = yaml.safe_load(profile_path.read_text())
    if not isinstance(profile, dict) or "version" in profile or "workloads" in profile:
        raise ValueError("connection must reference an advanced operator profile, not another policy")
    if "messaging" in profile:
        raise ValueError("declare messaging in the workload policy, not the connection profile")
    cfg = Config.model_validate(profile)
    if not cfg.inventory:
        raise ValueError("connection profile must configure inventory")
    # Profile-owned paths have the profile's directory as their base, regardless of the caller's cwd.
    cfg.inventory = str((profile_path.parent / cfg.inventory).resolve())
    cfg.capacity.model = str((profile_path.parent / cfg.capacity.model).resolve())
    cfg.assignment.store = str((profile_path.parent / cfg.assignment.store).resolve())
    cfg.actuation.kill_switch_file = str((profile_path.parent / cfg.actuation.kill_switch_file).resolve())
    routes: list[dict] = []
    groups: dict[str, set[str]] = {}
    for name, workload in policy.workloads.items():
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", name):
            raise ValueError("workload names use lowercase letters, digits and hyphens")
        fields: dict[str, int] = {}
        levels = workload.topic.split("/")
        for i, level in enumerate(levels):
            match = re.fullmatch(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}", level)
            if match:
                field = match.group(1)
                if field in fields:
                    raise ValueError(f"{name}: duplicate named topic field {field}")
                fields[field] = i
                levels[i] = "*"
            elif "{" in level or "}" in level:
                raise ValueError(f"{name}: topic fields must occupy a complete level")
        together = workload.keep_together
        rule: dict = {"pattern": "/".join(levels), "shard": name}
        if together in ("topic", "all"):
            rule["dispatch"] = "by-topic" if together == "topic" else "single"
        else:
            names = [together] if isinstance(together, str) else together
            if not names or any(n not in fields for n in names):
                raise ValueError(f"{name}: keep_together must name topic fields, 'topic', or 'all'")
            rule["key_levels"] = [fields[n] for n in names]
        routes.append(rule)
        for group, patterns in workload.subscribers.items():
            values = [patterns] if isinstance(patterns, str) else patterns
            groups.setdefault(group, set()).update(values)
    effective = cfg.model_dump(mode="json")
    effective["assignment"].update(routing="partitioned", strategy="rendezvous")
    effective["workload"]["delivery"] = "guaranteed"
    effective["automation"]["enabled"] = True
    for name in policy.workloads:
        effective["automation"]["shards"].setdefault(name, {})["enabled"] = policy.scaling.mode == "automatic"
    effective["fleet"]["max_brokers"] = policy.scaling.max_brokers
    effective["policy"]["warm_pool"] = policy.scaling.warm_brokers
    effective["actuation"].update(mode="scale-up-only", dry_run=False, require_confirmation=False)
    if policy.scaling.mode == "paused":
        effective["provisioning"]["enabled"] = False
    effective["messaging"] = {
        "enabled": True,
        "allow_dynamic_groups": False,
        "routes": routes,
        "subscriptions": [{"group": g, "topics": sorted(v)} for g, v in sorted(groups.items())],
    }
    return Config.model_validate(effective)
