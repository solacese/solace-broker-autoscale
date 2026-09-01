"""Shared test fixtures. Fictional data only (§12)."""

from __future__ import annotations

from pathlib import Path

import pytest

from solace_autoscale.capacity.model import load_model
from solace_autoscale.capacity.schema import CapacityModel
from solace_autoscale.config import Config
from solace_autoscale.decision.types import MetricSample

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def synthetic_model() -> CapacityModel:
    return load_model(REPO / "models" / "synthetic-v0.json")


def make_test_model(
    *,
    synthetic: bool = False,
    connections_max: int = 10_000,
    spool_bytes_max: int = 500_000_000_000,
) -> CapacityModel:
    """A small, exactly-known model for arithmetic-precise engine tests.

    enterprise-10k, direct+guaranteed, buckets at 1000 and 10000 bytes.
    msg_rate: 10000 @1KB, 1000 @10KB (so byte_rate is 10e6 at both — flat byte ceiling).
    """
    data = {
        "schema_version": "1",
        "model_version": "test-v1",
        "synthetic": synthetic,
        "provenance": {
            "source_filename": "TEST",
            "source_sha256": "0" * 64,
            "compiled_at": "1970-01-01T00:00:00Z",
            "compiler_version": "1",
            "row_count": 2,
            "platform": "test",
            "measured_range": {"msg_size_bytes": [1000, 10000], "fanout": [1, 1]},
            "notes": [],
        },
        "service_classes": {
            "enterprise-10k": {
                "service_class_id": "ENTERPRISE_10K_HIGHAVAILABILITY",
                "connections_max": connections_max,
                "spool_bytes_max": spool_bytes_max,
                "delivery": {
                    "direct": {"size_buckets": [
                        {"msg_size_bytes": 1000, "msg_rate": 10000.0, "byte_rate": 10_000_000.0},
                        {"msg_size_bytes": 10000, "msg_rate": 1000.0, "byte_rate": 10_000_000.0},
                    ]},
                    "guaranteed": {"size_buckets": [
                        {"msg_size_bytes": 1000, "msg_rate": 8000.0, "byte_rate": 8_000_000.0},
                        {"msg_size_bytes": 10000, "msg_rate": 800.0, "byte_rate": 8_000_000.0},
                    ]},
                },
            }
        },
    }
    if synthetic:
        data["WARNING"] = "synthetic placeholder data, not measured, do not use for planning"
    return CapacityModel.model_validate(data)


def window(
    *,
    n: int = 15,
    cadence: float = 30.0,
    t0: float = 1_000_000.0,
    ingress_msg_rate: float = 1000.0,
    egress_msg_rate: float | None = None,
    avg_msg_size: float = 1000.0,
    connection_count: int = 100,
    spool_used: float = 1_000_000.0,
    current_brokers: int = 1,
    growth_per_sample: float = 0.0,
) -> list[MetricSample]:
    """Build a chronological sample window. ``growth_per_sample`` multiplies rates each step."""
    egress = egress_msg_rate if egress_msg_rate is not None else ingress_msg_rate
    out = []
    ing = ingress_msg_rate
    egr = egress
    for i in range(n):
        out.append(MetricSample(
            timestamp=t0 + i * cadence,
            ingress_msg_rate=ing,
            egress_msg_rate=egr,
            ingress_byte_rate=ing * avg_msg_size,
            egress_byte_rate=egr * avg_msg_size,
            avg_msg_size=avg_msg_size,
            connection_count=connection_count,
            spool_used=spool_used,
            current_brokers=current_brokers,
        ))
        ing *= (1 + growth_per_sample)
        egr *= (1 + growth_per_sample)
    return out


def default_config(**overrides) -> Config:
    """Config with test-friendly defaults: elastic billing so scale-down isn't suppressed by default."""
    base = {
        "fleet": {"service_class": "enterprise-10k", "min_brokers": 1, "max_brokers": 8},
        "billing": {"model": "elastic"},
        "metrics": {"scrape_interval": "30s", "staleness_limit": "3m"},
    }
    _deep_merge(base, overrides)
    return Config.model_validate(base)


def _deep_merge(base: dict, over: dict) -> None:
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
