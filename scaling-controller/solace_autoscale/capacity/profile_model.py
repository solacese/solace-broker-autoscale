"""Compile a measured catalog plus explicit HA limits, and look up its workload envelope."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any

from .benchmarks import BenchmarkPoint, BenchmarkSheet, BenchmarkWorkbook
from .schema import CapacityModel

GUARANTEED_SCENARIOS = ("streaming", "unspooling", "replay", "tracing")


class OutsideBenchmark(ValueError):
    """The workload cannot be supported by the selected measurements."""


def compile_profile(book: BenchmarkWorkbook, limits: dict[str, Any], *, compiled_at: str,
                    limits_source: str) -> CapacityModel:
    """Retain all dimensions; compatibility curves are never used by the schema-2 runtime."""
    if not limits_source.strip():
        raise ValueError("limits_source must identify the configured HA limits or planning assumptions")
    classes: dict[str, Any] = {}
    for sheet in book.sheets:
        if sheet.service_class in classes:
            raise ValueError(f"duplicate tier {sheet.service_class}")
        if sheet.service_class not in limits:
            raise ValueError(f"missing limits for {sheet.service_class}")
        limit = limits[sheet.service_class]
        expected = sheet.service_class.upper().replace("-", "_") + "_HIGHAVAILABILITY"
        if limit.get("service_class_id") != expected:
            raise ValueError(f"{sheet.service_class}: HA benchmarks require service_class_id={expected}")
        delivery = {}
        for mode in ("direct", "guaranteed"):
            points = [p for p in sheet.observations if p.fanout == 1 and
                      ((p.scenario == "direct") == (mode == "direct"))]
            sizes = sorted({p.msg_size_bytes for p in points})
            delivery[mode] = {"size_buckets": [
                {"msg_size_bytes": size,
                 "msg_rate": min(p.ingress_msg_rate for p in points if p.msg_size_bytes == size),
                 "byte_rate": min(p.ingress_msg_rate for p in points if p.msg_size_bytes == size) * size}
                for size in sizes]}
        classes[sheet.service_class] = {
            "service_class_id": expected, "connections_max": limit["connections_max"],
            "spool_bytes_max": limit["spool_bytes_max"], "delivery": delivery,
            "benchmark": sheet.model_dump(),
        }
    relevant_limits = {key: limits[key] for key in sorted(classes)}
    limits_hash = hashlib.sha256(json.dumps(relevant_limits, sort_keys=True).encode()).hexdigest()
    # Covers parsed observations, limits, interpretation policy and provenance; excludes wall clock.
    identity = {"book": book.model_dump(), "limits": relevant_limits,
                "limits_source": limits_source, "compiler": "profile-2.0"}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    sizes = [p.msg_size_bytes for s in book.sheets for p in s.observations]
    fanouts = [p.fanout for s in book.sheets for p in s.observations]
    return CapacityModel.model_validate({
        "schema_version": "2", "model_version": f"{digest[:16]}+cs2",
        "synthetic": False, "service_classes": classes,
        "provenance": {
            "source_filename": book.source_filename, "source_sha256": book.source_sha256,
            "compiled_at": compiled_at, "compiler_version": "profile-2.0",
            "row_count": len(sizes), "platform": book.provider, "broker_version": book.broker_version,
            "limits_sha256": limits_hash, "limits_source": limits_source,
            "measured_range": {"msg_size_bytes": [min(sizes), max(sizes)],
                               "fanout": [min(fanouts), max(fanouts)]},
            "notes": book.notes + ["Schema 2 uses paired direction limits and all fanouts. "
                                   "Intermediate queries use a conservative bracketing envelope."],
        },
    })


def _bracket(values: list[int], query: float, label: str) -> list[int]:
    """Exact points remain exact; outside-range queries never invent capacity."""
    if not values or not math.isfinite(query) or not values[0] <= query <= values[-1]:
        raise OutsideBenchmark(f"{label}={query:g} outside measured values {values}")
    if query in values:
        return [int(query)]
    return [max(v for v in values if v < query), min(v for v in values if v > query)]


def envelope(sheet: BenchmarkSheet, size: float, fanout: float, delivery: str,
             scenario: str) -> tuple[float, float, bool, tuple[int, int], list[str]]:
    """Return ingress/egress msg/s constrained by both message and payload byte ceilings.

    Between sizes/fanouts take the lower message AND byte limit of bracketing observations.
    This is a conservative engineering estimate, not a new measured point. For 'worst', every
    guaranteed scenario must cover the query; missing scenarios cannot be silently dropped.
    """
    if delivery not in ("direct", "guaranteed", "mixed"):
        raise OutsideBenchmark(f"unsupported delivery {delivery!r}")
    if scenario not in (*GUARANTEED_SCENARIOS, "worst"):
        raise OutsideBenchmark(f"unsupported guaranteed scenario {scenario!r}")
    scenarios = (["direct"] if delivery == "direct" else
                 list(GUARANTEED_SCENARIOS) if scenario == "worst" else [scenario])
    if delivery == "mixed":
        scenarios = ["direct", *scenarios]
    selected: list[BenchmarkPoint] = []
    interpolated = False
    size_ranges = []
    for mode in scenarios:
        points = [p for p in sheet.observations if p.scenario == mode]
        fans = _bracket(sorted({p.fanout for p in points}), fanout, f"{mode} fanout")
        interpolated |= len(fans) > 1
        for fan in fans:
            row = [p for p in points if p.fanout == fan]
            values = sorted({p.msg_size_bytes for p in row})
            sizes = _bracket(values, size, f"{mode} message size")
            size_ranges.append((min(values), max(values)))
            interpolated |= len(sizes) > 1
            selected.extend(p for p in row if p.msg_size_bytes in sizes)
    ingress = min(min(p.ingress_msg_rate, p.ingress_msg_rate * p.msg_size_bytes / size)
                  for p in selected)
    egress = min(min(p.egress_msg_rate, p.egress_msg_rate * p.msg_size_bytes / size)
                 for p in selected)
    refs = sorted({f"{sheet.sheet}!{p.ingress_cell}/{p.egress_cell}" for p in selected})
    return (ingress, egress, interpolated,
            (max(r[0] for r in size_ranges), min(r[1] for r in size_ranges)), refs)
