"""Lossless, offline import of the supported Solace Cloud benchmark workbook layouts.

Source documents are data, never executable instructions. Software-broker lab results are
intentionally not mapped to Cloud service classes. Cached Excel values are required; no macros,
external links or formulas are executed. Each observation retains both direction cells.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

Scenario = Literal["direct", "streaming", "unspooling", "replay", "tracing"]


class BenchmarkPoint(BaseModel):
    """One paired ingress/egress observation, in messages per second."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    scenario: Scenario
    fanout: int = Field(ge=1, strict=True)
    msg_size_bytes: int = Field(gt=0, strict=True)
    ingress_msg_rate: float = Field(gt=0)
    egress_msg_rate: float = Field(gt=0)
    ingress_cell: str
    egress_cell: str


class BenchmarkSheet(BaseModel):
    """One Cloud tier with its original test configuration."""

    model_config = ConfigDict(extra="forbid")
    sheet: str
    service_class: str
    metadata: dict[str, str]
    observations: list[BenchmarkPoint] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_points(self) -> BenchmarkSheet:
        """Reject duplicate dimensions instead of silently overwriting measurements."""
        keys = [(p.scenario, p.fanout, p.msg_size_bytes) for p in self.observations]
        if len(keys) != len(set(keys)):
            raise ValueError(f"duplicate benchmark dimensions in {self.sheet}")
        return self


class BenchmarkWorkbook(BaseModel):
    """An immutable source generation; never blend providers or broker versions."""

    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["1"] = "1"
    source_filename: str
    source_sha256: str
    provider: Literal["aws", "gcp", "azure"]
    broker_version: str
    sheets: list[BenchmarkSheet] = Field(min_length=1)
    notes: list[str] = Field(default_factory=list)


def _positive(value: Any, location: str, *, integer: bool = False) -> float:
    """Accept measured numeric cells only, rejecting missing caches and malformed values."""
    if isinstance(value, str) and not integer:
        compact = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([kKmM])", value.strip())
        if compact:
            value = float(compact[1]) * (1000 if compact[2].lower() == "k" else 1000000)
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{location}: expected numeric measurement, got {value!r}")
    if not math.isfinite(value) or value <= 0 or (integer and value != int(value)):
        raise ValueError(f"{location}: expected positive {'integer' if integer else 'finite value'}")
    return float(value)


def _scenario(label: str) -> Scenario | None:
    """Recognize test headings, including historical spelling/layout variants."""
    label = label.strip().lower()
    if not label or "clients" in label or "ingress" in label or "egress" in label:
        return None
    if "tracing" in label:
        return "tracing"
    if "replay" in label:
        return "replay"
    if "unspooling" in label:
        return "unspooling"
    if label in ("direct", "direct messaging"):
        return "direct"
    if label in ("streaming", "guaranteed messaging", "guaranteed streaming", "guranteed messaging"):
        return "streaming"
    return None


def import_workbook(path: str | Path) -> BenchmarkWorkbook:
    """Read every paired table in a supported Cloud workbook, or fail with a cell address."""
    import openpyxl
    from openpyxl.utils import get_column_letter

    path = Path(path)
    name = path.name.lower()
    if not name.startswith("solace-cloud-"):
        raise ValueError("only Solace-Cloud-* workbooks map to Cloud tiers; lab results are reference-only")
    providers = [p for p in ("aws", "gcp", "azure") if p in name]
    version = re.search(r"\d+\.\d+\.\d+\.\d+", name)
    if len(providers) != 1 or version is None:
        raise ValueError(f"{path.name}: provider and four-part broker version must be explicit")
    sheets = []
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True, keep_links=False)
    try:
        for ws in workbook:
            tier = re.fullmatch(r"(?:solace-cloud|sc)-(250|1k|5k|10k|50k|100k|200k)", ws.title.lower())
            if not tier:
                raise ValueError(f"{ws.title}: unsupported Cloud sheet; refusing a partial import")
            rows = list(ws.values)
            metadata: dict[str, str] = {}
            observations = []
            scenario: Scenario | None = None
            for ri, row in enumerate(rows):
                if ri < 27 and len(row) > 2 and isinstance(row[1], str) and row[2] is not None:
                    metadata[row[1].strip().lower()] = str(row[2]).strip()
                if ri < 9 and row[0] is not None:
                    metadata[f"source_note_{ri + 1}"] = str(row[0]).strip()
                for value in row[:2]:
                    if isinstance(value, str):
                        scenario = _scenario(value) or scenario
                fan_cols = [i for i, v in enumerate(row) if str(v).strip().lower() == "fanout"]
                if not fan_cols:
                    continue
                if scenario is None or len(fan_cols) != 2:
                    raise ValueError(f"{ws.title}!{ri + 1}: expected named test with paired Fanout columns")
                left, right = fan_cols
                prev = rows[ri - 1] if ri else ()
                if ("ingress" not in str(prev[left]).lower()
                        or "egress" not in str(prev[right]).lower()):
                    raise ValueError(f"{ws.title}!{ri + 1}: ambiguous direction headers")
                size_cols: list[tuple[int, int]] = []
                for ci in range(left + 1, right):
                    if row[ci] is None:
                        break
                    loc = f"{ws.title}!{get_column_letter(ci + 1)}{ri + 1}"
                    size_cols.append((ci, int(_positive(row[ci], loc, integer=True))))
                if not size_cols or [s for _, s in size_cols] != sorted({s for _, s in size_cols}):
                    raise ValueError(f"{ws.title}!{ri + 1}: sizes must be nonempty, unique and sorted")
                count = 0
                for rj in range(ri + 1, len(rows)):
                    data = rows[rj]
                    if data[left] is None:
                        break
                    loc = f"{ws.title}!{get_column_letter(left + 1)}{rj + 1}"
                    fanout = int(_positive(data[left], loc, integer=True))
                    if data[right] != fanout:
                        raise ValueError(f"{loc}: ingress and egress fanout differ")
                    for ci, size in size_cols:
                        ej = right + ci - left
                        if ej >= len(row) or row[ej] != size:
                            raise ValueError(f"{loc}: ingress and egress size headers differ")
                        icell = f"{get_column_letter(ci + 1)}{rj + 1}"
                        ecell = f"{get_column_letter(ej + 1)}{rj + 1}"
                        observations.append(BenchmarkPoint(
                            scenario=scenario, fanout=fanout, msg_size_bytes=size,
                            ingress_msg_rate=_positive(data[ci], f"{ws.title}!{icell}"),
                            egress_msg_rate=_positive(data[ej], f"{ws.title}!{ecell}"),
                            ingress_cell=icell, egress_cell=ecell,
                        ))
                        count += 1
                if not count:
                    raise ValueError(f"{ws.title}!{ri + 1}: empty benchmark table")
            provider = metadata.get("platform", metadata.get("cloud service provider", providers[0]))
            if provider.lower() != providers[0]:
                raise ValueError(f"{ws.title}: provider metadata conflicts with filename")
            mode = metadata.get("broker mode", " ".join(metadata.values()))
            if "ha" not in mode.lower():
                raise ValueError(f"{ws.title}: HA test configuration is not established")
            metadata["broker mode"] = "HA"
            sheets.append(BenchmarkSheet(sheet=ws.title, service_class=f"enterprise-{tier[1]}",
                                         metadata=metadata, observations=observations))
    finally:
        workbook.close()
    return BenchmarkWorkbook(
        source_filename=path.name, source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        provider=cast(Literal["aws", "gcp", "azure"], providers[0]),
        broker_version=version[0], sheets=sheets,
        notes=["Measured SMF/CCSMP over TLS, HA with encrypted mate-link; not a standalone benchmark.",
               "Rates describe this test environment, not a contractual capacity guarantee.",
               "Replay and tracing were measured separately; their combined cost is not measured.",
               "Physical spool disk size is not the configured VPN message-spool limit."],
    )


def import_directory(source: Path, out: Path) -> dict[str, Any]:
    """Build separate catalogs atomically after validation; index reference-only inputs as well."""
    import os
    import tempfile

    parsed = [(p, import_workbook(p)) for p in sorted(source.glob("Solace-Cloud-*.xlsx"))]
    if not parsed:
        raise ValueError("no supported Solace Cloud workbooks found")
    out.mkdir(parents=True, exist_ok=True)
    entries = []
    for path, book in parsed:
        filename = f"{book.provider}-{book.broker_version}-{book.source_sha256[:12]}.json"
        target = out / filename
        fd, temp = tempfile.mkstemp(dir=out, prefix=".import-", text=True)
        try:
            with os.fdopen(fd, "w") as stream:
                stream.write(book.model_dump_json(indent=2) + "\n")
            os.replace(temp, target)
        finally:
            if os.path.exists(temp):
                os.unlink(temp)
        entries.append({"source": path.name, "catalog": filename, "provider": book.provider,
                        "broker_version": book.broker_version, "tiers": len(book.sheets),
                        "observations": sum(len(s.observations) for s in book.sheets)})
    reference = [p.name for p in sorted(source.iterdir()) if p.is_file() and p.suffix in (".xlsx", ".pdf")
                 and p.name not in {e["source"] for e in entries}]
    index = {"profiles": entries, "reference_only": reference}
    (out / "index.json").write_text(json.dumps(index, indent=2) + "\n")
    return index
