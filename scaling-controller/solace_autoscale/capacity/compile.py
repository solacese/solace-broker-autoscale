"""Capacity model compiler (§6): performance.xlsx -> versioned JSON.

Reads a Solace Cloud performance workbook, validates it, and emits a compiled model conforming to
``schema.CapacityModel``. The runtime NEVER runs this; it runs in CI / at build time (ADR 0005).

Workbook shape (see docs/capacity-model.md):
  - one sheet per service class ("Solace-Cloud-250", "-1k", ... "-100k")
  - a metadata block (Platform, Instance Type, Spool Disk Size, ...)
  - a "Direct Messaging" table and one or more "Guaranteed Messaging" tables
  - each table: rows=Fanout, cols=Message Size (bytes), split Ingress/Egress halves; values=msg/s

The per-broker capacity curve is the FANOUT=1, INGRESS row (sustained single-stream max at each
size). When multiple Guaranteed tables exist, the most conservative (lowest) value per (size) is
used. Connections and spool ceilings are NOT in the workbook; they are supplied by a
service-class table (``--service-classes``), themselves sourced from Mission Control
``getServiceClasses``.

Requires openpyxl (install extra: ``pip install 'solace-autoscale[compile]'``).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from .schema import COMPILER_SCHEMA_VERSION, CapacityModel

# Map sheet-name size token -> config service_class key.
SHEET_TOKEN_TO_KEY = {
    "250": "enterprise-250",
    "1k": "enterprise-1k",
    "5k": "enterprise-5k",
    "10k": "enterprise-10k",
    "50k": "enterprise-50k",
    "100k": "enterprise-100k",
    "200k": "enterprise-200k",
}

_SIZE_GIB_RE = re.compile(r"([\d.]+)\s*GiB", re.IGNORECASE)


class CompileError(Exception):
    """Raised when the workbook fails a validation rule (§6). Fails the build."""


@dataclass
class CompileResult:
    model: CapacityModel
    notes: list[str] = field(default_factory=list)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _norm(s: object) -> str:
    return str(s).strip().lower() if s is not None else ""


def _read_metadata(rows: list[list]) -> dict[str, object]:
    """Scan the leading metadata block for label:value pairs (label in col B, value col C)."""
    meta: dict[str, object] = {}
    for row in rows[:30]:
        # cells often start with a blank col A
        cells = [c for c in row]
        # find first non-empty label and its following value
        for i in range(len(cells) - 1):
            label = _norm(cells[i])
            if label in ("platform", "instance type", "memory", "spool disk size", "broker mode"):
                meta[label] = cells[i + 1]
    return meta


def _find_tables(rows: list[list]) -> list[tuple[str, int]]:
    """Return (table_type, header_row_index) for each Direct/Guaranteed table.

    ``header_row_index`` points at the 'Fanout | <sizes...>' row.
    """
    tables: list[tuple[str, int]] = []
    pending: str | None = None
    for idx, row in enumerate(rows):
        first_cells = " ".join(_norm(c) for c in row[:3])
        if "direct messaging" in first_cells:
            pending = "direct"
            continue
        if "guaranteed messaging" in first_cells:
            pending = "guaranteed"
            continue
        # header row: contains 'fanout' followed by numeric sizes
        joined = [_norm(c) for c in row]
        if pending and "fanout" in joined:
            tables.append((pending, idx))
            pending = None
    return tables


def _parse_fanout1_ingress(rows: list[list], header_idx: int) -> dict[int, float]:
    """Parse the Ingress half's fanout=1 row → {msg_size_bytes: msg_rate}.

    The header row is: [ , Fanout, s1, s2, ..., sN, (blank), Fanout, s1, ...]. We take the LEFT
    (Ingress) half: sizes are the columns after the first 'Fanout' cell up to the blank/second
    'Fanout'. The fanout=1 data row is the next row whose first numeric cell equals 1.
    """
    header = rows[header_idx]
    # locate first 'fanout' column
    fan_cols = [i for i, c in enumerate(header) if _norm(c) == "fanout"]
    if not fan_cols:
        raise CompileError(f"no Fanout column in header row {header_idx + 1}")
    start = fan_cols[0] + 1
    # sizes run until a blank cell or the second 'fanout'
    end = fan_cols[1] if len(fan_cols) > 1 else len(header)
    sizes: list[tuple[int, int]] = []  # (col_index, size_bytes)
    for col in range(start, end):
        val = header[col] if col < len(header) else None
        if val is None or _norm(val) == "":
            break
        try:
            sizes.append((col, int(float(val))))
        except (TypeError, ValueError):
            break
    if not sizes:
        raise CompileError(f"no message-size columns found for table at row {header_idx + 1}")

    # find the fanout=1 data row below the header
    for r in range(header_idx + 1, min(header_idx + 8, len(rows))):
        row = rows[r]
        fan_cell = row[fan_cols[0]] if fan_cols[0] < len(row) else None
        try:
            if fan_cell is not None and int(float(fan_cell)) == 1:
                out: dict[int, float] = {}
                for col, size in sizes:
                    v = row[col] if col < len(row) else None
                    if v is None or (isinstance(v, str) and v.strip() == ""):
                        continue  # genuinely unmeasured cell → skip
                    fv = float(v)
                    # A present-but-zero/negative measured cell is a broken model: fail the build
                    # (§6) rather than silently dropping it.
                    if fv <= 0:
                        raise CompileError(
                            f"fanout=1 row under header row {header_idx + 1}: size {size} has "
                            f"non-positive measured value {fv}"
                        )
                    out[size] = fv
                return out
        except (TypeError, ValueError):
            continue
    raise CompileError(f"no fanout=1 row found under header row {header_idx + 1}")


def compile_workbook(
    xlsx_path: str | Path,
    service_classes_path: str | Path,
    compiled_at: str,
    platform_hint: str | None = None,
) -> CompileResult:
    """Compile a workbook into a CapacityModel. ``compiled_at`` is injected (no clock read here)."""
    try:
        import openpyxl
    except ImportError as e:  # pragma: no cover
        raise CompileError("openpyxl is required to compile; install 'solace-autoscale[compile]'") from e

    xlsx_path = Path(xlsx_path)
    sc_table = json.loads(Path(service_classes_path).read_text())
    notes: list[str] = []

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    source_sha = _sha256_file(xlsx_path)

    service_classes: dict[str, dict] = {}
    total_rows = 0
    min_size = None
    max_size = None
    max_fanout = 1
    platform = platform_hint

    for ws in wb.worksheets:
        token = _sheet_token(ws.title)
        if token is None:
            notes.append(f"skipped sheet {ws.title!r} (no recognised service-class token)")
            continue
        key = SHEET_TOKEN_TO_KEY[token]
        rows = [list(r) for r in ws.iter_rows(values_only=True)]
        total_rows += len(rows)
        meta = _read_metadata(rows)
        if platform is None and "platform" in meta:
            platform = _norm(meta["platform"])

        tables = _find_tables(rows)
        by_mode: dict[str, list[dict[int, float]]] = {"direct": [], "guaranteed": []}
        for mode, hidx in tables:
            # Structural parse problems (no fanout row, no size columns) are recorded as notes so a
            # stray table does not fail the build; a non-positive MEASURED value is fatal (§6) and
            # is re-raised.
            try:
                by_mode[mode].append(_parse_fanout1_ingress(rows, hidx))
            except CompileError as e:
                if "non-positive" in str(e):
                    raise
                notes.append(f"{ws.title}: {e}")

        delivery: dict[str, dict] = {}
        for mode in ("direct", "guaranteed"):
            variants = [v for v in by_mode[mode] if v]
            if not variants:
                continue
            # conservative merge: min msg_rate per size across variants
            all_sizes = sorted({s for v in variants for s in v})
            buckets = []
            for size in all_sizes:
                rates = [v[size] for v in variants if size in v and v[size] > 0]
                if not rates:
                    continue
                rate = min(rates)
                if len(variants) > 1:
                    notes.append(
                        f"{key}/{mode} size {size}: took conservative min {rate} across "
                        f"{len(variants)} tables"
                    )
                buckets.append({
                    "msg_size_bytes": size,
                    "msg_rate": float(rate),
                    "byte_rate": float(rate) * size,
                })
                min_size = size if min_size is None else min(min_size, size)
                max_size = size if max_size is None else max(max_size, size)
            if buckets:
                delivery[mode] = {"size_buckets": buckets}

        if not delivery:
            notes.append(f"{key}: no delivery curves parsed; skipping service class")
            continue

        sc_meta = sc_table.get(key)
        if sc_meta is None:
            raise CompileError(
                f"service class {key!r} present in workbook but missing from service-classes table "
                f"{service_classes_path}"
            )

        service_classes[key] = {
            "service_class_id": sc_meta["service_class_id"],
            "connections_max": int(sc_meta["connections_max"]),
            "spool_bytes_max": int(sc_meta["spool_bytes_max"]),
            "delivery": delivery,
        }

    wb.close()

    if not service_classes:
        raise CompileError("no service classes compiled from workbook")

    _validate(service_classes)

    model_version = f"{source_sha[:16]}+cs{COMPILER_SCHEMA_VERSION}"
    data = {
        "schema_version": "1",
        "model_version": model_version,
        "synthetic": False,
        "provenance": {
            "source_filename": xlsx_path.name,
            "source_sha256": source_sha,
            "compiled_at": compiled_at,
            "compiler_version": COMPILER_SCHEMA_VERSION,
            "row_count": total_rows,
            "platform": platform,
            "measured_range": {
                "msg_size_bytes": [int(min_size or 0), int(max_size or 0)],
                "fanout": [1, int(max_fanout)],
            },
            "notes": notes,
        },
        "service_classes": service_classes,
    }
    model = CapacityModel.model_validate(data)
    return CompileResult(model=model, notes=notes)


def _sheet_token(title: str) -> str | None:
    low = title.lower()
    for token in SHEET_TOKEN_TO_KEY:
        # match '-250', '250', '-10k' etc as a suffix/word
        if re.search(rf"(^|[-_ ]){re.escape(token)}($|[-_ ])", low):
            return token
    return None


def _validate(service_classes: dict[str, dict]) -> None:
    """§6 validation. Fatal violations raise CompileError."""
    for key, sc in service_classes.items():
        if sc["connections_max"] <= 0:
            raise CompileError(f"{key}: connections_max must be > 0")
        if sc["spool_bytes_max"] <= 0:
            raise CompileError(f"{key}: spool_bytes_max must be > 0")
        for mode, curve in sc["delivery"].items():
            buckets = curve["size_buckets"]
            if not buckets:
                raise CompileError(f"{key}/{mode}: no size buckets")
            prev_size = None
            for b in buckets:
                if b["msg_rate"] <= 0 or b["byte_rate"] <= 0:
                    raise CompileError(
                        f"{key}/{mode} size {b['msg_size_bytes']}: zero/negative capacity"
                    )
                if prev_size is not None and b["msg_size_bytes"] <= prev_size:
                    raise CompileError(f"{key}/{mode}: sizes not strictly ascending")
                # Monotonicity of msg_rate as size grows is expected but not enforced fatally
                # (measurement noise). Zero/negative capacities are already fatal above.
                prev_size = b["msg_size_bytes"]
