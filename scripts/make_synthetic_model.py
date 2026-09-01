"""Generate models/synthetic-v0.json with obviously fabricated round numbers (§6).

This model is committed on purpose and is the only committed model. It carries synthetic:true and a
WARNING, so the report prints a banner and actuation is hard-blocked. Round numbers make it obvious
the data is fake.
"""

from __future__ import annotations

import json
from pathlib import Path

# Obviously-fake round numbers. NOT measured. Do not use for planning.
GB = 1_000_000_000

DIRECT_SIZES = [100, 1000, 10000, 100000]
GUARANTEED_SIZES = [100, 1000, 10000, 100000]


def curve(sizes: list[int], base_msg_rate_at_100: int) -> list[dict]:
    """Round, monotonically-decreasing msg_rate as size grows; byte_rate = msg_rate * size."""
    out = []
    for size in sizes:
        # halve the rate per 10x size, kept to round numbers
        factor = 100 / size
        msg_rate = round(base_msg_rate_at_100 * factor, -2) or 100
        out.append({
            "msg_size_bytes": size,
            "msg_rate": float(msg_rate),
            "byte_rate": float(msg_rate * size),
        })
    return out


def service_class(sc_id: str, connections: int, spool_gb: int, direct_base: int, guar_base: int) -> dict:
    return {
        "service_class_id": sc_id,
        "connections_max": connections,
        "spool_bytes_max": spool_gb * GB,
        "delivery": {
            "direct": {"size_buckets": curve(DIRECT_SIZES, direct_base)},
            "guaranteed": {"size_buckets": curve(GUARANTEED_SIZES, guar_base)},
        },
    }


model = {
    "schema_version": "1",
    "model_version": "synthetic-v0",
    "WARNING": "synthetic placeholder data, not measured, do not use for planning",
    "synthetic": True,
    "provenance": {
        "source_filename": "SYNTHETIC (no workbook)",
        "source_sha256": "0" * 64,
        "compiled_at": "1970-01-01T00:00:00Z",
        "compiler_version": "1",
        "row_count": 0,
        "platform": "synthetic",
        "measured_range": {"msg_size_bytes": [100, 100000], "fanout": [1, 1]},
        "notes": ["Fabricated round numbers. Replace with a compiled model before planning."],
    },
    "service_classes": {
        "enterprise-250": service_class("ENTERPRISE_250_HIGHAVAILABILITY", 250, 25, 100000, 10000),
        "enterprise-1k": service_class("ENTERPRISE_1K_HIGHAVAILABILITY", 1000, 100, 200000, 20000),
        "enterprise-5k": service_class("ENTERPRISE_5K_HIGHAVAILABILITY", 5000, 250, 400000, 40000),
        "enterprise-10k": service_class("ENTERPRISE_10K_HIGHAVAILABILITY", 10000, 500, 800000, 80000),
        "enterprise-50k": service_class("ENTERPRISE_50K_HIGHAVAILABILITY", 50000, 1000, 1600000, 160000),
        "enterprise-100k": service_class("ENTERPRISE_100K_HIGHAVAILABILITY", 100000, 1500, 3200000, 320000),
    },
}


def main() -> None:
    out = Path(__file__).resolve().parents[1] / "models" / "synthetic-v0.json"
    out.write_text(json.dumps(model, indent=2) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
