"""Schema for the compiled capacity model.

The runtime NEVER reads the source Excel (ADR 0005). ``compile.py`` reads a workbook, validates it,
and emits JSON conforming to these models. Everything here is pure data + validation; no I/O.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

COMPILER_SCHEMA_VERSION = "1"

# Delivery modes the capacity model measures. ``mixed`` is a workload concept resolved by the
# decision engine (worst-case of both), not a stored curve.
DELIVERY_MODES = ("direct", "guaranteed")


class SizeBucket(BaseModel):
    """One measured point on a capacity curve for a (service_class, delivery) pair."""

    model_config = ConfigDict(extra="forbid")

    msg_size_bytes: int = Field(gt=0)
    msg_rate: float = Field(gt=0, description="Sustained max messages/sec at this size (fanout=1).")
    byte_rate: float = Field(gt=0, description="msg_rate * msg_size_bytes, bytes/sec.")


class DeliveryCurve(BaseModel):
    model_config = ConfigDict(extra="forbid")

    size_buckets: list[SizeBucket]

    @field_validator("size_buckets")
    @classmethod
    def _sorted_nonempty(cls, v: list[SizeBucket]) -> list[SizeBucket]:
        if not v:
            raise ValueError("delivery curve has no size buckets")
        sizes = [b.msg_size_bytes for b in v]
        if sizes != sorted(sizes):
            raise ValueError("size_buckets must be sorted ascending by msg_size_bytes")
        if len(set(sizes)) != len(sizes):
            raise ValueError("duplicate msg_size_bytes in size_buckets")
        return v

    @property
    def min_size(self) -> int:
        return self.size_buckets[0].msg_size_bytes

    @property
    def max_size(self) -> int:
        return self.size_buckets[-1].msg_size_bytes


class ServiceClassCapacity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_class_id: str = Field(
        description="Mission Control ServiceClassId, e.g. ENTERPRISE_10K_HIGHAVAILABILITY"
    )
    connections_max: int = Field(gt=0, description="ServiceClass.vpnConnections")
    spool_bytes_max: int = Field(gt=0, description="ServiceClass.vpnMaxSpoolSize, bytes")
    delivery: dict[str, DeliveryCurve]

    @field_validator("delivery")
    @classmethod
    def _known_modes(cls, v: dict[str, DeliveryCurve]) -> dict[str, DeliveryCurve]:
        if not v:
            raise ValueError("service class has no delivery curves")
        for mode in v:
            if mode not in DELIVERY_MODES:
                raise ValueError(f"unknown delivery mode {mode!r}; expected one of {DELIVERY_MODES}")
        return v


class MeasuredRange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    msg_size_bytes: tuple[int, int]
    fanout: tuple[int, int]


class Provenance(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_filename: str
    source_sha256: str
    compiled_at: str
    compiler_version: str
    row_count: int
    platform: str | None = None
    measured_range: MeasuredRange
    notes: list[str] = Field(default_factory=list)


class CapacityModel(BaseModel):
    """Top-level compiled model. Loaded at runtime; validated on load."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str
    model_version: str
    provenance: Provenance
    synthetic: bool = False
    warning: str | None = Field(default=None, alias="WARNING")
    service_classes: dict[str, ServiceClassCapacity]

    @field_validator("service_classes")
    @classmethod
    def _nonempty(cls, v: dict[str, ServiceClassCapacity]) -> dict[str, ServiceClassCapacity]:
        if not v:
            raise ValueError("capacity model has no service classes")
        return v
