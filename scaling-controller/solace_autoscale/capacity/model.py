"""Runtime capacity lookup over a compiled model.

Pure: no I/O, no clock. ``load_model`` is the only function that touches disk, and it is called by
callers of the engine (CLI), never by the engine itself.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .schema import CapacityModel, DeliveryCurve, ServiceClassCapacity


@dataclass(frozen=True)
class CapacityPoint:
    """Per-broker maxima for a given (service_class, msg_size, delivery) query."""

    msg_rate: float
    byte_rate: float
    connections: int
    spool_bytes: int
    interpolated: bool
    extrapolated: bool
    #: The measured [min, max] size range for the delivery curve, for extrapolation reporting.
    measured_size_range: tuple[int, int]


def load_model(path: str | Path) -> CapacityModel:
    """Load and validate a compiled capacity model from JSON. The only disk read here."""
    data = json.loads(Path(path).read_text())
    return CapacityModel.model_validate(data)


def _interp(curve: DeliveryCurve, size: float) -> tuple[float, float, bool, bool]:
    """Return (msg_rate, byte_rate, interpolated, extrapolated) for a message size.

    Linear interpolation between bracketing buckets. A size outside the measured range clamps to the
    nearest bucket and is flagged ``extrapolated`` (the engine surfaces this as a §5.6 warning
    rather than extrapolating a fabricated slope).
    """
    buckets = curve.size_buckets
    if size <= buckets[0].msg_size_bytes:
        b = buckets[0]
        return b.msg_rate, b.byte_rate, False, size < b.msg_size_bytes
    if size >= buckets[-1].msg_size_bytes:
        b = buckets[-1]
        return b.msg_rate, b.byte_rate, False, size > b.msg_size_bytes
    for lo, hi in zip(buckets, buckets[1:], strict=False):
        if lo.msg_size_bytes <= size <= hi.msg_size_bytes:
            span = hi.msg_size_bytes - lo.msg_size_bytes
            frac = (size - lo.msg_size_bytes) / span if span else 0.0
            msg_rate = lo.msg_rate + frac * (hi.msg_rate - lo.msg_rate)
            byte_rate = lo.byte_rate + frac * (hi.byte_rate - lo.byte_rate)
            return msg_rate, byte_rate, True, False
    # Unreachable given sorted buckets, but keep it total.
    b = buckets[-1]
    return b.msg_rate, b.byte_rate, False, True


def lookup(
    model: CapacityModel,
    service_class: str,
    msg_size_bytes: float,
    delivery: str,
) -> CapacityPoint:
    """Per-broker capacity for the query. Raises KeyError for an unknown class/delivery.

    ``delivery='mixed'`` resolves to the more conservative (lower msg_rate) of direct/guaranteed at
    this size, since a mixed workload cannot be promised the higher figure.
    """
    sc: ServiceClassCapacity = model.service_classes[service_class]

    if delivery == "mixed":
        modes = [m for m in ("direct", "guaranteed") if m in sc.delivery]
        if not modes:
            raise KeyError(f"service class {service_class!r} has no direct/guaranteed curves")
        points = [_interp(sc.delivery[m], msg_size_bytes) for m in modes]
        # conservative: pick the mode with the lowest msg_rate at this size
        msg_rate, byte_rate, interp, extrap = min(points, key=lambda p: p[0])
        # measured range: intersection isn't meaningful; report the chosen mode's - recompute
        chosen = min(zip(modes, points, strict=True), key=lambda mp: mp[1][0])[0]
        curve = sc.delivery[chosen]
    else:
        if delivery not in sc.delivery:
            raise KeyError(f"service class {service_class!r} has no {delivery!r} curve")
        curve = sc.delivery[delivery]
        msg_rate, byte_rate, interp, extrap = _interp(curve, msg_size_bytes)

    return CapacityPoint(
        msg_rate=msg_rate,
        byte_rate=byte_rate,
        connections=sc.connections_max,
        spool_bytes=sc.spool_bytes_max,
        interpolated=interp,
        extrapolated=extrap,
        measured_size_range=(curve.min_size, curve.max_size),
    )
