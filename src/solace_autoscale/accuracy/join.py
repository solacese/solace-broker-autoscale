"""Join later samples back to compute observed capacity at observed load (§7).

When a broker is running at or near saturation on an axis, the load it sustains across its brokers
is an empirical measurement of per-broker capacity. We compare that to what the model predicted for
the same size bucket, and record the pair for the accuracy report.

Pure helpers; the recorder does the I/O.
"""

from __future__ import annotations

from ..capacity.model import lookup
from ..capacity.schema import CapacityModel
from ..decision.engine import _axis_capacity, _axis_raw_demand  # reuse the engine's axis maths
from ..decision.types import AXES, Axis, MetricSample
from .recorder import AccuracyRecorder


def nearest_bucket(model: CapacityModel, service_class: str, delivery: str, size: float) -> int:
    curve = model.service_classes[service_class].delivery[delivery]
    return min((b.msg_size_bytes for b in curve.size_buckets), key=lambda s: abs(s - size))


def record_observed_capacity(
    recorder: AccuracyRecorder,
    model: CapacityModel,
    service_class: str,
    delivery: str,
    shard: str,
    sample: MetricSample,
    ts: float,
    *,
    saturation_floor: float = 0.85,
) -> int:
    """For each axis where the observed load is a meaningful capacity signal, record observed vs
    predicted per-broker capacity.

    Observed per-broker capacity on an axis = observed fleet demand / current_brokers. We only record
    an axis when its observed utilisation (observed demand / predicted capacity) is above
    ``saturation_floor`` — below that, the load says nothing about the ceiling. Returns how many
    axis observations were recorded.
    """
    if sample.current_brokers <= 0:
        return 0
    cap = lookup(model, service_class, sample.avg_msg_size, delivery)
    bucket = nearest_bucket(model, service_class, delivery, sample.avg_msg_size)
    recorded = 0
    for axis_name in AXES:
        axis = Axis(axis_name)
        predicted = _axis_capacity(axis, cap)
        if predicted <= 0:
            continue
        demand = _axis_raw_demand(axis, sample, sample.current_brokers, mesh=False)
        observed_per_broker = demand / sample.current_brokers
        utilisation = observed_per_broker / predicted
        if utilisation < saturation_floor:
            continue  # not near the ceiling → not a capacity measurement
        recorder.record_observation(
            ts=ts, shard=shard, msg_size_bucket=bucket, axis=axis_name,
            observed_capacity=observed_per_broker, predicted_capacity=predicted,
            model_version=model.model_version,
        )
        recorded += 1
    return recorded
