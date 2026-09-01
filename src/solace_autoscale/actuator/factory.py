"""Actuator construction (ADR 0004, §4).

``actuation.mode: recommend`` means the actuator is NEVER constructed — not instantiated, not held
behind a flag. ``build_actuator`` returns ``None`` in recommend mode. Callers must treat ``None`` as
"no actuator exists" and skip actuation entirely.

Building the actuator does not change the default mode; flipping to scale-up-only/full requires an
explicit config edit by the user.
"""

from __future__ import annotations

from ..capacity.schema import CapacityModel
from ..config import Config
from .base import CloudClient
from .safety import AuditLog, SafetyGate


def build_actuator(
    config: Config,
    model: CapacityModel,
    cloud: CloudClient | None,
    audit_path: str = "./actuator-audit.jsonl",
) -> SafetyGate | None:
    """Return a SafetyGate, or ``None`` in recommend mode (actuator not constructed).

    Refuses to construct if the model is synthetic AND mode is not recommend — a synthetic model
    hard-blocks actuation (§10), so there is no point constructing an actuator that would refuse
    every operation; we surface it as a clear error instead.
    """
    if config.actuation.mode == "recommend":
        return None  # ADR 0004: not constructed, not instantiated

    if cloud is None:
        raise ValueError(
            "actuation.mode is not recommend but no Solace Cloud client was provided; "
            "the actuator is the only component permitted to call the Cloud API"
        )
    if model.synthetic:
        raise ValueError(
            "cannot construct actuator against a synthetic capacity model; a synthetic model "
            "hard-blocks all actuation (§10). Compile a real model first."
        )
    return SafetyGate(config=config, model=model, audit=AuditLog(audit_path), cloud=cloud)
