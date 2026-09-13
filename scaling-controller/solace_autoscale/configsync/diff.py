"""Pure config diff: given a source-of-truth broker config and a target broker config, produce the
ordered list of SEMPv2 config operations that make the target match the source.

Deterministic and side-effect free — the same two snapshots always yield the same op list, so a
reconcile can be shown as a dry-run plan before anything is applied. The thin SEMP client
(``semp_config.py``) executes these ops under the actuator's dry-run / kill-switch safety.

Two replication modes:
  - ``mirror``  — target should look exactly like source: create missing, update differing, DELETE
                  extras the source does not have.
  - ``additive`` — only create missing and update differing; never delete. Safer default: an edit on
                  the source propagates, but a target-local object is left alone.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .model import KIND_SPECS, BrokerConfig, ConfigObject

Verb = Literal["create", "update", "delete"]

#: Order kinds so parents are created before children and deleted after. Index = create order.
_CREATE_ORDER = ["clientProfile", "aclProfile", "queue", "topicEndpoint", "queueSubscription"]


@dataclass(frozen=True)
class ConfigOp:
    """One replication operation against a target broker."""

    verb: Verb
    kind: str
    identity: tuple[str, ...]
    attributes: dict[str, str | int | float | bool | None]  # body for create/update; {} for delete

    def describe(self) -> str:
        ident = "/".join(self.identity)
        return f"{self.verb} {self.kind} {ident}"


def _kind_rank(kind: str) -> int:
    try:
        return _CREATE_ORDER.index(kind)
    except ValueError:  # pragma: no cover - unknown kinds sort last
        return len(_CREATE_ORDER)


def diff(
    source: BrokerConfig,
    target: BrokerConfig,
    *,
    mode: Literal["mirror", "additive"] = "additive",
    kinds: list[str] | None = None,
) -> list[ConfigOp]:
    """Return ops that make ``target`` match ``source`` for the selected ``kinds``.

    ``kinds`` defaults to every kind in KIND_SPECS. Creates/updates are ordered parents-first;
    deletes (mirror mode only) are ordered children-first.
    """
    selected = set(kinds) if kinds is not None else set(KIND_SPECS)
    ops: list[ConfigOp] = []

    # creates + updates
    for key, src_obj in source.objects.items():
        if src_obj.kind not in selected:
            continue
        tgt_obj = target.objects.get(key)
        if tgt_obj is None:
            ops.append(_op("create", src_obj))
        elif tgt_obj.attributes != src_obj.attributes:
            ops.append(_op("update", src_obj))

    # deletes (mirror only)
    if mode == "mirror":
        for key, tgt_obj in target.objects.items():
            if tgt_obj.kind not in selected:
                continue
            if key not in source.objects:
                ops.append(_op("delete", tgt_obj))

    ops.sort(key=_op_sort_key)
    return ops


def _op(verb: Verb, obj: ConfigObject) -> ConfigOp:
    body = {} if verb == "delete" else obj.attrs
    return ConfigOp(verb=verb, kind=obj.kind, identity=obj.identity, attributes=body)


def _op_sort_key(op: ConfigOp) -> tuple[int, int, tuple[str, ...]]:
    # deletes last, children-first among deletes; creates/updates first, parents-first.
    if op.verb == "delete":
        return (1, -_kind_rank(op.kind), op.identity)
    return (0, _kind_rank(op.kind), op.identity)


def summarise(ops: list[ConfigOp]) -> dict[str, int]:
    """Count ops by verb for a one-line reconcile summary."""
    out = {"create": 0, "update": 0, "delete": 0}
    for op in ops:
        out[op.verb] += 1
    return out
