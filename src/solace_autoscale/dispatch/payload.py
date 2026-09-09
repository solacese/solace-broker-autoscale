"""Pure payload/topic matching primitives for the dispatch rule engine.

No I/O, no clock, no logging — just deterministic functions from (topic, payload, predicate) to a
bool. Payloads are matched by decoding JSON and reading dotted field paths; non-JSON payloads still
match on raw size / prefix so binary workloads are not excluded.

Kept dependency-free on purpose (no ``jsonpath_ng``): a tiny dotted-path reader covers object keys
and list indices, which is all a routing rule needs.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

#: Operators usable in a payload predicate. ``raw_*`` operate on the undecoded bytes so a rule can
#: match non-JSON payloads (e.g. large binary blobs) by size or byte prefix.
OPERATORS = frozenset({
    "eq", "ne", "in", "nin", "gt", "gte", "lt", "lte",
    "exists", "missing", "prefix", "contains", "regex",
    "raw_size_gt", "raw_size_lt", "raw_prefix",
})

#: Sentinel for "path not present". Shared across the package so ``is`` checks work everywhere.
MISSING = object()
_MISSING = MISSING


class RuleError(ValueError):
    """A rule / predicate that cannot be evaluated as written (bad op, bad regex, ...)."""


def get_path(obj: Any, path: str) -> Any:
    """Read a dotted path out of a decoded JSON value.

    ``order.region`` reads obj["order"]["region"]; a numeric segment indexes a list
    (``items.0.sku``). Returns the ``_MISSING`` sentinel if any segment is absent, so callers can
    distinguish "present and null" from "absent".
    """
    cur = obj
    if path == "":
        return cur
    for seg in path.split("."):
        if isinstance(cur, dict):
            if seg not in cur:
                return _MISSING
            cur = cur[seg]
        elif isinstance(cur, list):
            try:
                idx = int(seg)
            except ValueError:
                return _MISSING
            if idx < 0 or idx >= len(cur):
                return _MISSING
            cur = cur[idx]
        else:
            return _MISSING
    return cur


def decode_json(payload: bytes | str | None) -> Any:
    """Decode a payload to a JSON value, or return ``_MISSING`` if it is not JSON.

    Never raises: a non-JSON payload is a legitimate case handled by ``raw_*`` operators.
    """
    if payload is None:
        return _MISSING
    if isinstance(payload, bytes):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError:
            return _MISSING
    try:
        return json.loads(payload)
    except (json.JSONDecodeError, ValueError):
        return _MISSING


@dataclass(frozen=True)
class Predicate:
    """One payload condition: ``<path> <op> <value>``.

    For ``raw_*`` operators, ``path`` is ignored and the operator acts on the raw payload bytes.
    ``exists``/``missing`` ignore ``value``.
    """

    path: str
    op: str
    value: Any = None

    def __post_init__(self) -> None:
        if self.op not in OPERATORS:
            raise RuleError(f"unknown operator {self.op!r}; valid: {sorted(OPERATORS)}")
        if self.op == "regex":
            try:
                re.compile(str(self.value))
            except re.error as e:
                raise RuleError(f"invalid regex {self.value!r}: {e}") from e


def _as_bytes(payload: bytes | str | None) -> bytes:
    if payload is None:
        return b""
    return payload if isinstance(payload, bytes) else payload.encode("utf-8")


def eval_predicate(pred: Predicate, decoded: Any, payload: bytes | str | None) -> bool:
    """Evaluate a single predicate. Pure. Missing fields are False for every op except ``missing``."""
    op = pred.op

    if op in ("raw_size_gt", "raw_size_lt", "raw_prefix"):
        raw = _as_bytes(payload)
        if op == "raw_size_gt":
            return len(raw) > int(pred.value)
        if op == "raw_size_lt":
            return len(raw) < int(pred.value)
        # raw_prefix
        pref = pred.value
        pref_b = pref.encode("utf-8") if isinstance(pref, str) else bytes(pref)
        return raw.startswith(pref_b)

    actual = get_path(decoded, pred.path)

    if op == "exists":
        return actual is not _MISSING
    if op == "missing":
        return actual is _MISSING
    if actual is _MISSING:
        return False  # any comparison against an absent field is False

    if op == "eq":
        return bool(actual == pred.value)
    if op == "ne":
        return bool(actual != pred.value)
    if op == "in":
        return actual in _iterable(pred.value)
    if op == "nin":
        return actual not in _iterable(pred.value)
    if op in ("gt", "gte", "lt", "lte"):
        return _compare(op, actual, pred.value)
    if op == "prefix":
        return isinstance(actual, str) and actual.startswith(str(pred.value))
    if op == "contains":
        if isinstance(actual, str):
            return str(pred.value) in actual
        if isinstance(actual, list):
            return pred.value in actual
        return False
    if op == "regex":
        return isinstance(actual, str) and re.search(str(pred.value), actual) is not None
    raise RuleError(f"unhandled operator {op!r}")  # pragma: no cover


def _iterable(value: Any) -> Any:
    return value if isinstance(value, (list, tuple, set)) else (value,)


def _compare(op: str, a: Any, b: Any) -> bool:
    try:
        if op == "gt":
            return bool(a > b)
        if op == "gte":
            return bool(a >= b)
        if op == "lt":
            return bool(a < b)
        return bool(a <= b)  # lte
    except TypeError:
        return False  # e.g. comparing str to int — a rule that does not apply, not an error


def topic_matches(pattern: str, topic: str) -> bool:
    """Match a topic against a Solace-style pattern using ``*`` (one level) and ``>`` (rest).

    Levels are ``/``-separated. ``*`` matches exactly one level; ``>`` matches this level and every
    level after it and must be the final token. A pattern with no wildcards is an exact match.
    """
    if pattern == topic:
        return True
    pat = pattern.split("/")
    top = topic.split("/")
    for i, seg in enumerate(pat):
        if seg == ">":
            return i < len(top)  # matches one-or-more remaining levels
        if i >= len(top):
            return False
        if seg == "*":
            continue
        if seg != top[i]:
            return False
    return len(pat) == len(top)
