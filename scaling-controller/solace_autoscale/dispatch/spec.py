"""Portable rule-spec serialization for the smart shim.

The rule engine (``rules.py``) is pure Python, but a rule set is not tied to Python: any language
adapter should be able to load the same rules and route identically. This module round-trips a
:class:`~solace_autoscale.dispatch.rules.DispatchPlan` to and from a plain, JSON-serializable dict,
so the rules can be authored and tested here and shipped as data to a native adapter.

The dict shape is exactly the config shape ``rules_from_config`` already accepts (one ``rules`` list
of ``{name, when:{topic, payload:[{path, op, value}]}, route:{broker, key, topic}}`` objects) plus a
``default_broker`` and a ``version``. ``from_spec`` reuses ``rules_from_config`` so the loader and the
existing config path never drift.

Pure: no I/O, no clock, no logging. ``to_spec`` returns a dict and ``from_spec`` takes one;
``json.dumps``/``json.load`` stay with the caller.

Any conforming adapter, in any language, must honour the same evaluation semantics:
  - topic match uses Solace wildcards: ``*`` matches exactly one level, ``>`` matches the rest;
  - a rule fires when the topic matches AND every payload predicate matches (AND across predicates);
  - rules are evaluated in order and the first match wins; if none match, ``default_broker`` applies;
  - ``key`` and ``route.topic`` are templates: ``{topic}`` and ``{dotted.path}`` placeholders are
    filled from the topic and decoded JSON payload, absent fields render empty, and runs of ``.``
    left by an empty field are collapsed and trimmed.
"""

from __future__ import annotations

from typing import Any

from .rules import DispatchPlan, DispatchRule, rules_from_config

#: Bump when the spec shape changes in a way adapters must gate on.
SPEC_VERSION = 1


def _predicate_to_dict(path: str, op: str, value: Any) -> dict[str, Any]:
    d: dict[str, Any] = {"op": op}
    if path:
        d["path"] = path
    if value is not None:
        d["value"] = value
    return d


def rule_to_dict(rule: DispatchRule) -> dict[str, Any]:
    """Serialize one rule to the portable ``{name, when, route}`` shape."""
    when: dict[str, Any] = {"topic": rule.when.topic}
    if rule.when.payload:
        when["payload"] = [
            _predicate_to_dict(p.path, p.op, p.value) for p in rule.when.payload
        ]
    route: dict[str, Any] = {"broker": rule.route.broker}
    if rule.route.key is not None:
        route["key"] = rule.route.key
    if rule.route.topic is not None:
        route["topic"] = rule.route.topic
    return {"name": rule.name, "when": when, "route": route}


def to_spec(plan: DispatchPlan) -> dict[str, Any]:
    """Serialize a plan to a JSON-serializable spec dict (round-trips via :func:`from_spec`)."""
    return {
        "version": SPEC_VERSION,
        "default_broker": plan.default_broker,
        "rules": [rule_to_dict(r) for r in plan.rules],
    }


def from_spec(spec: dict[str, Any]) -> DispatchPlan:
    """Build a plan from a spec dict produced by :func:`to_spec` or hand-authored to the same shape.

    Reuses ``rules_from_config`` so the loader shares one construction path with the config file
    format and cannot drift from it.
    """
    rules = rules_from_config(spec.get("rules", []))
    return DispatchPlan(rules=rules, default_broker=spec.get("default_broker"))
