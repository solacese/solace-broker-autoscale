"""Rule-based dispatch engine (the brain of the smart SHIM).

Pure. Given a message ``(topic, payload)`` and an ordered list of rules, it decides:

  - **which broker** the message goes to (by name; the SHIM resolves that to an endpoint), and
  - a **partition/group key** — a stable string derived from the topic + payload that the target
    broker uses for per-key ordering/stickiness and that the *listener* SHIM reads back to demux, and
  - the **address** to publish to (optionally a rewritten topic).

A rule fires when its topic pattern matches AND every payload predicate matches (AND semantics).
Rules are evaluated in order; the first match wins. If none match, the ``default`` decision applies.
This is deliberately NOT round-robin: the same message always routes the same way, so guaranteed
per-key ordering holds and the listener can reconstruct a coherent stream.

No I/O, no clock, no logging — unit-tested against literals and guarded by a purity test.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .payload import (
    MISSING as _MISSING,
)
from .payload import (
    Predicate,
    RuleError,
    decode_json,
    eval_predicate,
    get_path,
    topic_matches,
)

_TEMPLATE_RE = re.compile(r"\{([^{}]+)\}")


@dataclass(frozen=True)
class Match:
    """The condition half of a rule: a topic pattern and zero or more payload predicates (AND)."""

    topic: str = ">"
    payload: tuple[Predicate, ...] = ()

    def matches(self, topic: str, decoded: Any, raw: bytes | str | None) -> bool:
        if not topic_matches(self.topic, topic):
            return False
        return all(eval_predicate(p, decoded, raw) for p in self.payload)


@dataclass(frozen=True)
class Route:
    """The action half of a rule.

    ``broker`` — target broker name (resolved to an endpoint by the SHIM).
    ``key`` — a template for the partition/group key, e.g. ``"vip.{order.region}"``; ``{topic}`` and
              ``{path.to.field}`` placeholders are filled from the topic and decoded payload.
    ``topic`` — optional template to rewrite the publish address (e.g. ``"vip/{topic}"``); when
                ``None`` the original topic is used.
    """

    broker: str
    key: str | None = None
    topic: str | None = None


@dataclass(frozen=True)
class DispatchRule:
    name: str
    when: Match
    route: Route


@dataclass(frozen=True)
class Decision:
    """The dispatch outcome for one message."""

    broker: str
    address: str          #: topic to publish to (rewritten or original)
    key: str | None       #: partition/group key, or None
    rule: str             #: name of the rule that fired, or "default"
    matched: bool         #: whether a real rule fired (vs. the default fallback)


class NoRouteError(RuleError):
    """No rule matched and no default broker was configured — the message cannot be placed."""


def render_template(template: str, topic: str, decoded: Any) -> str:
    """Fill ``{topic}`` and ``{dotted.path}`` placeholders. Absent fields render as empty string.

    Consecutive separators left by an empty field are collapsed so ``vip.{region}.{tier}`` with a
    missing ``tier`` yields ``vip.region`` rather than ``vip.region.``.
    """
    def sub(m: re.Match[str]) -> str:
        ref = m.group(1)
        if ref == "topic":
            return topic
        val = get_path(decoded, ref)
        if val is _MISSING or val is None:
            return ""
        return str(val)

    out = _TEMPLATE_RE.sub(sub, template)
    # collapse separators around empties and trim, without touching topic '/' segments
    out = re.sub(r"\.{2,}", ".", out).strip(".")
    return out


def evaluate(
    topic: str,
    payload: bytes | str | None,
    rules: list[DispatchRule],
    *,
    default_broker: str | None = None,
) -> Decision:
    """Route one message. First matching rule wins; else the default broker (or NoRouteError)."""
    decoded = decode_json(payload)
    for rule in rules:
        if rule.when.matches(topic, decoded, payload):
            address = (
                render_template(rule.route.topic, topic, decoded)
                if rule.route.topic is not None
                else topic
            )
            key = (
                render_template(rule.route.key, topic, decoded)
                if rule.route.key is not None
                else None
            )
            return Decision(
                broker=rule.route.broker, address=address,
                key=key or None, rule=rule.name, matched=True,
            )
    if default_broker is None:
        raise NoRouteError(
            f"no dispatch rule matched topic {topic!r} and no default_broker is configured"
        )
    return Decision(broker=default_broker, address=topic, key=None, rule="default", matched=False)


def target_brokers(rules: list[DispatchRule], default_broker: str | None) -> list[str]:
    """The set of brokers these rules can ever route to — what the listener SHIM must subscribe to."""
    seen: dict[str, None] = {}
    for r in rules:
        seen.setdefault(r.route.broker, None)
    if default_broker is not None:
        seen.setdefault(default_broker, None)
    return list(seen)


# ---- construction from plain config dicts ---------------------------------------------------

def rule_from_dict(d: dict[str, Any]) -> DispatchRule:
    """Build a rule from the config shape validated by ``config.DispatchRuleSpec``."""
    when = d.get("when", {})
    preds = tuple(
        Predicate(path=p.get("path", ""), op=p["op"], value=p.get("value"))
        for p in when.get("payload", [])
    )
    route = d["route"]
    return DispatchRule(
        name=d["name"],
        when=Match(topic=when.get("topic", ">"), payload=preds),
        route=Route(broker=route["broker"], key=route.get("key"), topic=route.get("topic")),
    )


def rules_from_config(specs: list[dict[str, Any]]) -> list[DispatchRule]:
    return [rule_from_dict(s) for s in specs]


@dataclass
class DispatchPlan:
    """A compiled, reusable dispatch plan: the rules plus the default and derived target set."""

    rules: list[DispatchRule]
    default_broker: str | None = None
    _targets: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._targets = target_brokers(self.rules, self.default_broker)

    @property
    def targets(self) -> list[str]:
        return list(self._targets)

    def decide(self, topic: str, payload: bytes | str | None) -> Decision:
        return evaluate(topic, payload, self.rules, default_broker=self.default_broker)
