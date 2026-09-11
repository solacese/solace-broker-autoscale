"""Unit tests for the pure dispatch rule engine (no broker, no I/O)."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from solace_autoscale.dispatch import (
    SPEC_VERSION,
    DispatchPlan,
    NoRouteError,
    Predicate,
    eval_predicate,
    evaluate,
    from_spec,
    render_template,
    rules_from_config,
    to_spec,
    topic_matches,
)
from solace_autoscale.dispatch.payload import decode_json, get_path
from solace_autoscale.dispatch.rules import DispatchRule, Match, Route

# ---- topic matching -------------------------------------------------------------------------

@pytest.mark.parametrize("pattern,topic,expected", [
    ("orders/>", "orders/eu/new", True),
    ("orders/>", "orders", False),          # '>' needs at least one more level
    ("orders/*/new", "orders/eu/new", True),
    ("orders/*/new", "orders/eu/us/new", False),
    ("orders/eu/new", "orders/eu/new", True),
    (">", "anything/at/all", True),
    ("orders/*", "orders/eu", True),
    ("orders/*", "orders/eu/new", False),
])
def test_topic_matches(pattern, topic, expected):
    assert topic_matches(pattern, topic) is expected


# ---- payload predicates ---------------------------------------------------------------------

def test_get_path_object_and_list():
    obj = {"order": {"region": "EU"}, "items": [{"sku": "A"}, {"sku": "B"}]}
    assert get_path(obj, "order.region") == "EU"
    assert get_path(obj, "items.1.sku") == "B"


def test_predicate_numeric_and_membership():
    decoded = {"amount": 1500, "priority": "high"}
    assert eval_predicate(Predicate("amount", "gt", 1000), decoded, None)
    assert not eval_predicate(Predicate("amount", "gt", 2000), decoded, None)
    assert eval_predicate(Predicate("priority", "in", ["high", "urgent"]), decoded, None)
    assert not eval_predicate(Predicate("priority", "in", ["low"]), decoded, None)


def test_predicate_missing_field_is_false():
    assert not eval_predicate(Predicate("nope", "eq", 1), {"a": 1}, None)
    assert eval_predicate(Predicate("nope", "missing", None), {"a": 1}, None)
    assert eval_predicate(Predicate("a", "exists", None), {"a": 1}, None)


def test_predicate_type_mismatch_is_false_not_error():
    # comparing a string field with a numeric bound must not raise
    assert not eval_predicate(Predicate("p", "gt", 10), {"p": "high"}, None)


def test_raw_operators_for_non_json():
    assert eval_predicate(Predicate("", "raw_size_gt", 3), None, b"hello")
    assert eval_predicate(Predicate("", "raw_prefix", "GIF"), None, b"GIF89a")
    assert decode_json(b"\xff\xfenot json") is not None  # returns sentinel, never raises


def test_bad_operator_and_bad_regex_raise():
    from solace_autoscale.dispatch import RuleError
    with pytest.raises(RuleError):
        Predicate("a", "not_an_op", 1)
    with pytest.raises(RuleError):
        Predicate("a", "regex", "(")


# ---- template rendering ---------------------------------------------------------------------

def test_render_template_topic_and_fields():
    decoded = {"order": {"region": "EU"}}
    assert render_template("vip.{order.region}", "orders/new", decoded) == "vip.EU"
    assert render_template("{topic}", "orders/new", decoded) == "orders/new"


def test_render_template_collapses_missing_segments():
    assert render_template("vip.{region}.{tier}", "t", {"region": "EU"}) == "vip.EU"


# ---- end-to-end evaluate --------------------------------------------------------------------

def _vip_rules():
    return [
        DispatchRule(
            name="vip-orders",
            when=Match(topic="orders/>", payload=(Predicate("priority", "in", ["high", "urgent"]),)),
            route=Route(broker="broker-2", key="vip.{region}", topic="vip/{topic}"),
        ),
        DispatchRule(
            name="big-orders",
            when=Match(topic="orders/>", payload=(Predicate("amount", "gt", 1000),)),
            route=Route(broker="broker-3", key="big.{region}"),
        ),
    ]


def test_first_match_wins_and_builds_key_and_address():
    d = evaluate("orders/eu/new", b'{"priority":"high","region":"EU","amount":500}', _vip_rules(),
                 default_broker="broker-1")
    assert d.broker == "broker-2"
    assert d.rule == "vip-orders"
    assert d.key == "vip.EU"
    assert d.address == "vip/orders/eu/new"
    assert d.matched


def test_second_rule_matches_when_first_does_not():
    d = evaluate("orders/eu/new", b'{"priority":"low","region":"EU","amount":5000}', _vip_rules(),
                 default_broker="broker-1")
    assert d.broker == "broker-3"
    assert d.rule == "big-orders"
    assert d.key == "big.EU"
    assert d.address == "orders/eu/new"  # no topic template → unchanged


def test_default_broker_when_no_rule_matches():
    d = evaluate("telemetry/x", b'{"priority":"low"}', _vip_rules(), default_broker="broker-1")
    assert d.broker == "broker-1"
    assert d.rule == "default"
    assert d.key is None
    assert not d.matched


def test_no_route_raises_without_default():
    with pytest.raises(NoRouteError):
        evaluate("telemetry/x", b"{}", _vip_rules(), default_broker=None)


def test_non_json_payload_routes_by_raw_size():
    rules = [DispatchRule(
        name="big-blob",
        when=Match(topic="uploads/>", payload=(Predicate("", "raw_size_gt", 4),)),
        route=Route(broker="broker-blob", key="blob"),
    )]
    d = evaluate("uploads/img", b"\x00\x01\x02\x03\x04\x05", rules, default_broker="broker-1")
    assert d.broker == "broker-blob"


def test_dispatch_plan_targets_are_the_listener_subscription_set():
    plan = DispatchPlan(rules=_vip_rules(), default_broker="broker-1")
    assert set(plan.targets) == {"broker-2", "broker-3", "broker-1"}
    assert plan.decide("orders/eu/new", b'{"priority":"urgent","region":"US"}').key == "vip.US"


def test_rules_from_config_roundtrip():
    specs = [{
        "name": "vip",
        "when": {"topic": "orders/>", "payload": [{"path": "priority", "op": "eq", "value": "high"}]},
        "route": {"broker": "broker-2", "key": "vip.{region}"},
    }]
    rules = rules_from_config(specs)
    d = evaluate("orders/x", b'{"priority":"high","region":"EU"}', rules, default_broker="b1")
    assert d.broker == "broker-2" and d.key == "vip.EU"


# ---- portable spec round-trip ---------------------------------------------------------------

_SPEC_RULES = [
    {
        "name": "vip-orders",
        "when": {"topic": "orders/>", "payload": [{"path": "priority", "op": "in",
                                                   "value": ["high", "urgent"]}]},
        "route": {"broker": "broker-vip", "key": "vip.{region}", "topic": "vip/{topic}"},
    },
    {
        "name": "big-orders",
        "when": {"topic": "orders/>", "payload": [{"path": "amount", "op": "gt", "value": 1000}]},
        "route": {"broker": "broker-big", "key": "big.{region}"},
    },
    {
        "name": "telemetry",
        "when": {"topic": "telemetry/>"},
        "route": {"broker": "broker-bulk"},
    },
]

# (topic, payload) probes that exercise every rule + the default fallback.
_PROBES = [
    ("orders/eu/new", '{"priority": "high", "region": "EU"}'),
    ("orders/us/new", '{"priority": "low", "region": "US", "amount": 5000}'),
    ("telemetry/x", '{"v": 1}'),
    ("misc/thing", '{"anything": true}'),
    ("orders/eu/new", "not-json"),
]


def test_spec_roundtrip_decides_identically():
    plan = DispatchPlan(rules=rules_from_config(_SPEC_RULES), default_broker="broker-default")
    rebuilt = from_spec(to_spec(plan))
    assert rebuilt.default_broker == plan.default_broker
    assert rebuilt.targets == plan.targets
    for topic, payload in _PROBES:
        a, b = plan.decide(topic, payload), rebuilt.decide(topic, payload)
        assert (a.broker, a.address, a.key, a.rule, a.matched) == \
               (b.broker, b.address, b.key, b.rule, b.matched), (topic, payload)


def test_spec_is_json_serialisable_and_versioned():
    import json
    plan = DispatchPlan(rules=rules_from_config(_SPEC_RULES), default_broker="broker-default")
    spec = to_spec(plan)
    assert spec["version"] == SPEC_VERSION
    # survives a real JSON round-trip (no non-serialisable types leaked in)
    reloaded = from_spec(json.loads(json.dumps(spec)))
    assert reloaded.targets == plan.targets


# ---- purity guard ---------------------------------------------------------------------------

def test_dispatch_engine_is_pure_no_forbidden_imports():
    forbidden = {"httpx", "requests", "logging", "sqlite3", "socket", "urllib", "time", "proton"}
    base = Path(__file__).resolve().parents[1] / "solace_autoscale" / "dispatch"
    for name in ("rules.py", "payload.py", "spec.py", "__init__.py"):
        tree = ast.parse((base / name).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    assert a.name.split(".")[0] not in forbidden, f"{name} imports {a.name}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert node.module.split(".")[0] not in forbidden, f"{name} imports {node.module}"
