"""Unit tests for the pure config-replication core (normalise + diff). No broker, no I/O."""

from __future__ import annotations

import ast
from pathlib import Path

from solace_autoscale.configsync import BrokerConfig, diff, normalise, summarise


def _broker(broker_id: str, *objs) -> BrokerConfig:
    bc = BrokerConfig(broker_id=broker_id)
    for kind, raw, parent in objs:
        bc.add(normalise(kind, raw, parent=parent))
    return bc


def test_normalise_strips_volatile_fields():
    obj = normalise("queue", {
        "queueName": "orders", "accessType": "exclusive", "maxMsgSpoolUsage": 5000,
        "msgSpoolUsage": 123, "bindCount": 4,  # volatile → ignored
    })
    assert obj.identity == ("orders",)
    assert dict(obj.attributes) == {"accessType": "exclusive", "maxMsgSpoolUsage": 5000}


def test_diff_creates_missing_object():
    src = _broker("src", ("queue", {"queueName": "orders", "accessType": "exclusive"}, ()))
    tgt = _broker("tgt")
    ops = diff(src, tgt)
    assert len(ops) == 1
    assert ops[0].verb == "create" and ops[0].identity == ("orders",)
    assert ops[0].attributes == {"accessType": "exclusive"}


def test_diff_updates_differing_object():
    src = _broker("src", ("queue", {"queueName": "orders", "accessType": "exclusive"}, ()))
    tgt = _broker("tgt", ("queue", {"queueName": "orders", "accessType": "non-exclusive"}, ()))
    ops = diff(src, tgt)
    assert [o.verb for o in ops] == ["update"]


def test_diff_no_ops_when_identical_ignoring_volatile():
    src = _broker("src", ("queue", {"queueName": "orders", "accessType": "exclusive",
                                     "msgSpoolUsage": 1}, ()))
    tgt = _broker("tgt", ("queue", {"queueName": "orders", "accessType": "exclusive",
                                    "msgSpoolUsage": 999}, ()))  # only volatile differs
    assert diff(src, tgt) == []


def test_additive_mode_never_deletes_but_mirror_does():
    src = _broker("src")
    tgt = _broker("tgt", ("queue", {"queueName": "local-only", "accessType": "exclusive"}, ()))
    assert diff(src, tgt, mode="additive") == []
    mirror = diff(src, tgt, mode="mirror")
    assert [o.verb for o in mirror] == ["delete"]
    assert mirror[0].identity == ("local-only",)


def test_ops_ordered_parents_before_children():
    src = _broker(
        "src",
        ("queueSubscription", {"subscriptionTopic": "orders/>"}, ("orders",)),
        ("queue", {"queueName": "orders", "accessType": "exclusive"}, ()),
    )
    tgt = _broker("tgt")
    ops = diff(src, tgt)
    kinds = [o.kind for o in ops]
    assert kinds.index("queue") < kinds.index("queueSubscription")


def test_summarise_counts():
    src = _broker("src",
                  ("queue", {"queueName": "a", "accessType": "exclusive"}, ()),
                  ("queue", {"queueName": "b", "accessType": "exclusive"}, ()))
    tgt = _broker("tgt", ("queue", {"queueName": "a", "accessType": "non-exclusive"}, ()))
    s = summarise(diff(src, tgt))
    assert s == {"create": 1, "update": 1, "delete": 0}


def test_configsync_core_is_pure_no_forbidden_imports():
    forbidden = {"httpx", "requests", "logging", "sqlite3", "socket", "urllib", "time"}
    base = Path(__file__).resolve().parents[1] / "src" / "solace_autoscale" / "configsync"
    for name in ("model.py", "diff.py", "__init__.py"):
        tree = ast.parse((base / name).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    assert a.name.split(".")[0] not in forbidden, f"{name} imports {a.name}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert node.module.split(".")[0] not in forbidden, f"{name} imports {node.module}"
