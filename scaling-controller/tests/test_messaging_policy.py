"""YAML dispatch and subscription policies are enforced by service and application shim."""

import copy
import json

import pytest
from fastapi.testclient import TestClient

from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore
from solace_autoscale.config import AssignmentConfig, Config, MessagingConfig, TopicRoute
from solace_autoscale_client.messaging import MessagingClient


@pytest.mark.parametrize(
    "mode,levels,keys",
    [
        ("by-key", [1], ['["account"]', '["account"]']),
        ("by-topic", [], ['["topic","payments/account/created"]', '["topic","payments/account/settled"]']),
        ("single", [], ['["route","payments/*/*"]', '["route","payments/*/*"]']),
    ],
)
def test_yaml_dispatch_extracts_correct_durable_keys(tmp_path, monkeypatch, mode, levels, keys):
    rule = TopicRoute(pattern="payments/*/*", shard="payments", dispatch=mode, key_levels=levels)
    config = {
        "fleet_id": "payments",
        "partitions": 128,
        "routes": [rule.routing_contract()],
        "groups": {"ledger": ["payments/>"]},
        "ready_groups": ["ledger"],
    }
    monkeypatch.setattr(MessagingClient, "_request", lambda *args: copy.deepcopy(config))
    monkeypatch.setattr(MessagingClient, "_run", lambda self: None)
    with MessagingClient("http://unused", state_dir=tmp_path, credentials=lambda _: ("u", "p")) as client:
        for event in ["created", "settled"]:
            client.publish(f"payments/account/{event}", {}, event_id=event)
        box = client.outboxes["payments"]
        partitions = [row[0] for row in box.db.execute("SELECT partition FROM outbox ORDER BY seq")]
        assert partitions == [box.router.partition_for(k) for k in keys]


def test_yaml_declares_groups_and_refuses_application_created_groups(tmp_path):
    messaging = MessagingConfig.model_validate(
        {
            "enabled": True,
            "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}],
            "allow_dynamic_groups": False,
            "subscriptions": [{"group": "ledger", "topics": ["payments/>"]}],
        }
    )
    db = AssignmentStore(tmp_path / "state.db")
    policy = AssignmentConfig(routing="partitioned", partitions=4)
    with TestClient(create_app(db, policy=policy, fleet_id="payments", messaging=messaging)) as client:
        assert client.get("/messaging/config").json()["groups"] == {"ledger": ["payments/>"]}
        assert (
            client.post(
                "/messaging/subscriptions", json={"group": "ledger", "topics": ["payments/>"]}
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/messaging/subscriptions", json={"group": "audit", "topics": ["payments/>"]}
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/messaging/subscriptions", json={"group": "ledger", "topics": ["audit/>"]}
            ).status_code
            == 409
        )
    # Adding a declared group is an operational change, not a routing-contract change.
    messaging.subscriptions.append(
        type(messaging.subscriptions[0])(group="audit", topics=["payments/*/created"])
    )
    with TestClient(create_app(db, policy=policy, fleet_id="payments", messaging=messaging)) as client:
        assert "audit" in client.get("/messaging/config").json()["groups"]
    db.close()


def test_original_native_contract_stays_compatible():
    config = MessagingConfig.model_validate(
        {"enabled": True, "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}]}
    )
    assert json.loads(json.dumps(config.routing_contract())) == {
        "enabled": True,
        "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}],
    }


@pytest.mark.parametrize(
    "rule",
    [
        {"dispatch": "by-key"},
        {"dispatch": "single", "key_levels": [1]},
        {"dispatch": "by-topic", "key_levels": [1]},
        {"dispatch": "round-robin"},
    ],
)
def test_ambiguous_dispatch_settings_refused(rule):
    with pytest.raises(ValueError):
        TopicRoute(pattern="payments/*/*", shard="payments", **rule)


@pytest.mark.parametrize(
    "override",
    [
        {"trigger_utilization": 0.6},
        {"target_utilization": 0.9},
        {"scale_up_window": "1s"},
    ],
)
def test_invalid_inherited_scaling_policy_refused(override):
    with pytest.raises(ValueError):
        Config.model_validate({"automation": {"shards": {"payments": override}}})


def test_subscriber_uses_yaml_patterns_when_app_only_names_group(tmp_path, monkeypatch):
    import solace_autoscale_client.messaging as module

    config = {
        "fleet_id": "payments",
        "partitions": 4,
        "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}],
        "groups": {"ledger": ["payments/>"]},
        "ready_groups": [],
    }
    requests = []

    def request(self, path, body=None):
        if body is not None:
            requests.append(body)
            return {"shards": ["payments"]}
        return copy.deepcopy(config)

    class Workers:
        def __init__(self, *args, **kwargs):
            pass

        def close(self):
            pass

    monkeypatch.setattr(MessagingClient, "_request", request)
    monkeypatch.setattr(MessagingClient, "_run", lambda self: None)
    monkeypatch.setattr(module, "ManagedConsumers", Workers)
    with MessagingClient("http://unused", state_dir=tmp_path, credentials=lambda _: ("u", "p")) as client:
        client.subscribe(group="ledger", handler=lambda message: None, timeout=0)
        assert requests == [{"group": "ledger", "topics": ["payments/>"]}]
        with pytest.raises(ValueError, match="declared"):
            client.subscribe(group="unknown", handler=lambda message: None, timeout=0)
