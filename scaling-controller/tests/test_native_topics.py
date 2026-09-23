"""Native topic rules, immutable groups, readiness and migration boundaries."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from solace_autoscale.assignment.placement import assign
from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
from solace_autoscale.assignment.topics import TopicRegistry, matches, validate_topic
from solace_autoscale.config import AssignmentConfig, MessagingConfig
from solace_autoscale.controller.store import ControllerStore
from solace_autoscale_client.messaging import _matches


@pytest.mark.parametrize(
    "pattern,topic,expected",
    [
        ("payments/>", "payments", False),
        ("payments/>", "payments/a/created", True),
        ("payments/*/created", "payments/a/created", True),
        ("payments/*/created", "payments/a/x/created", False),
        ("payments/*/created", "payments/a/settled", False),
        (">", "payments", True),
    ],
)
def test_smf_matching_and_client_contract_agree(pattern, topic, expected):
    assert matches(pattern, topic) == expected
    assert _matches(pattern, topic) == expected


@pytest.mark.parametrize("pattern", ["a/>/b", "a/pre*", "a//b", "#P2P/QUE/a", "a\x00b"])
def test_reject_unsupported_or_reserved_subscription_patterns(pattern):
    with pytest.raises(ValueError):
        validate_topic(pattern, subscription=True)


def native_config():
    return MessagingConfig.model_validate(
        {"enabled": True, "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}]}
    )


def test_registry_and_api_keep_subscriptions_durable_and_freeze_during_migration(tmp_path):
    db = AssignmentStore(tmp_path / "state.db")
    for broker in ["a", "b"]:
        db.upsert_broker(Broker(broker, "payments", "vpn", BrokerState.ACTIVE, {"smf": f"tcps://{broker}"}))
    app = create_app(
        db,
        policy=AssignmentConfig(routing="partitioned", partitions=4),
        fleet_id="payments",
        messaging=native_config(),
        api_key="test-key",
    )
    headers = {"Authorization": "Bearer test-key"}
    with TestClient(app) as client:
        assert client.get("/messaging/config").status_code == 401
        response = client.post(
            "/messaging/subscriptions", headers=headers, json={"group": "ledger", "topics": ["payments/>"]}
        )
        assert response.status_code == 200 and not response.json()["ready"]
        registry = TopicRegistry(db)
        assign(db, "payments", "partition:0", "guaranteed", 1, 10)
        state = ControllerStore(db)
        with pytest.raises(ValueError, match="prepare all"):
            state.begin("payments", 0, "a", "b", 100)
        registry.mark_ready(["ledger"])
        state.begin("payments", 0, "a", "b", 100)
        same = client.post(
            "/messaging/subscriptions", headers=headers, json={"group": "ledger", "topics": ["payments/>"]}
        )
        assert same.status_code == 200  # A replica/restarted worker is allowed during handover.
        for body in [
            {"group": "audit", "topics": ["payments/>"]},
            {"group": "ledger", "topics": ["inventory/>"]},
        ]:
            assert client.post("/messaging/subscriptions", headers=headers, json=body).status_code == 409
        discovery = client.get(
            "/partitions", headers=headers, params={"shard": "payments", "group": "ledger"}
        )
        partition = discovery.json()["partitions"][0]
        assert len(partition["locations"]) == 2
        assert "/g" in partition["queue_name"]
        assert client.get("/partitions", headers=headers, params={"shard": "payments"}).status_code == 400
    db.close()
    db = AssignmentStore(tmp_path / "state.db")
    assert TopicRegistry(db).groups() == {"ledger": ["payments/>"]}
    assert TopicRegistry(db).ready(["ledger"])
    with pytest.raises(ValueError, match="routing contract changed"):
        TopicRegistry(db).contract({"enabled": False, "routes": []})
    db.close()


def test_broker_metrics_include_live_connections_and_vpn_rates(monkeypatch):
    from solace_autoscale.actuator.solace_cloud import SempConnection
    from solace_autoscale.controller.semp import QueueManager

    queues = QueueManager(
        "payments", {"a": SempConnection("http://127.0.0.1:8080", "u", "p")}, {"a": "vpn"}
    )

    class Response:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self.body

    def request(broker, method, path, body=None):
        del broker, method, body
        if path.endswith("/clients?count=1"):
            return Response({"meta": {"count": 37}})
        return Response({"data": {
            "averageRxMsgRate": 100, "averageTxMsgRate": 180,
            "averageRxByteRate": 100_000, "averageTxByteRate": 180_000,
            "msgSpoolUsage": 4096,
        }})

    monkeypatch.setattr(queues, "_request", request)
    try:
        sample = queues.broker_metrics("a", "payments", 100)
        assert sample.connection_count == 37
        assert sample.ingress_msg_rate == 100
        assert sample.egress_msg_rate == 180
        assert sample.spool_used == 4096
    finally:
        queues.close()


def test_group_fencing_retries_partial_failure_and_waits_for_every_group(tmp_path, monkeypatch):
    from solace_autoscale.controller.semp import QueueManager, QueueStatus

    db = AssignmentStore(tmp_path / "groups.db")
    # Initialise the migration table, as normal service/controller startup does.
    ControllerStore(db)
    registry = TopicRegistry(db)
    for group in ["audit", "ledger"]:
        registry.register(group, ["payments/>"])
    queues = QueueManager("payments", {}, {})
    queues.registry = registry
    ingress = {"audit": True, "ledger": True}
    fail = True

    def fence(broker, shard, partition, enabled, group=None):
        nonlocal fail
        if group == "ledger" and fail:
            fail = False
            raise OSError("temporary SEMP failure")
        ingress[group] = enabled

    monkeypatch.setattr(queues, "_ingress_one", fence)
    monkeypatch.setattr(
        queues,
        "_status_one",
        lambda broker, shard, partition, group=None: QueueStatus(
            0,
            int(group == "ledger"),
            2 if group == "ledger" else 1,
            0,
            ingress[group],
            10,
            100,
        ),
    )
    try:
        with pytest.raises(OSError):
            queues.ingress("a", "payments", 0, False)
        status = queues.status("a", "payments", 0)
        assert status.ingress_enabled  # Any enabled group means the bundle is not fully fenced.
        assert not status.fully_enabled  # Every group must accept ingress during normal ownership.
        assert not status.drained  # One group's outstanding ACK blocks migration.
        assert status.consumers == 1  # Every group needs a bound consumer.
        assert status.spooled_messages == 20  # Group queue counters are delivery copies.
        assert status.spooled_bytes == 200
        queues.ingress("a", "payments", 0, False)
        assert not queues.status("a", "payments", 0).ingress_enabled
    finally:
        queues.close()
        db.close()


def test_shim_extracts_topic_key_without_network_and_recovers_local_outbox(tmp_path, monkeypatch):
    import copy
    import json

    from solace_autoscale_client.messaging import MessagingClient

    config = {
        "fleet_id": "payments",
        "partitions": 4,
        "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}],
        "groups": {"ledger": ["payments/>"]},
        "ready_groups": ["ledger"],
    }
    monkeypatch.setattr(MessagingClient, "_request", lambda *args: copy.deepcopy(config))
    # Exercise persistence deterministically without background delivery.
    monkeypatch.setattr(MessagingClient, "_run", lambda self: None)
    client = MessagingClient("http://unused", state_dir=tmp_path, credentials=lambda _: ("u", "p"))
    client._request = lambda *args: pytest.fail("publish must not make a control-plane request")
    client.publish("payments/account-42/created", {"amount": 25}, event_id="one")
    client.publish("payments/account-42/settled", {"amount": 25}, event_id="two")
    box = client.outboxes["payments"]
    rows = box.db.execute("SELECT partition,payload FROM outbox ORDER BY seq").fetchall()
    assert rows[0][0] == rows[1][0] == box.router.partition_for('["account-42"]')
    assert json.loads(rows[0][1])["topic"] == "payments/account-42/created"
    with pytest.raises(ValueError, match="exactly one"):
        client.publish("inventory/item/created", {}, event_id="wrong-route")
    client._config["ready_groups"] = []
    with pytest.raises(ValueError, match="ready"):
        client.publish("payments/account-42/created", {}, event_id="not-ready")
    client.close()
    with MessagingClient("http://unused", state_dir=tmp_path, credentials=lambda _: ("u", "p")) as restarted:
        assert restarted.pending() == 2
        restarted._config["routes"].append(dict(config["routes"][0]))
        with pytest.raises(ValueError, match="exactly one"):
            restarted.publish("payments/account-42/created", {}, event_id="ambiguous")
    config["partitions"] = 8
    with pytest.raises(ValueError, match="contract changed"):
        MessagingClient("http://unused", state_dir=tmp_path, credentials=lambda _: ("u", "p"))


def test_additive_routes_prepare_again_and_scope_groups(tmp_path):
    db = AssignmentStore(tmp_path / "state.db")
    ControllerStore(db)
    registry = TopicRegistry(db)
    original = native_config().routing_contract()
    registry.contract(original)
    registry.register("ledger", ["payments/>"])
    registry.register("stock", ["inventory/>"])
    registry.mark_ready(["ledger", "stock"])
    assert registry.groups("payments") == {"ledger": ["payments/>"]}
    updated = {"enabled": True, "routes": [*original["routes"],
               {"pattern": "inventory/*/*", "shard": "inventory", "key_levels": [1]}]}
    registry.contract(updated)
    assert not registry.ready(["ledger"])
    assert registry.groups("inventory") == {"stock": ["inventory/>"]}
    with pytest.raises(ValueError, match="routing contract changed"):
        registry.contract(original)
    db.close()


def test_topology_cannot_bypass_auth_or_managed_queue_ownership(tmp_path):
    db = AssignmentStore(tmp_path / "state.db")
    with TestClient(create_app(db, policy=AssignmentConfig(routing="partitioned"),
                              messaging=native_config(), fleet_id="payments", api_key="test")) as client:
        assert client.get("/topology", params={"shard": "payments"}).status_code == 401
        response = client.get("/topology", params={"shard": "payments"},
                              headers={"Authorization": "Bearer test"})
        assert response.status_code == 409
    db.close()
