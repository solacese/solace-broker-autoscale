"""Managed control hints accelerate authoritative refresh without becoming ownership state."""

from __future__ import annotations

import threading
import time

from fastapi.testclient import TestClient

from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
from solace_autoscale.config import AssignmentConfig, ManagedEventsConfig, MessagingConfig
from solace_autoscale.controller.cli import _wait_for_reconcile
from solace_autoscale.controller.events import ManagedControlBus, ReconcileSchedule, _SmfEndpoint
from solace_autoscale.controller.store import ControllerStore
from solace_autoscale.metrics.fleet import BrokerEndpoint
from solace_autoscale_client.messaging import MessagingClient


def messaging() -> MessagingConfig:
    return MessagingConfig.model_validate({
        "enabled": True,
        "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}],
        "subscriptions": [{"group": "ledger", "topics": ["payments/>"]}],
    })


def test_control_revision_and_outbox_are_atomic_and_bounded(tmp_path):
    store = AssignmentStore(tmp_path / "state.db")
    control = ControllerStore(store)
    control.signal(1, "one")
    control.signal(2, "two")
    assert control.revision() == 2
    assert control.pending_notifications() == [(2, 2.0, "two")]
    control.notification_published(1)
    assert control.pending_notifications() == [(2, 2.0, "two")]
    control.notification_published(2)
    assert control.pending_notifications() == []
    store.close()


def test_subscription_registration_and_ready_emit_revision(tmp_path):
    store = AssignmentStore(tmp_path / "state.db")
    store.upsert_broker(Broker(
        "a", "payments", "vpn", BrokerState.ACTIVE,
        {"smf": "tcp://a:55555", "amqp": "amqp://a:5672"},
    ))
    policy = AssignmentConfig(routing="partitioned", partitions=1)
    events = ManagedEventsConfig(enabled=True, broker_id="a")
    app = create_app(
        store, clock=lambda: 10, policy=policy, fleet_id="payments",
        messaging=messaging(), events=events,
    )
    with TestClient(app) as client:
        first = client.get("/messaging/config").json()
        assert first["events"]["endpoints"]["amqp"] == "amqp://a:5672"
        response = client.post(
            "/messaging/subscriptions", json={"group": "audit", "topics": ["payments/>"]}
        )
        assert response.status_code == 200
        assert client.get("/messaging/config").json()["revision"] == first["revision"] + 1
    control = ControllerStore(store)
    with store.transaction():
        from solace_autoscale.assignment.topics import TopicRegistry

        topics = TopicRegistry(store)
        assert topics.mark_ready(["audit"])
        control.signal(11, "subscription-ready")
    assert control.pending_notifications()[-1][2] == "subscription-ready"
    store.close()


def test_disabled_event_bus_still_waits_until_periodic_deadline(monkeypatch):
    waits = []
    monkeypatch.setattr("solace_autoscale.controller.cli.time.sleep", waits.append)
    assert not _wait_for_reconcile(None, 3.5)
    assert waits == [3.5]


def test_reconcile_schedule_bounds_event_flood_and_preserves_periodic_deadline():
    schedule = ReconcileSchedule.start(10, 0)
    assert schedule.should_run(0, False)
    assert not schedule.should_run(1, True)
    assert schedule.should_run(5, True)
    assert not schedule.should_run(5.1, True)
    assert schedule.should_run(10, False)
    assert schedule.next_periodic == 20
    assert schedule.should_run(15, True)
    assert schedule.should_run(20, False)


def test_control_bus_subscribes_every_active_warm_endpoint(monkeypatch, tmp_path):
    endpoints = [
        BrokerEndpoint.model_validate({
            "broker_id": name, "shard": "payments", "msg_vpn": "vpn",
            "base_url": f"https://{name}.example", "username_env": "U", "password_env": "P",
            "endpoints": {"smf": f"tcp://{name}:55555"}, "role": role,
        })
        for name, role in (("a", "active"), ("b", "warm"))
    ]
    started = []

    def listen(self, link):
        started.append(link.endpoint.broker_id)

    monkeypatch.setattr(ManagedControlBus, "_listen", listen)
    monkeypatch.setattr(ManagedControlBus, "_publish", lambda self: None)
    store = AssignmentStore(tmp_path / "events.db")
    bus = ManagedControlBus(
        "payments", ManagedEventsConfig(enabled=True, broker_id="a"), endpoints,
        {"a": ("u", "p"), "b": ("u", "p")}, ControllerStore(store),
    )
    try:
        for thread in bus.listeners:
            thread.join(1)
        assert sorted(started) == ["a", "b"]
    finally:
        bus.close()
        store.close()


def test_smf_connect_does_not_hold_cache_lock_while_connector_hangs(monkeypatch):
    endpoint = BrokerEndpoint.model_validate({
        "broker_id": "a", "shard": "payments", "msg_vpn": "vpn",
        "base_url": "https://a.example", "username_env": "U", "password_env": "P",
        "endpoints": {"smf": "tcp://a:55555"},
    })
    link = _SmfEndpoint(endpoint, "u", "p", ManagedEventsConfig())
    entered = threading.Event()
    release = threading.Event()

    class Builder:
        def from_properties(self, _): return self
        def with_connection_retry_strategy(self, _): return self
        def with_reconnection_retry_strategy(self, _): return self
        def build(self): return self
        def connect(self):
            entered.set()
            release.wait(2)

    monkeypatch.setattr(
        "solace.messaging.messaging_service.MessagingService.builder", lambda: Builder()
    )
    thread = threading.Thread(target=link.connect)
    thread.start()
    assert entered.wait(1)
    started = time.monotonic()
    link.discard()
    assert time.monotonic() - started < 0.2
    release.set()
    thread.join(1)


def test_control_bus_close_does_not_wait_for_hung_connector(monkeypatch, tmp_path):
    endpoint = BrokerEndpoint.model_validate({
        "broker_id": "a", "shard": "payments", "msg_vpn": "vpn",
        "base_url": "https://a.example", "username_env": "U", "password_env": "P",
        "endpoints": {"smf": "tcp://a:55555"},
    })
    entered = threading.Event()
    release = threading.Event()

    def connect(self):
        entered.set()
        release.wait(2)
        raise OSError("simulated hung connect")

    monkeypatch.setattr(_SmfEndpoint, "connect", connect)
    monkeypatch.setattr(ManagedControlBus, "_publish", lambda self: self.stop.wait())
    store = AssignmentStore(tmp_path / "events.db")
    bus = ManagedControlBus(
        "payments", ManagedEventsConfig(enabled=True, broker_id="a", operation_timeout=.1),
        [endpoint], {"a": ("u", "p")}, ControllerStore(store),
    )
    assert entered.wait(1)
    started = time.monotonic()
    bus.close()
    assert time.monotonic() - started < .3
    release.set()
    store.close()


def test_python_hint_during_http_refresh_is_not_lost(tmp_path, monkeypatch):
    config = {
        "fleet_id": "payments", "partitions": 1, "revision": 1,
        "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}],
        "groups": {"ledger": ["payments/>"]}, "ready_groups": ["ledger"],
        "events": {"enabled": False},
    }
    calls = 0
    second = threading.Event()

    def request(self, path, body=None):
        nonlocal calls
        calls += 1
        if calls == 2:
            self.refresh.set()  # event arrives while the HTTP refresh is in flight
        if calls >= 3:
            second.set()
        return dict(config)

    monkeypatch.setattr(MessagingClient, "_request", request)
    client = MessagingClient(
        "http://unused", state_dir=tmp_path, credentials=lambda _: ("u", "p"), poll_interval=5
    )
    try:
        assert second.wait(1), "in-flight notification did not trigger the next HTTP refresh"
    finally:
        client.close()


def test_trusted_http_revision_only_invalidates_older_assignment(tmp_path, monkeypatch):
    config = {
        "fleet_id": "payments", "partitions": 1, "revision": 4,
        "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}],
        "groups": {"ledger": ["payments/>"]}, "ready_groups": ["ledger"],
        "events": {"enabled": False},
    }
    monkeypatch.setattr(MessagingClient, "_request", lambda *args: dict(config))
    monkeypatch.setattr(MessagingClient, "_run", lambda self: None)
    client = MessagingClient("http://unused", state_dir=tmp_path, credentials=lambda _: ("u", "p"))
    try:
        router = client.outboxes["payments"].router
        router.required_revision = 4
        client.refresh.set()  # body/revision of an untrusted hint is never parsed
        assert router.required_revision == 4
        client._cycle()
        config["revision"] = 5
        client._cycle()
        assert router.required_revision == 5
    finally:
        client.close()
