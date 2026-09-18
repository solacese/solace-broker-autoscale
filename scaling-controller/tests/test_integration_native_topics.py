"""Native topic fanout, wildcard groups, exclusive replicas and automatic broker handover."""

from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager

import pytest
import uvicorn

from solace_autoscale.actuator.solace_cloud import SempConnection
from solace_autoscale.assignment.placement import assign
from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
from solace_autoscale.assignment.topics import TopicRegistry
from solace_autoscale.config import AssignmentConfig, AutomationConfig, MessagingConfig
from solace_autoscale.controller.migration import MigrationEngine
from solace_autoscale.controller.semp import QueueManager
from solace_autoscale.controller.store import ControllerStore
from solace_autoscale_client import MessagingClient

pytestmark = pytest.mark.integration


@contextmanager
def live_api(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


def eventually(condition, timeout=10):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition did not become true"
        time.sleep(0.05)


def test_native_topic_fanout_and_migration_with_single_application_api(tmp_path, monkeypatch):
    fleet = "native-" + str(int(time.time() * 1000))
    db = AssignmentStore(tmp_path / "state.db")
    for broker, port in [("a", 15556), ("b", 15557)]:
        db.upsert_broker(
            Broker(broker, "payments", "default", BrokerState.ACTIVE, {"smf": f"tcp://127.0.0.1:{port}"})
        )
    messaging = MessagingConfig.model_validate(
        {"enabled": True, "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}]}
    )
    app = create_app(
        db, policy=AssignmentConfig(routing="partitioned", partitions=1), fleet_id=fleet, messaging=messaging
    )
    queues = QueueManager(
        fleet,
        {
            b: SempConnection(f"http://127.0.0.1:{p}", "admin", "admin")
            for b, p in [("a", 18081), ("b", 18082)]
        },
        {"a": "default", "b": "default"},
        100,
    )
    registry = TopicRegistry(db)
    queues.registry = registry
    queues.client_username = "default"
    queues.configure_native("a")
    queues.configure_native("b")
    queues._native_configured.clear()
    queues.configure_native("a")  # Existing client profiles must reconcile after restart.
    queues.configure_native("b")
    assign(db, "payments", "partition:0", "guaranteed", 1, 1)
    state = ControllerStore(db)
    gate = threading.Event()
    ledger, audit, stock = [], [], []

    def process(message):
        if not gate.is_set():
            raise RuntimeError("slow business transaction")
        ledger.append((message.event_id, message.topic))

    with (
        live_api(app) as url,
        MessagingClient(
            url,
            state_dir=tmp_path / "client",
            credentials=lambda _: ("default", "default"),
            poll_interval=0.05,
        ) as client,
        MessagingClient(
            url,
            state_dir=tmp_path / "replica",
            credentials=lambda _: ("default", "default"),
            poll_interval=0.05,
        ) as replica,
    ):
        try:
            client.subscribe(["payments/>", "payments/*/created"], group="ledger", handler=process, timeout=0)
            replica.subscribe(
                ["payments/>", "payments/*/created"], group="ledger", handler=process, timeout=0
            )
            client.subscribe(
                "payments/*/created", group="audit", handler=lambda m: audit.append(m.event_id), timeout=0
            )
            client.subscribe(
                "inventory/>", group="stock", handler=lambda m: stock.append(m.event_id), timeout=0
            )
            queues.prepare("a", "payments", 0, enabled=True)
            queues.prepare("a", "payments", 0, enabled=True)  # Subscription creation must be idempotent.
            registry.mark_ready(list(registry.groups()))
            eventually(lambda: queues.status("a", "payments", 0).consumers >= 1)
            eventually(lambda: set(client._config["ready_groups"]) == {"ledger", "audit", "stock"})
            location = client.outboxes["payments"].router.resolve_partition(0)
            with pytest.raises(Exception, match="[Nn]o [Ss]ubscription [Mm]atch"):
                client.pool.send(location, "unmatched", b"{}", topic="unmatched/topic")
            client.publish("payments/account-1/created", {"amount": 1}, event_id="one")
            client.publish("payments/account-1/settled", {"amount": 1}, event_id="two")
            assert client.flush(10)
            eventually(lambda: audit == ["one"])
            assert not stock
            assert not queues.status("a", "payments", 0).drained
            state.begin("payments", 0, "a", "b", 100)
            engine = MigrationEngine(
                state,
                queues,
                AutomationConfig(poll_interval=1, migration_grace=2, empty_settle=1, migration_timeout=100),
            )
            engine.advance(state.pending()[0], 100)
            eventually(lambda: queues.status("b", "payments", 0).consumers >= 1)
            for now in [101, 102, 103]:
                engine.advance(state.pending()[0], now)
            assert state.pending()[0].phase == "draining"
            client.publish("payments/account-1/created", {"amount": 2}, event_id="three")
            assert not client.flush(0.3)
            assert client.pending() == 1
            gate.set()
            eventually(lambda: len(ledger) == 2)
            engine = MigrationEngine(ControllerStore(db), queues, engine.policy)
            for now in range(104, 115):
                if not state.pending():
                    break
                engine.advance(state.pending()[0], now)
                time.sleep(0.1)
            assert not state.pending()
            assert client.flush(10)
            eventually(lambda: len(ledger) == 3 and len(audit) == 2)
            assert [event for event, topic in ledger] == ["one", "two", "three"]
            assert audit == ["one", "three"]
            assert not stock
            assert queues.status("a", "payments", 0).drained
            assert not queues.status("a", "payments", 0).ingress_enabled
            assert all(topic.startswith("payments/account-1/") for _, topic in ledger)
            assert replica.workers["ledger", "payments"].flows
            assert ("stock", "payments") not in client.workers

            # Known broker delivery survives a temporary control-plane outage.
            def unavailable(*args, **kwargs):
                raise OSError("controller unavailable")

            monkeypatch.setattr(client, "_request", unavailable)
            eventually(lambda: client.last_error == "OSError")
            for i in range(128):
                client.publish("payments/account-1/settled", {"amount": i}, event_id=f"burst-{i}")
            assert client.flush(20), client.status()
            eventually(lambda: len(ledger) == 131, timeout=20)
            assert [event for event, _ in ledger[3:]] == [f"burst-{i}" for i in range(128)]
            assert audit == ["one", "three"]
            assert not client.status()["publishing_paused"]

        finally:
            gate.set()
    queues.close()
    db.close()
