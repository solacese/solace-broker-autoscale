"""Native topic fanout, wildcard groups, exclusive replicas and automatic broker handover."""

from __future__ import annotations

import json
import os
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
from solace_autoscale_client import KeyRouter, MessagingClient, Resolver

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


@pytest.mark.parametrize("broker_count", [2, 3, 4])
def test_multi_broker_partition_owners_preserve_fanout_and_order_after_restart(
    tmp_path, broker_count
):
    ports = json.loads(os.environ.get("SOLACE_TOPOLOGY_PORTS", "[]"))
    if len(ports) < broker_count:
        pytest.skip("set SOLACE_TOPOLOGY_PORTS to at least four local broker port pairs")
    fleet = f"topology-{broker_count}-{int(time.time() * 1000)}"
    db = AssignmentStore(tmp_path / "state.db")
    connections = {}
    vpns = {}
    credentials = {}
    for index, item in enumerate(ports[:broker_count]):
        broker = chr(ord("a") + index)
        db.upsert_broker(Broker(
            broker, "payments", "default", BrokerState.ACTIVE,
            {"smf": f"tcp://127.0.0.1:{item['smf']}"},
        ))
        connections[broker] = SempConnection(
            f"http://127.0.0.1:{item['semp']}", "admin", "admin"
        )
        vpns[broker] = "default"
        credentials[broker] = ("default", "default")
    messaging = MessagingConfig.model_validate({
        "enabled": True,
        "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}],
        "subscriptions": [
            {"group": "ledger", "topics": ["payments/>"]},
            {"group": "audit", "topics": ["payments/*/created"]},
        ],
    })
    partitions = broker_count * 2
    app = create_app(
        db,
        policy=AssignmentConfig(routing="partitioned", partitions=partitions),
        fleet_id=fleet,
        messaging=messaging,
    )
    queues = QueueManager(fleet, connections, vpns, 100)
    registry = TopicRegistry(db)
    queues.registry = registry
    queues.client_username = "default"
    for broker in connections:
        queues.configure_native(broker)
    for group in messaging.subscriptions:
        registry.register(group.group, group.topics)
    owners = {}
    for partition in range(partitions):
        owner = chr(ord("a") + partition % broker_count)
        assign(
            db, "payments", f"partition:{partition}", "guaranteed", 1, 60,
            allowed_broker_ids=frozenset({owner}),
        )
        queues.prepare(owner, "payments", partition, enabled=True)
        owners[partition] = owner
    ControllerStore(db).mark_partition_ready("payments", 0)
    for partition in range(1, partitions):
        ControllerStore(db).mark_partition_ready("payments", partition)
    registry.mark_ready(["ledger", "audit"])
    ledger, audit = [], []
    router = KeyRouter(Resolver("http://unused"), "payments", "test", partitions=partitions)
    accounts = {}
    candidate = 0
    while len(accounts) < partitions:
        account = f"account-{candidate}"
        partition = router.partition_for(json.dumps([account], separators=(",", ":")))
        accounts.setdefault(partition, account)
        candidate += 1
    assert set(accounts) == set(range(partitions))
    with live_api(app) as url:
        def start_client(state_dir):
            client = MessagingClient(
                url,
                state_dir=state_dir,
                credentials=lambda broker: credentials[broker],
                poll_interval=.05,
            )
            client.subscribe(group="ledger", handler=lambda m: ledger.append(m.event_id), timeout=20)
            client.subscribe(group="audit", handler=lambda m: audit.append(m.event_id), timeout=20)
            return client

        client = start_client(tmp_path / "client")
        try:
            for index in range(24):
                partition = index % partitions
                client.publish(
                    f"payments/{accounts[partition]}/created",
                    {"sequence": index // partitions, "partition": partition},
                    event_id=f"before-{index}",
                )
            assert client.flush(30)
            eventually(lambda: len(ledger) == len(audit) == 24, timeout=30)
        finally:
            client.close()
        client = start_client(tmp_path / "client")
        try:
            for index in range(24):
                partition = index % partitions
                client.publish(
                    f"payments/{accounts[partition]}/created",
                    {"sequence": 2 + index // partitions, "partition": partition},
                    event_id=f"after-{index}",
                )
            assert client.flush(30)
            eventually(lambda: len(ledger) == len(audit) == 48, timeout=30)
        finally:
            client.close()
    expected = {f"before-{index}" for index in range(24)} | {f"after-{index}" for index in range(24)}
    assert set(ledger) == set(audit) == expected
    assert len(ledger) == len(set(ledger)) == len(audit) == len(set(audit)) == 48
    for partition in range(partitions):
        expected_order = [
            *(f"before-{index}" for index in range(partition, 24, partitions)),
            *(f"after-{index}" for index in range(partition, 24, partitions)),
        ]
        assert [event for event in ledger if int(event.split("-")[1]) % partitions == partition] == expected_order
        assert [event for event in audit if int(event.split("-")[1]) % partitions == partition] == expected_order
    assert set(owners.values()) == set(connections)
    for partition, owner in owners.items():
        status = queues.status(owner, "payments", partition)
        expected_publications = len(range(partition, 24, partitions)) * 2
        assert status.drained
        assert status.spooled_messages == expected_publications * 2
        for nonowner in set(connections) - {owner}:
            for group in ("ledger", "audit"):
                response = queues._request(
                    nonowner,
                    "GET",
                    queues._path(nonowner, "payments", partition, group=group),
                )
                assert response.status_code in (400, 404)
    queues.close()
    db.close()


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
    state.mark_partition_ready("payments", 0)
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
