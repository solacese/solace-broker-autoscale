"""Real two-broker fencing/ACK/outbox/consumer handover test; isolated local fixtures only.

Requires SEMP at localhost:18081/18082 and SMF at localhost:15556/15557, default/default.
Does not use Cloud credentials or deployed services. Run with pytest -m integration.
"""

from __future__ import annotations

import json
import sys
import threading
import time
import urllib.error
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from solace_autoscale.actuator.solace_cloud import SempConnection
from solace_autoscale.assignment.placement import assign
from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
from solace_autoscale.config import AssignmentConfig, AutomationConfig
from solace_autoscale.controller.migration import MigrationEngine
from solace_autoscale.controller.semp import QueueManager
from solace_autoscale.controller.store import ControllerStore

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "adapters" / "python"))
from solace_autoscale_client import KeyRouter, Resolver  # noqa: E402
from solace_autoscale_client.managed_smf import ManagedConsumers, SmfConnections  # noqa: E402
from solace_autoscale_client.outbox import DurableOutbox  # noqa: E402

pytestmark = pytest.mark.integration


def test_real_queue_fence_buffer_drain_and_restart(tmp_path):
    fleet = "migration-" + str(int(time.time() * 1000))
    db = AssignmentStore(tmp_path / "state.db")
    for name, port in [("a", 15556), ("b", 15557)]:
        db.upsert_broker(
            Broker(name, "payments", "default", BrokerState.ACTIVE, {"smf": f"tcp://127.0.0.1:{port}"})
        )
    policy = AssignmentConfig(routing="partitioned", partitions=1, lease_seconds=1)
    app = create_app(db, policy=policy, fleet_id=fleet)
    queues = QueueManager(
        fleet,
        {
            b: SempConnection(f"http://127.0.0.1:{p}", "admin", "admin")
            for b, p in [("a", 18081), ("b", 18082)]
        },
        {"a": "default", "b": "default"},
        100,
    )
    assign(db, "payments", "partition:0", "guaranteed", 1, 1)
    queues.prepare("a", "payments", 0, enabled=True)
    state = ControllerStore(db)
    delivered = []
    gate = threading.Event()
    connections = SmfConnections(lambda broker: ("default", "default"))
    consumers = None
    outbox = None
    with TestClient(app) as http:

        def opener(url):
            from urllib.parse import urlsplit

            u = urlsplit(url)
            response = http.get(u.path + "?" + u.query)
            if response.status_code >= 400:
                raise urllib.error.HTTPError(url, response.status_code, "handover", {}, None)
            return response.content

        resolver = Resolver("http://testserver", _opener=opener)
        router = KeyRouter(resolver, "payments", "publisher", partitions=1)
        outbox = DurableOutbox(tmp_path / "payments.outbox.db", router)

        def handler(event_id, payload):
            if not gate.is_set():
                raise RuntimeError("simulate temporarily slow payment processor")
            if event_id not in delivered:
                delivered.append(event_id)

        consumers = ManagedConsumers(resolver, "payments", connections, handler)
        consumers._discover = lambda: http.get("/partitions", params={"shard": "payments"}).json()
        try:
            consumers.sync()
            for i in range(10):
                outbox.enqueue("account-a", str(i), json.dumps({"sequence": i}).encode())
            assert outbox.flush(connections.send) == 10
            before = queues.status("a", "payments", 0)
            assert before.messages > 0 or before.unacked > 0
            state.begin("payments", 0, "a", "b", 100)
            auto = AutomationConfig(poll_interval=1, migration_grace=2, empty_settle=1, migration_timeout=60)
            engine = MigrationEngine(state, queues, auto)
            engine.advance(state.pending()[0], 100)  # Prepare destination; wait for a consumer.
            consumers.sync()
            for now in [101, 102, 103, 104]:
                engine.advance(state.pending()[0], now)
            assert state.pending()[0].phase == "draining"
            assert not queues.status("a", "payments", 0).ingress_enabled
            # Stale cached publishers hit a BROKER NACK. Accepted local payments remain on disk.
            outbox.enqueue("account-a", "10", b'{"sequence":10}')
            assert outbox.flush(connections.send) == 0
            assert outbox.pending() == 1
            assert db.get_placement("payments", "partition:0").broker_id == "a"
            gate.set()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and len(delivered) < 10:
                time.sleep(0.05)
            assert delivered == [str(i) for i in range(10)]
            # A new engine resumes from the persisted phase; no in-memory migration state.
            engine = MigrationEngine(ControllerStore(db), queues, auto)
            for now in range(105, 115):
                if not state.pending():
                    break
                engine.advance(state.pending()[0], now)
                time.sleep(0.1)
            assert not state.pending()
            assert db.get_placement("payments", "partition:0").broker_id == "b"
            assert outbox.flush(connections.send) == 1
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and len(delivered) < 11:
                time.sleep(0.05)
            assert delivered == [str(i) for i in range(11)]
            assert outbox.pending() == 0
            assert queues.status("a", "payments", 0).drained
            assert not queues.status("a", "payments", 0).ingress_enabled
        finally:
            gate.set()
            if consumers:
                consumers.close()
            connections.close()
            if outbox:
                outbox.close()
            queues.close()
            db.close()
