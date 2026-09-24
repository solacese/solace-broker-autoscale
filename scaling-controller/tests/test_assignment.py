"""Assignment service tests (§9.1, §13 Phase 3 gate).

Gate: a durable guaranteed placement survives service restart, and survives a broker entering and
leaving DRAINING.

FastAPI is in the ``dev`` extra, so it is always present in CI (which installs ``.[dev]``). These
tests therefore RUN in CI rather than skipping - a safety-relevant test that silently skips is not a
test. The contract is enforced by ``test_dev_extra_contract.py``, which lives OUTSIDE this module's
``importorskip`` and fails hard if fastapi is missing while the ``dev`` extra is installed; the
module-level ``importorskip`` here remains only so a bare local checkout without the dev extra
degrades gracefully rather than erroring on import.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from solace_autoscale.assignment.placement import assign  # noqa: E402
from solace_autoscale.assignment.service import create_app  # noqa: E402
from solace_autoscale.assignment.store import (  # noqa: E402
    AssignmentStore,
    Broker,
    BrokerState,
    OptimisticLockError,
    Placement,
)
from solace_autoscale.config import AssignmentConfig  # noqa: E402


def _endpoints(host: str) -> dict:
    # per-protocol map; ports illustrative (would be read from broker config in production)
    return {
        "smf": f"tcps://{host}:55443",
        "amqp": f"amqps://{host}:5671",
        "mqtt": f"ssl://{host}:8883",
        "rest": f"https://{host}:9443",
        "web": f"wss://{host}:443",
    }


def _seed(store, n=3, shard="shard-a"):
    for i in range(n):
        store.upsert_broker(Broker(
            broker_id=f"{shard}-{i:02d}", shard=shard, msg_vpn="acme-prod",
            state=BrokerState.ACTIVE, endpoints=_endpoints(f"{shard}-{i:02d}.example.com"),
        ))


def make_client(store, clock_val=None):
    clock = (lambda: clock_val) if clock_val is not None else None
    app = create_app(store, clock=clock) if clock else create_app(store)
    return TestClient(app)


def test_assignment_returns_per_protocol_map(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    _seed(store)
    client = make_client(store, clock_val=1000.0)
    r = client.get("/assignment", params={"shard": "shard-a", "client_id": "c1", "mode": "guaranteed"})
    assert r.status_code == 200
    body = r.json()
    assert set(body["endpoints"]) >= {"smf", "amqp", "mqtt", "rest"}
    assert body["msg_vpn"] == "acme-prod"
    assert body["lease_seconds"] == 300
    # never a credential
    assert "password" not in body and "token" not in body


def test_managed_assignment_waits_for_controller_bootstrap(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    _seed(store, n=1)
    client = TestClient(create_app(
        store,
        policy=AssignmentConfig(routing="partitioned", partitions=1),
        fleet_id="payments",
    ))
    response = client.get(
        "/assignment",
        params={"shard": "shard-a", "client_id": "c1", "mode": "guaranteed", "partition": 0},
    )
    assert response.status_code == 503
    assert "not ready" in response.json()["detail"]
    store.close()


def test_managed_assignment_waits_when_placement_exists_but_queue_is_not_ready(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    _seed(store, n=1)
    store.ensure_routing("partitioned", 1)
    assign(store, "shard-a", "partition:0", "guaranteed", 1, 300)
    client = TestClient(create_app(
        store,
        policy=AssignmentConfig(routing="partitioned", partitions=1),
        fleet_id="payments",
    ))
    response = client.get(
        "/assignment",
        params={"shard": "shard-a", "client_id": "c1", "mode": "guaranteed", "partition": 0},
    )
    assert response.status_code == 503
    store.close()


def test_initial_assignment_respects_allowed_broker_ids(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    _seed(store, n=2)
    assignment = assign(
        store,
        "shard-a",
        "c1",
        "guaranteed",
        1,
        300,
        allowed_broker_ids=frozenset({"shard-a-01"}),
    )
    assert assignment.broker.broker_id == "shard-a-01"
    store.close()


def test_guaranteed_placement_is_sticky(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    _seed(store)
    client = make_client(store, clock_val=1000.0)
    b1 = client.get("/assignment", params={"shard": "shard-a", "client_id": "c1", "mode": "guaranteed"}).json()
    b2 = client.get("/assignment", params={"shard": "shard-a", "client_id": "c1", "mode": "guaranteed"}).json()
    assert b1["broker_id"] == b2["broker_id"]  # same broker every time


def test_guaranteed_placement_survives_restart(tmp_path):
    path = tmp_path / "a.db"
    store = AssignmentStore(path)
    _seed(store)
    client = make_client(store, clock_val=1000.0)
    first = client.get("/assignment", params={"shard": "shard-a", "client_id": "durable", "mode": "guaranteed"}).json()
    store.close()

    # "restart": brand new store + app over the same DB file
    store2 = AssignmentStore(path)
    client2 = make_client(store2, clock_val=2000.0)
    again = client2.get("/assignment", params={"shard": "shard-a", "client_id": "durable", "mode": "guaranteed"}).json()
    assert again["broker_id"] == first["broker_id"]  # placement persisted across restart
    store2.close()


def test_guaranteed_placement_survives_draining(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    _seed(store)
    client = make_client(store, clock_val=1000.0)
    first = client.get("/assignment", params={"shard": "shard-a", "client_id": "durable", "mode": "guaranteed"}).json()
    home = first["broker_id"]

    # broker enters DRAINING → still serves existing guaranteed placement
    store.set_broker_state(home, BrokerState.DRAINING)
    during = client.get("/assignment", params={"shard": "shard-a", "client_id": "durable", "mode": "guaranteed"}).json()
    assert during["broker_id"] == home
    assert during["state"] == "draining"

    # a NEW guaranteed client must NOT land on the draining broker
    newc = client.get("/assignment", params={"shard": "shard-a", "client_id": "fresh", "mode": "guaranteed"}).json()
    assert newc["broker_id"] != home

    # broker leaves DRAINING (back to ACTIVE) → durable client still on home
    store.set_broker_state(home, BrokerState.ACTIVE)
    after = client.get("/assignment", params={"shard": "shard-a", "client_id": "durable", "mode": "guaranteed"}).json()
    assert after["broker_id"] == home


def test_guaranteed_survives_lease_expiry(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    _seed(store)
    # assign at t=1000 with 300s lease
    a1 = assign(store, "shard-a", "c1", "guaranteed", now=1000.0, lease_seconds=300)
    # much later, lease long expired - guaranteed placement still returns same broker (queue exists)
    a2 = assign(store, "shard-a", "c1", "guaranteed", now=100000.0, lease_seconds=300)
    assert a1.broker.broker_id == a2.broker.broker_id
    assert a2.reused_existing is True


def test_new_assignment_only_to_active(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    store.upsert_broker(Broker("b-drain", "shard-a", "vpn", BrokerState.DRAINING, _endpoints("d")))
    store.upsert_broker(Broker("b-active", "shard-a", "vpn", BrokerState.ACTIVE, _endpoints("a")))
    a = assign(store, "shard-a", "c1", "guaranteed", now=1.0, lease_seconds=300)
    assert a.broker.broker_id == "b-active"


def test_no_broker_available_503(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    store.upsert_broker(Broker("b-drain", "shard-a", "vpn", BrokerState.DRAINING, _endpoints("d")))
    client = make_client(store, clock_val=1.0)
    r = client.get("/assignment", params={"shard": "shard-a", "client_id": "c1", "mode": "guaranteed"})
    assert r.status_code == 503


def test_optimistic_lock_detects_concurrent_write(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    _seed(store)
    store.put_placement(Placement("shard-a", "c1", "shard-a-00", "guaranteed", 2000.0))
    # simulate a stale writer: read version 1, someone else bumps it, our CAS at v1 must fail
    p_stale = store.get_placement("shard-a", "c1")
    store.renew_lease("shard-a", "c1", 3000.0)  # bumps version to 2
    with pytest.raises(OptimisticLockError):
        store.put_placement(p_stale)  # still thinks version is 1


def test_health_and_ready(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    client = make_client(store)
    assert client.get("/healthz").json()["status"] == "ok"
    assert client.get("/readyz").json()["status"] == "ready"


def test_protocol_filter_404_for_unavailable(tmp_path):
    store = AssignmentStore(tmp_path / "a.db")
    store.upsert_broker(Broker("b0", "shard-a", "vpn", BrokerState.ACTIVE, {"smf": "tcps://x:55443"}))
    client = make_client(store, clock_val=1.0)
    r = client.get("/assignment", params={"shard": "shard-a", "client_id": "c1", "protocol": "mqtt"})
    assert r.status_code == 404


@pytest.mark.parametrize('state', [BrokerState.DELETING, BrokerState.GONE])
def test_guaranteed_home_disappears_without_silent_migration(tmp_path, state):
    store = AssignmentStore(tmp_path / 'a.db')
    _seed(store)
    first = assign(store, 'shard-a', 'durable', 'guaranteed', 1, 300)
    store.set_broker_state(first.broker.broker_id, state)
    client = make_client(store, clock_val=1000)
    response = client.get('/assignment', params={'shard': 'shard-a', 'client_id': 'durable',
                                                'mode': 'guaranteed'})
    assert response.status_code == 503
    assert 'migration' in response.json()['detail']
    assert store.get_placement('shard-a', 'durable').broker_id == first.broker.broker_id
