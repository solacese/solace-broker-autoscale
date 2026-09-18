"""Real SQLite concurrency/restart and HTTP contracts for customer-facing assignment."""
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from solace_autoscale.assignment.placement import assign
from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState


def seed(path):
    store = AssignmentStore(path)
    for name in ['a', 'b', 'c']:
        store.upsert_broker(Broker(name, 'orders', 'vpn', BrokerState.ACTIVE,
                                  {'smf': f'tcps://{name}:55443'}))
    return store


@pytest.mark.parametrize('shared_connection', [True, False])
def test_concurrent_first_assignment_has_one_durable_home(tmp_path, shared_connection):
    path = tmp_path/'assignment.db'
    store = seed(path)
    def request(i):
        connection = store if shared_connection else AssignmentStore(path)
        try:
            return assign(connection, 'orders', 'queue-x', 'guaranteed', i, 300).broker.broker_id
        finally:
            if not shared_connection:
                connection.close()
    with ThreadPoolExecutor(max_workers=12) as pool:
        homes = list(pool.map(request, range(80)))
    assert len(set(homes)) == 1
    store.close()
    reopened = AssignmentStore(path)
    assert reopened.get_placement('orders', 'queue-x').broker_id == homes[0]
    reopened.close()


def test_concurrent_distinct_placements_balance_atomically(tmp_path):
    store = seed(tmp_path/'a.db')
    def request(i):
        return assign(store, 'orders', f'queue-{i}', 'guaranteed', 1, 300)
    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(request, range(60)))
    assert [len(store.placements_on_broker(b)) for b in ['a','b','c']] == [20,20,20]
    store.close()


def test_transaction_failure_rolls_back_inventory_and_placement(tmp_path):
    store = seed(tmp_path/'a.db')
    with pytest.raises(RuntimeError):
        with store.transaction():
            assign(store, 'orders', 'q', 'guaranteed', 1, 300)
            store.set_broker_state('a', BrokerState.DRAINING)
            raise RuntimeError('power loss simulation')
    assert store.get_placement('orders','q') is None
    assert store.get_broker('a').state == BrokerState.ACTIVE
    store.close()


def test_mode_switch_cannot_bypass_guaranteed_stickiness(tmp_path):
    store = seed(tmp_path/'a.db')
    home = assign(store, 'orders', 'q', 'guaranteed', 1, 300).broker.broker_id
    store.set_broker_state(home, BrokerState.GONE)
    with pytest.raises(ValueError, match='migration'):
        assign(store, 'orders', 'q', 'direct', 400, 300)
    assert store.get_placement('orders','q').broker_id == home
    store.close()


def test_protocol_selection_precedes_persistent_assignment(tmp_path):
    store = seed(tmp_path/'a.db')
    store.upsert_broker(Broker('z', 'orders', 'vpn', BrokerState.ACTIVE, {'mqtt':'mqtts://z:8883'}))
    with TestClient(create_app(store)) as client:
        result = client.get('/assignment', params={'shard':'orders','client_id':'c','protocol':'mqtt'})
        assert result.status_code == 200
        assert result.json()['broker_id'] == 'z'
        bad = client.get('/assignment', params={'shard':'orders','client_id':'bad','protocol':'rest'})
        assert bad.status_code == 404
        assert store.get_placement('orders','bad') is None
    store.close()


def test_authorization_denial_never_creates_placement(tmp_path):
    store = seed(tmp_path/'a.db')
    with TestClient(create_app(store, api_key='test-only-key')) as client:
        params={'shard':'orders','client_id':'c'}
        assert client.get('/assignment', params=params).status_code == 401
        assert store.get_placement('orders','c') is None
        response=client.get('/assignment', params=params, headers={'Authorization':'Bearer test-only-key'})
        assert response.status_code == 200
        assert response.headers['cache-control'] == 'no-store'
        assert client.get('/healthz').status_code == 200
    store.close()


def test_managed_queue_namespace_cannot_change_on_restart(tmp_path):
    path = tmp_path / 'assignments.db'
    db = AssignmentStore(path)
    db.ensure_managed_namespace('payments')
    db.close()
    db = AssignmentStore(path)
    db.ensure_managed_namespace('payments')
    for invalid in ('other-payments', None):
        with pytest.raises(ValueError, match='fleet_id changed'):
            db.ensure_managed_namespace(invalid)
    db.close()
