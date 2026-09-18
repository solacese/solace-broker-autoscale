"""Key locality, bounded remapping, durable ownership and YAML-to-HTTP routing integration."""
import json
import sys
from collections import Counter
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from solace_autoscale.assignment.placement import assign
from solace_autoscale.assignment.routing import partition_for, rendezvous_broker
from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
from solace_autoscale.config import AssignmentConfig, Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'adapters' / 'python'))
from solace_autoscale_client import KeyRouter, Resolver  # noqa: E402


def brokers(n=3):
    return [Broker(str(i),'orders','vpn',BrokerState.ACTIVE,{'smf':f'tcps://b{i}:55443'}) for i in range(n)]


def test_rendezvous_addition_only_moves_keys_to_new_broker():
    old, new = brokers(3), brokers(4)
    changed=0
    for i in range(4000):
        before=rendezvous_broker(old,'orders',str(i),{}).broker_id
        after=rendezvous_broker(new,'orders',str(i),{}).broker_id
        if before != after:
            changed+=1
            assert after=='3'
        assert after==rendezvous_broker(list(reversed(new)),'orders',str(i),{}).broker_id
    assert 800 < changed < 1200


def test_rendezvous_weights_are_relative_capacity():
    counts=Counter(rendezvous_broker(brokers(2),'orders',str(i),{'0':1,'1':3}).broker_id for i in range(6000))
    assert 2.6 < counts['1']/counts['0'] < 3.4


def test_guaranteed_hash_owner_does_not_move_on_broker_addition(tmp_path):
    store=AssignmentStore(tmp_path/'a.db')
    for b in brokers(2):
        store.upsert_broker(b)
    homes={str(i):assign(store,'orders',str(i),'guaranteed',1,300,strategy='rendezvous').broker.broker_id
           for i in range(100)}
    store.upsert_broker(brokers(3)[-1])
    assert all(assign(store,'orders',key,'guaranteed',999,300,strategy='rendezvous').broker.broker_id==home
               for key,home in homes.items())
    store.close()


def test_shared_business_key_routes_distinct_clients_to_same_partition(tmp_path):
    store=AssignmentStore(tmp_path/'a.db')
    for b in brokers():
        store.upsert_broker(b)
    policy=AssignmentConfig(routing='partitioned',strategy='rendezvous',partitions=128)
    with TestClient(create_app(store,policy=policy)) as client:
        base={'shard':'orders','mode':'guaranteed','routing_key':'tenant-a/order-123'}
        a=client.get('/assignment',params={**base,'client_id':'publisher'}).json()
        b=client.get('/assignment',params={**base,'client_id':'consumer'}).json()
        assert a['broker_id']==b['broker_id']
        assert a['partition_id']==b['partition_id']==partition_for('orders',base['routing_key'],128)
        explicit=client.get('/assignment',params={'shard':'orders','client_id':'worker',
                                                  'mode':'guaranteed','partition':a['partition_id']}).json()
        assert explicit['broker_id']==a['broker_id']
        assert client.get('/assignment',params={'shard':'orders','client_id':'bad'}).status_code==400
    store.close()


def test_partition_count_change_refused_after_restart(tmp_path):
    path=tmp_path/'a.db'
    store=AssignmentStore(path)
    create_app(store,policy=AssignmentConfig(routing='partitioned',partitions=128))
    store.close()
    store=AssignmentStore(path)
    with pytest.raises(ValueError,match='migrate'):
        create_app(store,policy=AssignmentConfig(routing='partitioned',partitions=256))
    store.close()


def test_local_key_router_cache_and_cross_client_hash():
    calls=[]
    def opener(url):
        from urllib.parse import parse_qs, urlparse
        args=parse_qs(urlparse(url).query); p=int(args['partition'][0]); calls.append(p)
        return json.dumps({'broker_id':'b','msg_vpn':'vpn','state':'active','lease_seconds':300,
                           'endpoints':{'smf':'tcps://b:55443'},'partition_id':p,'partition_count':128}).encode()
    resolver=Resolver('http://svc',_opener=opener,_clock=lambda:100)
    router=KeyRouter(resolver,'orders','publisher')
    for key in ['tenant-a/order-1','é/订单/123','customers & partners']:
        assert router.partition_for(key)==partition_for('orders',key,128)
        a=router.resolve_key(key)
        n=len(calls)
        assert router.resolve_key(key) is a
        assert len(calls)==n
        router.resolve_partition(a.partition_id,refresh=True)
        assert len(calls)==n+1


@pytest.mark.parametrize('policy', [{'strategy':'round-robin'},{'partitions':0},
                                    {'broker_weights':{'b':0}},{'broker_weights':{'b':float('nan')}}])
def test_yaml_rejects_invalid_routing_policy(policy):
    with pytest.raises(ValueError):
        Config.model_validate({'assignment':policy})
