"""Fleet totals must be complete, weighted, correctly scoped and independent of secrets."""
from dataclasses import replace

import pytest

from solace_autoscale.decision.types import MetricSample
from solace_autoscale.metrics.base import CollectorError
from solace_autoscale.metrics.fleet import BrokerEndpoint, FleetCollector, FleetInventory, aggregate


def inventory():
    return FleetInventory(provider='aws',broker_version='10.1.2.3',service_class='enterprise-1k',
                          brokers=[BrokerEndpoint(broker_id=x,shard='orders',msg_vpn='vpn',
                                                  base_url=f'https://{x}.example.com',
                                                  username_env='TEST_USER',password_env='TEST_PASSWORD')
                                   for x in ['a','b']])


def test_aggregation_weights_message_size_by_traffic():
    a=MetricSample(100,100,200,100000,200000,1000,10,1000,1)
    b=MetricSample(101,1,2,10000,20000,10000,1,100,1)
    result=aggregate([a,b])
    assert result.avg_msg_size == pytest.approx(110000/101)
    assert result.timestamp==100
    assert result.current_brokers==2
    assert result.spool_used==1100
    assert result.connection_count==11
    with pytest.raises(CollectorError):
        aggregate([replace(a,current_brokers=2)])


def test_duplicate_endpoint_and_standby_double_counting_rejected():
    inv=inventory().model_dump()
    inv['brokers'][1]['base_url']=inv['brokers'][0]['base_url']+'/'
    with pytest.raises(ValueError, match='exactly once'):
        FleetInventory.model_validate(inv)


@pytest.mark.parametrize('url', ['http://outside.example','https://user:password@outside.example',
                                'https://outside.example?token=oops','https://outside.example/path'])
def test_inventory_does_not_accept_inline_secrets_or_unencrypted_remote_endpoints(url):
    args=inventory().brokers[0].model_dump(); args['base_url']=url
    with pytest.raises(ValueError):
        BrokerEndpoint.model_validate(args)


def test_partial_snapshot_is_never_returned(monkeypatch):
    monkeypatch.setenv('TEST_USER','fixture-user'); monkeypatch.setenv('TEST_PASSWORD','fixture-password')
    collector=FleetCollector(inventory())
    sample=MetricSample(100,1,1,1000,1000,1000,1,0,1)
    monkeypatch.setattr(collector.collectors['a'],'collect',lambda *args: sample)
    def fail(*args):
        raise CollectorError('transport failure with untrusted response')
    monkeypatch.setattr(collector.collectors['b'],'collect',fail)
    with pytest.raises(CollectorError, match='incomplete fleet snapshot; failed brokers: b'):
        collector.collect(100)
    monkeypatch.setattr(collector.collectors['b'],'collect',lambda *args: sample)
    complete=collector.collect(100)
    assert complete.shards['orders'].current_brokers==2
    assert complete.shards['orders'].ingress_msg_rate==2
    collector.close()


def test_missing_env_has_no_secret_material(monkeypatch):
    monkeypatch.delenv('TEST_PASSWORD',raising=False)
    with pytest.raises(CollectorError,match='missing credential environment variables for a'):
        FleetCollector(inventory())


def test_warm_spare_is_not_reported_as_active_capacity(monkeypatch):
    monkeypatch.setenv('TEST_USER', 'fixture-user')
    monkeypatch.setenv('TEST_PASSWORD', 'fixture-password')
    inv = inventory()
    inv.brokers[1].role = 'warm'
    collector = FleetCollector(inv)
    sample = MetricSample(100, 1, 1, 1000, 1000, 1000, 1, 0, 1)
    monkeypatch.setattr(collector.collectors['a'], 'collect', lambda *args: sample)
    snapshot = collector.collect(100)
    assert snapshot.shards['orders'].current_brokers == 1
    assert set(snapshot.brokers) == {'a'}
    collector.close()
