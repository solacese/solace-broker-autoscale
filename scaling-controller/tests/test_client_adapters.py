"""Tier-0/1/2 client adapter tests (§9.2, §9.3, §9.4). No live broker needed for these unit tests;
the live protocol integration test lives in test_integration_broker.py."""

from __future__ import annotations

import sys
import types

import pytest

from solace_autoscale_client.adapters import amqp_uri, mqtt_config, rest_target
from solace_autoscale_client.managed_smf import SmfConnections
from solace_autoscale_client.resolver import Assignment, Resolver, ResolverError
from solace_autoscale_client.smf_wrapper import (
    GuaranteedReassignmentRefused,
    SmfClient,
)


def _assignment(host="shard-a-00.example.com", reused=False):
    return Assignment(
        broker_id="shard-a-00", msg_vpn="acme-prod", state="active", lease_seconds=300,
        endpoints={
            "smf": f"tcps://{host}:55443",
            "amqp": f"amqps://{host}:5671",
            "mqtt": f"ssl://{host}:8883",
            "rest": f"https://{host}:9443",
        },
        fetched_at=0.0, reused_existing=reused,
    )


def test_amqp_uri_and_failover():
    a = _assignment()
    assert amqp_uri(a) == "amqps://shard-a-00.example.com:5671"
    b = _assignment(host="shard-a-01.example.com")
    fo = amqp_uri(a, failover=[b])
    assert fo.startswith("failover:(")
    assert "shard-a-00" in fo and "shard-a-01" in fo


def test_mqtt_config_parses_tls_port():
    cfg = mqtt_config(_assignment())
    assert cfg["host"] == "shard-a-00.example.com"
    assert cfg["port"] == 8883
    assert cfg["tls"] is True


def test_rest_target():
    assert rest_target(_assignment()) == "https://shard-a-00.example.com:9443"


def test_resolver_caches_and_fails_open():
    calls = {"n": 0}

    def opener(url):
        calls["n"] += 1
        if calls["n"] == 1:
            import json
            return json.dumps({
                "broker_id": "b0", "msg_vpn": "vpn", "state": "active", "lease_seconds": 300,
                "endpoints": {"smf": "tcps://b0:55443"}, "reused_existing": True,
            }).encode()
        raise OSError("assignment service down")

    r = Resolver(base_url="http://svc", _opener=opener)
    a1 = r.resolve("shard-a", "c1", "guaranteed")
    assert a1.broker_id == "b0"
    # service now down → cached assignment returned, does NOT raise (fail open)
    a2 = r.resolve("shard-a", "c1", "guaranteed")
    assert a2.broker_id == "b0"


def test_resolver_raises_when_no_cache_and_down():
    def opener(url):
        raise OSError("down")

    r = Resolver(base_url="http://svc", _opener=opener)
    with pytest.raises(ResolverError):
        r.resolve("shard-a", "c1")


class StubMessagingServiceBuilder:
    def __init__(self, properties):
        self.properties = properties

    def from_properties(self, value):
        self.properties.update(value)
        return self

    def build(self):
        return self

    def connect(self):
        return None

    def disconnect(self):
        return None


def install_messaging_service(monkeypatch, properties):
    module = types.ModuleType("solace.messaging.messaging_service")

    class MessagingService:
        @staticmethod
        def builder():
            return StubMessagingServiceBuilder(properties)

    module.MessagingService = MessagingService
    monkeypatch.setitem(sys.modules, "solace.messaging.messaging_service", module)


def test_managed_smf_injects_trust_store_for_tls(monkeypatch, tmp_path):
    properties = {}
    install_messaging_service(monkeypatch, properties)
    monkeypatch.setenv("SOLACE_TLS_TRUST_STORE_DIR", str(tmp_path))
    connections = SmfConnections(lambda _: ("user", "password"))
    try:
        connections.service({
            "broker_id": "b0", "msg_vpn": "vpn", "endpoints": {"smf": "tcps://b0:55443"}
        })
    finally:
        connections.close()
    assert properties["solace.messaging.tls.trust-store-path"] == str(tmp_path)


def test_managed_smf_omits_trust_store_for_plaintext(monkeypatch):
    properties = {}
    install_messaging_service(monkeypatch, properties)
    connections = SmfConnections(lambda _: ("user", "password"))
    try:
        connections.service({
            "broker_id": "b0", "msg_vpn": "vpn", "endpoints": {"smf": "tcp://b0:55555"}
        })
    finally:
        connections.close()
    assert "solace.messaging.tls.trust-store-path" not in properties


def test_smf_wrapper_connects_and_reconnects():
    seq = ["shard-a-00.example.com", "shard-a-01.example.com"]
    import json

    def opener(url):
        host = seq.pop(0) if seq else "shard-a-01.example.com"
        return json.dumps({
            "broker_id": host.split(".")[0], "msg_vpn": "vpn", "state": "active",
            "lease_seconds": 300, "endpoints": {"smf": f"tcps://{host}:55443"},
        }).encode()

    r = Resolver(base_url="http://svc", _opener=opener)
    built = []
    client = SmfClient(r, "shard-a", "c1", "direct",
                       connect_fn=lambda host, a: built.append(host) or object())
    c1 = client.connect()
    assert c1.assignment.broker_id == "shard-a-00"
    c2 = client.reconnect()  # re-resolves → new broker
    assert c2.assignment.broker_id == "shard-a-01"


def test_guaranteed_reassignment_refused():
    import json

    def opener(url):
        return json.dumps({
            "broker_id": "b0", "msg_vpn": "vpn", "state": "active", "lease_seconds": 300,
            "endpoints": {"smf": "tcps://b0:55443"},
        }).encode()

    r = Resolver(base_url="http://svc", _opener=opener)
    client = SmfClient(r, "shard-a", "cons", "guaranteed", connect_fn=lambda host, a: object())
    client.connect()
    with pytest.raises(GuaranteedReassignmentRefused):
        client.on_reassignment_signal()


def test_dns_desired_records(tmp_path):
    from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
    from solace_autoscale.dns.updater import desired_records

    store = AssignmentStore(tmp_path / "a.db")
    store.upsert_broker(Broker("b0", "shard-a", "vpn", BrokerState.ACTIVE,
                               {"smf": "tcps://b0.example.com:55443"}))
    store.upsert_broker(Broker("b1", "shard-a", "vpn", BrokerState.DRAINING,
                               {"smf": "tcps://b1.example.com:55443"}))
    recs = desired_records(store, ["shard-a"], "brokers.example.com", ttl=30)
    assert len(recs) == 1
    assert recs[0].name == "shard-a.brokers.example.com"
    # draining broker excluded from DNS
    assert recs[0].hostnames == ["b0.example.com"]


def test_resolver_query_ids_cannot_inject_parameters():
    import json
    from urllib.parse import parse_qs, urlparse
    seen = []
    def opener(url):
        seen.append(parse_qs(urlparse(url).query))
        return json.dumps({'broker_id':'b','msg_vpn':'v','state':'active','lease_seconds':300,
                           'endpoints':{'smf':'tcps://b:55443'}}).encode()
    resolver=Resolver('http://svc', _opener=opener)
    resolver.resolve('orders/eu & asia', 'x&mode=direct', 'guaranteed')
    assert seen[0]['mode']==['guaranteed']
    assert seen[0]['client_id']==['x&mode=direct']
    assert seen[0]['shard']==['orders/eu & asia']


def test_resolver_auth_denial_does_not_use_cache():
    import json
    from urllib.error import HTTPError
    calls=[]
    def opener(url):
        calls.append(url)
        if len(calls)>1:
            raise HTTPError(url,401,'unauthorized',{},None)
        return json.dumps({'broker_id':'b','msg_vpn':'v','state':'active','lease_seconds':300,
                           'endpoints':{'smf':'tcps://b:55443'}}).encode()
    resolver=Resolver('http://svc', _opener=opener)
    resolver.resolve('s','c')
    with pytest.raises(ResolverError, match='401'):
        resolver.resolve('s','c')
