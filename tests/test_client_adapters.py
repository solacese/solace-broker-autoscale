"""Tier-0/1/2 client adapter tests (§9.2, §9.3, §9.4). No live broker needed for these unit tests;
the live protocol integration test lives in test_integration_broker.py."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "adapters" / "python"))

from solace_autoscale_client.adapters import amqp_uri, mqtt_config, rest_target  # noqa: E402
from solace_autoscale_client.resolver import Assignment, Resolver, ResolverError  # noqa: E402
from solace_autoscale_client.smf_wrapper import (  # noqa: E402
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


def _load_dns_updater():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "dns_updater", REPO / "dns" / "updater.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["dns_updater"] = mod  # dataclass annotation resolution needs the module registered
    spec.loader.exec_module(mod)
    return mod


def test_dns_desired_records(tmp_path):
    from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
    desired_records = _load_dns_updater().desired_records

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
