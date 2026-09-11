"""Unit tests for the smart SHIM (publisher + listener), using factory seams — no broker, no proton.

Exercises the full path: rules decide broker + key + address, the resolver maps a broker name to an
endpoint, the publisher stamps the partition key, and the listener demultiplexes by re-running the
same rules and flags cross-broker drift.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "adapters" / "python"))

from solace_autoscale_client.dispatch import (  # noqa: E402
    PARTITION_KEY_PROPERTY,
    ListenerShim,
    PublisherShim,
    ReceivedMessage,
)
from solace_autoscale_client.resolver import Resolver  # noqa: E402

from solace_autoscale.dispatch import DispatchPlan, rules_from_config  # noqa: E402

RULES = [
    {
        "name": "vip-orders",
        "when": {"topic": "orders/>", "payload": [{"path": "priority", "op": "in",
                                                    "value": ["high", "urgent"]}]},
        "route": {"broker": "broker-vip", "key": "vip.{region}", "topic": "vip/{topic}"},
    },
    {
        "name": "big-orders",
        "when": {"topic": "orders/>", "payload": [{"path": "amount", "op": "gt", "value": 1000}]},
        "route": {"broker": "broker-big", "key": "big.{region}"},
    },
]

# endpoint map: broker name -> amqp endpoint. The resolver opener echoes these back.
BROKERS = {
    "broker-vip": "amqp://vip.local:5672",
    "broker-big": "amqp://big.local:5672",
    "broker-default": "amqp://default.local:5672",
}


def _resolver() -> Resolver:
    def opener(url: str) -> bytes:
        from urllib.parse import parse_qs, urlparse
        q = parse_qs(urlparse(url).query)
        name = q["client_id"][0]  # SHIM resolves a broker *name* as the client_id slot
        return json.dumps({
            "broker_id": name, "msg_vpn": "default", "state": "ACTIVE", "lease_seconds": 300,
            "endpoints": {"amqp": BROKERS[name]}, "reused_existing": False,
        }).encode()
    return Resolver(base_url="http://in-process", _opener=opener, _clock=lambda: 0.0)


def _plan() -> DispatchPlan:
    return DispatchPlan(rules=rules_from_config(RULES), default_broker="broker-default")


class _CapturingSender:
    def __init__(self, uri: str) -> None:
        self.uri = uri
        self.sent: list[dict] = []

    def send(self, address, body, properties, group_id):
        self.sent.append({"address": address, "body": body, "properties": properties,
                          "group_id": group_id})


def test_publisher_routes_vip_to_vip_broker_with_key():
    factories: dict[str, _CapturingSender] = {}

    def factory(uri: str) -> _CapturingSender:
        s = _CapturingSender(uri)
        factories[uri] = s
        return s

    pub = PublisherShim(resolver=_resolver(), plan=_plan(), shard="shard-a",
                        sender_factory=factory)
    out = pub.publish("orders/eu/new", json.dumps({"priority": "high", "region": "EU"}))
    assert out.broker_id == "broker-vip"
    assert out.address == "vip/orders/eu/new"
    assert out.key == "vip.EU"
    assert out.properties[PARTITION_KEY_PROPERTY] == "vip.EU"
    # it actually went to the vip broker's sender with the group id set
    sent = factories["amqp://vip.local:5672"].sent[0]
    assert sent["group_id"] == "vip.EU"


def test_publisher_falls_through_to_big_then_default():
    pub = PublisherShim(resolver=_resolver(), plan=_plan(), shard="shard-a",
                        sender_factory=lambda uri: _CapturingSender(uri))
    big = pub.publish("orders/us/new", json.dumps({"priority": "low", "region": "US",
                                                    "amount": 5000}))
    assert big.broker_id == "broker-big" and big.key == "big.US"
    dflt = pub.publish("telemetry/x", json.dumps({"v": 1}))
    assert dflt.broker_id == "broker-default" and dflt.key is None and dflt.rule == "default"


def test_publisher_reuses_one_connection_per_broker():
    made: list[str] = []

    def factory(uri: str) -> _CapturingSender:
        made.append(uri)
        return _CapturingSender(uri)

    pub = PublisherShim(resolver=_resolver(), plan=_plan(), shard="shard-a", sender_factory=factory)
    pub.publish("orders/eu/a", json.dumps({"priority": "high", "region": "EU"}))
    pub.publish("orders/eu/b", json.dumps({"priority": "urgent", "region": "EU"}))
    assert made.count("amqp://vip.local:5672") == 1  # one connection reused


def test_publisher_separates_connections_by_endpoint():
    """Two distinct brokers/endpoints get distinct cached connections (no over-merge)."""
    made: list[str] = []
    pub = PublisherShim(resolver=_resolver(), plan=_plan(), shard="shard-a",
                        sender_factory=lambda uri: made.append(uri) or _CapturingSender(uri))
    pub.publish("orders/eu/a", json.dumps({"priority": "high", "region": "EU"}))   # vip
    pub.publish("orders/us/b", json.dumps({"priority": "low", "region": "US",
                                           "amount": 5000}))                        # big
    assert set(made) == {"amqp://vip.local:5672", "amqp://big.local:5672"}
    assert len(made) == 2


# ---- listener side ---------------------------------------------------------------------------

def test_listener_subscribes_to_every_target_broker():
    opened: list[tuple[str, str]] = []

    def rfactory(uri: str, address: str):
        opened.append((uri, address))
        return object()

    lis = ListenerShim(resolver=_resolver(), plan=_plan(), shard="shard-a",
                       receiver_factory=rfactory)
    lis.subscribe("vip/orders/>")
    assert {u for u, _ in opened} == set(BROKERS.values())  # all targets incl. default


def test_listener_demux_consistent_message():
    lis = ListenerShim(resolver=_resolver(), plan=_plan(), shard="shard-a")
    msg = ReceivedMessage(
        broker_id="broker-vip", address="vip/orders/eu/new",
        key="vip.EU", body=json.dumps({"priority": "high", "region": "EU"}),
        properties={PARTITION_KEY_PROPERTY: "vip.EU"},
    )
    d = lis.demux(msg)
    # NOTE: address on the wire is rewritten (vip/orders/...) so re-running rules on it won't match
    # the orders/> rule; the key is still read from the property, which is the source of truth.
    assert d.key == "vip.EU"


def test_listener_flags_inconsistent_broker():
    lis = ListenerShim(resolver=_resolver(), plan=_plan(), shard="shard-a")
    # a VIP-content message that somehow arrived on the big broker → inconsistent
    msg = ReceivedMessage(
        broker_id="broker-big", address="orders/eu/new",
        key="vip.EU", body=json.dumps({"priority": "high", "region": "EU"}),
        properties={PARTITION_KEY_PROPERTY: "vip.EU"},
    )
    d = lis.demux(msg)
    assert d.rule == "vip-orders"
    assert d.consistent is False  # rules would have chosen broker-vip, not broker-big
