"""Live-broker integration test for the smart SHIM (both sides) over real AMQP 1.0.

The dispatch decision (topic + payload -> broker + partition key) is exercised end to end: the
PublisherShim routes three payloads to different targets and stamps the partition key on the wire;
the ListenerShim subscribes across every target and demultiplexes by re-running the same rules.

Two "brokers" are modelled as two AMQP addresses on the SAME local broker (the routing logic is
address/endpoint-driven, so this proves the SHIM without a second container). Marked ``integration``
and deselected by default. Env matches test_integration_broker.py (SOLACE_AMQP_PORT default 5673).
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "adapters" / "python"))

from solace_autoscale_client.dispatch import (  # noqa: E402
    PARTITION_KEY_PROPERTY,
    ListenerShim,
    PublisherShim,
)
from solace_autoscale_client.resolver import Resolver  # noqa: E402

from solace_autoscale.dispatch import DispatchPlan, rules_from_config  # noqa: E402

HOST = os.environ.get("SOLACE_HOST", "127.0.0.1")
AMQP_PORT = int(os.environ.get("SOLACE_AMQP_PORT", "5673"))
USER = os.environ.get("SOLACE_USER", "default")
PASS = os.environ.get("SOLACE_PASS", "default")

AMQP = f"amqp://{HOST}:{AMQP_PORT}"

# All three "brokers" resolve to the same live broker; routing differs by address/key, which is what
# we are proving. In production these would be distinct endpoints.
RULES = [
    {
        "name": "vip-orders",
        "when": {"topic": "shimtest/orders/>",
                 "payload": [{"path": "priority", "op": "in", "value": ["high", "urgent"]}]},
        "route": {"broker": "broker-vip", "key": "vip.{region}",
                  "topic": "shimtest/vip/{region}"},
    },
    {
        "name": "big-orders",
        "when": {"topic": "shimtest/orders/>",
                 "payload": [{"path": "amount", "op": "gt", "value": 1000}]},
        "route": {"broker": "broker-big", "key": "big.{region}",
                  "topic": "shimtest/big/{region}"},
    },
]


def _resolver() -> Resolver:
    def opener(url: str) -> bytes:
        from urllib.parse import parse_qs, urlparse
        q = parse_qs(urlparse(url).query)
        name = q["client_id"][0]
        return json.dumps({
            "broker_id": name, "msg_vpn": "default", "state": "ACTIVE", "lease_seconds": 300,
            "endpoints": {"amqp": AMQP}, "reused_existing": False,
        }).encode()
    return Resolver(base_url="http://in-process", _opener=opener, _clock=lambda: 0.0)


def _plan() -> DispatchPlan:
    return DispatchPlan(rules=rules_from_config(RULES), default_broker="broker-default")


def test_shim_routes_and_demuxes_over_amqp():
    plan = _plan()

    # ---- listener side: open receivers on the concrete addresses the rules publish to -----------
    listener = ListenerShim(resolver=_resolver(), plan=plan, shard="shard-a", user=USER, password=PASS)
    # subscribe to each distinct published address (vip and big use templated addresses)
    listener.subscribe("shimtest/vip/EU")
    listener.subscribe("shimtest/big/US")
    time.sleep(1)

    # ---- publisher side: route three messages by content ---------------------------------------
    pub = PublisherShim(resolver=_resolver(), plan=plan, shard="shard-a", user=USER, password=PASS)
    try:
        vip = pub.publish("shimtest/orders/eu/new",
                          json.dumps({"priority": "high", "region": "EU", "amount": 50}))
        big = pub.publish("shimtest/orders/us/new",
                          json.dumps({"priority": "low", "region": "US", "amount": 9999}))

        assert vip.address == "shimtest/vip/EU"
        assert vip.properties[PARTITION_KEY_PROPERTY] == "vip.EU"
        assert big.address == "shimtest/big/US"
        assert big.properties[PARTITION_KEY_PROPERTY] == "big.US"
        time.sleep(2.0)
    finally:
        pub.close()

    # ---- listener demuxes: collect by partition key --------------------------------------------
    by_key: dict[str, list] = {}
    for dm in listener.receive(timeout=3.0):
        by_key.setdefault(dm.key or "(none)", []).append(dm)
    listener.close()

    assert "vip.EU" in by_key, f"missing vip stream; got keys {list(by_key)}"
    assert "big.US" in by_key, f"missing big stream; got keys {list(by_key)}"
