"""Live-broker protocol integration tests (§9.3, §13 Phase 3 gate).

Every protocol (SMF, AMQP 1.0, MQTT, REST) has a passing integration test against a REAL broker:
resolve via the assignment service → adapt the endpoint → connect and move a message.

These are marked ``integration`` and are DESELECTED by default (``-m "not integration"``). They run
against a local Solace broker. Configure via env:

    SOLACE_HOST        default 127.0.0.1
    SOLACE_SMF_PORT    default 55556   (plaintext SMF)
    SOLACE_AMQP_PORT   default 5673
    SOLACE_MQTT_PORT   default 1884
    SOLACE_REST_PORT   default 9001
    SOLACE_USER        default default
    SOLACE_PASS        default default
    SOLACE_VPN         default default

Start one with:
    docker run -d --name solace-dev --shm-size=1g --ulimit nofile=1048576:1048576 \
      -p 8081:8080 -p 55556:55555 -p 5673:5672 -p 1884:1883 -p 9001:9000 \
      -e username_admin_globalaccesslevel=admin -e username_admin_password=admin \
      solace/solace-pubsub-standard
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "adapters" / "python"))

from solace_autoscale_client.resolver import Resolver  # noqa: E402

from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState  # noqa: E402

HOST = os.environ.get("SOLACE_HOST", "127.0.0.1")
SMF_PORT = int(os.environ.get("SOLACE_SMF_PORT", "55556"))
AMQP_PORT = int(os.environ.get("SOLACE_AMQP_PORT", "5673"))
MQTT_PORT = int(os.environ.get("SOLACE_MQTT_PORT", "1884"))
REST_PORT = int(os.environ.get("SOLACE_REST_PORT", "9001"))
USER = os.environ.get("SOLACE_USER", "default")
PASS = os.environ.get("SOLACE_PASS", "default")
VPN = os.environ.get("SOLACE_VPN", "default")


def _store(tmp_path) -> AssignmentStore:
    store = AssignmentStore(tmp_path / "assign.db")
    store.upsert_broker(Broker(
        broker_id="local-00", shard="shard-a", msg_vpn=VPN, state=BrokerState.ACTIVE,
        endpoints={
            # plaintext endpoints for the local test broker (production uses TLS ports)
            "smf": f"tcp://{HOST}:{SMF_PORT}",
            "amqp": f"amqp://{HOST}:{AMQP_PORT}",
            "mqtt": f"tcp://{HOST}:{MQTT_PORT}",
            "rest": f"http://{HOST}:{REST_PORT}",
        },
    ))
    return store


def _resolver(store) -> Resolver:
    # drive the resolver directly against the store via an in-process opener (no HTTP server needed)
    from solace_autoscale.assignment.placement import assign

    def opener(url: str) -> bytes:
        import json
        from urllib.parse import parse_qs, urlparse
        q = parse_qs(urlparse(url).query)
        a = assign(store, q["shard"][0], q["client_id"][0], q.get("mode", ["direct"])[0],
                   now=time.time(), lease_seconds=300)
        return json.dumps({
            "broker_id": a.broker.broker_id, "msg_vpn": a.broker.msg_vpn,
            "state": a.broker.state.value, "lease_seconds": a.lease_seconds,
            "endpoints": a.broker.endpoints, "reused_existing": a.reused_existing,
        }).encode()

    return Resolver(base_url="http://in-process", _opener=opener)


# ---- REST -----------------------------------------------------------------------------------

def test_rest_publish(tmp_path):
    import requests
    from solace_autoscale_client.adapters import rest_target

    r = _resolver(_store(tmp_path))
    a = r.resolve("shard-a", "rest-pub", "direct", protocol="rest")
    base = rest_target(a)
    resp = requests.post(f"{base}/TOPIC/test/direct/rest", data="hello-rest",
                         auth=(USER, PASS), timeout=10)
    assert resp.status_code in (200, 202), resp.text


# ---- MQTT -----------------------------------------------------------------------------------

def test_mqtt_pubsub(tmp_path):
    import paho.mqtt.client as mqtt
    from solace_autoscale_client.adapters import mqtt_config

    r = _resolver(_store(tmp_path))
    a = r.resolve("shard-a", "mqtt-client", "direct", protocol="mqtt")
    cfg = mqtt_config(a)

    received = []
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="sub")
    sub.username_pw_set(USER, PASS)
    sub.on_message = lambda c, u, m: received.append(m.payload)
    sub.connect(cfg["host"], cfg["port"], keepalive=30)
    sub.subscribe("test/direct/mqtt")
    sub.loop_start()
    time.sleep(1)

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="pub")
    pub.username_pw_set(USER, PASS)
    pub.connect(cfg["host"], cfg["port"], keepalive=30)
    pub.publish("test/direct/mqtt", b"hello-mqtt", qos=0)
    time.sleep(1.5)
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()
    assert b"hello-mqtt" in received


# ---- AMQP 1.0 -------------------------------------------------------------------------------

def test_amqp_send(tmp_path):
    from proton import Message
    from proton.utils import BlockingConnection
    from solace_autoscale_client.adapters import amqp_uri

    r = _resolver(_store(tmp_path))
    a = r.resolve("shard-a", "amqp-pub", "direct", protocol="amqp")
    uri = amqp_uri(a)  # amqp://host:port
    conn = BlockingConnection(uri, user=USER, password=PASS, timeout=15)
    try:
        sender = conn.create_sender("topic://test/direct/amqp")
        sender.send(Message(body="hello-amqp"), timeout=10)
    finally:
        conn.close()


# ---- SMF (Solace messaging API) --------------------------------------------------------------

def test_smf_guaranteed_publish(tmp_path):
    from solace.messaging.config.transport_security_strategy import TLS  # noqa: F401
    from solace.messaging.messaging_service import MessagingService
    from solace.messaging.resources.topic import Topic
    from solace_autoscale_client.adapters import smf_host

    r = _resolver(_store(tmp_path))
    a = r.resolve("shard-a", "smf-pub", "guaranteed", protocol="smf")
    host = smf_host(a)  # tcp://host:port

    service = (
        MessagingService.builder()
        .from_properties({
            "solace.messaging.transport.host": host,
            "solace.messaging.service.vpn-name": VPN,
            "solace.messaging.authentication.scheme.basic.username": USER,
            "solace.messaging.authentication.scheme.basic.password": PASS,
        })
        .build()
    )
    service.connect()
    try:
        publisher = service.create_persistent_message_publisher_builder().build()
        publisher.start()
        msg = service.message_builder().build("hello-smf-guaranteed")
        publisher.publish(msg, Topic.of("test/guaranteed/smf"))
        time.sleep(1)
        publisher.terminate()
    finally:
        service.disconnect()
