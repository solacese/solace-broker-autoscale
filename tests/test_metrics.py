"""Metrics collector tests against captured SEMP fixtures (§13: no live calls in the suite)."""

from __future__ import annotations

import json

import pytest

from solace_autoscale.metrics.cloud_api import CloudApiCollector
from solace_autoscale.metrics.prometheus import PrometheusCollector
from solace_autoscale.metrics.semp import MB, map_vpn_monitor
from solace_autoscale.metrics.static import StaticCollector

from .conftest import REPO

FIX = REPO / "tests" / "fixtures" / "semp"


def test_semp_mapping_from_captured_fixture():
    vpn = json.loads((FIX / "msgvpn_default_monitor.json").read_text())["data"]
    clients = json.loads((FIX / "msgvpn_default_clients.json").read_text())
    conns = clients["meta"]["count"]
    s = map_vpn_monitor(vpn, conns, now=123.0, current_brokers=2)
    assert s.timestamp == 123.0
    assert s.current_brokers == 2
    assert s.connection_count == conns
    # rates come straight from the verified fields
    assert s.ingress_msg_rate == vpn["averageRxMsgRate"]
    assert s.egress_msg_rate == vpn["averageTxMsgRate"]
    assert s.ingress_byte_rate == vpn["averageRxByteRate"]
    assert s.egress_byte_rate == vpn["averageTxByteRate"]
    # spool converts MB → bytes
    assert s.spool_used == vpn["msgSpoolUsage"] * MB


def test_semp_avg_size_zero_when_idle():
    vpn = {"averageRxMsgRate": 0, "averageTxMsgRate": 0, "averageRxByteRate": 0,
           "averageTxByteRate": 0, "msgSpoolUsage": 0}
    s = map_vpn_monitor(vpn, 0, now=1.0, current_brokers=1)
    assert s.avg_msg_size == 0.0


def test_semp_avg_size_derived():
    vpn = {"averageRxMsgRate": 100, "averageTxMsgRate": 100, "averageRxByteRate": 102400,
           "averageTxByteRate": 102400, "msgSpoolUsage": 10}
    s = map_vpn_monitor(vpn, 5, now=1.0, current_brokers=1)
    assert s.avg_msg_size == pytest.approx(1024.0)
    assert s.spool_used == 10 * MB


def test_static_collector(tmp_path):
    doc = {"shards": {"domain-a": {"msg_vpn": "acme-prod", "samples": [
        {"timestamp": 1, "ingress_msg_rate": 10, "egress_msg_rate": 10, "ingress_byte_rate": 1000,
         "egress_byte_rate": 1000, "avg_msg_size": 100, "connection_count": 5, "spool_used": 0,
         "current_brokers": 1},
        {"timestamp": 2, "ingress_msg_rate": 20, "egress_msg_rate": 20, "ingress_byte_rate": 2000,
         "egress_byte_rate": 2000, "avg_msg_size": 100, "connection_count": 6, "spool_used": 0,
         "current_brokers": 1},
    ]}}}
    p = tmp_path / "m.json"
    p.write_text(json.dumps(doc))
    c = StaticCollector(p)
    assert c.msg_vpn("domain-a") == "acme-prod"
    assert len(c.window("domain-a")) == 2
    latest = c.collect("domain-a", "acme-prod", now=99, current_brokers=1)
    assert latest.timestamp == 2  # newest


def test_stub_collectors_raise_not_implemented():
    for coll in (CloudApiCollector(), PrometheusCollector()):
        with pytest.raises(NotImplementedError) as e:
            coll.collect("s", "vpn", 1.0, 1)
        assert "not implemented" in str(e.value)
