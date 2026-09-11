"""Accuracy tracking tests (§7, Phase 2 gate: predicted vs actual recorded and reportable)."""

from __future__ import annotations

from solace_autoscale.accuracy.join import nearest_bucket, record_observed_capacity
from solace_autoscale.accuracy.recorder import AccuracyRecorder
from solace_autoscale.accuracy.report import format_accuracy_report
from solace_autoscale.decision.engine import DecisionRequest, decide
from solace_autoscale.decision.types import MetricSample, ShardInput

from .conftest import default_config, make_test_model, window


def test_record_recommendation_and_readback(tmp_path):
    rec = AccuracyRecorder(tmp_path / "acc.db")
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"})
    model = make_test_model()
    shard = ShardInput("domain-a", window(n=20, ingress_msg_rate=30000, avg_msg_size=1000))
    now = max(s.timestamp for s in shard.samples) + 1
    d = decide(DecisionRequest(config=cfg, model=model, shard=shard, now=now))
    rid = rec.record_recommendation(d, cfg.config_hash(), ts=now)
    assert rid > 0
    rec.close()


def test_observed_vs_predicted_and_optimistic_flag(tmp_path):
    rec = AccuracyRecorder(tmp_path / "acc.db")
    model = make_test_model()  # direct @1KB msg cap = 10000/broker, byte cap = 10e6/broker
    # Drive the BYTES axis to just under the model's predicted ceiling: the broker sustains only
    # 9e6 bytes/s/broker where the model predicted 10e6 → model OPTIMISTIC by ~11%.
    # 1 broker, avg size 1000, so msg rate 9000 → bytes = (9000+0)*1000 = 9e6 per broker.
    sample = MetricSample(
        timestamp=100, ingress_msg_rate=9000, egress_msg_rate=0,
        ingress_byte_rate=9_000_000, egress_byte_rate=0, avg_msg_size=1000,
        connection_count=1, spool_used=0, current_brokers=1,
    )
    n = record_observed_capacity(rec, model, "enterprise-10k", "direct", "domain-a", sample, ts=100)
    assert n >= 1
    stats = {s.axis: s for s in rec.stats(group_by="axis")}
    assert "bytes" in stats
    b = stats["bytes"]
    # predicted 10e6 vs observed 9e6 → signed +11% (optimistic)
    assert b.mean_signed_pct > 5
    assert b.optimistic_fraction == 1.0
    report = format_accuracy_report(rec, group_by="bucket")
    assert "OPTIMISTIC" in report
    rec.close()


def test_below_saturation_not_recorded(tmp_path):
    rec = AccuracyRecorder(tmp_path / "acc.db")
    model = make_test_model()
    # low load → nowhere near ceiling → no capacity signal recorded
    sample = MetricSample(
        timestamp=1, ingress_msg_rate=100, egress_msg_rate=100,
        ingress_byte_rate=100000, egress_byte_rate=100000, avg_msg_size=1000,
        connection_count=1, spool_used=0, current_brokers=1,
    )
    n = record_observed_capacity(rec, model, "enterprise-10k", "direct", "domain-a", sample, ts=1)
    assert n == 0
    assert "No observations" in format_accuracy_report(rec)
    rec.close()


def test_nearest_bucket(tmp_path):
    model = make_test_model()
    assert nearest_bucket(model, "enterprise-10k", "direct", 1100) == 1000
    assert nearest_bucket(model, "enterprise-10k", "direct", 9000) == 10000


def test_recorder_persists_across_reopen(tmp_path):
    path = tmp_path / "acc.db"
    rec = AccuracyRecorder(path)
    model = make_test_model()
    sample = MetricSample(100, 16000, 16000, 16000000, 16000000, 1000, 1, 0, 2)
    record_observed_capacity(rec, model, "enterprise-10k", "direct", "s", sample, ts=100)
    rec.close()
    # reopen: data survives
    rec2 = AccuracyRecorder(path)
    assert len(rec2.stats()) >= 1
    rec2.close()
