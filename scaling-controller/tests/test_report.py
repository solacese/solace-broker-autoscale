"""Report tests (§11): markdown + JSON from one data structure."""

from __future__ import annotations

import json

from solace_autoscale.decision.engine import DecisionRequest, decide
from solace_autoscale.decision.types import ShardInput
from solace_autoscale.report import json as jr
from solace_autoscale.report import markdown as mr

from .conftest import default_config, make_test_model, window


def _decisions(cfg, model):
    shard = ShardInput("domain-a", window(n=20, ingress_msg_rate=30000, avg_msg_size=1000))
    now = max(s.timestamp for s in shard.samples) + 1
    return [decide(DecisionRequest(config=cfg, model=model, shard=shard, now=now))]


def test_json_report_carries_provenance_and_model_version():
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"})
    model = make_test_model()
    rep = jr.build_report(cfg, model, _decisions(cfg, model))
    assert rep["model_version"] == model.model_version
    assert "provenance" in rep
    assert rep["shards"][0]["shard"] == "domain-a"
    # round-trips as JSON
    json.dumps(rep)


def test_synthetic_banner_in_reports():
    cfg = default_config(workload={"delivery": "direct"})
    model = make_test_model(synthetic=True)
    rep = jr.build_report(cfg, model, _decisions(cfg, model))
    assert rep["synthetic_model"] is True
    assert rep["synthetic_warning"]
    md = mr.render(cfg, model, _decisions(cfg, model))
    assert "SYNTHETIC CAPACITY MODEL" in md


def test_markdown_leads_with_recommendation():
    cfg = default_config(workload={"delivery": "direct", "bottleneck": "messages"})
    model = make_test_model()
    md = mr.render(cfg, model, _decisions(cfg, model))
    assert "# solace-autoscale recommendation" in md
    assert "Scale up" in md or "Hold" in md
    assert "Binding axis" in md
    assert "Provenance" in md


def test_committed_warm_pool_cost_reported():
    cfg = default_config(billing={"model": "committed"}, policy={"warm_pool": 2})
    model = make_test_model()
    rep = jr.build_report(cfg, model, _decisions(cfg, model))
    assert rep["warm_pool_cost"]["warm_pool_brokers"] == 2
    assert rep["warm_pool_cost"]["billed_idle"] is True
