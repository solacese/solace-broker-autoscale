"""Customer policy compiles deterministically without connecting to any service."""
import json

import pytest
import yaml
from click.testing import CliRunner

from solace_autoscale.cli import main
from solace_autoscale.config import load_config
from solace_autoscale.controller.planner import PartitionLoad, propose_move


def policy_file(tmp_path, **changes):
    (tmp_path / "connection.yaml").write_text(yaml.safe_dump({
        "inventory": "fleet.yaml", "assignment": {"store": "state.db"},
        "capacity": {"model": "model.json"},
    }))
    raw = {"version": 1, "connection": "connection.yaml",
           "scaling": {"max_brokers": 4, "warm_brokers": 1},
           "workloads": {"payments": {
               "topic": "payments/{account}/{event}", "keep_together": "account",
               "subscribers": {"ledger": "payments/>", "audit": "payments/*/created"}}}}
    raw.update(changes)
    path = tmp_path / "policy.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_short_policy_and_read_only_explanation(tmp_path):
    path = policy_file(tmp_path)
    cfg = load_config(path)
    assert cfg.assignment.routing == "partitioned"
    assert cfg.messaging.routes[0].key_levels == [1]
    assert cfg.inventory == str(tmp_path / "fleet.yaml")
    assert cfg.assignment.store == str(tmp_path / "state.db")
    assert cfg.automation.enabled and cfg.automation.shards["payments"].enabled
    assert not cfg.provisioning.enabled
    result = CliRunner().invoke(main, ["explain", "--json", "--config", str(path),
                                      "--topic", "payments/account-42/created"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["topic"]["key"] == '["account-42"]'
    assert not (tmp_path / "state.db").exists()


def test_paused_and_bad_policy(tmp_path):
    cfg = load_config(policy_file(tmp_path, scaling={"mode": "paused"}))
    assert not cfg.automation.shards["payments"].enabled
    with pytest.raises(ValueError):
        load_config(policy_file(tmp_path, scaling={"max_brokers": 1, "warm_brokers": 1}))
    with pytest.raises(ValueError):
        load_config(policy_file(tmp_path, scaling={"max_broker": 4}))


def test_backlog_is_not_treated_as_movable_capacity():
    loads = [PartitionLoad("payments", 0, "a", 0.1, 0.1, spool=0.95)]
    assert propose_move(loads, ["a", "b"], trigger=0.8, target=0.65, excluded=set()) is None
