"""Feature compatibility is a hard gate before load-aware placement."""

import json

import pytest
from click.testing import CliRunner

from solace_autoscale.assignment.store import AssignmentStore, Placement
from solace_autoscale.cli import main
from solace_autoscale.config import Config
from solace_autoscale.controller.features import (
    FeatureRequirements,
    assess_migration,
    contract_digest,
    feature_contract,
    requirements_for,
)


def test_supported_guaranteed_baseline_remains_movable():
    decision = assess_migration(
        FeatureRequirements(), deployment_mode="HA", target_role="warm"
    )
    assert decision.allowed
    assert decision.reason == "supported"


@pytest.mark.parametrize("transactions", ["local", "xa"])
def test_broker_transaction_boundary_is_pinned(transactions):
    decision = assess_migration(
        FeatureRequirements(transactions=transactions),
        deployment_mode="HA",
        target_role="active",
    )
    assert not decision.allowed
    assert decision.reason == "transaction-boundary-unverified"


def test_disaster_recovery_is_not_warm_scaling_capacity():
    decision = assess_migration(
        FeatureRequirements(), deployment_mode="HA", target_role="dr"
    )
    assert not decision.allowed
    assert decision.reason == "destination-not-scaling-capacity"


def test_replay_and_tracing_combination_fails_closed():
    decision = assess_migration(
        FeatureRequirements(replay=True, tracing=True),
        deployment_mode="HA",
        target_role="active",
    )
    assert not decision.allowed
    assert decision.reason == "unsupported-feature-combination"


@pytest.mark.parametrize(
    "scenario,reason",
    [
        ("replay", "replay-state-migration-unverified"),
        ("tracing", "tracing-migration-unverified"),
    ],
)
def test_measured_scenario_does_not_claim_migration_evidence(scenario, reason):
    cfg = Config.model_validate({"capacity": {"scenario": scenario}})
    requirements = requirements_for(cfg, "payments")
    decision = assess_migration(requirements, deployment_mode="HA", target_role="active")
    assert not decision.allowed
    assert decision.reason == reason


def test_dr_inventory_role_is_accepted_without_becoming_broker_state():
    from solace_autoscale.metrics.fleet import FleetInventory

    inventory = FleetInventory.model_validate(
        {
            "provider": "aws",
            "broker_version": "10.1.2.3",
            "service_class": "enterprise-1k",
            "brokers": [
                {
                    "broker_id": "primary",
                    "shard": "payments",
                    "msg_vpn": "vpn",
                    "base_url": "https://primary.example",
                    "username_env": "U",
                    "password_env": "P",
                    "role": "active",
                },
                {
                    "broker_id": "standby-site",
                    "shard": "payments",
                    "msg_vpn": "vpn",
                    "base_url": "https://standby.example",
                    "username_env": "U",
                    "password_env": "P",
                    "role": "dr",
                },
            ],
        }
    )
    assert inventory.brokers[1].role == "dr"


def test_existing_state_requires_explicit_feature_contract_adoption(tmp_path):
    cfg = Config()
    contract = feature_contract(cfg, ["payments"], deployment_mode="HA")
    store = AssignmentStore(tmp_path / "state.db")
    store.put_placement(Placement("payments", "partition:0", "a", "guaranteed", 100))
    with pytest.raises(ValueError, match="adopt-feature-contract"):
        store.ensure_feature_contract(contract)
    store.ensure_feature_contract(contract, adopt_existing=True)
    assert store.feature_contract() == contract
    assert contract_digest(contract) == contract_digest(dict(reversed(list(contract.items()))))
    store.close()


def test_feature_contract_adoption_cli_requires_explicit_confirmation(tmp_path):
    store_path = tmp_path / "state.db"
    store = AssignmentStore(store_path)
    store.put_placement(Placement("payments", "partition:0", "a", "guaranteed", 100))
    store.close()
    inventory = tmp_path / "fleet.json"
    inventory.write_text(json.dumps({
        "provider": "aws", "broker_version": "10.1.2.3", "service_class": "enterprise-1k",
        "brokers": [{"broker_id": "a", "shard": "payments", "msg_vpn": "vpn",
                     "base_url": "https://a.example", "username_env": "U", "password_env": "P",
                     "endpoints": {"smf": "tcps://a"}}],
    }))
    config = tmp_path / "config.yaml"
    config.write_text(
        "assignment:\n  store: " + str(store_path) + "\n"
        "automation:\n  enabled: true\n  shards:\n    payments: {}\n"
    )
    review = CliRunner().invoke(
        main, ["adopt-feature-contract", "--config", str(config), "--inventory", str(inventory)]
    )
    assert review.exit_code != 0
    assert "digest" in review.output and "rerun with --yes" in review.output
    adopted = CliRunner().invoke(
        main,
        ["adopt-feature-contract", "--yes", "--config", str(config), "--inventory", str(inventory)],
    )
    assert adopted.exit_code == 0, adopted.output
    reopened = AssignmentStore(store_path)
    assert reopened.feature_contract() is not None
    reopened.close()


def test_feature_contract_is_stable_and_persisted(tmp_path):
    cfg = Config.model_validate(
        {
            "capacity": {"scenario": "worst"},
            "automation": {
                "shards": {"payments": {"features": {"transactions": "local"}}}
            },
        }
    )
    contract = feature_contract(cfg, ["payments"], deployment_mode="HA")
    store = AssignmentStore(tmp_path / "state.db")
    store.ensure_feature_contract(contract)
    store.ensure_feature_contract(contract)
    store.ensure_feature_contract(
        feature_contract(cfg, ["orders", "payments"], deployment_mode="HA")
    )

    changed = Config.model_validate(
        {"automation": {"shards": {"payments": {"features": {"transactions": "none"}}}}}
    )
    with pytest.raises(ValueError, match="feature contract changed"):
        store.ensure_feature_contract(
            feature_contract(changed, ["orders", "payments"], deployment_mode="HA")
        )
    with pytest.raises(ValueError, match="feature contract changed"):
        store.ensure_feature_contract(feature_contract(cfg, ["payments"], deployment_mode="HA"))
    store.close()
