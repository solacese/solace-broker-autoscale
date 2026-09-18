"""Config loading/validation tests (§4)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from solace_autoscale.config import Config, load_config, parse_duration

from .conftest import REPO


def test_example_config_loads():
    cfg = load_config(REPO / "examples" / "config.example.yaml")
    assert cfg.fleet.service_class == "enterprise-10k"
    assert cfg.billing.model == "committed"
    assert cfg.actuation.mode == "recommend"  # default stays recommend
    assert cfg.actuation.dry_run is True


def test_unknown_key_rejected():
    with pytest.raises(ValidationError):
        Config.model_validate({"fleet": {"service_class": "x", "bogus": 1}})


def test_unknown_top_level_key_rejected():
    with pytest.raises(ValidationError):
        Config.model_validate({"nope": {}})


@pytest.mark.parametrize("value,secs", [
    ("30s", 30), ("3m", 180), ("45m", 2700), ("1h", 3600), ("30", 30), (30, 30), ("500ms", 0.5),
])
def test_duration_parsing(value, secs):
    assert parse_duration(value) == pytest.approx(secs)


def test_scale_up_window_floor_enforced():
    # §5.8: below 3 * scrape_interval is rejected
    with pytest.raises(ValidationError):
        Config.model_validate({
            "metrics": {"scrape_interval": "30s"},
            "policy": {"scale_up_window": "60s"},  # < 90s
        })


def test_scale_up_window_auto_ok():
    cfg = Config.model_validate({"policy": {"scale_up_window": "auto"}})
    assert cfg.policy.scale_up_window == "auto"


def test_max_below_min_rejected():
    with pytest.raises(ValidationError):
        Config.model_validate({"fleet": {"min_brokers": 5, "max_brokers": 2}})


def test_service_class_accepts_label_and_resolves():
    from solace_autoscale.cloud import ServiceClassId
    cfg = Config.model_validate({"fleet": {"service_class": "enterprise-10k-ha"}})
    assert cfg.fleet.service_class == "enterprise-10k-ha"  # stored label unchanged
    assert cfg.fleet.service_class_id is ServiceClassId.ENTERPRISE_10K_HIGHAVAILABILITY


def test_service_class_accepts_raw_id():
    from solace_autoscale.cloud import ServiceClassId
    cfg = Config.model_validate({"fleet": {"service_class": "ENTERPRISE_5K_STANDALONE"}})
    assert cfg.fleet.service_class_id is ServiceClassId.ENTERPRISE_5K_STANDALONE


def test_service_class_typo_rejected():
    with pytest.raises(ValidationError):
        Config.model_validate({"fleet": {"service_class": "enterprise-9k"}})


def test_cloud_region_resolves_base_url():
    from solace_autoscale.cloud import Region
    cfg = Config.model_validate({"cloud": {"region": "eu"}})
    assert cfg.cloud.region is Region.EU
    assert cfg.cloud.effective_base_url() == "https://api.solacecloud.eu"


def test_cloud_base_url_overrides_region():
    cfg = Config.model_validate({
        "cloud": {"region": "eu", "base_url": "https://api.solace.cloud/"},
    })
    # explicit base_url wins over region, trailing slash trimmed
    assert cfg.cloud.effective_base_url() == "https://api.solace.cloud"


def test_cloud_defaults_to_us():
    cfg = Config()
    assert cfg.cloud.effective_base_url() == "https://api.solace.cloud"
    assert cfg.cloud.timeout == pytest.approx(30.0)


def test_config_hash_stable_and_sensitive():
    a = Config()
    b = Config()
    assert a.config_hash() == b.config_hash()
    c = Config.model_validate({"fleet": {"max_brokers": 16}})
    assert c.config_hash() != a.config_hash()


def test_enabled_protocols():
    cfg = Config()
    assert "smf" in cfg.protocols.enabled_protocols()
    assert "mqtt" not in cfg.protocols.enabled_protocols()  # default disabled


@pytest.mark.parametrize('data', [
    {'metrics': {'scrape_interval': 0}},
    {'metrics': {'staleness_limit': -1}},
    {'policy': {'scale_down_window': -1}},
    {'policy': {'cooldown': -1}},
    {'metrics': {'scrape_interval': float('inf')}},
    {'policy': {'scale_up_window': float('nan')}},
])
def test_invalid_timing_configuration_is_rejected(data):
    from solace_autoscale.config import Config
    with pytest.raises(ValueError):
        Config.model_validate(data)
