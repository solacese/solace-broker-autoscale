"""Config loading/validation tests (§4)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from solace_autoscale.config import Config, load_config, parse_duration

from .conftest import REPO


def test_example_config_loads():
    cfg = load_config(REPO / "config.example.yaml")
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
