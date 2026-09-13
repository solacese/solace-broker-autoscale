"""Mission Control enum/identifier tests (Solace Cloud alignment)."""

from __future__ import annotations

import pytest

from solace_autoscale.cloud import (
    OperationStatus,
    Region,
    ServiceAdminState,
    ServiceClassId,
    ServiceCreationState,
    resolve_service_class,
)
from solace_autoscale.cloud.enums import REGION_BASE_URLS, base_url_for


def test_all_fifteen_service_class_ids_present():
    # Exactly the set from Mission Control OpenAPI v2.0.0 - no more, no fewer.
    expected = {
        "DEVELOPER",
        "ENTERPRISE_250_STANDALONE", "ENTERPRISE_1K_STANDALONE", "ENTERPRISE_5K_STANDALONE",
        "ENTERPRISE_10K_STANDALONE", "ENTERPRISE_50K_STANDALONE", "ENTERPRISE_100K_STANDALONE",
        "ENTERPRISE_200K_STANDALONE",
        "ENTERPRISE_250_HIGHAVAILABILITY", "ENTERPRISE_1K_HIGHAVAILABILITY",
        "ENTERPRISE_5K_HIGHAVAILABILITY", "ENTERPRISE_10K_HIGHAVAILABILITY",
        "ENTERPRISE_50K_HIGHAVAILABILITY", "ENTERPRISE_100K_HIGHAVAILABILITY",
        "ENTERPRISE_200K_HIGHAVAILABILITY",
    }
    assert {c.value for c in ServiceClassId} == expected


def test_resolve_friendly_labels():
    assert resolve_service_class("enterprise-10k") is ServiceClassId.ENTERPRISE_10K_STANDALONE
    assert resolve_service_class("enterprise-10k-ha") is ServiceClassId.ENTERPRISE_10K_HIGHAVAILABILITY
    assert resolve_service_class("developer") is ServiceClassId.DEVELOPER
    # case-insensitive on labels
    assert resolve_service_class("Enterprise-5K") is ServiceClassId.ENTERPRISE_5K_STANDALONE


def test_resolve_raw_service_class_id():
    assert (
        resolve_service_class("ENTERPRISE_100K_HIGHAVAILABILITY")
        is ServiceClassId.ENTERPRISE_100K_HIGHAVAILABILITY
    )
    # case-insensitive on the raw id
    assert resolve_service_class("enterprise_1k_standalone") is ServiceClassId.ENTERPRISE_1K_STANDALONE
    # an actual enum member resolves to itself
    assert resolve_service_class(ServiceClassId.DEVELOPER) is ServiceClassId.DEVELOPER


def test_resolve_rejects_unknown_with_helpful_message():
    with pytest.raises(ValueError) as exc:
        resolve_service_class("enterprise-9k")
    msg = str(exc.value)
    assert "enterprise-9k" in msg
    assert "enterprise-10k" in msg  # lists valid labels
    assert "ENTERPRISE_10K_STANDALONE" in msg  # lists valid ids


def test_operation_status_terminal_and_success():
    assert OperationStatus.SUCCEEDED.is_terminal()
    assert OperationStatus.SUCCEEDED.is_success()
    assert OperationStatus.FAILED.is_terminal()
    assert not OperationStatus.FAILED.is_success()
    assert not OperationStatus.PENDING.is_terminal()
    assert not OperationStatus.INPROGRESS.is_terminal()


def test_creation_state_completed_not_succeeded():
    # A subtle spec fact: Service.creationState reaches COMPLETED, not SUCCEEDED.
    assert {s.value for s in ServiceCreationState} == {
        "PENDING", "INPROGRESS", "COMPLETED", "FAILED"
    }
    assert ServiceCreationState.COMPLETED.is_ready()
    assert ServiceCreationState.COMPLETED.is_terminal()
    assert not ServiceCreationState.INPROGRESS.is_terminal()


def test_admin_state_values():
    assert {s.value for s in ServiceAdminState} == {"INITIAL", "START", "STOP", "DESTROY"}


def test_region_base_urls():
    assert base_url_for(Region.US) == "https://api.solace.cloud"
    assert base_url_for(Region.AU) == "https://api.solacecloud.com.au"
    assert base_url_for(Region.EU) == "https://api.solacecloud.eu"
    assert base_url_for(Region.SG) == "https://api.solacecloud.sg"
    assert base_url_for(Region.US_STATIC_IP) == "https://static-ip-api.solace.cloud"
    # every region has a base url
    assert set(REGION_BASE_URLS) == set(Region)
