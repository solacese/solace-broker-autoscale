"""Mission Control identifiers, mirrored from the OpenAPI (version 2.0.0).

Every value here is copied verbatim from the authoritative spec. We never invent identifiers; if
the spec adds a service class or a region, this file is the one place that changes. Callers compare
against these enums rather than hand-typing strings, so a typo fails at the boundary, not in a
Cloud API round-trip.
"""

from __future__ import annotations

from enum import StrEnum


class ServiceClassId(StrEnum):
    """``ServiceClassId`` - the broker size/redundancy tiers Mission Control provisions.

    Standalone tiers are single-node; ``_HIGHAVAILABILITY`` tiers are the HA (redundancy-group)
    counterparts. ``DEVELOPER`` is the free single-node tier.
    """

    DEVELOPER = "DEVELOPER"
    ENTERPRISE_250_STANDALONE = "ENTERPRISE_250_STANDALONE"
    ENTERPRISE_1K_STANDALONE = "ENTERPRISE_1K_STANDALONE"
    ENTERPRISE_5K_STANDALONE = "ENTERPRISE_5K_STANDALONE"
    ENTERPRISE_10K_STANDALONE = "ENTERPRISE_10K_STANDALONE"
    ENTERPRISE_50K_STANDALONE = "ENTERPRISE_50K_STANDALONE"
    ENTERPRISE_100K_STANDALONE = "ENTERPRISE_100K_STANDALONE"
    ENTERPRISE_200K_STANDALONE = "ENTERPRISE_200K_STANDALONE"
    ENTERPRISE_250_HIGHAVAILABILITY = "ENTERPRISE_250_HIGHAVAILABILITY"
    ENTERPRISE_1K_HIGHAVAILABILITY = "ENTERPRISE_1K_HIGHAVAILABILITY"
    ENTERPRISE_5K_HIGHAVAILABILITY = "ENTERPRISE_5K_HIGHAVAILABILITY"
    ENTERPRISE_10K_HIGHAVAILABILITY = "ENTERPRISE_10K_HIGHAVAILABILITY"
    ENTERPRISE_50K_HIGHAVAILABILITY = "ENTERPRISE_50K_HIGHAVAILABILITY"
    ENTERPRISE_100K_HIGHAVAILABILITY = "ENTERPRISE_100K_HIGHAVAILABILITY"
    ENTERPRISE_200K_HIGHAVAILABILITY = "ENTERPRISE_200K_HIGHAVAILABILITY"


class OperationStatus(StrEnum):
    """``Operation.status`` / ``MultiResourceOperation.status`` - the async operation lifecycle."""

    PENDING = "PENDING"
    INPROGRESS = "INPROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

    def is_terminal(self) -> bool:
        return self in (OperationStatus.SUCCEEDED, OperationStatus.FAILED)

    def is_success(self) -> bool:
        return self is OperationStatus.SUCCEEDED


class ServiceCreationState(StrEnum):
    """``Service.creationState`` - note this reaches ``COMPLETED`` (not ``SUCCEEDED``)."""

    PENDING = "PENDING"
    INPROGRESS = "INPROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    def is_terminal(self) -> bool:
        return self in (ServiceCreationState.COMPLETED, ServiceCreationState.FAILED)

    def is_ready(self) -> bool:
        return self is ServiceCreationState.COMPLETED


class ServiceAdminState(StrEnum):
    """``Service.adminState`` - the desired lifecycle state of the broker service."""

    INITIAL = "INITIAL"
    START = "START"
    STOP = "STOP"
    DESTROY = "DESTROY"


class Region(StrEnum):
    """Mission Control control-plane regions. Each maps to a distinct API base URL."""

    US = "us"
    AU = "au"
    EU = "eu"
    SG = "sg"
    US_STATIC_IP = "us-static-ip"


#: Region -> Mission Control API base URL. Verified against the Solace Cloud REST documentation.
REGION_BASE_URLS: dict[Region, str] = {
    Region.US: "https://api.solace.cloud",
    Region.AU: "https://api.solacecloud.com.au",
    Region.EU: "https://api.solacecloud.eu",
    Region.SG: "https://api.solacecloud.sg",
    Region.US_STATIC_IP: "https://static-ip-api.solace.cloud",
}


def base_url_for(region: Region) -> str:
    """Return the API base URL for a region."""
    return REGION_BASE_URLS[region]


#: Friendly config labels -> canonical ServiceClassId. Operators write ``enterprise-10k`` in YAML;
#: we resolve it to ``ENTERPRISE_10K_STANDALONE``. HA variants take a ``-ha`` suffix. A raw
#: ServiceClassId also resolves to itself (see ``resolve_service_class``).
SERVICE_CLASS_ALIASES: dict[str, ServiceClassId] = {
    "developer": ServiceClassId.DEVELOPER,
    "enterprise-250": ServiceClassId.ENTERPRISE_250_STANDALONE,
    "enterprise-1k": ServiceClassId.ENTERPRISE_1K_STANDALONE,
    "enterprise-5k": ServiceClassId.ENTERPRISE_5K_STANDALONE,
    "enterprise-10k": ServiceClassId.ENTERPRISE_10K_STANDALONE,
    "enterprise-50k": ServiceClassId.ENTERPRISE_50K_STANDALONE,
    "enterprise-100k": ServiceClassId.ENTERPRISE_100K_STANDALONE,
    "enterprise-200k": ServiceClassId.ENTERPRISE_200K_STANDALONE,
    "enterprise-250-ha": ServiceClassId.ENTERPRISE_250_HIGHAVAILABILITY,
    "enterprise-1k-ha": ServiceClassId.ENTERPRISE_1K_HIGHAVAILABILITY,
    "enterprise-5k-ha": ServiceClassId.ENTERPRISE_5K_HIGHAVAILABILITY,
    "enterprise-10k-ha": ServiceClassId.ENTERPRISE_10K_HIGHAVAILABILITY,
    "enterprise-50k-ha": ServiceClassId.ENTERPRISE_50K_HIGHAVAILABILITY,
    "enterprise-100k-ha": ServiceClassId.ENTERPRISE_100K_HIGHAVAILABILITY,
    "enterprise-200k-ha": ServiceClassId.ENTERPRISE_200K_HIGHAVAILABILITY,
}


def resolve_service_class(value: str) -> ServiceClassId:
    """Resolve a friendly label or a raw ServiceClassId to the canonical ``ServiceClassId``.

    Accepts, case-insensitively:
      * a friendly label from ``SERVICE_CLASS_ALIASES`` (e.g. ``enterprise-10k``, ``developer``);
      * a raw ServiceClassId (e.g. ``ENTERPRISE_10K_HIGHAVAILABILITY``).

    Raises ``ValueError`` naming the valid options on a miss, so a typo fails at config load.
    """
    if isinstance(value, ServiceClassId):
        return value
    raw = str(value).strip()
    # raw ServiceClassId (case-insensitive on the enum name/value)
    try:
        return ServiceClassId(raw.upper())
    except ValueError:
        pass
    alias = SERVICE_CLASS_ALIASES.get(raw.lower())
    if alias is not None:
        return alias
    valid_labels = ", ".join(sorted(SERVICE_CLASS_ALIASES))
    valid_ids = ", ".join(c.value for c in ServiceClassId)
    raise ValueError(
        f"unknown service class {value!r}; use a friendly label ({valid_labels}) "
        f"or a Mission Control ServiceClassId ({valid_ids})"
    )
