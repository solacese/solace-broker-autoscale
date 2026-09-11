"""Canonical model of the replicable slice of a broker's SEMPv2 *config* (not monitor).

A broker's message-VPN config is a set of named objects — queues, their subscriptions,
topic-endpoints, client-profiles, ACL-profiles. To replicate config across brokers we normalise each
object to ``(kind, identity, attributes)`` where *identity* is the object's SEMP name(s) and
*attributes* is the mutable body with volatile/monitor-only fields stripped. Two brokers are "in
sync" for an object when their normalised attributes are equal.

Pure and dependency-free: parsing a SEMP snapshot into ``ConfigObject``s and comparing them needs no
network. The thin client that actually fetches/writes SEMP lives in ``semp_config.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: Object kinds we replicate, in dependency order (parents before children). The SEMPv2 config
#: collection path and the attribute(s) that form each object's identity within its parent.
KIND_SPECS: dict[str, dict[str, Any]] = {
    "queue": {
        "collection": "queues",
        "identity": ["queueName"],
        # attributes that are runtime/volatile and must not drive a diff
        "ignore": {"msgSpoolUsage", "bindCount", "spooledMsgCount", "averageRxMsgRate"},
    },
    "queueSubscription": {
        "collection": "subscriptions",
        "parent": "queue",
        "identity": ["subscriptionTopic"],
        "ignore": set(),
    },
    "topicEndpoint": {
        "collection": "topicEndpoints",
        "identity": ["topicEndpointName"],
        "ignore": {"msgSpoolUsage", "bindCount"},
    },
    "clientProfile": {
        "collection": "clientProfiles",
        "identity": ["clientProfileName"],
        "ignore": set(),
    },
    "aclProfile": {
        "collection": "aclProfiles",
        "identity": ["aclProfileName"],
        "ignore": set(),
    },
}

#: Attributes present in every object that never carry replicable intent.
_GLOBAL_IGNORE = {"msgVpnName", "count", "meta"}


@dataclass(frozen=True)
class ConfigObject:
    """One normalised SEMP config object.

    ``kind`` — key into KIND_SPECS. ``identity`` — the ordered identity attribute values (e.g. a
    queueName, or (queueName, subscriptionTopic) for a subscription). ``attributes`` — the mutable
    body with volatile fields removed, so equality means "same intended config".
    """

    kind: str
    identity: tuple[str, ...]
    attributes: tuple[tuple[str, Any], ...]  # sorted items, hashable

    @property
    def attrs(self) -> dict[str, Any]:
        return dict(self.attributes)

    def key(self) -> tuple[str, tuple[str, ...]]:
        return (self.kind, self.identity)


def normalise(kind: str, raw: dict[str, Any], *, parent: tuple[str, ...] = ()) -> ConfigObject:
    """Normalise one raw SEMP object of ``kind`` into a ConfigObject."""
    spec = KIND_SPECS[kind]
    ignore = _GLOBAL_IGNORE | set(spec["ignore"]) | set(spec["identity"])
    ident = tuple(str(raw[a]) for a in spec["identity"])
    identity = parent + ident
    attributes = tuple(
        sorted((k, v) for k, v in raw.items() if k not in ignore)
    )
    return ConfigObject(kind=kind, identity=identity, attributes=attributes)


@dataclass
class BrokerConfig:
    """The full replicable config snapshot of one broker's VPN, indexed by object key."""

    broker_id: str
    objects: dict[tuple[str, tuple[str, ...]], ConfigObject] = field(default_factory=dict)

    def add(self, obj: ConfigObject) -> None:
        self.objects[obj.key()] = obj

    def of_kind(self, kind: str) -> list[ConfigObject]:
        return [o for o in self.objects.values() if o.kind == kind]
