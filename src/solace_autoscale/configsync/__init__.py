"""Broker config replication.

Keeps the replicable slice of a message-VPN's SEMPv2 *config* (queues, subscriptions,
topic-endpoints, client/ACL profiles) in sync across a fleet. Pick a source-of-truth broker, snapshot
every broker, diff, and apply the ops so the fleet converges — re-runnable, so an edit made on the
source propagates to the others.

Pure core (``model``, ``diff``) has no I/O and is unit-tested against literals; the thin SEMP config
client (``semp_config``) fetches and writes, gated by the actuator's dry-run / kill-switch safety.
"""

from .diff import ConfigOp, diff, summarise
from .model import KIND_SPECS, BrokerConfig, ConfigObject, normalise

__all__ = [
    "ConfigOp",
    "diff",
    "summarise",
    "KIND_SPECS",
    "BrokerConfig",
    "ConfigObject",
    "normalise",
]
