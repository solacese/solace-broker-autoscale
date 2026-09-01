"""Tier-0 DNS updater (§9.2).

Maintains one DNS record per shard, e.g. ``shard-a.brokers.example.com``, pointing at the ACTIVE
brokers for that shard. Clients connect to the shard name and are unaware brokers exist behind it —
zero client change, any protocol, any library.

Documented limits (these MUST be clear to users):
  - DNS resolves per SHARD, not per client, so it cannot express sticky placement for a specific
    guaranteed consumer. Use Tier 0 for direct messaging and publishers, NOT durable consumers.
  - TTL bounds reassignment propagation, and many clients cache DNS for the process lifetime.

This module computes the desired record set from the assignment store and hands it to a pluggable
DNS backend (the actual zone update is provider-specific and out of scope here; a dry-run/echo
backend ships for tests). No data-path involvement.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from solace_autoscale.assignment.store import AssignmentStore, BrokerState


@dataclass(frozen=True)
class DnsRecord:
    name: str  # e.g. shard-a.brokers.example.com
    hostnames: list[str]  # broker hostnames the shard name should resolve to
    ttl: int


def desired_records(
    store: AssignmentStore,
    shards: list[str],
    zone: str,
    ttl: int,
) -> list[DnsRecord]:
    """One record per shard listing the ACTIVE brokers' hostnames (from their endpoint map).

    A DRAINING broker is excluded from the DNS record (it takes no new connections) but keeps
    serving existing ones directly — consistent with §9.2.
    """
    records = []
    for shard in shards:
        hostnames = []
        for b in store.brokers_for_shard(shard):
            if b.state != BrokerState.ACTIVE:
                continue
            host = _host_from_endpoints(b.endpoints)
            if host:
                hostnames.append(host)
        records.append(DnsRecord(name=f"{shard}.{zone}", hostnames=sorted(hostnames), ttl=ttl))
    return records


def _host_from_endpoints(endpoints: dict[str, str]) -> str | None:
    """Extract the hostname from any endpoint URI (scheme://host:port)."""
    for uri in endpoints.values():
        rest = uri.split("://", 1)[-1]
        host = rest.split(":", 1)[0].split("/", 1)[0]
        if host:
            return host
    return None


def apply_records(records: list[DnsRecord], backend: Callable[[DnsRecord], None]) -> None:
    """Push each record through a provider backend. The default backend in tests just records calls;
    a real deployment supplies one that talks to its DNS provider."""
    for r in records:
        backend(r)
