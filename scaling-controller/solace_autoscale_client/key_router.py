"""Local business-key partitioning; resolve once per partition lease, not once per message."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from .resolver import Assignment, Resolver, ResolverError


@dataclass
class KeyRouter:
    """Applications pool their own protocol connections by the returned broker_id.

    Publishers route each key through this helper; consumers resolve explicit partition IDs
    and bind queues on the SAME returned broker. Queue creation/subscriptions remain application
    responsibilities. A durable partition does not move merely because a broker joins.
    """

    resolver: Resolver
    shard: str
    client_id: str
    partitions: int = 128
    mode: str = "guaranteed"
    protocol: str = "smf"
    _assignments: dict[int, Assignment] = field(default_factory=dict, repr=False)
    required_revision: int = field(default=0, repr=False)

    def partition_for(self, key: str) -> int:
        """sha256-json-v1; kept identical to the assignment server for cross-client stability."""
        if not key or self.partitions < 1:
            raise ValueError("nonempty key and positive partition count are required")
        data = json.dumps([self.shard, key], ensure_ascii=False, separators=(",", ":")).encode()
        return int.from_bytes(hashlib.sha256(data).digest(), "big") % self.partitions

    def resolve_key(self, key: str) -> Assignment:
        """Map a business key to a leased partition owner, with no message-path proxy."""
        return self.resolve_partition(self.partition_for(key))

    def resolve_partition(self, partition: int, *, refresh: bool = False) -> Assignment:
        """Consumers can use this for their assigned partition numbers; refresh on reconnect."""
        if not 0 <= partition < self.partitions:
            raise ValueError("partition outside configured range")
        cached = self._assignments.get(partition)
        if (not refresh and cached is not None and cached.revision >= self.required_revision
                and self.resolver._usable(cached)
                and self.resolver._clock() < cached.fetched_at + cached.lease_seconds):
            return cached
        result = self.resolver.resolve(self.shard, self.client_id, self.mode, self.protocol,
                                       partition=partition)
        if self.resolver._clock() >= result.fetched_at + result.lease_seconds:
            raise ResolverError("managed assignment lease expired before authoritative refresh")
        if result.revision < self.required_revision:
            raise ResolverError("controller returned a stale managed revision")
        if result.partition_id != partition or result.partition_count != self.partitions:
            raise ResolverError("client/server partition contract differs; refusing to route")
        self._assignments[partition] = result
        self.required_revision = max(self.required_revision, result.revision)
        return result
