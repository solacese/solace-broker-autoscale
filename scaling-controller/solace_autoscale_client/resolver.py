"""Tier-1 resolver: calls the assignment service, caches the result with an optional maximum stale age.

The resolver returns a location (broker + per-protocol endpoint map). It does not vend credentials
and does not touch the message path.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field


class ResolverError(Exception):
    pass


@dataclass
class Assignment:
    broker_id: str
    msg_vpn: str
    state: str
    lease_seconds: int
    endpoints: dict[str, str]
    fetched_at: float
    reused_existing: bool = False
    partition_id: int | None = None
    partition_count: int | None = None
    queue_name: str | None = None
    topic_prefix: str | None = None

    def endpoint(self, protocol: str) -> str:
        if protocol not in self.endpoints:
            raise ResolverError(
                f"protocol {protocol!r} not in endpoint map; available: {sorted(self.endpoints)}"
            )
        return self.endpoints[protocol]


@dataclass
class Resolver:
    """Resolve a (shard, client_id) to an Assignment, with a fail-open cache.

    On a successful call the result is cached. If the assignment service is later unreachable, the
    cached assignment is returned while it satisfies max_stale_seconds. Authorization rejection
    never falls back. Missing or expired cache raises ResolverError; durable callers retain work.
    """

    base_url: str
    timeout: float = 5.0
    max_stale_seconds: float | None = None
    api_key: str | None = field(default=None, repr=False)
    _clock: callable = time.time  # type: ignore[valid-type]
    _cache: dict[tuple, Assignment] = field(default_factory=dict)
    _opener: callable | None = None  # test seam: (url)->bytes

    def resolve(self, shard: str, client_id: str, mode: str = "direct",
                protocol: str | None = None, *, routing_key: str | None = None,
                partition: int | None = None) -> Assignment:
        key = (shard, client_id, mode, protocol, routing_key, partition)
        try:
            body = self._fetch(shard, client_id, mode, protocol, routing_key, partition)
            a = Assignment(
                broker_id=body["broker_id"], msg_vpn=body["msg_vpn"], state=body["state"],
                lease_seconds=body["lease_seconds"], endpoints=body["endpoints"],
                fetched_at=self._clock(), reused_existing=body.get("reused_existing", False),
                partition_id=body.get("partition_id"), partition_count=body.get("partition_count"),
                queue_name=body.get("queue_name"), topic_prefix=body.get("topic_prefix"),
            )
            self._cache[key] = a
            return a
        except urllib.error.HTTPError as e:
            # Authorization/validation failures must not reuse a stale assignment.
            if e.code < 500:
                raise ResolverError(f"assignment request rejected (HTTP {e.code})") from e
            cached = self._cache.get(key)
            if cached is not None and self._usable(cached):
                return cached
            raise ResolverError(f"assignment service unavailable (HTTP {e.code}); no cache") from e
        except (urllib.error.URLError, OSError, ResolverError, KeyError, ValueError) as e:
            cached = self._cache.get(key)
            if cached is not None and self._usable(cached):
                return cached  # fail open
            raise ResolverError(
                f"assignment service unreachable and no cached assignment for {key}: {e}"
            ) from e

    def _usable(self, cached: Assignment) -> bool:
        return self.max_stale_seconds is None or self._clock() - cached.fetched_at <= self.max_stale_seconds

    def _fetch(self, shard: str, client_id: str, mode: str, protocol: str | None,
               routing_key: str | None, partition: int | None) -> dict:
        params = {"shard": shard, "client_id": client_id, "mode": mode}
        if protocol:
            params["protocol"] = protocol
        if routing_key is not None:
            params["routing_key"] = routing_key
        if partition is not None:
            params["partition"] = str(partition)
        url = f"{self.base_url.rstrip('/')}/assignment?{urllib.parse.urlencode(params)}"
        if self._opener is not None:
            return json.loads(self._opener(url))
        req = urllib.request.Request(url)
        if self.api_key:
            req.add_header("Authorization", f"Bearer {self.api_key}")
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310
            return json.loads(resp.read())
