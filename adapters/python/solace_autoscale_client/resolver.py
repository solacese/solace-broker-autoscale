"""Tier-1 resolver: calls the assignment service, caches the result, never fails closed.

The resolver returns a location (broker + per-protocol endpoint map). It does not vend credentials
and does not touch the message path.
"""

from __future__ import annotations

import json
import time
import urllib.error
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
    cached assignment is returned rather than raising - the service being down must never take the
    application down (§9.4). If there is no cache AND the service is unreachable, ResolverError is
    raised (there is genuinely nothing to connect to).
    """

    base_url: str
    timeout: float = 5.0
    _clock: callable = time.time  # type: ignore[valid-type]
    _cache: dict[tuple[str, str, str], Assignment] = field(default_factory=dict)
    _opener: callable | None = None  # test seam: (url)->bytes

    def resolve(self, shard: str, client_id: str, mode: str = "direct",
                protocol: str | None = None) -> Assignment:
        key = (shard, client_id, mode)
        try:
            body = self._fetch(shard, client_id, mode, protocol)
            a = Assignment(
                broker_id=body["broker_id"], msg_vpn=body["msg_vpn"], state=body["state"],
                lease_seconds=body["lease_seconds"], endpoints=body["endpoints"],
                fetched_at=self._clock(), reused_existing=body.get("reused_existing", False),
            )
            self._cache[key] = a
            return a
        except (urllib.error.URLError, OSError, ResolverError, KeyError) as e:
            cached = self._cache.get(key)
            if cached is not None:
                return cached  # fail open
            raise ResolverError(
                f"assignment service unreachable and no cached assignment for {key}: {e}"
            ) from e

    def _fetch(self, shard: str, client_id: str, mode: str, protocol: str | None) -> dict:
        params = f"shard={shard}&client_id={client_id}&mode={mode}"
        if protocol:
            params += f"&protocol={protocol}"
        url = f"{self.base_url.rstrip('/')}/assignment?{params}"
        if self._opener is not None:
            return json.loads(self._opener(url))
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310
            return json.loads(resp.read())
