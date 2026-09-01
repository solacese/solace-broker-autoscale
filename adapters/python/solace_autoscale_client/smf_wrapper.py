"""Tier-2 SMF SDK wrapper (§9.2). Only for protocols Solace owns (SMF, Solace JMS).

Wraps connection creation: caches the assignment, re-looks-up on reconnect, and honours a
reassignment signal. Guaranteed consumers are NEVER silently reassigned (§9.4) — a reassignment
signal applies only to direct-mode clients and publishers; for a guaranteed consumer the wrapper
raises so the application decides.

This wrapper deliberately does not import the Solace messaging library, so it is testable without it;
the ``connect_fn`` seam is where the real ``solace.messaging`` MessagingService is built from the
resolved SMF host.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .adapters import smf_host
from .resolver import Assignment, Resolver


class GuaranteedReassignmentRefused(Exception):
    """A reassignment signal arrived for a guaranteed consumer. Not silently honoured (§9.4)."""


@dataclass
class SmfConnection:
    assignment: Assignment
    handle: object  # whatever connect_fn returns (a real MessagingService in production)


class SmfClient:
    def __init__(
        self,
        resolver: Resolver,
        shard: str,
        client_id: str,
        mode: str,
        connect_fn: Callable[[str, Assignment], object],
    ) -> None:
        self._resolver = resolver
        self._shard = shard
        self._client_id = client_id
        self._mode = mode
        self._connect_fn = connect_fn
        self._conn: SmfConnection | None = None

    def connect(self) -> SmfConnection:
        a = self._resolver.resolve(self._shard, self._client_id, self._mode, protocol="smf")
        handle = self._connect_fn(smf_host(a), a)
        self._conn = SmfConnection(assignment=a, handle=handle)
        return self._conn

    def reconnect(self) -> SmfConnection:
        """On reconnect, re-look-up the assignment (it may have moved) and reconnect."""
        return self.connect()

    def on_reassignment_signal(self) -> SmfConnection:
        """Handle a reassignment signal. Direct/publisher: re-resolve and reconnect. Guaranteed
        consumer: refuse — its durable queue lives on one broker and must not move silently."""
        if self._mode == "guaranteed":
            raise GuaranteedReassignmentRefused(
                f"guaranteed consumer {self._client_id!r} received a reassignment signal; a durable "
                "queue lives on one broker and is never silently reassigned (§9.4). The application "
                "must migrate deliberately."
            )
        return self.reconnect()

    @property
    def current(self) -> SmfConnection | None:
        return self._conn
