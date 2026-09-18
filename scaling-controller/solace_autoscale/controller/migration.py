"""Restart-safe queue handover: prepare → fence → grace/drain → commit → activate.

Time alone never permits movement. The source must reject writes and have no ready, stored
or unacknowledged messages. Consumers must be bound on the destination before cutover.
"""

from __future__ import annotations

import math
from typing import Protocol

from ..config import AutomationConfig
from .semp import QueueStatus
from .store import ControllerStore, Migration


class QueueControl(Protocol):
    def prepare(self, broker: str, shard: str, partition: int, *, enabled: bool = False) -> None: ...
    def ingress(self, broker: str, shard: str, partition: int, enabled: bool) -> None: ...
    def status(self, broker: str, shard: str, partition: int) -> QueueStatus: ...


class MigrationEngine:
    """One idempotent step per tick. A single controller writer holds the process lock."""

    def __init__(self, store: ControllerStore, queues: QueueControl, policy: AutomationConfig) -> None:
        self.store, self.queues, self.policy = store, queues, policy

    def advance(self, migration: Migration, now: float) -> None:
        """Record progress or a retryable error; never skip a safety condition after restart."""
        if not math.isfinite(now) or now < migration.updated_at:
            return
        try:
            self._step(migration, now)
        except Exception as exc:
            # No response payload/credentials in audit logs. Reset the continuous-empty proof.
            self.store.update(migration, now, detail=f"retry required: {type(exc).__name__}")
            self.store.event(
                now, "step-failed", {"phase": migration.phase, "error_type": type(exc).__name__}, migration.id
            )

    def _step(self, m: Migration, now: float) -> None:
        p, q = self.policy, self.queues
        if now - m.created_at >= p.migration_timeout and m.phase not in ("activating", "rolling-back"):
            self.store.update(m, now, phase="rolling-back", detail="handover timed out; restoring source")
            return
        if m.phase == "preparing":
            self.store.event(now, "prepare-target-intent", {"target": m.target}, m.id)
            q.prepare(m.target, m.shard, m.partition)
            target = q.status(m.target, m.shard, m.partition)
            if target.ingress_enabled or not target.drained:
                raise ValueError("target must be empty and ingress-disabled before handover")
            if target.consumers < p.min_target_consumers:
                self.store.update(m, now, detail="waiting for target consumers")
                return
            self.store.update(m, now, phase="fencing")
        elif m.phase == "fencing":
            self.store.event(now, "fence-source-intent", {"source": m.source}, m.id)
            q.ingress(m.source, m.shard, m.partition, False)
            self.store.update(m, now, phase="draining")
        elif m.phase == "draining":
            source = q.status(m.source, m.shard, m.partition)
            target = q.status(m.target, m.shard, m.partition)
            if source.ingress_enabled or target.ingress_enabled:
                raise ValueError("both ingress fences must remain in place")
            if not target.drained or target.consumers < p.min_target_consumers:
                self.store.update(m, now, detail="destination readiness changed")
                return
            continuous = m.empty_since
            if not source.drained or now - m.updated_at > 2 * p.poll_interval:
                continuous = None
            if source.drained and continuous is None:
                continuous = now
            if (
                continuous is not None
                and now - continuous >= p.empty_settle
                and now - m.phase_started_at >= p.migration_grace
            ):
                self.store.commit_owner(m, now)
                return
            self.store.update(m, now, empty_since=continuous, detail="grace period / acknowledged drain")
        elif m.phase == "activating":
            # Ownership already committed. Never roll back to the source after this boundary.
            source = q.status(m.source, m.shard, m.partition)
            if source.ingress_enabled or not source.drained:
                raise ValueError("source fence/drain changed after ownership commit")
            target = q.status(m.target, m.shard, m.partition)
            if target.consumers < p.min_target_consumers:
                self.store.update(m, now, detail="waiting for destination consumers before activation")
                return
            self.store.event(now, "activate-target-intent", {"target": m.target}, m.id)
            q.ingress(m.target, m.shard, m.partition, True)
            self.store.update(m, now, phase="complete")
        elif m.phase == "rolling-back":
            owner = self.store.assignments.get_placement(m.shard, f"partition:{m.partition}")
            if not owner or owner.broker_id != m.source:
                raise ValueError("cannot roll back after ownership changed")
            # Target was never allowed to receive data. If this is not provable, keep both fenced.
            q.prepare(m.target, m.shard, m.partition)
            target = q.status(m.target, m.shard, m.partition)
            if target.ingress_enabled or not target.drained:
                raise ValueError("cannot prove rollback safe")
            self.store.event(now, "restore-source-intent", {"source": m.source}, m.id)
            q.ingress(m.source, m.shard, m.partition, True)
            self.store.update(m, now, phase="rolled-back")
