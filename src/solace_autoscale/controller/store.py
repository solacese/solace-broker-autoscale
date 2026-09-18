"""Durable migration intents, transitions and ownership commits in the assignment database."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any

from ..assignment.store import AssignmentStore

TERMINAL = ("complete", "rolled-back")


@dataclass(frozen=True)
class Migration:
    id: str
    shard: str
    partition: int
    source: str
    target: str
    phase: str
    created_at: float
    updated_at: float
    phase_started_at: float
    empty_since: float | None
    detail: str | None


def queue_name(fleet_id: str, shard: str, partition: int) -> str:
    """Namespace only the controller's own queues; never infer ownership from customer queue names."""
    digest = hashlib.sha256(shard.encode()).hexdigest()[:16]
    return f"autoscale/{fleet_id}/{digest}/p{partition}"


class ControllerStore:
    """Uses the same SQLite transaction as placement changes for atomic cutover."""

    def __init__(self, assignments: AssignmentStore) -> None:
        self.assignments = assignments
        with assignments.transaction():
            assignments._conn.execute("""CREATE TABLE IF NOT EXISTS migrations (
                id TEXT PRIMARY KEY, shard TEXT NOT NULL, partition INTEGER NOT NULL,
                source TEXT NOT NULL, target TEXT NOT NULL, phase TEXT NOT NULL,
                created_at REAL NOT NULL, updated_at REAL NOT NULL, phase_started_at REAL NOT NULL,
                empty_since REAL, detail TEXT)""")
            assignments._conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS one_partition_move
                ON migrations(shard,partition) WHERE phase NOT IN ('complete','rolled-back')""")
            assignments._conn.execute("""CREATE TABLE IF NOT EXISTS controller_events (
                id INTEGER PRIMARY KEY, ts REAL NOT NULL, migration_id TEXT, event TEXT NOT NULL,
                detail TEXT NOT NULL)""")

    def event(self, now: float, event: str, detail: dict[str, Any], migration_id: str | None = None) -> None:
        """Write durable intent before issuing network mutations; never include credentials."""
        with self.assignments.transaction():
            self.assignments._conn.execute(
                "INSERT INTO controller_events(ts,migration_id,event,detail) VALUES (?,?,?,?)",
                (now, migration_id, event, json.dumps(detail)),
            )

    def begin(self, shard: str, partition: int, source: str, target: str, now: float) -> Migration:
        """Record intent only if the source still owns the guaranteed partition."""
        with self.assignments.transaction():
            if self.assignments._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='topic_groups'"
            ).fetchone() and self.assignments._conn.execute(
                'SELECT COUNT(*) FROM topic_groups WHERE ready=0'
            ).fetchone()[0]:
                raise ValueError('prepare all newly registered subscriber groups before migration')
            owner = self.assignments.get_placement(shard, f"partition:{partition}")
            if not owner or owner.broker_id != source or owner.mode != "guaranteed" or source == target:
                raise ValueError("migration source must own this guaranteed partition")
            m = Migration(
                uuid.uuid4().hex, shard, partition, source, target, "preparing", now, now, now, None, None
            )
            self.assignments._conn.execute(
                "INSERT INTO migrations VALUES (?,?,?,?,?,?,?,?,?,?,?)", tuple(m.__dict__.values())
            )
            self.event(
                now, "migration-planned", {"partition": partition, "source": source, "target": target}, m.id
            )
            return m

    def pending(self) -> list[Migration]:
        """Read persisted work after any restart; terminal records remain as audit history."""
        with self.assignments.transaction():
            rows = self.assignments._conn.execute(
                "SELECT * FROM migrations WHERE phase NOT IN ('complete','rolled-back') ORDER BY created_at"
            ).fetchall()
            return [Migration(**dict(r)) for r in rows]

    def for_partition(self, shard: str, partition: int) -> Migration | None:
        """Return the active handover advertised to publishers and consumer workers."""
        return next((m for m in self.pending() if m.shard == shard and m.partition == partition), None)

    def update(
        self,
        m: Migration,
        now: float,
        *,
        phase: str | None = None,
        empty_since: float | None = None,
        detail: str | None = None,
    ) -> None:
        """Persist the next recovery phase and its observation clock."""
        with self.assignments.transaction():
            self.assignments._conn.execute(
                "UPDATE migrations SET phase=?,updated_at=?,phase_started_at=?,empty_since=?,detail=? "
                "WHERE id=?",
                (
                    phase or m.phase,
                    now,
                    now if phase and phase != m.phase else m.phase_started_at,
                    empty_since,
                    detail,
                    m.id,
                ),
            )
            if phase and phase != m.phase:
                self.event(now, "transition", {"from": m.phase, "to": phase, "detail": detail}, m.id)

    def commit_owner(self, m: Migration, now: float) -> None:
        """CAS the owner and migration phase together; ingress remains fenced until activation."""
        with self.assignments.transaction():
            owner = self.assignments.get_placement(m.shard, f"partition:{m.partition}")
            if not owner or owner.broker_id != m.source or owner.mode != "guaranteed":
                raise ValueError("ownership changed outside the controller; cutover refused")
            owner.broker_id = m.target
            self.assignments.put_placement(owner)
            self.update(m, now, phase="activating")

    def started_since(self, since: float) -> int:
        """Enforce migration-rate limits across restarts."""
        with self.assignments.transaction():
            return int(
                self.assignments._conn.execute(
                    "SELECT COUNT(*) FROM migrations WHERE created_at>=?", (since,)
                ).fetchone()[0]
            )

    def recently_moved(self, since: float) -> set[tuple[str, int]]:
        """Per-partition cooldown prevents ping-pong and immediate retries of a failed handover."""
        with self.assignments.transaction():
            rows = self.assignments._conn.execute(
                "SELECT shard,partition FROM migrations WHERE updated_at>=?", (since,)
            ).fetchall()
            return {(row["shard"], row["partition"]) for row in rows}
