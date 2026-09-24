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
            assignments._conn.execute("""CREATE TABLE IF NOT EXISTS managed_partitions (
                shard TEXT NOT NULL, partition INTEGER NOT NULL, ready INTEGER NOT NULL,
                PRIMARY KEY (shard,partition))""")
            assignments._conn.execute("""CREATE TABLE IF NOT EXISTS managed_control_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision INTEGER NOT NULL)""")
            assignments._conn.execute(
                "INSERT OR IGNORE INTO managed_control_state VALUES (1,0)"
            )
            assignments._conn.execute("""CREATE TABLE IF NOT EXISTS managed_control_outbox (
                revision INTEGER PRIMARY KEY, created_at REAL NOT NULL, kind TEXT NOT NULL)""")

    def _signal(self, now: float, kind: str) -> int:
        """Advance the authoritative revision and enqueue its hint in the current transaction."""
        self.assignments._conn.execute(
            "UPDATE managed_control_state SET revision=revision+1 WHERE singleton=1"
        )
        revision = int(self.assignments._conn.execute(
            "SELECT revision FROM managed_control_state WHERE singleton=1"
        ).fetchone()[0])
        # Hints are level-triggered: one latest revision is enough, and bounds storage while disabled.
        self.assignments._conn.execute("DELETE FROM managed_control_outbox")
        self.assignments._conn.execute(
            "INSERT INTO managed_control_outbox(revision,created_at,kind) VALUES (?,?,?)",
            (revision, now, kind),
        )
        return revision

    def signal(self, now: float, kind: str) -> int:
        with self.assignments.transaction():
            return self._signal(now, kind)

    def revision(self) -> int:
        with self.assignments.transaction():
            return int(self.assignments._conn.execute(
                "SELECT revision FROM managed_control_state WHERE singleton=1"
            ).fetchone()[0])

    def pending_notifications(self, limit: int = 128) -> list[tuple[int, float, str]]:
        if not 1 <= limit <= 1024:
            raise ValueError("notification batch limit must be 1..1024")
        with self.assignments.transaction():
            rows = self.assignments._conn.execute(
                "SELECT revision,created_at,kind FROM managed_control_outbox "
                "ORDER BY revision LIMIT ?", (limit,)
            ).fetchall()
            return [(int(row["revision"]), float(row["created_at"]), row["kind"]) for row in rows]

    def notification_published(self, revision: int) -> None:
        with self.assignments.transaction():
            self.assignments._conn.execute(
                "DELETE FROM managed_control_outbox WHERE revision<=?", (revision,)
            )

    def mark_partition_ready(self, shard: str, partition: int, now: float | None = None) -> None:
        with self.assignments.transaction():
            previous = self.assignments._conn.execute(
                "SELECT ready FROM managed_partitions WHERE shard=? AND partition=?",
                (shard, partition),
            ).fetchone()
            self.assignments._conn.execute(
                "INSERT INTO managed_partitions VALUES (?,?,1) "
                "ON CONFLICT(shard,partition) DO UPDATE SET ready=1",
                (shard, partition),
            )
            if not previous or not previous["ready"]:
                self._signal(0.0 if now is None else now, "partition-ready")

    def partition_ready(self, shard: str, partition: int) -> bool:
        with self.assignments.transaction():
            row = self.assignments._conn.execute(
                "SELECT ready FROM managed_partitions WHERE shard=? AND partition=?",
                (shard, partition),
            ).fetchone()
            return bool(row and row["ready"])

    def event(self, now: float, event: str, detail: dict[str, Any], migration_id: str | None = None) -> None:
        """Write durable intent before issuing network mutations; never include credentials."""
        with self.assignments.transaction():
            self.assignments._conn.execute(
                "INSERT INTO controller_events(ts,migration_id,event,detail) VALUES (?,?,?,?)",
                (now, migration_id, event, json.dumps(detail)),
            )

    def begin(
        self,
        shard: str,
        partition: int,
        source: str,
        target: str,
        now: float,
        *,
        activate_warm: bool = False,
        explanation: dict[str, Any] | None = None,
    ) -> Migration:
        """Record intent and any warm activation in one local transaction."""
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
            target_broker = self.assignments.get_broker(target)
            if target_broker is None:
                raise ValueError("migration target is not in the durable broker inventory")
            if activate_warm:
                if target_broker.state.value != "warm":
                    raise ValueError("planned warm target is no longer warm")
                self.assignments.set_broker_state(target, target_broker.state.ACTIVE)
            elif target_broker.state.value != "active":
                raise ValueError("migration target is not active")
            self.assignments._conn.execute(
                "INSERT INTO migrations VALUES (?,?,?,?,?,?,?,?,?,?,?)", tuple(m.__dict__.values())
            )
            detail: dict[str, Any] = {
                "partition": partition,
                "source": source,
                "target": target,
                "activated_warm": activate_warm,
            }
            if explanation:
                detail["planner"] = explanation
            self.event(now, "migration-planned", detail, m.id)
            self._signal(now, "migration-planned")
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
                if phase == "rolled-back":
                    target = self.assignments.get_broker(m.target)
                    if target is not None and target.state.value == "active":
                        event = self.assignments._conn.execute(
                            "SELECT detail FROM controller_events "
                            "WHERE migration_id=? AND event='migration-planned' ORDER BY id LIMIT 1",
                            (m.id,),
                        ).fetchone()
                        planned = json.loads(event["detail"]) if event else {}
                        unrelated = self.assignments._conn.execute(
                            "SELECT 1 FROM placements WHERE broker_id=? "
                            "AND NOT (shard=? AND client_id=?) LIMIT 1",
                            (m.target, m.shard, f"partition:{m.partition}"),
                        ).fetchone()
                        if planned.get("activated_warm") and not unrelated:
                            self.assignments.set_broker_state(m.target, target.state.WARM)
                self.event(now, "transition", {"from": m.phase, "to": phase, "detail": detail}, m.id)
                self._signal(now, "migration-transition")

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
