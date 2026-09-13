"""Assignment store (§9.1).

Persists broker inventory (per shard, with per-protocol endpoints and state) and durable guaranteed
placements. SQLite by default; a Postgres backend is provided for multi-instance deployment using
**optimistic locking** on placement writes (a version column with a compare-and-set), documented in
docs and chosen over leader election because placement writes are low-rate and idempotent.

Guaranteed placement is sticky and durable: a queue lives on exactly one broker, so a consumer must
return to the same broker. Placements survive service restart and lease expiry as long as the queue
exists. A DRAINING broker takes no NEW assignments but keeps serving existing ones.

Nothing here vends credentials - only locations.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path


class BrokerState(StrEnum):
    ACTIVE = "active"
    DRAINING = "draining"
    DRAINED = "drained"
    DELETING = "deleting"
    GONE = "gone"


# States in which a broker may still SERVE existing guaranteed placements.
_SERVING_STATES = {BrokerState.ACTIVE, BrokerState.DRAINING, BrokerState.DRAINED}
# States in which a broker may take a NEW assignment.
_ASSIGNABLE_STATES = {BrokerState.ACTIVE}


@dataclass
class Broker:
    broker_id: str
    shard: str
    msg_vpn: str
    state: BrokerState
    #: protocol -> connection URI (per-protocol endpoint map; ports from broker config, not hardcoded)
    endpoints: dict[str, str] = field(default_factory=dict)


@dataclass
class Placement:
    shard: str
    client_id: str
    broker_id: str
    mode: str  # direct | guaranteed
    lease_expires_at: float
    version: int = 1


_SCHEMA = """
CREATE TABLE IF NOT EXISTS brokers (
    broker_id TEXT PRIMARY KEY,
    shard TEXT NOT NULL,
    msg_vpn TEXT NOT NULL,
    state TEXT NOT NULL,
    endpoints TEXT NOT NULL  -- JSON {protocol: uri}
);
CREATE TABLE IF NOT EXISTS placements (
    shard TEXT NOT NULL,
    client_id TEXT NOT NULL,
    broker_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    lease_expires_at REAL NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (shard, client_id)
);
CREATE INDEX IF NOT EXISTS idx_brokers_shard ON brokers(shard);
"""


class AssignmentStore:
    """SQLite-backed store. Postgres uses the same interface (see PostgresAssignmentStore note)."""

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # ---- broker inventory --------------------------------------------------------------------

    def upsert_broker(self, broker: Broker) -> None:
        self._conn.execute(
            """INSERT INTO brokers (broker_id, shard, msg_vpn, state, endpoints)
               VALUES (?,?,?,?,?)
               ON CONFLICT(broker_id) DO UPDATE SET
                 shard=excluded.shard, msg_vpn=excluded.msg_vpn, state=excluded.state,
                 endpoints=excluded.endpoints""",
            (broker.broker_id, broker.shard, broker.msg_vpn, broker.state.value,
             json.dumps(broker.endpoints)),
        )
        self._conn.commit()

    def set_broker_state(self, broker_id: str, state: BrokerState) -> None:
        self._conn.execute("UPDATE brokers SET state=? WHERE broker_id=?", (state.value, broker_id))
        self._conn.commit()

    def get_broker(self, broker_id: str) -> Broker | None:
        row = self._conn.execute("SELECT * FROM brokers WHERE broker_id=?", (broker_id,)).fetchone()
        return _row_to_broker(row) if row else None

    def brokers_for_shard(self, shard: str) -> list[Broker]:
        rows = self._conn.execute("SELECT * FROM brokers WHERE shard=?", (shard,)).fetchall()
        return [_row_to_broker(r) for r in rows]

    def assignable_brokers(self, shard: str) -> list[Broker]:
        return [b for b in self.brokers_for_shard(shard) if b.state in _ASSIGNABLE_STATES]

    # ---- placements --------------------------------------------------------------------------

    def get_placement(self, shard: str, client_id: str) -> Placement | None:
        row = self._conn.execute(
            "SELECT * FROM placements WHERE shard=? AND client_id=?", (shard, client_id)
        ).fetchone()
        return _row_to_placement(row) if row else None

    def put_placement(self, p: Placement) -> None:
        """Idempotent upsert with optimistic locking (compare-and-set on ``p.version``).

        For an update, the CAS matches on the version the CALLER holds (``p.version``): if another
        writer bumped it in between, ``rowcount`` is 0 and we raise so the caller re-reads and
        retries. This is the multi-instance safety mechanism (ADR / docs), chosen over leader
        election because placement writes are low-rate and idempotent.
        """
        existing = self.get_placement(p.shard, p.client_id)
        if existing is None:
            self._conn.execute(
                """INSERT INTO placements (shard, client_id, broker_id, mode, lease_expires_at, version)
                   VALUES (?,?,?,?,?,1)""",
                (p.shard, p.client_id, p.broker_id, p.mode, p.lease_expires_at),
            )
        else:
            cur = self._conn.execute(
                """UPDATE placements SET broker_id=?, mode=?, lease_expires_at=?, version=version+1
                   WHERE shard=? AND client_id=? AND version=?""",
                (p.broker_id, p.mode, p.lease_expires_at, p.shard, p.client_id, p.version),
            )
            if cur.rowcount == 0:
                raise OptimisticLockError(
                    f"placement {p.shard}/{p.client_id} changed concurrently "
                    f"(held version {p.version}); re-read and retry"
                )
        self._conn.commit()

    def renew_lease(self, shard: str, client_id: str, lease_expires_at: float) -> None:
        self._conn.execute(
            "UPDATE placements SET lease_expires_at=?, version=version+1 WHERE shard=? AND client_id=?",
            (lease_expires_at, shard, client_id),
        )
        self._conn.commit()

    def delete_placement(self, shard: str, client_id: str) -> None:
        self._conn.execute(
            "DELETE FROM placements WHERE shard=? AND client_id=?", (shard, client_id)
        )
        self._conn.commit()

    def placements_on_broker(self, broker_id: str, mode: str | None = None) -> list[Placement]:
        if mode:
            rows = self._conn.execute(
                "SELECT * FROM placements WHERE broker_id=? AND mode=?", (broker_id, mode)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM placements WHERE broker_id=?", (broker_id,)
            ).fetchall()
        return [_row_to_placement(r) for r in rows]

    def close(self) -> None:
        self._conn.close()


class OptimisticLockError(Exception):
    """Raised when a compare-and-set placement write loses a race. Caller retries."""


def _row_to_broker(row: sqlite3.Row) -> Broker:
    return Broker(
        broker_id=row["broker_id"], shard=row["shard"], msg_vpn=row["msg_vpn"],
        state=BrokerState(row["state"]), endpoints=json.loads(row["endpoints"]),
    )


def _row_to_placement(row: sqlite3.Row) -> Placement:
    return Placement(
        shard=row["shard"], client_id=row["client_id"], broker_id=row["broker_id"],
        mode=row["mode"], lease_expires_at=row["lease_expires_at"], version=row["version"],
    )
