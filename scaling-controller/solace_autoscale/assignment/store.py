"""Assignment store (§9.1).

Persists broker inventory (per shard, with per-protocol endpoints and state) and durable guaranteed
placements. SQLite uses a local file, WAL and complete assignment transactions. Threads and processes
sharing that local file serialize writers. This is not a distributed Postgres implementation.

Guaranteed placement is sticky and durable: a queue lives on exactly one broker, so a consumer must
return to the same broker. Placements survive service restart and lease expiry as long as the queue
exists. A DRAINING broker takes no NEW assignments but keeps serving existing ones.

Nothing here vends credentials - only locations.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from functools import wraps
from pathlib import Path
from typing import Concatenate, ParamSpec, TypeVar


class BrokerState(StrEnum):
    WARM = "warm"
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
CREATE INDEX IF NOT EXISTS idx_placements_broker ON placements(broker_id);
CREATE TABLE IF NOT EXISTS routing_settings (name TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


P = ParamSpec("P")
T = TypeVar("T")


def _transactional(fn: Callable[Concatenate[AssignmentStore, P], T]
                   ) -> Callable[Concatenate[AssignmentStore, P], T]:
    """Serialize all access, nesting store calls inside the complete assignment transaction."""
    @wraps(fn)
    def wrapped(self: AssignmentStore, /, *args: P.args, **kwargs: P.kwargs) -> T:
        with self.transaction():
            return fn(self, *args, **kwargs)
    return wrapped


class AssignmentStore:
    """SQLite-backed single-host store; never place the database on a network filesystem."""

    def __init__(self, path: str | Path) -> None:
        self._lock = threading.RLock()
        self._depth = 0
        self._path = str(path)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Commit the whole lookup/select/write operation, or roll it all back on failure."""
        with self._lock:
            outer = self._depth == 0
            if outer:
                self._conn.execute("BEGIN IMMEDIATE")
            self._depth += 1
            try:
                yield
                if outer:
                    self._conn.commit()
            except BaseException:
                if outer:
                    self._conn.rollback()
                raise
            finally:
                self._depth -= 1

    # ---- broker inventory --------------------------------------------------------------------

    @_transactional
    def ensure_routing(self, routing: str, partitions: int) -> None:
        """Persist the partition contract so a configuration edit cannot strand existing queues."""
        value = json.dumps({"routing": routing, "partitions": partitions if routing == "partitioned" else 0,
                            "hash": "sha256-json-v1"}, sort_keys=True)
        row = self._conn.execute("SELECT value FROM routing_settings WHERE name='contract'").fetchone()
        if row is not None and row["value"] != value:
            raise ValueError("routing/partition count changed; migrate queues and placements explicitly")
        if row is None:
            existing = self._conn.execute("SELECT COUNT(*) FROM placements").fetchone()[0]
            if existing and routing != "client":
                raise ValueError("existing client placements require migration before partitioned routing")
            self._conn.execute("INSERT INTO routing_settings VALUES ('contract',?)", (value,))

    @_transactional
    def ensure_managed_namespace(self, fleet_id: str | None) -> None:
        """Keep queue names stable across controller and assignment service restarts."""
        row = self._conn.execute(
            "SELECT value FROM routing_settings WHERE name='managed_namespace'"
        ).fetchone()
        if row is not None and row["value"] != fleet_id:
            raise ValueError(
                "managed fleet_id changed or disabled; existing queue ownership must be preserved"
            )
        if row is None and fleet_id is not None:
            self._conn.execute("INSERT INTO routing_settings VALUES ('managed_namespace',?)", (fleet_id,))

    @_transactional
    def feature_contract(self) -> dict[str, object] | None:
        row = self._conn.execute(
            "SELECT value FROM routing_settings WHERE name='feature_contract'"
        ).fetchone()
        return json.loads(row["value"]) if row else None

    @_transactional
    def ensure_feature_contract(
        self, contract: dict[str, object], *, adopt_existing: bool = False
    ) -> None:
        """Allow additive shards but never silently attest existing durable ownership."""
        value = json.dumps(contract, sort_keys=True, separators=(",", ":"))
        row = self._conn.execute(
            "SELECT value FROM routing_settings WHERE name='feature_contract'"
        ).fetchone()
        if row is None:
            existing = any(
                self._conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
                for table in ("placements", "migrations", "controller_events")
                if self._conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone()
            )
            if existing and not adopt_existing:
                raise ValueError(
                    "existing durable state has no feature contract; run adopt-feature-contract"
                )
            self._conn.execute("INSERT INTO routing_settings VALUES ('feature_contract',?)", (value,))
            return
        previous = json.loads(row["value"])
        prior_shards = previous.get("shards", {})
        current_shards = contract.get("shards", {})
        unchanged = (
            all(previous.get(key) == contract.get(key) for key in (
                "schema", "bundle_scope", "cross_partition_transactions", "deployment_mode",
                "routing_libraries",
            ))
            and isinstance(prior_shards, dict)
            and isinstance(current_shards, dict)
            and all(current_shards.get(shard) == features for shard, features in prior_shards.items())
        )
        if not unchanged:
            raise ValueError("feature contract changed; migrate broker-local state explicitly")
        if previous != contract:
            self._conn.execute(
                "UPDATE routing_settings SET value=? WHERE name='feature_contract'", (value,)
            )

    @_transactional
    def placement_contract(self) -> dict[str, object] | None:
        row = self._conn.execute(
            "SELECT value FROM routing_settings WHERE name='placement_contract'"
        ).fetchone()
        return json.loads(row["value"]) if row else None

    @_transactional
    def ensure_placement_contract(
        self, contract: dict[str, object], *, adopt_existing: bool = False
    ) -> None:
        """Allow new brokers while preserving every recorded capability/domain assertion."""
        value = json.dumps(contract, sort_keys=True, separators=(",", ":"))
        row = self._conn.execute(
            "SELECT value FROM routing_settings WHERE name='placement_contract'"
        ).fetchone()
        if row is None:
            existing = self._conn.execute("SELECT 1 FROM placements LIMIT 1").fetchone()
            if existing and not adopt_existing:
                raise ValueError(
                    "existing durable state has no broker placement contract; "
                    "run adopt-feature-contract"
                )
            self._conn.execute("INSERT INTO routing_settings VALUES ('placement_contract',?)", (value,))
            return
        previous = json.loads(row["value"])
        prior_brokers = previous.get("brokers", {})
        current_brokers = contract.get("brokers", {})
        prior_placement = previous.get("placement", {})
        current_placement = contract.get("placement", {})
        prior_shards = prior_placement.get("shards", {}) if isinstance(prior_placement, dict) else {}
        current_shards = current_placement.get("shards", {}) if isinstance(current_placement, dict) else {}
        unchanged = (
            previous.get("schema") == contract.get("schema")
            and isinstance(prior_brokers, dict)
            and isinstance(current_brokers, dict)
            and isinstance(prior_shards, dict)
            and isinstance(current_shards, dict)
            and all(current_brokers.get(broker) == facts for broker, facts in prior_brokers.items())
            and all(current_shards.get(shard) == rules for shard, rules in prior_shards.items())
        )
        if not unchanged:
            raise ValueError("broker placement contract changed; migrate ownership explicitly")
        if previous != contract:
            self._conn.execute(
                "UPDATE routing_settings SET value=? WHERE name='placement_contract'", (value,)
            )

    @_transactional
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

    @_transactional
    def set_broker_state(self, broker_id: str, state: BrokerState) -> None:
        self._conn.execute("UPDATE brokers SET state=? WHERE broker_id=?", (state.value, broker_id))

    @_transactional
    def get_broker(self, broker_id: str) -> Broker | None:
        row = self._conn.execute("SELECT * FROM brokers WHERE broker_id=?", (broker_id,)).fetchone()
        return _row_to_broker(row) if row else None

    @_transactional
    def brokers_for_shard(self, shard: str) -> list[Broker]:
        rows = self._conn.execute("SELECT * FROM brokers WHERE shard=?", (shard,)).fetchall()
        return [_row_to_broker(r) for r in rows]

    @_transactional
    def assignable_brokers(self, shard: str) -> list[Broker]:
        return [b for b in self.brokers_for_shard(shard) if b.state in _ASSIGNABLE_STATES]

    # ---- placements --------------------------------------------------------------------------

    @_transactional
    def get_placement(self, shard: str, client_id: str) -> Placement | None:
        row = self._conn.execute(
            "SELECT * FROM placements WHERE shard=? AND client_id=?", (shard, client_id)
        ).fetchone()
        return _row_to_placement(row) if row else None

    @_transactional
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

    @_transactional
    def renew_lease(self, shard: str, client_id: str, lease_expires_at: float) -> None:
        self._conn.execute(
            "UPDATE placements SET lease_expires_at=?, version=version+1 WHERE shard=? AND client_id=?",
            (lease_expires_at, shard, client_id),
        )

    @_transactional
    def delete_placement(self, shard: str, client_id: str) -> None:
        self._conn.execute(
            "DELETE FROM placements WHERE shard=? AND client_id=?", (shard, client_id)
        )

    @_transactional
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
        with self._lock:
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
