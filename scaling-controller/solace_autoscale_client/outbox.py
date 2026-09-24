"""Durable retry buffer for managed guaranteed partitions; business handlers deduplicate IDs."""

from __future__ import annotations

import fcntl
import json
import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path

from .key_router import KeyRouter
from .resolver import Assignment


class DurableOutbox:
    """Persist before send, delete only after a positive broker acknowledgment.

    ``send`` must block until ACK and raise on NACK/timeout. Ambiguous acknowledgments may
    produce duplicates: consumers must commit an idempotency key with the payment transaction.
    A failed partition does not block other partitions, but preserves its own pending order.
    """

    def __init__(self, path: str | Path, router: KeyRouter, *, max_bytes: int = 100_000_000) -> None:
        if router.mode != "guaranteed" or max_bytes < 1:
            raise ValueError("outbox requires guaranteed routing and positive bounded storage")
        self.file_lock = Path(str(path) + ".lock").open("a")
        try:
            fcntl.flock(self.file_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.file_lock.close()
            raise ValueError("another publisher owns this outbox file") from exc
        self.router, self.max_bytes = router, max_bytes
        self.lock = threading.RLock()
        self.flush_lock = threading.Lock()
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS outbox (
            seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL,
            partition INTEGER NOT NULL, payload BLOB NOT NULL,
            routing_kind TEXT, routing_value TEXT, evaluator_name TEXT, evaluator_version TEXT)""")
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(outbox)")}
        for name in (
            "routing_kind", "routing_value", "evaluator_name", "evaluator_version"
        ):
            if name not in columns:
                self.db.execute(f"ALTER TABLE outbox ADD COLUMN {name} TEXT")
        self.db.execute("CREATE TABLE IF NOT EXISTS routing_contract (value TEXT NOT NULL)")
        contract = json.dumps([router.shard, router.partitions, router.mode, "sha256-json-v1"])
        saved = self.db.execute("SELECT value FROM routing_contract").fetchone()
        if saved is not None and saved[0] != contract:
            self.db.close()
            self.file_lock.close()
            raise ValueError("outbox routing contract differs; use the original shard and partition count")
        if saved is None:
            self.db.execute("INSERT INTO routing_contract VALUES (?)", (contract,))
        self.db.execute("CREATE INDEX IF NOT EXISTS outbox_partition_seq ON outbox(partition,seq)")
        self.db.commit()
        self.bytes_used, self.rows = self.db.execute(
            "SELECT COALESCE(SUM(length(payload)),0), COUNT(*) FROM outbox"
        ).fetchone()

    def enqueue(
        self,
        key: str,
        event_id: str,
        payload: bytes,
        *,
        partition: int | None = None,
        routing_kind: str = "key",
        evaluator_name: str | None = None,
        evaluator_version: str | None = None,
    ) -> None:
        """Persist the resolved result/partition/library once; retries never run application code."""
        if not event_id or not isinstance(payload, bytes):
            raise ValueError("event_id and bytes payload are required")
        if (evaluator_name is None) != (evaluator_version is None):
            raise ValueError("routing evaluator name and version must be stored together")
        if routing_kind not in ("key", "sha256"):
            raise ValueError("routing kind must be key or sha256")
        if partition is None:
            if routing_kind != "key":
                raise ValueError("digest routing requires an explicitly resolved partition")
            partition = self.router.partition_for(key)
        if not 0 <= partition < self.router.partitions:
            raise ValueError("partition outside configured range")
        identity = (
            partition, payload, routing_kind, key, evaluator_name, evaluator_version
        )
        with self.lock:
            with self.db:
                existing = self.db.execute(
                    "SELECT partition,payload,routing_kind,routing_value,evaluator_name,"
                    "evaluator_version FROM outbox WHERE event_id=?", (event_id,)
                ).fetchone()
                if existing:
                    # Rows accepted by an older client have no stored result/library. Preserve their
                    # original partition/payload idempotency without manufacturing metadata.
                    if existing[2] is None:
                        if existing[:2] != identity[:2]:
                            raise ValueError("event_id reused with different partition or payload")
                    elif existing != identity:
                        raise ValueError("event_id reused with different content or routing")
                    return
                if self.bytes_used + len(payload) > self.max_bytes or self.rows >= 100000:
                    raise BufferError("durable outbox full; apply upstream backpressure")
                self.db.execute(
                    "INSERT INTO outbox(event_id,partition,payload,routing_kind,routing_value,"
                    "evaluator_name,evaluator_version) VALUES (?,?,?,?,?,?,?)",
                    (event_id, *identity),
                )
            self.bytes_used += len(payload)
            self.rows += 1

    def heads(self) -> list[tuple]:
        """Read the oldest durable publication for each independent partition."""
        with self.lock:
            return self.db.execute(
                'SELECT seq,event_id,partition,payload FROM outbox WHERE seq IN '
                '(SELECT MIN(seq) FROM outbox GROUP BY partition) ORDER BY seq'
            ).fetchall()

    def acknowledge(self, sequence: int) -> None:
        """Delete only a positively acknowledged sequence, idempotently."""
        with self.lock:
            with self.db:
                row = self.db.execute(
                    'SELECT length(payload) FROM outbox WHERE seq=?', (sequence,)
                ).fetchone()
                if row:
                    self.db.execute('DELETE FROM outbox WHERE seq=?', (sequence,))
            if row:
                self.bytes_used -= row[0]
                self.rows -= 1

    def flush(self, send: Callable[[Assignment, str, bytes], None], *, limit: int = 100) -> int:
        """Flush independent partitions, retaining failed heads and their subsequent messages."""
        acknowledged = 0
        attempts = 0
        blocked: set[int] = set()
        locations: dict[int, Assignment] = {}
        with self.flush_lock:
            while attempts < limit:
                with self.lock:
                    rows = self.db.execute(
                        'SELECT seq,event_id,partition,payload FROM outbox WHERE seq IN '
                        '(SELECT MIN(seq) FROM outbox GROUP BY partition) ORDER BY seq'
                    ).fetchall()
                eligible = [row for row in rows if row[2] not in blocked]
                if not eligible:
                    break
                for seq, event_id, partition, payload in eligible:
                    if attempts >= limit:
                        break
                    attempts += 1
                    try:
                        if partition not in locations:
                            locations[partition] = self.router.resolve_partition(partition, refresh=True)
                        send(locations[partition], event_id, payload)
                    except Exception:
                        blocked.add(partition)
                        continue
                    self.acknowledge(seq)
                    acknowledged += 1
        return acknowledged

    def pending(self) -> int:
        with self.lock:
            return int(self.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0])

    def close(self) -> None:
        with self.flush_lock, self.lock:
            self.db.close()
            self.file_lock.close()


def payment_envelope(event_id: str, payload: bytes) -> str:
    """Example JSON envelope; consumers atomically deduplicate event_id with their business effect."""
    return json.dumps({"event_id": event_id, "payload": json.loads(payload)})
