"""Logical topic rules and durable native subscription groups for managed messaging."""

from __future__ import annotations

import hashlib
import json
import re

from .store import AssignmentStore


def validate_topic(value: str, *, subscription: bool = False) -> str:
    """Validate managed SMF topics: whole-level *, terminal >; reject reserved namespaces."""
    if not value or len(value.encode()) > 128 or value.startswith("#") or "\x00" in value:
        raise ValueError("topic must be nonempty, at most 128 bytes, and outside reserved namespaces")
    levels = value.split("/")
    if any(not level for level in levels):
        raise ValueError("empty topic levels are not supported")
    for i, level in enumerate(levels):
        if "*" in level or ">" in level:
            if not subscription or not (level == "*" or level == ">" and i == len(levels) - 1):
                raise ValueError("subscriptions support whole-level * and terminal > only")
    return value


def matches(pattern: str, topic: str) -> bool:
    """SMF > consumes one or more remaining levels; * consumes exactly one."""
    parts, values = pattern.split("/"), topic.split("/")
    for i, part in enumerate(parts):
        if i >= len(values):
            return False
        if part == ">":
            return True
        if part != "*" and part != values[i]:
            return False
    return len(parts) == len(values)


def overlaps(first: str, second: str) -> bool:
    """Whether two supported SMF subscription patterns can match a common concrete topic."""
    left, right = first.split("/"), second.split("/")
    for a, b in zip(left, right, strict=False):
        if a == ">" or b == ">":
            return True
        if a != "*" and b != "*" and a != b:
            return False
    return len(left) == len(right)


def topic_prefix(fleet: str, broker: str, shard: str, partition: int) -> str:
    """Owner-specific native namespace avoids mesh forwarding to a preparing destination."""
    owner = hashlib.sha256(broker.encode()).hexdigest()[:16]
    shard_hash = hashlib.sha256(shard.encode()).hexdigest()[:16]
    return f"autoscale/{fleet}/{owner}/{shard_hash}/p{partition}/"


def group_queue(base: str, group: str) -> str:
    return base + "/g" + hashlib.sha256(group.encode()).hexdigest()[:16]


class TopicRegistry:
    """Durable groups outlive worker sessions; definition changes require an explicit migration."""

    def __init__(self, store: AssignmentStore) -> None:
        self.store = store
        with store.transaction():
            store._conn.execute(
                "CREATE TABLE IF NOT EXISTS topic_groups "
                "(name TEXT PRIMARY KEY, patterns TEXT NOT NULL, ready INTEGER NOT NULL DEFAULT 0)"
            )

    def contract(self, value: dict) -> None:
        """Do not silently change routing extraction or disable topic mode against existing queues."""
        encoded = json.dumps(value, sort_keys=True)
        with self.store.transaction():
            row = self.store._conn.execute(
                "SELECT value FROM routing_settings WHERE name='topic_contract'"
            ).fetchone()
            if row and row["value"] != encoded:
                previous = json.loads(row["value"])
                old = {json.dumps(r, sort_keys=True) for r in previous.get("routes", [])}
                new = {json.dumps(r, sort_keys=True) for r in value.get("routes", [])}
                if not (previous.get("enabled") and value.get("enabled") and old <= new):
                    raise ValueError(
                        "topic routing contract changed; preserve existing topic/queue ownership"
                    )
                pending = self.store._conn.execute(
                    "SELECT COUNT(*) FROM migrations WHERE phase NOT IN ('complete','rolled-back')"
                ).fetchone()[0]
                if pending:
                    raise ValueError("finish pending migrations before adding topic routes")
                self.store._conn.execute(
                    "UPDATE routing_settings SET value=? WHERE name='topic_contract'", (encoded,)
                )
                self.store._conn.execute("UPDATE topic_groups SET ready=0")
            if not row:
                if (
                    value.get("enabled")
                    and self.store._conn.execute("SELECT COUNT(*) FROM placements").fetchone()[0]
                ):
                    raise ValueError(
                        "native topic mode requires a fresh store or explicit legacy queue migration"
                    )
                self.store._conn.execute(
                    "INSERT INTO routing_settings VALUES ('topic_contract',?)", (encoded,)
                )

    def groups(self, shard: str | None = None) -> dict[str, list[str]]:
        with self.store.transaction():
            groups = {
                r["name"]: json.loads(r["patterns"])
                for r in self.store._conn.execute("SELECT * FROM topic_groups ORDER BY name").fetchall()
            }
            if shard is None:
                return groups
            row = self.store._conn.execute(
                "SELECT value FROM routing_settings WHERE name='topic_contract'"
            ).fetchone()
            if row is None:
                return groups  # Legacy registry without topic scope.
            routes = [r["pattern"] for r in json.loads(row["value"])["routes"] if r["shard"] == shard]
            return {
                g: patterns
                for g, patterns in groups.items()
                if any(overlaps(p, r) for p in patterns for r in routes)
            }

    def register(self, name: str, patterns: list[str], *, now: float = 0.0) -> bool:
        """Same group = competing replicas; return whether durable policy changed."""
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name) or not 1 <= len(patterns) <= 64:
            raise ValueError("group needs a simple stable name and 1..64 subscriptions")
        patterns = sorted({validate_topic(p, subscription=True) for p in patterns})
        with self.store.transaction():
            known = self.groups()
            if name in known:
                if known[name] != patterns:
                    raise ValueError(
                        "durable group subscriptions differ; use a new group or explicit migration"
                    )
                return False
            if len(known) >= 64:
                raise ValueError("maximum 64 managed subscriber groups reached")
            pending = self.store._conn.execute(
                "SELECT COUNT(*) FROM migrations WHERE phase NOT IN ('complete','rolled-back')"
            ).fetchone()[0]
            if pending:
                raise ValueError("new groups wait until the current partition migration completes")
            self.store._conn.execute(
                "INSERT INTO topic_groups(name,patterns) VALUES (?,?)", (name, json.dumps(patterns))
            )
            return True

    def ready(self, groups: list[str]) -> bool:
        with self.store.transaction():
            ready = {
                r["name"]
                for r in self.store._conn.execute("SELECT name FROM topic_groups WHERE ready=1").fetchall()
            }
            return bool(groups) and set(groups) <= ready

    def mark_ready(self, groups: list[str]) -> bool:
        with self.store.transaction():
            placeholders = ",".join("?" for _ in groups)
            changed = bool(groups) and bool(self.store._conn.execute(
                f"SELECT 1 FROM topic_groups WHERE ready=0 AND name IN ({placeholders}) LIMIT 1",
                groups,
            ).fetchone())
            self.store._conn.executemany(
                "UPDATE topic_groups SET ready=1 WHERE name=?", [(name,) for name in groups]
            )
            return changed
