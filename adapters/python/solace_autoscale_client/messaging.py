"""One application API over native SMF topics, durable queues and controller-managed placement."""

from __future__ import annotations

import hashlib
import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .dispatch import OutboxDispatcher
from .key_router import KeyRouter
from .managed_smf import ManagedConsumers, SmfConnections
from .outbox import DurableOutbox
from .resolver import Resolver


def _matches(pattern: str, topic: str) -> bool:
    parts, levels = pattern.split("/"), topic.split("/")
    for i, part in enumerate(parts):
        if i >= len(levels):
            return False
        if part == ">":
            return True
        if part != "*" and part != levels[i]:
            return False
    return len(parts) == len(levels)


@dataclass(frozen=True)
class Message:
    topic: str
    payload: Any
    event_id: str


class MessagingClient:
    """Publish and subscribe without exposing broker identities, queues or partitions to the app.

    publish() confirms durable LOCAL acceptance; flush() waits for broker ACKs. The background
    worker refreshes routing policy, follows subscriptions and retries buffered publishes.
    Subscriber handlers return only after committing their idempotent business transaction.
    A durable group survives close(); closing a worker never deletes its subscriptions/backlog.
    """

    def __init__(
        self,
        controller_url: str,
        *,
        state_dir: str | Path,
        credentials: Callable[[str], tuple[str, str]],
        api_key: str | None = None,
        poll_interval: float = 1.0,
        max_outbox_bytes: int = 100_000_000,
        max_inflight: int = 128,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self.resolver = Resolver(controller_url, api_key=api_key, max_stale_seconds=300)
        if max_inflight < 1:
            raise ValueError("max_inflight must be positive")
        self.max_inflight = max_inflight
        self.dispatcher: OutboxDispatcher | None = None
        self.policy_rejected = False
        self.interval = poll_interval
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.last_error: str | None = None
        self.workers: dict[tuple[str, str], ManagedConsumers] = {}
        self._config = self._request("/messaging/config")
        self.contract = self._contract(self._config)
        path = self.state_dir / "contract.json"
        if path.exists() and not self._compatible(json.loads(path.read_text()), self.contract):
            raise ValueError("messaging routing contract changed; preserve the existing publisher state")
        self.pool = SmfConnections(credentials)
        self.outboxes: dict[str, DurableOutbox] = {}
        try:
            for shard in sorted({r["shard"] for r in self._config["routes"]}):
                filename = hashlib.sha256(shard.encode()).hexdigest()[:16] + ".outbox.db"
                router = KeyRouter(
                    self.resolver, shard, "native-publisher", partitions=self._config["partitions"]
                )
                self.outboxes[shard] = DurableOutbox(
                    self.state_dir / filename, router, max_bytes=max_outbox_bytes
                )
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps(self.contract, sort_keys=True))
            temporary.replace(path)
        except Exception:
            for box in self.outboxes.values():
                box.close()
            self.pool.close()
            raise
        self.thread = threading.Thread(target=self._run, name="solace-messaging-shim", daemon=True)
        self.thread.start()

    @staticmethod
    def _contract(config: dict) -> dict:
        return {k: config[k] for k in ("fleet_id", "partitions", "routes")}

    @staticmethod
    def _compatible(previous: dict, current: dict) -> bool:
        """Additive routes do not remap buffered work. Existing routes cannot change or disappear."""
        return all(previous[k] == current[k] for k in ("fleet_id", "partitions")) and {
            json.dumps(r, sort_keys=True) for r in previous["routes"]
        } <= {json.dumps(r, sort_keys=True) for r in current["routes"]}

    def _request(self, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.resolver.base_url.rstrip("/") + path, data=data)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        if self.resolver.api_key:
            request.add_header("Authorization", "Bearer " + self.resolver.api_key)
        with urllib.request.urlopen(request, timeout=self.resolver.timeout) as response:
            return json.loads(response.read())

    def publish(self, topic: str, payload: Any, *, event_id: str) -> None:
        """Persist one native publication. Broker subscriptions perform fanout, not publisher copies."""
        if (
            not topic
            or len(topic.encode()) > 128
            or topic.startswith("#")
            or any(c in topic for c in ("*", ">", "\x00"))
            or any(not p for p in topic.split("/"))
        ):
            raise ValueError("publish requires a concrete, nonreserved topic of at most 128 bytes")
        with self.lock:
            if self.stop.is_set():
                raise RuntimeError("messaging client is closed")
            if self.policy_rejected:
                raise ValueError("controller rejected the messaging policy or authorization")
            config = self._config
            routes = [r for r in config["routes"] if _matches(r["pattern"], topic)]
            if len(routes) != 1:
                raise ValueError("topic must match exactly one configured route")
            groups = [
                g for g, patterns in config["groups"].items() if any(_matches(p, topic) for p in patterns)
            ]
            if not groups or not set(groups) <= set(config["ready_groups"]):
                raise ValueError(
                    "no ready matching subscription; register subscribers and wait for readiness"
                )
            route = routes[0]
            levels = topic.split("/")
            dispatch = route.get("dispatch", "by-key")
            if dispatch == "by-key":
                key = json.dumps([levels[i] for i in route["key_levels"]], separators=(",", ":"))
            elif dispatch == "by-topic":
                key = json.dumps(["topic", topic], separators=(",", ":"))
            elif dispatch == "single":
                key = json.dumps(["route", route["pattern"]], separators=(",", ":"))
            else:
                raise ValueError("unsupported topic dispatch policy")
            self.outboxes[route["shard"]].enqueue(
                key, event_id, json.dumps({"topic": topic, "data": payload}, allow_nan=False).encode()
            )
            if self.dispatcher is not None:
                self.dispatcher.wake.set()

    def subscribe(
        self,
        topics: str | list[str] | None = None,
        *,
        group: str,
        handler: Callable[[Message], None],
        timeout: float = 60.0,
    ) -> None:
        """Register durable native subscriptions; replicas use the same group and topic set.

        Wait for controller queue provisioning and local receiver binding. timeout=0 registers
        asynchronously. Definitions remain durable on timeout; retrying the same group is idempotent.
        """
        with self.lock:
            if self.stop.is_set():
                raise RuntimeError("messaging client is closed")
            if topics is None:
                topics = self._config["groups"].get(group)
                if topics is None:
                    raise ValueError("group has no declared subscriptions; configure it in YAML")
        result = self._request(
            "/messaging/subscriptions",
            {"group": group, "topics": [topics] if isinstance(topics, str) else topics},
        )

        def handle(event_id: str, envelope: dict) -> None:
            handler(Message(envelope["topic"], envelope["data"], event_id))

        with self.lock:
            if self.stop.is_set():
                raise RuntimeError("messaging client is closed")
            for shard in result["shards"]:
                if (group, shard) not in self.workers:
                    self.workers[group, shard] = ManagedConsumers(
                        self.resolver, shard, self.pool, handle, group=group
                    )
        if timeout:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                with self.lock:
                    if group in self._config["ready_groups"] and all(
                        len(self.workers[group, shard].flows) >= self._config["partitions"]
                        for shard in result["shards"]
                    ):
                        return
                self.stop.wait(min(self.interval, max(0, deadline - time.monotonic())))
            raise TimeoutError(
                "subscription registered; controller provisioning or consumer binding is pending"
            )

    def _send(self, assignment: Any, event_id: str, payload: bytes) -> Any:
        envelope = json.loads(payload)
        return self.pool.send_async(assignment, event_id, payload, topic=envelope["topic"])

    def _cycle(self) -> None:
        config = self._request("/messaging/config")
        if not self._compatible(self.contract, self._contract(config)):
            raise ValueError("controller changed the durable routing contract")
        # New workloads are available to restarted clients. Existing clients continue their original routes.
        config["routes"] = self.contract["routes"]
        with self.lock:
            self._config = config
            workers = [
                worker for (group, _), worker in self.workers.items() if group in config["ready_groups"]
            ]
        # Never hold the application publish lock while doing network discovery/binding.
        for worker in workers:
            worker.sync()

    def _run(self) -> None:
        self.dispatcher = OutboxDispatcher(self.outboxes, self._send, max_inflight=self.max_inflight)
        while not self.stop.is_set():
            try:
                self._cycle()
                self.policy_rejected = False
                self.dispatcher.paused.clear()
                self.last_error = None
            except Exception as exc:
                self.last_error = type(exc).__name__
                if isinstance(exc, ValueError) or isinstance(exc, urllib.error.HTTPError) and exc.code < 500:
                    self.policy_rejected = True
                    self.dispatcher.paused.set()
                # Temporary control-plane failure leaves the independent delivery loop running.
                # Only bounded cached assignments can be used; old broker ingress fences still apply.
            self.stop.wait(self.interval)

    def status(self) -> dict:
        """Expose pending work and separate policy/delivery problems without leaking credentials."""
        return {
            "pending": self.pending(),
            "policy_error": self.last_error,
            "delivery_error": self.dispatcher.last_error if self.dispatcher else None,
            "publishing_paused": self.policy_rejected,
        }

    def pending(self) -> int:
        return sum(box.pending() for box in self.outboxes.values())

    def flush(self, timeout: float = 30) -> bool:
        """Wait for broker acceptance; timeout leaves unacknowledged work durably buffered."""
        deadline = time.monotonic() + timeout
        while self.pending():
            if time.monotonic() >= deadline or self.stop.is_set():
                return False
            self.stop.wait(min(self.interval, max(0, deadline - time.monotonic())))
        return True

    def close(self) -> None:
        """Stop workers, preserving unsent publications and durable broker queues."""
        self.stop.set()
        self.thread.join()
        if self.dispatcher is not None:
            self.dispatcher.close()
        with self.lock:
            for worker in self.workers.values():
                worker.close()
            for box in self.outboxes.values():
                box.close()
            self.pool.close()

    def __enter__(self) -> MessagingClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
