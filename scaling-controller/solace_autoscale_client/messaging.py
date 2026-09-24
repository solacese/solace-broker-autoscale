"""One application API over native SMF topics, durable queues and controller-managed placement."""

from __future__ import annotations

import hashlib
import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .dispatch import OutboxDispatcher
from .key_router import KeyRouter
from .managed_smf import ManagedConsumers, SmfConnections
from .outbox import DurableOutbox
from .resolver import Resolver
from .routing_library import RoutingEvaluatorRegistry, validate_headers


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


def _overlaps(first: str, second: str) -> bool:
    left, right = first.split("/"), second.split("/")
    for a, b in zip(left, right, strict=False):
        if a == ">" or b == ">":
            return True
        if a != "*" and b != "*" and a != b:
            return False
    return len(left) == len(right)


@dataclass(frozen=True)
class Message:
    """Application message and borrowed evaluator view.

    ``payload`` and ``headers`` are passed by reference to a local evaluator before serialization.
    Applications and evaluators must not mutate either object during the call. Delivery still uses
    the existing JSON envelope and therefore is not a zero-copy network or subscriber API.
    """

    topic: str
    payload: Any
    event_id: str
    headers: Mapping[str, str] = field(default_factory=dict)
    routing_key: str | None = None
    routing_evaluator: str | None = None
    routing_evaluator_version: str | None = None


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
        event_credentials: Callable[[str], tuple[str, str]] | None = None,
        routing_evaluators: RoutingEvaluatorRegistry | None = None,
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
        self.subscription_validation: dict[str, bool] = {}
        self.refresh = threading.Event()
        self.event_stop = threading.Event()
        self.event_thread: threading.Thread | None = None
        self.event_receiver: Any = None
        self._config = self._request("/messaging/config")
        self.routing_evaluators = routing_evaluators or RoutingEvaluatorRegistry()
        self._validate_evaluators(self._config)
        self.revision = int(self._config.get("revision", 0))
        self.contract = self._contract(self._config)
        path = self.state_dir / "contract.json"
        if path.exists() and not self._compatible(json.loads(path.read_text()), self.contract):
            raise ValueError("messaging routing contract changed; preserve the existing publisher state")
        self.pool = SmfConnections(credentials)
        self.event_pool = SmfConnections(event_credentials or credentials)
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
            self.event_pool.close()
            raise
        events = self._config.get("events", {})
        if events.get("enabled"):
            required = (
                events.get("broker_id"), events.get("endpoints", {}).get("smf"),
                events.get("msg_vpn"), events.get("topic"),
            )
            if not all(isinstance(value, str) and value for value in required):
                self.pool.close()
                self.event_pool.close()
                raise ValueError("controller returned an incomplete managed event endpoint")
            self.event_thread = threading.Thread(
                target=self._listen_events, name="solace-managed-refresh", daemon=True
            )
            self.event_thread.start()
        self.thread = threading.Thread(target=self._run, name="solace-messaging-shim", daemon=True)
        self.thread.start()

    def _validate_evaluators(self, config: dict) -> None:
        for route in config["routes"]:
            evaluator = route.get("key_evaluator")
            if evaluator is not None:
                if not isinstance(evaluator, dict):
                    raise ValueError("invalid routing evaluator contract")
                self.routing_evaluators.require(evaluator.get("name"), evaluator.get("version"))

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

    def publish(
        self,
        topic: str,
        payload: Any,
        *,
        event_id: str,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Compatibility API; payload/header objects are borrowed unchanged by an evaluator."""
        self.publish_message(Message(topic, payload, event_id, headers if headers is not None else {}))

    def _publication_route(self, topic: str) -> dict:
        """Validate current policy while the caller holds ``self.lock``."""
        if self.stop.is_set():
            raise RuntimeError("messaging client is closed")
        if self.policy_rejected:
            raise ValueError("controller rejected the messaging policy or authorization")
        routes = [r for r in self._config["routes"] if _matches(r["pattern"], topic)]
        if len(routes) != 1:
            raise ValueError("topic must match exactly one configured route")
        groups = [
            group
            for group, patterns in self._config["groups"].items()
            if any(_matches(pattern, topic) for pattern in patterns)
        ]
        if not groups or not set(groups) <= set(self._config["ready_groups"]):
            raise ValueError("no ready matching subscription; register subscribers and wait for readiness")
        return routes[0]

    def publish_message(self, message: Message) -> None:
        """Evaluate and durably accept the exact application-provided message object.

        Customer code is synchronous but runs without the client lock. The caller must not mutate
        the borrowed message concurrently. Deterministic failures occur before durable acceptance.
        The current policy is checked again immediately before the outbox transaction.
        """
        topic, event_id = message.topic, message.event_id
        if (
            not topic
            or len(topic.encode()) > 128
            or topic.startswith("#")
            or any(c in topic for c in ("*", ">", "\x00"))
            or any(not p for p in topic.split("/"))
        ):
            raise ValueError("publish requires a concrete, nonreserved topic of at most 128 bytes")
        if not event_id or len(event_id.encode()) > 512 or "\x00" in event_id:
            raise ValueError("event_id must be 1-512 UTF-8 bytes without NUL")
        validate_headers(message.headers)
        with self.lock:
            route = json.loads(json.dumps(self._publication_route(topic)))
        evaluator = route.get("key_evaluator")
        evaluator_name = evaluator_version = None
        box = self.outboxes[route["shard"]]
        if evaluator is not None:
            evaluator_name, evaluator_version = evaluator["name"], evaluator["version"]
            resolved = self.routing_evaluators.evaluate(
                evaluator_name,
                evaluator_version,
                evaluator.get("result", "key"),
                message,
                shard=route["shard"],
                partitions=box.router.partitions,
                key_partition=box.router.partition_for,
            )
            routing_kind, routing_value, partition = resolved.kind, resolved.value, resolved.partition
        else:
            levels = topic.split("/")
            dispatch = route.get("dispatch", "by-key")
            if dispatch == "by-key":
                routing_value = json.dumps(
                    [levels[i] for i in route["key_levels"]], separators=(",", ":")
                )
            elif dispatch == "by-topic":
                routing_value = json.dumps(["topic", topic], separators=(",", ":"))
            elif dispatch == "single":
                routing_value = json.dumps(["route", route["pattern"]], separators=(",", ":"))
            else:
                raise ValueError("unsupported topic dispatch policy")
            routing_kind = "key"
            partition = box.router.partition_for(routing_value)
        envelope: dict[str, Any] = {"topic": topic, "data": message.payload}
        if message.headers:
            envelope["headers"] = dict(message.headers)
        if evaluator is not None:
            envelope["routing"] = {
                "kind": routing_kind,
                "value": routing_value,
                "partition": partition,
                "evaluator": evaluator_name,
                "version": evaluator_version,
            }
        encoded = json.dumps(envelope, allow_nan=False).encode()
        with self.lock:
            if self._publication_route(topic) != route:
                raise ValueError("routing policy changed during evaluation; publication was not accepted")
            box.enqueue(
                routing_value,
                event_id,
                encoded,
                partition=partition,
                routing_kind=routing_kind,
                evaluator_name=evaluator_name,
                evaluator_version=evaluator_version,
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
        validate_routing: bool = False,
    ) -> None:
        """Register durable native subscriptions; replicas use the same group and topic set.

        Wait for controller queue provisioning and local receiver binding. timeout=0 registers
        asynchronously. Definitions remain durable on timeout; retrying the same group is idempotent.
        """
        with self.lock:
            if self.stop.is_set():
                raise RuntimeError("messaging client is closed")
            prior_validation = self.subscription_validation.get(group)
            if prior_validation is not None and prior_validation != validate_routing:
                raise ValueError("subscriber group routing-validation mode cannot change in one client")
            if topics is None:
                topics = self._config["groups"].get(group)
                if topics is None:
                    raise ValueError("group has no declared subscriptions; configure it in YAML")
            selected_topics = [topics] if isinstance(topics, str) else topics
            applicable = [
                route for route in self.contract["routes"]
                if any(_overlaps(route["pattern"], pattern) for pattern in selected_topics)
            ]
            if validate_routing and (
                not applicable or any(route.get("key_evaluator") is None for route in applicable)
            ):
                raise ValueError("routing validation requires evaluator routes for all group topics")
        result = self._request(
            "/messaging/subscriptions", {"group": group, "topics": selected_topics}
        )

        def handle(event_id: str, envelope: dict) -> None:
            routing = envelope.get("routing") or {}
            message = Message(
                envelope["topic"],
                envelope["data"],
                event_id,
                envelope.get("headers") or {},
                routing.get("value"),
                routing.get("evaluator"),
                routing.get("version"),
            )
            if validate_routing:
                routes = [
                    route for route in self.contract["routes"]
                    if _matches(route["pattern"], message.topic)
                ]
                if len(routes) != 1 or routes[0].get("key_evaluator") is None:
                    raise ValueError(
                        "consumer routing validation requires exactly one evaluator route; "
                        "message retained unacknowledged"
                    )
                expected = routes[0]["key_evaluator"]
                if (
                    message.routing_evaluator != expected["name"]
                    or message.routing_evaluator_version != expected["version"]
                ):
                    raise ValueError(
                        "consumer routing evaluator identity mismatch; message retained unacknowledged"
                    )
                box = self.outboxes[routes[0]["shard"]]
                resolved = self.routing_evaluators.evaluate(
                    expected["name"],
                    expected["version"],
                    expected.get("result", "key"),
                    message,
                    shard=routes[0]["shard"],
                    partitions=box.router.partitions,
                    key_partition=box.router.partition_for,
                )
                if (
                    resolved.kind != routing.get("kind")
                    or resolved.value != message.routing_key
                    or resolved.partition != routing.get("partition")
                ):
                    raise ValueError("consumer routing validation mismatch; message retained unacknowledged")
            handler(message)

        with self.lock:
            if self.stop.is_set():
                raise RuntimeError("messaging client is closed")
            prior_validation = self.subscription_validation.get(group)
            if prior_validation is not None and prior_validation != validate_routing:
                raise ValueError("subscriber group routing-validation mode cannot change in one client")
            self.subscription_validation[group] = validate_routing
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
        self._validate_evaluators(config)
        if not self._compatible(self.contract, self._contract(config)):
            raise ValueError("controller changed the durable routing contract")
        revision = int(config.get("revision", 0))
        # New workloads are available to restarted clients. Existing clients continue their original routes.
        config["routes"] = self.contract["routes"]
        with self.lock:
            self._config = config
            if revision > self.revision:
                self.revision = revision
                for box in self.outboxes.values():
                    box.router.required_revision = revision
                if self.dispatcher is not None:
                    self.dispatcher.wake.set()
            workers = [
                worker for (group, _), worker in self.workers.items() if group in config["ready_groups"]
            ]
        # Never hold the application publish lock while doing network discovery/binding.
        for worker in workers:
            worker.sync()

    def _listen_events(self) -> None:
        """Treat message contents as untrusted wakeups; only HTTP can advance revision state."""
        events = self._config["events"]
        broker = events["broker_id"]
        location = {
            "broker_id": broker,
            "msg_vpn": events["msg_vpn"],
            "endpoints": events["endpoints"],
        }
        while not self.event_stop.is_set():
            receiver = None
            try:
                receiver = self.event_pool.direct_receiver(location, events["topic"])
                with self.lock:
                    self.event_receiver = receiver
                while not self.event_stop.is_set():
                    if receiver.receive_message(timeout=1000) is not None:
                        self.refresh.set()
            except Exception as exc:
                self.last_error = type(exc).__name__
                self.event_pool.discard(broker)
            finally:
                with self.lock:
                    self.event_receiver = None
                if receiver is not None:
                    try:
                        receiver.terminate(grace_period=0)
                    except Exception:
                        pass
            self.event_stop.wait(min(self.interval, 1.0))

    def _run(self) -> None:
        self.dispatcher = OutboxDispatcher(self.outboxes, self._send, max_inflight=self.max_inflight)
        while not self.stop.is_set():
            self.refresh.clear()
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
            if self.refresh.wait(self.interval):
                self.stop.wait(min(0.25, self.interval))

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
        self.refresh.set()
        self.event_stop.set()
        with self.lock:
            receiver = self.event_receiver
        if receiver is not None:
            try:
                receiver.terminate(grace_period=0)
            except Exception:
                pass
        self.thread.join()
        if self.event_thread is not None:
            self.event_thread.join(timeout=max(2.0, self.interval + 1))
        if self.dispatcher is not None:
            self.dispatcher.close()
        with self.lock:
            for worker in self.workers.values():
                worker.close()
            for box in self.outboxes.values():
                box.close()
            self.pool.close()
            self.event_pool.close()

    def __enter__(self) -> MessagingClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
