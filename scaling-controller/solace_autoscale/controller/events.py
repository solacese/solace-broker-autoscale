"""Solace-backed wakeups for managed control and native broker events.

Messages on this channel are hints only. SQLite remains authoritative and every hint causes a fresh
HTTP or SEMP read; no event payload can assign an owner, renew a lease, or prove a safe drain.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import ManagedEventsConfig
from ..metrics.fleet import BrokerEndpoint
from .store import ControllerStore


def _default_trust_store() -> str:
    try:
        import certifi
    except ImportError as exc:  # pragma: no cover - httpx normally installs certifi
        raise ValueError("set SOLACE_TLS_TRUST_STORE_DIR for tcps connections") from exc
    return str(Path(certifi.where()).parent)


@dataclass(frozen=True)
class ControlHint:
    fleet_id: str
    revision: int
    kind: str

    def encode(self) -> bytearray:
        return bytearray(json.dumps(
            {"version": 1, "fleet_id": self.fleet_id, "revision": self.revision, "kind": self.kind},
            separators=(",", ":"),
            sort_keys=True,
        ).encode())


@dataclass
class ReconcileSchedule:
    """Let one event accelerate a sample without moving the periodic recovery deadline."""

    interval: float
    next_periodic: float
    last_run: float

    @classmethod
    def start(cls, interval: float, now: float) -> ReconcileSchedule:
        return cls(interval, now, now - interval)

    def due_in(self, now: float) -> float:
        return max(0.0, self.next_periodic - now)

    def should_run(self, now: float, event: bool) -> bool:
        periodic = now >= self.next_periodic
        event_due = event and now - self.last_run >= self.interval / 2
        if not periodic and not event_due:
            return False
        self.last_run = now
        if periodic:
            missed = int((now - self.next_periodic) // self.interval) + 1
            self.next_periodic += missed * self.interval
        return True


class _SmfEndpoint:
    """One bounded SMF service. A failed operation discards the complete cached session."""

    def __init__(
        self, endpoint: BrokerEndpoint, username: str, password: str, config: ManagedEventsConfig
    ) -> None:
        self.endpoint, self.username, self.password, self.config = endpoint, username, password, config
        self.lock = threading.RLock()
        self.service: Any = None
        self.publisher: Any = None
        self.receiver: Any = None

    def connect(self) -> Any:
        from solace.messaging.config.retry_strategy import RetryStrategy
        from solace.messaging.messaging_service import MessagingService

        with self.lock:
            existing = self.service
        if existing is not None:
            return existing
        properties = {
            "solace.messaging.transport.host": self.endpoint.endpoints["smf"],
            "solace.messaging.service.vpn-name": self.endpoint.msg_vpn,
            "solace.messaging.authentication.scheme.basic.username": self.username,
            "solace.messaging.authentication.scheme.basic.password": self.password,
            "solace.messaging.transport.connection-attempts-timeout": int(
                self.config.operation_timeout * 1000
            ),
        }
        if self.endpoint.endpoints["smf"].startswith("tcps://"):
            properties["solace.messaging.tls.trust-store-path"] = (
                os.environ.get("SOLACE_TLS_TRUST_STORE_DIR") or _default_trust_store()
            )
        service = (
            MessagingService.builder()
            .from_properties(properties)
            .with_connection_retry_strategy(RetryStrategy.never_retry())
            .with_reconnection_retry_strategy(RetryStrategy.never_retry())
            .build()
        )
        service.connect()
        with self.lock:
            if self.service is None:
                self.service = service
                return service
            existing = self.service
        try:
            service.disconnect()
        except Exception:
            pass
        return existing

    @staticmethod
    def _terminate(resource: Any) -> None:
        if resource is not None:
            try:
                resource.terminate(grace_period=0)
            except Exception:
                pass

    def discard(self) -> None:
        """Never reuse a receiver, publisher, or service after a connection-level failure."""
        with self.lock:
            receiver, self.receiver = self.receiver, None
            publisher, self.publisher = self.publisher, None
            service, self.service = self.service, None
        self._terminate(receiver)
        self._terminate(publisher)
        if service is not None:
            try:
                service.disconnect()
            except Exception:
                pass


class ManagedControlBus:
    """Publish durable managed hints and coalesce native ``#LOG`` wakeups across the fleet."""

    def __init__(
        self,
        fleet_id: str,
        config: ManagedEventsConfig,
        endpoints: list[BrokerEndpoint],
        credentials: dict[str, tuple[str, str]],
        store: ControllerStore,
        *,
        sample_due: Callable[[], None] | None = None,
    ) -> None:
        by_id = {endpoint.broker_id: endpoint for endpoint in endpoints}
        if config.broker_id not in by_id:
            raise ValueError("managed control broker must be active/warm and inventoried")
        if any("smf" not in endpoint.endpoints for endpoint in endpoints):
            raise ValueError("every managed event broker requires an SMF endpoint")
        # The native SDK imports extension modules; load them once before starting concurrent workers.
        from solace.messaging.config.retry_strategy import RetryStrategy  # noqa: F401
        from solace.messaging.messaging_service import MessagingService  # noqa: F401
        from solace.messaging.resources.topic import Topic
        from solace.messaging.resources.topic_subscription import TopicSubscription

        self.Topic, self.TopicSubscription = Topic, TopicSubscription
        self.fleet_id, self.config, self.store = fleet_id, config, store
        self.sample_due = sample_due
        self.topic = config.topic or f"_autoscale/managed/{fleet_id}/changed"
        self.stop = threading.Event()
        self.wake = threading.Event()
        self.last_error: str | None = None
        self.links = {
            endpoint.broker_id: _SmfEndpoint(endpoint, *credentials[endpoint.broker_id], config)
            for endpoint in endpoints
        }
        self.control = self.links[config.broker_id]
        self.listeners = [
            threading.Thread(
                target=self._listen,
                args=(link,),
                name=f"solace-native-events-{broker_id}",
                daemon=True,
            )
            for broker_id, link in self.links.items()
        ]
        self.publisher = threading.Thread(
            target=self._publish, name="solace-managed-hints", daemon=True
        )
        for thread in self.listeners:
            thread.start()
        self.publisher.start()

    def _listen(self, link: _SmfEndpoint) -> None:
        while not self.stop.is_set():
            try:
                service = link.connect()
                with link.lock:
                    if link.receiver is None:
                        subscriptions = [
                            self.TopicSubscription.of(topic) for topic in self.config.native_topics
                        ]
                        link.receiver = (
                            service.create_direct_message_receiver_builder()
                            .with_subscriptions(subscriptions)
                            .build()
                        )
                        link.receiver.start()
                    receiver = link.receiver
                message = receiver.receive_message(timeout=1000)
                if message is not None:
                    if self.sample_due is not None:
                        self.sample_due()
                    self.wake.set()
                self.last_error = None
            except Exception as exc:
                self.last_error = type(exc).__name__
                link.discard()
                self.stop.wait(self.config.publish_retry)

    def _publish_once(self) -> bool:
        pending = self.store.pending_notifications(limit=1)
        if not pending:
            return False
        revision, _, kind = pending[-1]
        service = self.control.connect()
        with self.control.lock:
            if self.control.publisher is None:
                self.control.publisher = service.create_direct_message_publisher_builder().build()
                self.control.publisher.start()
            publisher = self.control.publisher
        publisher.publish(ControlHint(self.fleet_id, revision, kind).encode(), self.Topic.of(self.topic))
        # Direct control topics are intentionally best effort. HTTP revision recovery closes any gap.
        self.store.notification_published(revision)
        return True

    def _publish(self) -> None:
        """Send the one coalesced hint off-loop; periodic HTTP recovery covers direct loss."""
        while not self.stop.is_set():
            try:
                self._publish_once()
                self.last_error = None
            except Exception as exc:
                self.last_error = type(exc).__name__
                self.control.discard()
            self.stop.wait(self.config.publish_retry)

    def wait(self, timeout: float) -> bool:
        """Wait for a native broker event and collapse a short burst into one reconcile."""
        if not self.wake.wait(timeout):
            return False
        self.wake.clear()
        if self.config.coalesce_window:
            self.stop.wait(min(self.config.coalesce_window, max(0.0, timeout)))
            self.wake.clear()
        return not self.stop.is_set()

    def close(self) -> None:
        started = time.monotonic()
        self.stop.set()
        self.wake.set()
        for link in self.links.values():
            link.discard()
        deadline = started + self.config.operation_timeout
        for thread in [*self.listeners, self.publisher]:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
