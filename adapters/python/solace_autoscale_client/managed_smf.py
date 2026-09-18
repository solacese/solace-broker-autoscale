"""Managed SMF publisher/consumer integration for automated queue handovers.

Optional dependency: solace-pubsubplus. Applications supply broker credentials and an
idempotent business handler. No credentials are returned by the assignment service.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from .outbox import payment_envelope
from .resolver import Assignment, Resolver


class SmfConnections:
    """Pool normal broker sessions; credentials remain in the application's existing mechanism."""

    def __init__(self, credentials: Callable[[str], tuple[str, str]]) -> None:
        self.credentials = credentials
        self.services: dict[str, Any] = {}
        self.publishers: dict[str, Any] = {}
        self.async_publishers: dict[str, Any] = {}
        self.lock = threading.RLock()
        self.consumers = ThreadPoolExecutor(max_workers=8, thread_name_prefix="solace-handler")

    def service(self, location: dict) -> Any:
        from solace.messaging.messaging_service import MessagingService

        broker = location["broker_id"]
        with self.lock:
            if broker not in self.services:
                username, password = self.credentials(broker)
                service = (
                    MessagingService.builder()
                    .from_properties(
                        {
                            "solace.messaging.transport.host": location["endpoints"]["smf"],
                            "solace.messaging.service.vpn-name": location["msg_vpn"],
                            "solace.messaging.authentication.scheme.basic.username": username,
                            "solace.messaging.authentication.scheme.basic.password": password,
                        }
                    )
                    .build()
                )
                service.connect()
                self.services[broker] = service
            return self.services[broker]

    def send(
        self, assignment: Assignment, event_id: str, payload: bytes, *, topic: str | None = None
    ) -> None:
        """Wait for positive ACK; NACK/timeout leaves the caller's durable outbox entry intact."""
        from solace.messaging.resources.topic import Topic

        if topic is not None and not assignment.topic_prefix:
            raise ValueError("native topic publishing requires a topic-aware controller")
        if not assignment.queue_name:
            raise ValueError("managed assignment must name its durable queue")
        broker = assignment.broker_id
        service = self.service(
            {"broker_id": broker, "msg_vpn": assignment.msg_vpn, "endpoints": assignment.endpoints}
        )
        with self.lock:
            if broker not in self.publishers:
                publisher = service.create_persistent_message_publisher_builder().build()
                publisher.start()
                self.publishers[broker] = publisher
            publisher = self.publishers[broker]
        publisher.publish_await_acknowledgement(
            payment_envelope(event_id, payload),
            Topic.of(
                assignment.topic_prefix + topic
                if topic is not None and assignment.topic_prefix
                else "#P2P/QUE/" + assignment.queue_name
            ),
            time_out=5000,
        )

    def send_async(self, assignment: Assignment, event_id: str, payload: bytes, *, topic: str) -> Future:
        """Submit native guaranteed pub/sub and complete the future from the broker receipt."""
        from solace.messaging.config.solace_properties.message_properties import PERSISTENT_ACK_IMMEDIATELY
        from solace.messaging.publisher.persistent_message_publisher import MessagePublishReceiptListener
        from solace.messaging.resources.topic import Topic

        class Receipts(MessagePublishReceiptListener):
            def on_publish_receipt(self, receipt: Any) -> None:
                result = receipt.user_context
                if not isinstance(result, Future) or result.done():
                    return
                if receipt.exception is not None:
                    result.set_exception(receipt.exception)
                elif not receipt.is_persisted:
                    result.set_exception(RuntimeError("broker did not confirm persistence"))
                else:
                    result.set_result(None)

        if not assignment.topic_prefix:
            raise ValueError("native topic publishing requires a topic-aware controller")
        service = self.service(
            {
                "broker_id": assignment.broker_id,
                "msg_vpn": assignment.msg_vpn,
                "endpoints": assignment.endpoints,
            }
        )
        with self.lock:
            if assignment.broker_id not in self.async_publishers:
                publisher = (
                    service.create_persistent_message_publisher_builder().on_back_pressure_reject(256).build()
                )
                publisher.start()
                publisher.set_message_publish_receipt_listener(Receipts())
                self.async_publishers[assignment.broker_id] = publisher
            publisher = self.async_publishers[assignment.broker_id]
        result: Future = Future()
        try:
            publisher.publish(
                payment_envelope(event_id, payload),
                Topic.of(assignment.topic_prefix + topic),
                user_context=result,
                # One outstanding head per partition: avoid the broker's idle ACK batching timer.
                additional_message_properties={PERSISTENT_ACK_IMMEDIATELY: 1},
            )
        except Exception as exc:
            result.set_exception(exc)
        return result

    def close(self) -> None:
        """Close publishers and connections after consumer workers have stopped."""
        self.consumers.shutdown(wait=True, cancel_futures=True)
        for publisher in [*self.publishers.values(), *self.async_publishers.values()]:
            from solace.messaging.errors.pubsubplus_client_error import IncompleteMessageDeliveryError

            try:
                publisher.terminate(grace_period=0)
            except IncompleteMessageDeliveryError:
                # Unconfirmed publications remain in the durable outbox for the next process.
                pass
        for service in self.services.values():
            service.disconnect()


class ManagedConsumers:
    """Follow current and preparing queue locations; ACK only after the business handler succeeds.

    Run sync() periodically. Multiple worker processes may bind exclusive queues (one active
    consumer, the others standby). The supplied handler must atomically deduplicate event_id
    with its business transaction; transport retries provide at-least-once delivery.
    """

    def __init__(
        self,
        resolver: Resolver,
        shard: str,
        connections: SmfConnections,
        handler: Callable[[str, Any], None],
        *,
        group: str | None = None,
    ) -> None:
        self.resolver, self.shard, self.connections, self.handler = resolver, shard, connections, handler
        self.group = group
        self.flows: dict[tuple[str, int], dict] = {}
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._poll, name="solace-group-receivers", daemon=True)
        self.thread.start()

    def _discover(self) -> dict:
        url = (
            self.resolver.base_url.rstrip("/")
            + "/partitions?"
            + urllib.parse.urlencode({"shard": self.shard, **({"group": self.group} if self.group else {})})
        )
        request = urllib.request.Request(url)
        if self.resolver.api_key:
            request.add_header("Authorization", f"Bearer {self.resolver.api_key}")
        with urllib.request.urlopen(request, timeout=self.resolver.timeout) as response:
            return json.loads(response.read())

    def sync(self) -> None:
        """Prepare destination consumers without dropping the source flow during a drain."""
        from solace.messaging.resources.queue import Queue

        discovered = self._discover()
        desired = set()
        for partition in discovered["partitions"]:
            for location in partition["locations"]:
                key = (location["broker_id"], partition["partition_id"])
                desired.add(key)
                if key in self.flows:
                    continue
                try:
                    service = self.connections.service(location)
                    receiver = (
                        service.create_persistent_message_receiver_builder()
                        .with_message_client_acknowledgement()
                        .build(Queue.durable_exclusive_queue(partition["queue_name"]))
                    )
                    receiver.start()
                except Exception:
                    continue  # Destination may not be provisioned yet; the controller waits for this bind.
                with self.lock:
                    self.flows[key] = {"receiver": receiver, "future": None, "pending": None, "retry_at": 0}
        for key in list(self.flows):
            if key not in desired:
                self._stop(key)

    def _poll(self) -> None:
        """Share a bounded handler pool instead of dedicating a Python thread to every queue."""
        while not self.stop.is_set():
            with self.lock:
                for flow in self.flows.values():
                    future = flow["future"]
                    if (future is None or future.done()) and time.monotonic() >= flow["retry_at"]:
                        flow["future"] = self.connections.consumers.submit(self._receive_one, flow)
            self.stop.wait(0.005)

    def _receive_one(self, flow: dict) -> None:
        """At most one business operation per flow, ACK only after success, retain failed heads."""
        try:
            if flow["pending"] is None:
                flow["pending"] = flow["receiver"].receive_message(timeout=1)
            pending = flow["pending"]
            if pending is None:
                return
            envelope = json.loads(pending.get_payload_as_string())
            self.handler(envelope["event_id"], envelope["payload"])
            flow["receiver"].ack(pending)
            flow["pending"] = None
        except Exception:
            flow["retry_at"] = time.monotonic() + 0.5

    def _stop(self, key: tuple[str, int]) -> None:
        with self.lock:
            flow = self.flows.pop(key)
        if flow["future"] is not None:
            flow["future"].result()
        flow["receiver"].terminate(grace_period=0)

    def close(self) -> None:
        self.stop.set()
        self.thread.join()
        for key in list(self.flows):
            self._stop(key)
