"""The spine publisher: the single I/O choke point for control events (ADR 0009).

Everything above this line is pure and returns events as data. This module is the ONE place that
turns a :class:`SpineEvent` into a message on the bus, so the bus dependency is isolated to a small,
swappable surface:

- :class:`Bus` - the narrow interface the publisher needs (``publish`` one message).
- :class:`InMemoryBus` - a deterministic fake for tests and the ``--dry-run`` path. Records every
  message and keeps the last-value snapshot per topic, so a test can assert what a subscriber would
  see without a broker.
- :class:`AmqpBus` - the real backend over ``python-qpid-proton``. Imported lazily so the module (and
  every test that uses the fake) has no hard dependency on a native AMQP library being importable.

:class:`SpinePublisher` is deliberately thin: it stamps the generation as a message property (so a
subscriber can order/fence on it without parsing the body), enforces monotonic generations per
last-value topic (a stale snapshot is dropped rather than published), and delegates the byte-pushing
to the :class:`Bus`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..assignment.topology import GENERATION_PROPERTY
from .events import SpineEvent, TopologySnapshotEvent


@dataclass(frozen=True)
class BusMessage:
    """One control message ready for the wire. JSON body plus routing/ordering properties."""

    topic: str
    body: bytes
    #: Message application properties. Always carries ``saas_gen`` so a subscriber can fence on the
    #: generation without decoding the body.
    properties: dict[str, Any]
    #: Retained / last-value publish. A late subscriber gets the most recent retained message.
    last_value: bool


class Bus(Protocol):
    """The narrow bus surface the publisher needs. The only thing a backend must implement."""

    def publish(self, message: BusMessage) -> None: ...

    def close(self) -> None: ...


class SpinePublisher:
    """Turns pure :class:`SpineEvent`s into :class:`BusMessage`s on a :class:`Bus`.

    Not pure (it calls the bus), but it holds no scaling logic: it stamps the generation, enforces
    per-topic monotonic generations for last-value snapshots, and publishes. The clock is injected
    (events already carry ``emitted_at``); the publisher never reads a clock itself.
    """

    def __init__(self, bus: Bus) -> None:
        self._bus = bus
        #: highest generation already published per last-value topic, for monotonic enforcement
        self._published_gen: dict[str, int] = {}

    def publish(self, event: SpineEvent) -> bool:
        """Publish one event. Returns True if it went to the bus, False if dropped as stale.

        A :class:`TopologySnapshotEvent` whose generation is not greater than the last one published
        on its topic is dropped: publishing it would only let a subscriber briefly regress. Append
        events are always published.
        """
        topic = event.topic()
        gen = getattr(event, "gen", 0)

        if isinstance(event, TopologySnapshotEvent):
            last = self._published_gen.get(topic)
            if last is not None and gen <= last:
                return False

        payload = event.to_payload()
        message = BusMessage(
            topic=topic,
            body=json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            properties={GENERATION_PROPERTY: gen},
            last_value=event.last_value,
        )
        self._bus.publish(message)
        if event.last_value:
            self._published_gen[topic] = gen
        return True

    def close(self) -> None:
        self._bus.close()


@dataclass
class InMemoryBus:
    """Deterministic in-memory :class:`Bus`. Records everything; keeps the last-value per topic.

    Used by tests and the dry-run path. ``last_value_of(topic)`` returns what a cold subscriber
    would receive on that last-value topic; ``messages`` is the full ordered publish log.
    """

    messages: list[BusMessage] = field(default_factory=list)
    _last_value: dict[str, BusMessage] = field(default_factory=dict)
    closed: bool = False

    def publish(self, message: BusMessage) -> None:
        self.messages.append(message)
        if message.last_value:
            self._last_value[message.topic] = message

    def last_value_of(self, topic: str) -> BusMessage | None:
        return self._last_value.get(topic)

    def decoded_last_value(self, topic: str) -> dict[str, Any] | None:
        msg = self._last_value.get(topic)
        return json.loads(msg.body) if msg else None

    def close(self) -> None:
        self.closed = True


class AmqpBus:
    """Real :class:`Bus` over ``python-qpid-proton``. Proton is imported lazily.

    Publishes to the reserved ``_autoscale`` topic tree on a Solace broker's AMQP listener. The
    generation is set both as an AMQP application property (for cheap subscriber fencing) and left in
    the JSON body (self-describing). Solace maps the AMQP address to a topic; last-value behaviour is
    provided by a Last-Value-Queue / retained mapping configured on the broker, so this publisher
    stays a plain sender and does not special-case retained delivery.
    """

    def __init__(self, url: str, *, target: str | None = None) -> None:
        # Lazy import: keeps the module importable (and the fake usable) with no native AMQP present.
        from proton import Message
        from proton.utils import BlockingConnection

        self._Message = Message
        self._conn = BlockingConnection(url)
        self._target = target
        self._senders: dict[str, Any] = {}

    def publish(self, message: BusMessage) -> None:
        sender = self._senders.get(message.topic)
        if sender is None:
            sender = self._conn.create_sender(self._target or message.topic)
            self._senders[message.topic] = sender
        msg = self._Message(
            body=message.body,
            properties=dict(message.properties),
            address=message.topic,
            durable=message.last_value,
        )
        sender.send(msg)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # pragma: no cover - best-effort teardown
            pass
