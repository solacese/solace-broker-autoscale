"""The smart SHIM — both sides.

Instead of pinning a client to one broker (the resolver's connection-time job) and instead of
round-robin, the SHIM makes a **per-message** routing decision from rules that combine the message
**topic** and **payload**, and stamps a **partition/group key** on the wire. It works on both sides:

  - :class:`PublisherShim` — for each ``(topic, payload)`` it asks the pure rule engine which broker
    and key, resolves that broker to an AMQP endpoint, and publishes there with the key set as the
    AMQP ``group-id`` and an application-property ``saas_partition_key``. One AMQP sender is kept per
    (broker, address); the AMQP setup itself is unchanged.
  - :class:`ListenerShim` — subscribes across every broker the rules can target, and re-runs the SAME
    rules over each received message to demultiplex: it hands the application a coherent per-key
    stream and rejects messages that landed on the "wrong" broker for their content (a
    misconfiguration or a stale publisher), so routing stays verifiable end to end.

proton is imported lazily inside the connect paths, so this module imports fine without it and the
rule wiring is unit-testable via the ``*_factory`` seams. Nothing here vends credentials; auth stays
with the caller's own mechanism, exactly as with the Tier-1 adapters.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

# The pure engine lives in the main package; the adapters depend on it one-way (never the reverse).
from solace_autoscale.dispatch import Decision, DispatchPlan  # noqa: E402

from .resolver import Assignment, Resolver

#: AMQP application-property carrying the partition key, and the message annotation used as group id.
PARTITION_KEY_PROPERTY = "saas_partition_key"


class DispatchError(Exception):
    pass


@dataclass
class OutboundMessage:
    """What the SHIM decided to put on the wire for one application message."""

    broker_id: str
    address: str
    key: str | None
    body: bytes | str
    properties: dict[str, Any]
    rule: str


def _amqp_host(assignment: Assignment) -> str:
    return assignment.endpoint("amqp")


@dataclass
class PublisherShim:
    """Rule-based publisher SHIM.

    ``resolver`` maps a broker *name* (from a rule) to an :class:`Assignment` (endpoint map).
    ``plan`` is the compiled dispatch rules. ``sender_factory(uri) -> sender`` builds a thing with a
    ``.send(address, body, properties, group_id)`` method; the default lazily builds a proton
    BlockingConnection-backed sender. Connections are cached per broker.
    """

    resolver: Resolver
    plan: DispatchPlan
    shard: str
    user: str | None = None
    password: str | None = None
    sender_factory: Callable[[str], Any] | None = None
    _conns: dict[str, Any] = field(default_factory=dict)

    def plan_message(self, topic: str, payload: bytes | str) -> OutboundMessage:
        """Pure-ish planning step: decide broker + key + address and resolve the endpoint.

        Resolution goes through the fail-open resolver cache, so a control-plane outage does not stop
        publishing. Does NOT touch the wire — useful for the demo and for tests.
        """
        decision: Decision = self.plan.decide(topic, payload)
        assignment = self.resolver.resolve(self.shard, decision.broker, mode="direct", protocol="amqp")
        props: dict[str, Any] = {}
        if decision.key is not None:
            props[PARTITION_KEY_PROPERTY] = decision.key
        return OutboundMessage(
            broker_id=assignment.broker_id, address=decision.address, key=decision.key,
            body=payload, properties=props, rule=decision.rule,
        )

    def _sender_for(self, assignment: Assignment) -> Any:
        bid = assignment.broker_id
        if bid not in self._conns:
            uri = _amqp_host(assignment)
            if self.sender_factory is not None:
                self._conns[bid] = self.sender_factory(uri)
            else:
                self._conns[bid] = _ProtonSender(uri, self.user, self.password)
        return self._conns[bid]

    def publish(self, topic: str, payload: bytes | str) -> OutboundMessage:
        """Route and send one message on the AMQP path. Returns what was sent (for assertions)."""
        out = self.plan_message(topic, payload)
        assignment = self.resolver.resolve(self.shard, self.plan.decide(topic, payload).broker,
                                            mode="direct", protocol="amqp")
        sender = self._sender_for(assignment)
        sender.send(out.address, out.body, out.properties, out.key)
        return out

    def close(self) -> None:
        for c in self._conns.values():
            close = getattr(c, "close", None)
            if callable(close):
                close()
        self._conns.clear()


@dataclass
class ReceivedMessage:
    broker_id: str
    address: str
    key: str | None
    body: bytes | str
    properties: dict[str, Any]


@dataclass
class DemuxedMessage:
    """A received message tagged with the rule the listener believes it belongs to."""

    message: ReceivedMessage
    key: str | None
    rule: str
    consistent: bool  #: did it arrive on the broker the rules would have chosen for it?


@dataclass
class ListenerShim:
    """Rule-based listener SHIM.

    Subscribes on every broker the plan can target (``plan.targets``) via ``receiver_factory(uri)``,
    and demultiplexes each received message by re-running the SAME rules: the key comes from the
    message's ``saas_partition_key`` property when present, else it is recomputed. ``consistent`` is
    False when a message landed on a broker the rules would not have chosen — surfacing drift rather
    than hiding it.
    """

    resolver: Resolver
    plan: DispatchPlan
    shard: str
    user: str | None = None
    password: str | None = None
    receiver_factory: Callable[[str, str], Any] | None = None
    #: keyed by (broker_id, address) so one broker can host receivers on several addresses
    _receivers: dict[tuple[str, str], Any] = field(default_factory=dict)
    _broker_of: dict[tuple[str, str], str] = field(default_factory=dict)

    def subscribe(self, address: str) -> None:
        """Open a receiver on ``address`` at every target broker."""
        for broker_name in self.plan.targets:
            a = self.resolver.resolve(self.shard, broker_name, mode="direct", protocol="amqp")
            key = (a.broker_id, address)
            if key in self._receivers:
                continue
            uri = _amqp_host(a)
            if self.receiver_factory is not None:
                self._receivers[key] = self.receiver_factory(uri, address)
            else:
                self._receivers[key] = _ProtonReceiver(uri, address, self.user, self.password)
            self._broker_of[key] = a.broker_id

    def demux(self, msg: ReceivedMessage) -> DemuxedMessage:
        """Classify one received message by the rules. Pure given the message."""
        key = msg.properties.get(PARTITION_KEY_PROPERTY)
        decision = self.plan.decide(msg.address, msg.body)
        recomputed_key = key if key is not None else decision.key
        # Consistency: the message is on broker X; would the rules have chosen X for this content?
        # We compare by broker_id via the resolver so names and ids line up.
        expected = self.resolver.resolve(self.shard, decision.broker, mode="direct", protocol="amqp")
        consistent = expected.broker_id == msg.broker_id
        return DemuxedMessage(message=msg, key=recomputed_key, rule=decision.rule,
                              consistent=consistent)

    def receive(self, timeout: float = 5.0) -> Iterator[DemuxedMessage]:
        """Yield demuxed messages from all receivers until each drains/timeouts."""
        for (broker_id, sub_address), recv in self._receivers.items():
            for raw in recv.drain(timeout):
                # Solace does not echo the AMQP `address` on delivery; fall back to the address this
                # receiver is subscribed to, so the demux re-runs the rules on a real topic.
                address = raw.get("address") or sub_address
                yield self.demux(ReceivedMessage(
                    broker_id=broker_id, address=address, key=raw.get("key"),
                    body=raw["body"], properties=raw.get("properties", {}),
                ))

    def close(self) -> None:
        for r in self._receivers.values():
            close = getattr(r, "close", None)
            if callable(close):
                close()
        self._receivers.clear()


# ---- proton-backed transport (lazy import; keeps the AMQP setup we already had) --------------

class _ProtonSender:
    def __init__(self, uri: str, user: str | None, password: str | None) -> None:
        from proton.utils import BlockingConnection
        self._conn = BlockingConnection(uri, user=user, password=password, timeout=15)
        self._senders: dict[str, Any] = {}

    def send(self, address: str, body: bytes | str, properties: dict[str, Any],
             group_id: str | None) -> None:
        from proton import Message
        if address not in self._senders:
            self._senders[address] = self._conn.create_sender(f"topic://{address}")
        msg = Message(body=body)
        if properties:
            msg.properties = dict(properties)
        if group_id:
            msg.group_id = group_id
        self._senders[address].send(msg, timeout=10)

    def close(self) -> None:
        self._conn.close()


class _ProtonReceiver:
    def __init__(self, uri: str, address: str, user: str | None, password: str | None) -> None:
        from proton.utils import BlockingConnection
        self._conn = BlockingConnection(uri, user=user, password=password, timeout=15)
        self._recv = self._conn.create_receiver(f"topic://{address}")

    def drain(self, timeout: float) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        while True:
            try:
                m = self._recv.receive(timeout=timeout)
            except Exception:  # proton.Timeout and friends → done draining
                break
            props = dict(m.properties or {})
            out.append({
                "address": (m.address or "").removeprefix("topic://"),
                "body": m.body,
                "key": props.get(PARTITION_KEY_PROPERTY) or m.group_id,
                "properties": props,
            })
            self._recv.accept()
        return out

    def close(self) -> None:
        self._conn.close()
