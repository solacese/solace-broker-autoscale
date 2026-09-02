"""Thin per-protocol adapters (§9.3). Each turns a resolved endpoint into the connection form the
customer's OWN client library expects. ~20 lines per protocol; nothing on the message path.

The endpoint map returned by the assignment service already carries the correct scheme+host+port per
protocol (ports read from broker config, never hardcoded). These helpers adapt that URI to each
library's expected shape.
"""

from __future__ import annotations

from .resolver import Assignment


def amqp_uri(assignment: Assignment, *, failover: list[Assignment] | None = None) -> str:
    """AMQP 1.0 (Qpid JMS / Proton / rhea). Returns the connection URI.

    When ``failover`` alternatives are given, returns a Qpid JMS failover URI list - useful for the
    warm path so the client can fall back without a resolver round-trip.
    """
    primary = assignment.endpoint("amqp")
    if not failover:
        return primary
    uris = [primary] + [a.endpoint("amqp") for a in failover]
    return "failover:(" + ",".join(uris) + ")"


def mqtt_config(assignment: Assignment) -> dict[str, object]:
    """MQTT (Paho). Returns host/port/tls kwargs parsed from the endpoint URI.

    MQTT 3.1.1 has no native redirect, so this is adapter-only. For MQTT 5, check whether the broker
    emits the CONNACK Server Reference property (see docs); if it does, no adapter is needed.
    """
    uri = assignment.endpoint("mqtt")  # e.g. ssl://host:8883
    scheme, rest = uri.split("://", 1)
    host, _, port = rest.partition(":")
    return {
        "host": host,
        "port": int(port) if port else (8883 if scheme in ("ssl", "mqtts") else 1883),
        "tls": scheme in ("ssl", "mqtts", "wss"),
    }


def rest_target(assignment: Assignment) -> str:
    """REST. Returns the base URL; the resolver can front this directly or return a 307."""
    return assignment.endpoint("rest")


def smf_host(assignment: Assignment) -> str:
    """SMF host URI for the Solace messaging API (used by the Tier-2 wrapper)."""
    return assignment.endpoint("smf")
