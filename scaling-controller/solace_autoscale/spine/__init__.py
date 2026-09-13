"""The event spine (ADR 0009).

The spine is the *only* component that touches the message bus with control events. The pure core
(decision engine, topology model, actuator) returns events as data; the spine publishes them onto a
reserved ``_autoscale/*`` topic tree on the same Solace brokers that carry client data.

Control and data are separate topic trees on the same brokers, so ADR 0002 (no proxy in the data
path) still holds: the spine publishes CONTROL, client DATA never flows through it.
"""

from __future__ import annotations

from .events import (
    ActivityEvent,
    SpineEvent,
    TopologySnapshotEvent,
    activity_topic,
    topology_topic,
)
from .publisher import Bus, InMemoryBus, SpinePublisher

__all__ = [
    "ActivityEvent",
    "Bus",
    "InMemoryBus",
    "SpineEvent",
    "SpinePublisher",
    "TopologySnapshotEvent",
    "activity_topic",
    "topology_topic",
]
