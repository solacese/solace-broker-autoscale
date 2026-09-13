"""Spine publisher + reconciler + daemon tests. All offline against the in-memory bus."""

from __future__ import annotations

import json

from solace_autoscale.assignment.generation import InMemoryGenerationCounter
from solace_autoscale.assignment.store import Broker, BrokerState
from solace_autoscale.assignment.topology import (
    GENERATION_PROPERTY,
    ShardTopology,
)
from solace_autoscale.spine.daemon import SpineDaemon
from solace_autoscale.spine.events import (
    ActivityEvent,
    TopologySnapshotEvent,
    activity_topic,
    topology_topic,
)
from solace_autoscale.spine.publisher import InMemoryBus, SpinePublisher
from solace_autoscale.spine.reconcile import build_topology, plan_events

KEYS = [f"key-{i}" for i in range(500)]


def _broker(bid: str, shard: str = "s1", state: BrokerState = BrokerState.ACTIVE) -> Broker:
    return Broker(broker_id=bid, shard=shard, msg_vpn="default", state=state,
                  endpoints={"amqp": f"amqp://{bid}:5672"})


class _FakeStore:
    def __init__(self) -> None:
        self._by_shard: dict[str, list[Broker]] = {}

    def set(self, shard: str, brokers: list[Broker]) -> None:
        self._by_shard[shard] = brokers

    def brokers_for_shard(self, shard: str) -> list[Broker]:
        return list(self._by_shard.get(shard, []))


class _Clock:
    def __init__(self) -> None:
        self.t = 1000

    def __call__(self) -> str:
        self.t += 1
        return f"2026-01-01T00:00:{self.t:02d}Z"


# ---- publisher -------------------------------------------------------------------------------

def test_publisher_stamps_generation_property_and_body():
    bus = InMemoryBus()
    pub = SpinePublisher(bus)
    topo = build_topology("s1", 3, [_broker("b1"), _broker("b2")])
    published = pub.publish(TopologySnapshotEvent(topology=topo, emitted_at="2026-01-01T00:00:00Z"))

    assert published is True
    msg = bus.last_value_of(topology_topic("s1"))
    assert msg is not None
    assert msg.properties[GENERATION_PROPERTY] == 3
    assert msg.last_value is True
    body = json.loads(msg.body)
    assert body["gen"] == 3
    assert body["shard"] == "s1"


def test_publisher_drops_stale_or_equal_generation_snapshot():
    bus = InMemoryBus()
    pub = SpinePublisher(bus)
    topo3 = build_topology("s1", 3, [_broker("b1")])
    topo2 = build_topology("s1", 2, [_broker("b1")])

    assert pub.publish(TopologySnapshotEvent(topology=topo3, emitted_at="2026-01-01T00:00:00Z")) is True
    assert pub.publish(TopologySnapshotEvent(topology=topo2, emitted_at="2026-01-01T00:00:00Z")) is False
    assert pub.publish(TopologySnapshotEvent(topology=topo3, emitted_at="2026-01-01T00:00:00Z")) is False  # equal gen
    # only the first (gen 3) is retained; no regression to gen 2
    assert bus.decoded_last_value(topology_topic("s1"))["gen"] == 3
    # exactly one message reached the bus
    assert len([m for m in bus.messages if m.topic == topology_topic("s1")]) == 1


def test_publisher_always_publishes_activity_events():
    bus = InMemoryBus()
    pub = SpinePublisher(bus)
    e1 = ActivityEvent(shard="s1", gen=1, kind="scale-up", emitted_at="2026-01-01T00:00:00Z")
    e2 = ActivityEvent(shard="s1", gen=1, kind="scale-up", emitted_at="2026-01-01T00:00:00Z")
    assert pub.publish(e1) is True
    assert pub.publish(e2) is True
    activity = [m for m in bus.messages if m.topic == activity_topic("s1")]
    assert len(activity) == 2
    assert all(m.last_value is False for m in activity)


# ---- reconciler ------------------------------------------------------------------------------

def test_plan_events_empty_when_membership_unchanged():
    prev = build_topology("s1", 1, [_broker("b1"), _broker("b2")])
    # same set, different order / different gen input: no change
    events = plan_events(
        shard="s1", brokers=[_broker("b2"), _broker("b1")], previous=prev,
        next_gen=2, emitted_at="2026-01-01T00:00:00Z", sample_keys=KEYS,
    )
    assert events == []


def test_plan_events_cold_start_emits_init_snapshot_without_handoffs():
    events = plan_events(
        shard="s1", brokers=[_broker("b1"), _broker("b2")], previous=None,
        next_gen=1, emitted_at="2026-01-01T00:00:00Z", sample_keys=KEYS,
    )
    snap = next(e for e in events if isinstance(e, TopologySnapshotEvent))
    act = next(e for e in events if isinstance(e, ActivityEvent))
    assert snap.topology.gen == 1
    assert snap.topology.handoffs == ()
    assert act.kind == "topology-init"


def test_plan_events_scale_up_emits_fenced_handoffs():
    prev = build_topology("s1", 1, [_broker("b1"), _broker("b2"), _broker("b3")])
    events = plan_events(
        shard="s1", brokers=[_broker("b1"), _broker("b2"), _broker("b3"), _broker("b4")],
        previous=prev, next_gen=2, emitted_at="2026-01-01T00:00:00Z", sample_keys=KEYS,
    )
    snap = next(e for e in events if isinstance(e, TopologySnapshotEvent))
    assert snap.topology.gen == 2
    assert snap.topology.handoffs, "scale-up must move some keys and produce handoffs"
    # every handoff is fenced at the new generation and lands on the new broker b4
    assert all(h.effective_gen == 2 for h in snap.topology.handoffs)
    assert all(h.to_broker == "b4" for h in snap.topology.handoffs)


# ---- daemon ----------------------------------------------------------------------------------

def test_daemon_publishes_once_per_change_and_is_quiet_when_steady():
    store = _FakeStore()
    bus = InMemoryBus()
    counter = InMemoryGenerationCounter()
    daemon = SpineDaemon(
        inventory=store, publisher=SpinePublisher(bus), counter=counter,
        sample_keys=KEYS, now=_Clock(),
    )
    store.set("s1", [_broker("b1")])

    first = daemon.tick("s1")
    assert topology_topic("s1") in first
    assert bus.decoded_last_value(topology_topic("s1"))["gen"] == 1

    # steady state: no change, no traffic, no generation spent
    assert daemon.tick("s1") == []
    assert counter.current("s1") == 1

    # scale up -> new generation, new retained snapshot with handoffs
    store.set("s1", [_broker("b1"), _broker("b2")])
    second = daemon.tick("s1")
    assert topology_topic("s1") in second
    assert bus.decoded_last_value(topology_topic("s1"))["gen"] == 2
    assert counter.current("s1") == 2


def test_daemon_generations_are_dense_only_spent_on_change():
    store = _FakeStore()
    bus = InMemoryBus()
    counter = InMemoryGenerationCounter()
    daemon = SpineDaemon(
        inventory=store, publisher=SpinePublisher(bus), counter=counter,
        sample_keys=KEYS, now=_Clock(),
    )
    store.set("s1", [_broker("b1")])
    daemon.tick("s1")            # gen 1
    daemon.tick("s1")            # no change
    daemon.tick("s1")            # no change
    store.set("s1", [_broker("b1"), _broker("b2")])
    daemon.tick("s1")            # gen 2 (not 4)
    assert counter.current("s1") == 2
    assert bus.decoded_last_value(topology_topic("s1"))["gen"] == 2


def test_daemon_last_value_matches_what_a_cold_subscriber_would_apply():
    store = _FakeStore()
    bus = InMemoryBus()
    daemon = SpineDaemon(
        inventory=store, publisher=SpinePublisher(bus),
        counter=InMemoryGenerationCounter(), sample_keys=KEYS, now=_Clock(),
    )
    store.set("s1", [_broker("b1"), _broker("b2")])
    daemon.tick("s1")
    store.set("s1", [_broker("b1"), _broker("b2"), _broker("b3")])
    daemon.tick("s1")

    # a cold subscriber reads the retained snapshot and reconstructs the exact topology
    body = bus.decoded_last_value(topology_topic("s1"))
    restored = ShardTopology.from_event(body)
    assert [b.broker_id for b in restored.ownable_brokers()] == ["b1", "b2", "b3"]
    assert restored.gen == 2
