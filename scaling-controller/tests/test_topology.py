"""Topology model tests (event spine, ADR 0009): rendezvous ownership, minimal movement, generation
monotonicity, handoff diff, and event round-trip. All pure and offline."""

from __future__ import annotations

import math

from solace_autoscale.assignment.generation import InMemoryGenerationCounter
from solace_autoscale.assignment.store import BrokerState
from solace_autoscale.assignment.topology import (
    TOPOLOGY_VERSION,
    BrokerRef,
    Handoff,
    ShardTopology,
    diff,
)


def _topo(shard: str, gen: int, broker_ids: list[str],
          states: dict[str, BrokerState] | None = None) -> ShardTopology:
    states = states or {}
    brokers = tuple(
        BrokerRef(
            broker_id=bid,
            state=states.get(bid, BrokerState.ACTIVE),
            endpoints={"amqp": f"amqps://{bid}.example.com:5671"},
        )
        for bid in broker_ids
    )
    return ShardTopology(shard=shard, gen=gen, brokers=brokers)


KEYS = [f"key-{i}" for i in range(1000)]


def test_owner_is_deterministic_and_total():
    t = _topo("a", 1, ["b1", "b2", "b3"])
    for k in KEYS:
        o1 = t.owner(k)
        o2 = t.owner(k)
        assert o1 == o2
        assert o1 in {"b1", "b2", "b3"}


def test_owner_none_when_no_ownable_broker():
    empty = ShardTopology(shard="a", gen=1, brokers=())
    assert empty.owner("key-1") is None
    draining = _topo("a", 1, ["b1"], states={"b1": BrokerState.DRAINING})
    assert draining.owner("key-1") is None  # DRAINING takes no new keys


def test_rendezvous_moves_minimal_keys_on_scale_up():
    # N -> N+1 should move only about 1/(N+1) of keys, and every moved key lands on the new broker.
    before = _topo("a", 1, ["b1", "b2", "b3"])
    after = _topo("a", 2, ["b1", "b2", "b3", "b4"])
    moved = [k for k in KEYS if before.owner(k) != after.owner(k)]
    # every moved key must now be owned by the newly added broker
    assert all(after.owner(k) == "b4" for k in moved)
    # and no key that stayed changed owner among the old brokers
    for k in KEYS:
        if after.owner(k) != "b4":
            assert before.owner(k) == after.owner(k)
    # movement is close to the ideal 1/4; allow generous slack for hashing variance
    ideal = len(KEYS) / 4
    assert len(moved) <= math.ceil(ideal * 1.6)
    assert len(moved) >= math.floor(ideal * 0.4)


def test_generation_is_monotonic_per_shard():
    gc = InMemoryGenerationCounter()
    assert gc.current("a") == 0
    assert gc.next("a") == 1
    assert gc.next("a") == 2
    assert gc.current("a") == 2
    # independent per shard
    assert gc.next("b") == 1
    assert gc.current("a") == 2


def test_diff_emits_one_handoff_per_broker_pair_fenced_at_new_gen():
    before = _topo("a", 5, ["b1", "b2"])
    after = _topo("a", 6, ["b1", "b2", "b3"])
    handoffs = diff(before, after, KEYS)
    # all moved keys go to b3, so at most a couple of distinct (from -> b3) pairs, deduped
    assert handoffs, "a scale-up that moves keys must produce handoffs"
    assert all(h.to_broker == "b3" for h in handoffs)
    assert all(h.effective_gen == 6 for h in handoffs)
    pairs = {(h.from_broker, h.to_broker) for h in handoffs}
    assert len(pairs) == len(handoffs)  # deduped, one per pair


def test_diff_empty_when_topology_unchanged():
    t1 = _topo("a", 1, ["b1", "b2"])
    t2 = _topo("a", 2, ["b1", "b2"])  # same brokers, new gen only
    assert diff(t1, t2, KEYS) == []


def test_event_round_trip_is_lossless():
    topo = ShardTopology(
        shard="shard-a",
        gen=42,
        brokers=(
            BrokerRef("shard-a-01", BrokerState.ACTIVE, {"amqp": "amqps://a01:5671"}),
            BrokerRef("shard-a-02", BrokerState.DRAINING, {"amqp": "amqps://a02:5671"}),
        ),
        handoffs=(Handoff("shard-a-02", "shard-a-03", 42, "scale-up"),),
    )
    event = topo.to_event(emitted_at="2026-09-11T14:00:00Z")
    assert event["version"] == TOPOLOGY_VERSION
    assert event["gen"] == 42
    assert event["emitted_at"] == "2026-09-11T14:00:00Z"
    back = ShardTopology.from_event(event)
    assert back == topo


def test_from_event_rejects_unknown_version():
    bad = {"version": 999, "shard": "a", "gen": 1, "brokers": [], "handoffs": []}
    try:
        ShardTopology.from_event(bad)
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected ValueError for unknown version")


def test_interop_golden_topology_event_matches_committed_file():
    """The Go shim parses shim/testdata/topology_event.json; assert Python still emits exactly it.

    Same cross-language contract as the dispatch rule-spec golden: the Python model writes the wire
    form and the Go topology package reads it, so both sides route identically. Regenerate the file
    from to_event if this fails.
    """
    import json
    from pathlib import Path

    golden = Path(__file__).resolve().parents[2] / "shim" / "testdata" / "topology_event.json"

    def ref(bid: str, state: BrokerState = BrokerState.ACTIVE) -> BrokerRef:
        return BrokerRef(
            broker_id=bid, state=state,
            endpoints={"amqp": f"amqp://{bid}:5672", "smf": f"tcp://{bid}:55555"},
        )

    topo = ShardTopology(
        shard="orders", gen=2,
        brokers=(ref("broker-a"), ref("broker-b"), ref("broker-c"), ref("broker-d"),
                 ref("broker-old", BrokerState.DRAINING)),
        handoffs=(Handoff("broker-a", "broker-d", 2, "scale-up"),),
    )
    expected = topo.to_event(emitted_at="2026-01-01T00:00:02Z")
    on_disk = json.loads(golden.read_text())
    assert on_disk == expected, (
        "shim/testdata/topology_event.json is stale; regenerate it from to_event so the Go "
        "cross-language golden test stays valid"
    )
