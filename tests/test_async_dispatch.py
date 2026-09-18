"""Guaranteed delivery is asynchronous without losing durable retry ordering."""
import time
from concurrent.futures import Future
from types import SimpleNamespace

from solace_autoscale_client.dispatch import OutboxDispatcher
from solace_autoscale_client.outbox import DurableOutbox


class Router:
    mode, shard, partitions = "guaranteed", "payments", 2

    def partition_for(self, key):
        return int(key)

    def resolve_partition(self, partition, refresh=False):
        return SimpleNamespace(partition_id=partition, refreshed=refresh)


def wait_for(condition):
    deadline = time.monotonic() + 3
    while not condition():
        assert time.monotonic() < deadline
        time.sleep(0.005)


def test_receipts_are_async_independent_and_ordered(tmp_path):
    box = DurableOutbox(tmp_path / "outbox.db", Router())
    for key, event in [("0", "first"), ("0", "next"), ("1", "other")]:
        box.enqueue(key, event, b"payload")
    calls = []

    def send(location, event, payload):
        future = Future()
        calls.append((event, location, future))
        return future

    pump = OutboxDispatcher({"payments": box}, send, retry_delay=0.01)
    try:
        wait_for(lambda: len(calls) == 2)
        assert {c[0] for c in calls} == {"first", "other"}
        assert box.pending() == 3  # Sending alone cannot delete durable data.
        first = next(c for c in calls if c[0] == "first")
        other = next(c for c in calls if c[0] == "other")
        first[2].set_exception(RuntimeError("fenced"))
        other[2].set_result(None)
        wait_for(lambda: len(calls) == 3)
        assert calls[2][0] == "first"
        assert calls[2][1].refreshed
        assert box.pending() == 2
        calls[2][2].set_result(None)
        wait_for(lambda: len(calls) == 4)
        assert calls[3][0] == "next"
        calls[3][2].set_result(None)
        wait_for(lambda: box.pending() == 0)
    finally:
        pump.close()
        box.close()


def test_late_receipt_does_not_acknowledge_current_attempt(tmp_path):
    box = DurableOutbox(tmp_path / "outbox.db", Router())
    box.enqueue("0", "one", b"payload")
    calls = []

    def send(*args):
        future = Future()
        calls.append(future)
        return future

    pump = OutboxDispatcher({"payments": box}, send, receipt_timeout=0.08, retry_delay=0.01)
    try:
        wait_for(lambda: len(calls) >= 2)
        pump.paused.set()
        calls[0].set_result(None)
        time.sleep(0.06)
        assert box.pending() == 1
    finally:
        pump.close()
        box.close()
    restarted = DurableOutbox(tmp_path / "outbox.db", Router())
    assert restarted.pending() == 1
    restarted.close()


def test_long_lease_cannot_bypass_stale_ownership_limit():
    import pytest
    from solace_autoscale_client.key_router import KeyRouter
    from solace_autoscale_client.resolver import Resolver, ResolverError

    clock = [0]
    response = {"broker_id": "a", "msg_vpn": "v", "state": "active", "endpoints": {},
                "lease_seconds": 10000, "partition_id": 0, "partition_count": 2}
    resolver = Resolver("unused", max_stale_seconds=5, _clock=lambda: clock[0])
    resolver._fetch = lambda *args: response
    router = KeyRouter(resolver, "payments", "publisher", partitions=2)
    router.resolve_partition(0)

    def unavailable(*args):
        raise OSError("offline")

    resolver._fetch = unavailable
    clock[0] = 4
    assert router.resolve_partition(0).broker_id == "a"
    clock[0] = 6
    with pytest.raises(ResolverError):
        router.resolve_partition(0)
