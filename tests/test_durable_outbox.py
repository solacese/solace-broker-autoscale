"""Payload persistence and ordering during broker rejection, outage and process restart."""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'adapters' / 'python'))
from solace_autoscale_client.outbox import DurableOutbox  # noqa: E402


class Router:
    mode = 'guaranteed'
    shard = 'payments'
    partitions = 2

    def partition_for(self, key):
        return int(key)

    def resolve_partition(self, partition, refresh=False):
        return SimpleNamespace(partition_id=partition)


def test_restart_retry_and_failed_partition_does_not_starve_others(tmp_path):
    path = tmp_path / 'payments.outbox.db'
    box = DurableOutbox(path, Router())
    for i in range(150):
        box.enqueue('0', f'blocked-{i}', b'payment')
    box.enqueue('1', 'independent', b'payment')
    box.close()
    box = DurableOutbox(path, Router())
    attempted = []

    def send(location, event, payload):
        attempted.append(event)
        if location.partition_id == 0:
            raise RuntimeError('broker NACK')

    assert box.flush(send, limit=10) == 1
    assert attempted == ['blocked-0', 'independent']
    assert box.pending() == 150
    delivered = []
    assert box.flush(lambda a, event, p: delivered.append(event), limit=200) == 150
    assert delivered == [f'blocked-{i}' for i in range(150)]
    box.close()


def test_backpressure_idempotency_and_contract(tmp_path):
    path = tmp_path / 'payments.outbox.db'
    box = DurableOutbox(path, Router(), max_bytes=3)
    box.enqueue('0', 'id', b'abc')
    box.enqueue('0', 'id', b'abc')
    with pytest.raises(ValueError, match='event_id reused'):
        box.enqueue('1', 'id', b'abc')
    with pytest.raises(BufferError):
        box.enqueue('1', 'next', b'x')
    assert box.pending() == 1
    box.close()
    router = Router()
    router.partitions = 3
    with pytest.raises(ValueError, match='routing contract'):
        DurableOutbox(path, router)


def test_second_publisher_cannot_flush_the_same_outbox(tmp_path):
    path = tmp_path / 'payments.outbox.db'
    first = DurableOutbox(path, Router())
    with pytest.raises(ValueError, match='another publisher'):
        DurableOutbox(path, Router())
    first.close()
    restarted = DurableOutbox(path, Router())
    restarted.close()
