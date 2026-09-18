"""Bounded asynchronous outbox delivery, independent of control-plane polling."""

from __future__ import annotations

import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any


@dataclass
class Attempt:
    sequence: int
    started: float
    token: object


class OutboxDispatcher:
    """One outstanding head per partition preserves order; independent partitions run concurrently.

    Transport callbacks only enqueue receipts. This worker alone commits ACKs, so a late callback
    cannot touch a closed database or acknowledge a newer retry. Unknown outcomes remain durable.
    """

    def __init__(
        self,
        boxes: dict,
        send: Any,
        *,
        max_inflight: int = 128,
        receipt_timeout: float = 30,
        retry_delay: float = 0.5,
    ) -> None:
        if max_inflight < 1 or receipt_timeout <= 0 or retry_delay <= 0:
            raise ValueError("delivery limits must be positive")
        self.boxes, self.send = boxes, send
        self.max_inflight, self.receipt_timeout, self.retry_delay = max_inflight, receipt_timeout, retry_delay
        self.wake = threading.Event()
        self.stop = threading.Event()
        self.paused = threading.Event()
        self.receipts: queue.SimpleQueue = queue.SimpleQueue()
        self.inflight: dict[tuple[str, int], Attempt] = {}
        self.retry_at: dict[tuple[str, int], float] = {}
        self.refresh: set[tuple[str, int]] = set()
        self.last_error: str | None = None
        self.submissions = threading.BoundedSemaphore(max_inflight)
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="solace-connect")
        self.thread = threading.Thread(target=self._run, name="solace-outbox-delivery", daemon=True)
        self.thread.start()

    def _submit(self, shard: str, partition: int, row: tuple, token: object, refresh: bool) -> None:
        def complete(error: Exception | None) -> None:
            self.receipts.put(((shard, partition), token, error))
            self.wake.set()

        try:
            location = self.boxes[shard].router.resolve_partition(partition, refresh=refresh)
            future = self.send(location, row[1], row[3])

            def received(result: Any) -> None:
                try:
                    result.result()
                    complete(None)
                except Exception as exc:
                    complete(exc)

            future.add_done_callback(received)
        except Exception as exc:
            complete(exc)
        finally:
            self.submissions.release()

    def _cycle(self) -> None:
        now = time.monotonic()
        while not self.receipts.empty():
            key, token, error = self.receipts.get()
            attempt = self.inflight.get(key)
            if attempt is None or attempt.token is not token:
                continue
            if error is None:
                self.boxes[key[0]].acknowledge(attempt.sequence)
                self.retry_at.pop(key, None)
                self.refresh.discard(key)
                self.last_error = None
            else:
                self.last_error = type(error).__name__
                self.retry_at[key] = now + self.retry_delay
                self.refresh.add(key)
            del self.inflight[key]
        for key, attempt in list(self.inflight.items()):
            if now - attempt.started >= self.receipt_timeout:
                del self.inflight[key]
                self.retry_at[key] = now + self.retry_delay
                self.refresh.add(key)
                self.last_error = "ReceiptTimeout"
        if self.paused.is_set():
            return
        for shard, box in self.boxes.items():
            for row in box.heads():
                key = (shard, row[2])
                if key in self.inflight or now < self.retry_at.get(key, 0):
                    continue
                if len(self.inflight) >= self.max_inflight or not self.submissions.acquire(blocking=False):
                    return
                token = object()
                self.inflight[key] = Attempt(row[0], now, token)
                self.executor.submit(self._submit, shard, row[2], row, token, key in self.refresh)

    def _run(self) -> None:
        while not self.stop.is_set():
            self.wake.clear()
            try:
                self._cycle()
            except Exception as exc:
                self.last_error = type(exc).__name__
            self.wake.wait(0.05)

    def close(self) -> None:
        self.stop.set()
        self.wake.set()
        self.thread.join()
        self.executor.shutdown(wait=True, cancel_futures=True)
