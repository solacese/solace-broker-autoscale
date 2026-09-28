#!/usr/bin/env python3
"""Small bounded state primitives for the live dashboard."""
from __future__ import annotations

import collections
import time


class BoundedSeen:
    def __init__(self, limit: int):
        if limit < 1:
            raise ValueError("limit must be positive")
        self.limit = limit
        self._values: collections.OrderedDict[str, None] = collections.OrderedDict()

    def add(self, value: str) -> bool:
        duplicate = value in self._values
        if duplicate:
            self._values.move_to_end(value)
            return False
        self._values[value] = None
        while len(self._values) > self.limit:
            self._values.popitem(last=False)
        return True

    def __len__(self) -> int:
        return len(self._values)


class PendingTimes:
    def __init__(self, limit: int, max_age: float):
        self.limit, self.max_age = limit, max_age
        self._values: collections.OrderedDict[str, float] = collections.OrderedDict()

    def add(self, key: str, at: float) -> None:
        self._values[key] = at
        self._values.move_to_end(key)
        self.expire(at)
        while len(self._values) > self.limit:
            self._values.popitem(last=False)

    def pop(self, key: str):
        return self._values.pop(key, None)

    def expire(self, now: float) -> None:
        while self._values:
            key, at = next(iter(self._values.items()))
            if now - at <= self.max_age:
                return
            self._values.pop(key, None)

    def __len__(self) -> int:
        return len(self._values)


class Pacer:
    """Monotonic pacer that never catches up with a burst after a stall."""
    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.deadline = clock()

    def delay(self, rate: int) -> float:
        if rate <= 0:
            self.deadline = self.clock()
            return 0.2
        interval = 1.0 / rate
        now = self.clock()
        if now > self.deadline + interval:
            self.deadline = now
        self.deadline += interval
        return max(0.0, self.deadline - now)
