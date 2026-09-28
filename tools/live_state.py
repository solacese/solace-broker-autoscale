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


class FamilyTracker:
    """Bounded per-family delivery evidence for the current synthetic run."""
    def __init__(self, families):
        self.values = {family: {"submitted": 0, "accepted": 0, "delivered": 0, "run_id": "", "last_sequence": -1, "gaps": 0, "ordering_errors": 0, "duplicates": 0, "broker": "", "broker_mismatches": 0} for family in families}

    def submitted(self, family, run_id=""):
        state = self.values[family]
        if run_id and state["run_id"] != run_id:
            state.update({"run_id": run_id, "last_sequence": -1, "gaps": 0, "ordering_errors": 0, "duplicates": 0})
        state["submitted"] += 1

    def accepted(self, family, broker):
        state = self.values[family]
        state["accepted"] += 1
        if state["broker"] and state["broker"] != broker:
            state["broker_mismatches"] += 1
        if broker:
            state["broker"] = broker

    def delivered(self, family, run_id, sequence, duplicate=False):
        state = self.values[family]
        if state["run_id"] and state["run_id"] != run_id:
            return False
        if not state["run_id"]:
            state["run_id"] = run_id
        if duplicate:
            state["duplicates"] += 1
            return False
        expected = state["last_sequence"] + 1
        anomaly = False
        if sequence <= state["last_sequence"]:
            state["ordering_errors"] += 1
            anomaly = True
        elif sequence > expected:
            state["gaps"] += sequence - expected
            anomaly = True
        state["last_sequence"] = max(state["last_sequence"], sequence)
        state["delivered"] += 1
        return anomaly


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
