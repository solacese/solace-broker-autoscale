"""Drain state machine (§10).

ACTIVE → DRAINING → DRAINED → DELETING → GONE, with a STUCK terminal for stalls.

- Entering DRAINING blocks new assignments (the assignment store's DRAINING state).
- DRAINED requires zero queue depth AND zero bound consumers held CONTINUOUSLY for a settle period.
  A transient dip to zero is not enough — if it comes back, the settle timer resets.
- Only DRAINED may transition to DELETING.
- A drain that does not reach DRAINED within a timeout goes to STUCK, which requires operator
  intervention and NEVER auto-resolves into deletion.

With large messages + guaranteed delivery a drain can take a long time. That is correct, not a bug
to optimise away.

Pure: the caller supplies observations (queue depth, bound consumers) and timestamps; the machine
never reads a clock. Transitions are deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class DrainState(StrEnum):
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    DRAINED = "DRAINED"
    DELETING = "DELETING"
    GONE = "GONE"
    STUCK = "STUCK"


@dataclass
class DrainObservation:
    ts: float
    queue_depth: int
    bound_consumers: int
    active_flows: int = 0
    spooled_bytes: int = 0

    @property
    def is_empty(self) -> bool:
        return (self.queue_depth == 0 and self.bound_consumers == 0
                and self.active_flows == 0 and self.spooled_bytes == 0)


@dataclass
class DrainMachine:
    """Tracks one broker's drain. Call ``begin`` then ``observe`` repeatedly, then ``mark_*``."""

    broker_id: str
    settle_seconds: float
    stall_timeout_seconds: float
    state: DrainState = DrainState.ACTIVE
    #: when DRAINING began
    draining_since: float | None = None
    #: when the broker most recently BECAME empty and has stayed empty; None if not currently empty
    empty_since: float | None = None
    history: list[str] = field(default_factory=list)

    def begin_drain(self, now: float) -> None:
        if self.state is not DrainState.ACTIVE:
            raise ValueError(f"cannot begin drain from {self.state}")
        self.state = DrainState.DRAINING
        self.draining_since = now
        self.empty_since = None
        self.history.append(f"{now}:ACTIVE->DRAINING")

    def observe(self, obs: DrainObservation) -> DrainState:
        """Feed one observation. May transition DRAINING→DRAINED (settle met) or →STUCK (timeout)."""
        if self.state not in (DrainState.DRAINING, DrainState.DRAINED):
            return self.state

        if obs.is_empty:
            if self.empty_since is None:
                self.empty_since = obs.ts
            # settle: empty continuously for settle_seconds
            if (obs.ts - self.empty_since) >= self.settle_seconds and self.state is DrainState.DRAINING:
                self.state = DrainState.DRAINED
                self.history.append(f"{obs.ts}:DRAINING->DRAINED")
        else:
            # not empty → reset settle timer; a DRAINED broker that refills falls back to DRAINING
            self.empty_since = None
            if self.state is DrainState.DRAINED:
                self.state = DrainState.DRAINING
                self.history.append(f"{obs.ts}:DRAINED->DRAINING (refilled)")

        # stall detection: too long in DRAINING without reaching DRAINED → STUCK
        if (self.state is DrainState.DRAINING and self.draining_since is not None
                and (obs.ts - self.draining_since) >= self.stall_timeout_seconds):
            self.state = DrainState.STUCK
            self.history.append(f"{obs.ts}:DRAINING->STUCK (timeout)")

        return self.state

    def mark_deleting(self, now: float) -> None:
        """Only DRAINED → DELETING is permitted. STUCK never auto-resolves into deletion."""
        if self.state is not DrainState.DRAINED:
            raise ValueError(
                f"cannot delete broker {self.broker_id!r} from {self.state}; only DRAINED may delete"
            )
        self.state = DrainState.DELETING
        self.history.append(f"{now}:DRAINED->DELETING")

    def mark_gone(self, now: float) -> None:
        if self.state is not DrainState.DELETING:
            raise ValueError(f"cannot mark GONE from {self.state}")
        self.state = DrainState.GONE
        self.history.append(f"{now}:DELETING->GONE")

    def requires_operator(self) -> bool:
        return self.state is DrainState.STUCK
