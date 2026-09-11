"""Warm pool lifecycle (§10).

Maintain ``policy.warm_pool`` pre-provisioned idle brokers so activation is seconds, not minutes.
Record actual provisioning duration on every create and feed it back into ``minutes_to_capacity``
(§5.7). Warm brokers are billed capacity, so their cost is reported on every run.

Pure bookkeeping over provisioning-duration samples; the actuator does the actual create calls.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ProvisioningSample:
    ts: float
    duration_seconds: float


@dataclass
class WarmPool:
    target: int
    #: measured provisioning durations, newest last
    provision_samples: list[ProvisioningSample] = field(default_factory=list)

    def record_provisioning(self, ts: float, duration_seconds: float) -> None:
        self.provision_samples.append(ProvisioningSample(ts, duration_seconds))

    def observed_minutes_to_capacity(self) -> float | None:
        """Median provisioning duration (minutes) from measured samples, or None if none yet.

        Fed into §5.7 as the measured ``minutes_to_capacity``, replacing the assumption. With a warm
        pool present, activation is fast; the samples capture the real number.
        """
        if not self.provision_samples:
            return None
        durs = sorted(s.duration_seconds for s in self.provision_samples)
        mid = len(durs) // 2
        median = durs[mid] if len(durs) % 2 else (durs[mid - 1] + durs[mid]) / 2
        return median / 60.0

    def shortfall(self, current_idle: int) -> int:
        """How many brokers to pre-provision to reach the warm-pool target."""
        return max(0, self.target - current_idle)

    def cost_note(self, per_broker_monthly: float | None = None) -> dict:
        note: dict = {
            "warm_pool_target": self.target,
            "billed_idle": self.target > 0,
        }
        if per_broker_monthly is not None:
            note["estimated_monthly_cost"] = self.target * per_broker_monthly
        return note
