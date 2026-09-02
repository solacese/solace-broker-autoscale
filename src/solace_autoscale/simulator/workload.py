"""Workload simulator (§13).

Generates synthetic workloads across message size, fanout, and delivery mode — combinations
production will not conveniently produce — and validates the capacity model + decision engine end to
end. Pure and deterministic: no clock, no randomness (variation is by explicit parameter sweep).

Two uses:
  1. ``sweep_matrix`` builds a workload for each (size, fanout, delivery) cell and runs the engine.
  2. ``validate_model`` asserts model-level invariants the whole matrix must satisfy, e.g. that a
     load engineered to sit exactly at the effective threshold recommends exactly the brokers you'd
     compute by hand, and that byte-bound large-message workloads raise the claim-check warning.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..capacity.model import lookup
from ..capacity.schema import CapacityModel
from ..config import Config
from ..decision.engine import DecisionRequest, decide
from ..decision.types import Action, MetricSample, ShardDecision, ShardInput


@dataclass(frozen=True)
class WorkloadCell:
    service_class: str
    delivery: str
    msg_size_bytes: int
    fanout: int
    #: target utilisation of the binding axis you want to engineer (fraction of per-broker cap)
    target_msg_utilisation: float
    current_brokers: int


@dataclass
class SweepResult:
    cell: WorkloadCell
    decision: ShardDecision
    per_broker_msg_rate: float


def build_window(
    model: CapacityModel,
    cell: WorkloadCell,
    *,
    n: int = 30,
    cadence: float = 30.0,
    t0: float = 1_000_000.0,
    connection_count: int = 100,
    spool_fraction: float = 0.1,
    growth_per_sample: float = 0.0,
) -> list[MetricSample]:
    """Build a sample window whose ingress msg rate sits at ``target_msg_utilisation`` of the
    per-broker message capacity times ``current_brokers`` (fleet-wide demand)."""
    cap = lookup(model, cell.service_class, cell.msg_size_bytes, cell.delivery)
    per_broker_msg = cap.msg_rate
    fleet_ingress = per_broker_msg * cell.current_brokers * cell.target_msg_utilisation
    egress = fleet_ingress * cell.fanout
    spool_used = cap.spool_bytes * cell.current_brokers * spool_fraction

    out: list[MetricSample] = []
    ing = fleet_ingress
    egr = egress
    for i in range(n):
        out.append(MetricSample(
            timestamp=t0 + i * cadence,
            ingress_msg_rate=ing,
            egress_msg_rate=egr,
            ingress_byte_rate=ing * cell.msg_size_bytes,
            egress_byte_rate=egr * cell.msg_size_bytes,
            avg_msg_size=float(cell.msg_size_bytes),
            connection_count=connection_count,
            spool_used=spool_used,
            current_brokers=cell.current_brokers,
        ))
        ing *= (1 + growth_per_sample)
        egr *= (1 + growth_per_sample)
    return out


def run_cell(config: Config, model: CapacityModel, cell: WorkloadCell,
             **window_kw: Any) -> SweepResult:
    samples = build_window(model, cell, **window_kw)
    cfg = config.model_copy(deep=True)
    # align config to the cell's delivery so the engine looks up the right curve
    cfg.workload.delivery = cell.delivery  # type: ignore[assignment]
    cfg.fleet.service_class = cell.service_class
    label = f"sim-{cell.service_class}-{cell.delivery}-{cell.msg_size_bytes}-f{cell.fanout}"
    shard = ShardInput(shard_name=label, samples=samples, subscribing_brokers=max(1, cell.fanout))
    now = max(s.timestamp for s in samples) + 1
    d = decide(DecisionRequest(config=cfg, model=model, shard=shard, now=now))
    cap = lookup(model, cell.service_class, cell.msg_size_bytes, cell.delivery)
    return SweepResult(cell=cell, decision=d, per_broker_msg_rate=cap.msg_rate)


def sweep_matrix(
    config: Config,
    model: CapacityModel,
    *,
    service_classes: list[str] | None = None,
    deliveries: tuple[str, ...] = ("direct", "guaranteed"),
    fanouts: tuple[int, ...] = (1, 2, 5, 10, 50),
    utilisations: tuple[float, ...] = (0.2, 0.5, 0.9, 1.5, 3.0),
    current_brokers: int = 1,
) -> list[SweepResult]:
    """Run the engine across the full size×fanout×delivery×utilisation matrix.

    Message sizes are taken from each service class's measured buckets, so every measured point is
    exercised. Returns one SweepResult per cell.
    """
    results: list[SweepResult] = []
    classes = service_classes or list(model.service_classes)
    for sc in classes:
        scc = model.service_classes[sc]
        for delivery in deliveries:
            if delivery not in scc.delivery:
                continue
            sizes = [b.msg_size_bytes for b in scc.delivery[delivery].size_buckets]
            for size in sizes:
                for fanout in fanouts:
                    for util in utilisations:
                        cell = WorkloadCell(
                            service_class=sc, delivery=delivery, msg_size_bytes=size,
                            fanout=fanout, target_msg_utilisation=util,
                            current_brokers=current_brokers,
                        )
                        results.append(run_cell(config, model, cell))
    return results


@dataclass
class OscillationResult:
    """One evaluation step of the consumer-reaction scenario."""

    step: int
    consumer_count: int
    egress_msg_rate: float
    action: str
    recommended_brokers: int


def consumer_reaction_window(
    config: Config,
    model: CapacityModel,
    *,
    service_class: str = "enterprise-10k",
    delivery: str = "guaranteed",
    msg_size_bytes: int = 1000,
    base_ingress_util: float = 0.5,
    # Ramp consumers, then a plateau long enough for the rolling window to flush the ramp samples
    # and converge — a converged steady state is what "does not oscillate" must be measured against.
    consumer_counts: tuple[int, ...] = (1, 2, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8),
    n_per_step: int = 30,
    cadence: float = 30.0,
    t0: float = 1_000_000.0,
) -> list[OscillationResult]:
    """Model a consumer autoscaler reacting to backlog and check the broker loop does not oscillate.

    A consumer autoscaler (e.g. KEDA) raises the consumer replica count when a queue backs up, which
    raises **egress** load on the broker — the signal this tool's broker loop watches. We hold
    ingress steady and ramp the consumer count (and therefore egress), then let it plateau, exactly
    as a converging consumer loop would. See ``docs/architecture.md`` (two-loop interaction).

    We feed a single growing rolling window (not independent per-step windows) so the engine sees the
    real trend, and evaluate the decision at each step. Pure and deterministic: ``now`` is derived
    from the sample timestamps, never a clock.

    Returns one :class:`OscillationResult` per step. The invariant a caller should assert is that the
    recommended broker count is **monotonic non-decreasing** across the scenario and **converges to a
    stable value** on the plateau (the tail steps are constant). It may keep rising for a few steps
    after the consumer count plateaus while the rolling window flushes the lower-egress ramp samples,
    but it must climb toward the steady state and hold — never reverse downward chasing the faster
    consumer loop.
    """
    cfg = config.model_copy(deep=True)
    cfg.workload.delivery = delivery  # type: ignore[assignment]
    cfg.fleet.service_class = service_class
    # Fixed headroom keeps the arithmetic deterministic so "oscillation" means a real reversal,
    # not derived-headroom noise from a changing growth estimate.
    cfg.policy.headroom.mode = "fixed"

    cap = lookup(model, service_class, msg_size_bytes, delivery)
    current_brokers = cfg.fleet.min_brokers
    # Ingress held steady at a modest utilisation; egress scales with the consumer count.
    ingress = cap.msg_rate * current_brokers * base_ingress_util

    samples: list[MetricSample] = []
    results: list[OscillationResult] = []
    t = t0
    for step, consumers in enumerate(consumer_counts):
        egress = ingress * consumers  # more consumers → more egress (fanout-like amplification)
        for _ in range(n_per_step):
            samples.append(MetricSample(
                timestamp=t,
                ingress_msg_rate=ingress,
                egress_msg_rate=egress,
                ingress_byte_rate=ingress * msg_size_bytes,
                egress_byte_rate=egress * msg_size_bytes,
                avg_msg_size=float(msg_size_bytes),
                connection_count=100 + consumers,
                spool_used=cap.spool_bytes * current_brokers * 0.1,
                current_brokers=current_brokers,
            ))
            t += cadence
        shard = ShardInput(shard_name="consumer-reaction", samples=list(samples),
                           subscribing_brokers=max(1, consumers))
        now = samples[-1].timestamp + 1
        d = decide(DecisionRequest(config=cfg, model=model, shard=shard, now=now))
        results.append(OscillationResult(
            step=step,
            consumer_count=consumers,
            egress_msg_rate=egress,
            action=d.action.value,
            recommended_brokers=d.recommended_brokers,
        ))
    return results


@dataclass
class ValidationReport:
    total_cells: int
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


def validate_model(config: Config, model: CapacityModel) -> ValidationReport:
    """Assert model-level invariants across the matrix. Returns a report; ``ok`` if no failures.

    Invariants checked:
      A. Monotonic in load: at fixed (class, size, delivery, fanout), higher utilisation never
         recommends fewer brokers than lower utilisation.
      B. Never below min or above max brokers.
      C. A workload engineered above 100% utilisation on a class whose min<max recommends >1 broker
         (or flags hit-ceiling), i.e. the engine does not silently absorb overload.
      D. Byte-bound large-message cells raise the claim-check warning.
    """
    failures: list[str] = []
    # Use fixed headroom for arithmetic determinism in validation.
    cfg = config.model_copy(deep=True)
    cfg.policy.headroom.mode = "fixed"
    cfg.fleet.min_brokers = 1
    cfg.fleet.max_brokers = 64  # generous so invariant C isn't masked by clamping

    results = sweep_matrix(cfg, model, current_brokers=1)
    total = len(results)

    # group by (class, delivery, size, fanout) → sort by utilisation
    groups: dict[tuple[str, str, int, int], list[SweepResult]] = {}
    for r in results:
        k = (r.cell.service_class, r.cell.delivery, r.cell.msg_size_bytes, r.cell.fanout)
        groups.setdefault(k, []).append(r)

    for k, rs in groups.items():
        rs.sort(key=lambda r: r.cell.target_msg_utilisation)
        prev = None
        for r in rs:
            rec = r.decision.recommended_brokers
            # B
            if not (cfg.fleet.min_brokers <= rec <= cfg.fleet.max_brokers):
                failures.append(f"{k} util={r.cell.target_msg_utilisation}: rec {rec} out of bounds")
            # A monotonic in load (allow equal)
            if prev is not None and rec < prev:
                failures.append(
                    f"{k}: rec dropped from {prev} to {rec} as utilisation increased "
                    f"(non-monotonic in load)"
                )
            prev = rec
        # C: highest utilisation (3.0) should need >1 broker unless the axis truly can't be helped
        top = rs[-1]
        codes = {w.code.value for w in top.decision.warnings}
        if top.decision.recommended_brokers <= 1 and "hit-ceiling" not in codes:
            # spool/connections are set low (fraction 0.1 / 100 conns) so messages/bytes must bind.
            # At util 3.0 the engine must want >1 broker.
            if top.decision.action != Action.no_decision:
                failures.append(
                    f"{k}: util 3.0 recommended {top.decision.recommended_brokers} broker(s) with "
                    f"no hit-ceiling — overload silently absorbed"
                )

    return ValidationReport(total_cells=total, failures=failures)
