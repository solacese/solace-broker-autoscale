"""Pure constrained placement of indivisible managed partition bundles."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal

AXES = ("messages", "bytes", "connections", "spool")
_EPSILON = 1e-12


def _normal(value: float) -> float:
    return 0.0 if abs(value) <= _EPSILON else value


@dataclass(frozen=True)
class LoadVector:
    """Fractions of one broker's measured capacity, in deterministic axis order."""

    messages: float = 0
    bytes: float = 0
    connections: float = 0
    spool: float = 0

    def __post_init__(self) -> None:
        if any(not math.isfinite(value) or value < 0 for value in self.values):
            raise ValueError("load values must be finite and nonnegative")

    @property
    def values(self) -> tuple[float, float, float, float]:
        return self.messages, self.bytes, self.connections, self.spool

    @property
    def peak(self) -> float:
        return max(self.values)

    def add(self, other: LoadVector) -> LoadVector:
        values = zip(self.values, other.values, strict=True)
        return LoadVector(*(_normal(math.fsum((a, b))) for a, b in values))

    def subtract(self, other: LoadVector) -> LoadVector:
        values = tuple(a - b for a, b in zip(self.values, other.values, strict=True))
        if any(value < -_EPSILON for value in values):
            raise ValueError("cannot subtract more load than is present")
        return LoadVector(*(_normal(max(0.0, value)) for value in values))

    def fits(self, limit: float) -> bool:
        return all(value <= limit + _EPSILON for value in self.values)


@dataclass(frozen=True)
class PartitionLoad:
    shard: str
    partition: int
    broker_id: str
    messages: float
    bytes: float
    connections: float = 0
    spool: float = 0

    @property
    def pressure(self) -> float:
        return self.vector.peak

    @property
    def vector(self) -> LoadVector:
        return LoadVector(self.messages, self.bytes, self.connections, self.spool)


@dataclass(frozen=True)
class PlacementBundle:
    """One atomic migration unit: a partition and all of its subscriber-group queues."""

    bundle_id: str
    shard: str
    partition: int
    current_broker: str
    movable: LoadVector
    resident: LoadVector = field(default_factory=LoadVector)
    queue_ids: tuple[str, ...] = ()
    migration_cost: float = 1.0
    pinned_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.bundle_id or not self.shard or not self.current_broker:
            raise ValueError("bundle identity, shard and current broker are required")
        if self.partition < 0 or not math.isfinite(self.migration_cost) or self.migration_cost < 0:
            raise ValueError("partition and migration cost must be nonnegative")
        if self.movable.spool:
            raise ValueError("stored spool is source-resident, never movable")
        if tuple(sorted(set(self.queue_ids))) != self.queue_ids:
            raise ValueError("queue_ids must be unique and sorted")

    @property
    def total(self) -> LoadVector:
        return self.movable.add(self.resident)


@dataclass(frozen=True)
class BrokerCandidate:
    broker_id: str
    shard: str
    role: Literal["active", "warm", "dr"] = "active"
    state: str = "active"
    capabilities: frozenset[str] = frozenset()
    failure_domains: tuple[tuple[str, str], ...] = ()
    fixed_load: LoadVector = field(default_factory=LoadVector)

    def __post_init__(self) -> None:
        if not self.broker_id or not self.shard:
            raise ValueError("broker identity and shard are required")
        if self.role not in ("active", "warm", "dr"):
            raise ValueError("unknown broker role")
        if self.state not in ("active", "warm", "draining", "drained", "deleting", "gone"):
            raise ValueError("unknown broker state")
        if tuple(sorted(dict(self.failure_domains).items())) != self.failure_domains:
            raise ValueError("failure_domains must be unique and sorted")

    def domain(self, key: str) -> str | None:
        return dict(self.failure_domains).get(key)


@dataclass(frozen=True)
class PlacementConstraints:
    trigger: float
    target: float
    max_active_brokers: int
    required_capabilities: frozenset[str] = frozenset()
    allowed_failure_domains: tuple[tuple[str, tuple[str, ...]], ...] = ()
    max_bundles_per_broker: int | None = None
    max_bundles_per_domain: tuple[tuple[str, int], ...] = ()
    spread_by: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not (0 < self.target < self.trigger <= 1) or self.max_active_brokers < 1:
            raise ValueError("placement thresholds and active broker ceiling are invalid")
        if self.max_bundles_per_broker is not None and self.max_bundles_per_broker < 1:
            raise ValueError("max_bundles_per_broker must be positive")
        if any(limit < 1 for _, limit in self.max_bundles_per_domain):
            raise ValueError("max_bundles_per_domain values must be positive")

    @property
    def allowed_domains(self) -> dict[str, frozenset[str]]:
        return {key: frozenset(values) for key, values in self.allowed_failure_domains}

    @property
    def domain_limits(self) -> dict[str, int]:
        return dict(self.max_bundles_per_domain)


@dataclass(frozen=True)
class PlacementProblem:
    shard: str
    bundles: tuple[PlacementBundle, ...]
    brokers: tuple[BrokerCandidate, ...]
    constraints: PlacementConstraints
    excluded: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.shard:
            raise ValueError("placement shard is required")
        if tuple(sorted(self.bundles, key=lambda item: item.bundle_id)) != self.bundles:
            raise ValueError("bundles must be sorted by bundle_id")
        if tuple(sorted(self.brokers, key=lambda item: item.broker_id)) != self.brokers:
            raise ValueError("brokers must be sorted by broker_id")
        if len({bundle.bundle_id for bundle in self.bundles}) != len(self.bundles):
            raise ValueError("bundle IDs must be unique")
        if len({broker.broker_id for broker in self.brokers}) != len(self.brokers):
            raise ValueError("broker IDs must be unique")
        broker_ids = {broker.broker_id for broker in self.brokers}
        invalid_bundles = (
            bundle.shard != self.shard or bundle.current_broker not in broker_ids
            for bundle in self.bundles
        )
        if any(invalid_bundles):
            raise ValueError("every bundle must belong to the shard and a known current broker")
        if any(broker.shard != self.shard for broker in self.brokers):
            raise ValueError("every broker must belong to the placement shard")


@dataclass(frozen=True, order=True)
class PlacementObjective:
    overloaded_axes: int
    peak: float
    excess: float
    warm_activations: int
    moved_bundles: int
    moved_queue_count: int
    moved_load: float
    scarce_capability_cost: int
    spread: tuple[tuple[float, ...], ...]
    signature: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class MoveProposal:
    bundle_id: str
    shard: str
    partition: int
    source: str
    destination: str
    activate_warm: bool
    before: PlacementObjective
    after: PlacementObjective
    rejection_counts: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class PlacementOutcome:
    state: Literal["planned", "no-beneficial-move"]
    proposal: MoveProposal | None
    reason: str
    rejection_counts: tuple[tuple[str, int], ...] = ()


def _assignment(problem: PlacementProblem) -> dict[str, str]:
    return {bundle.bundle_id: bundle.current_broker for bundle in problem.bundles}


def _totals(problem: PlacementProblem, assignment: dict[str, str]) -> dict[str, LoadVector]:
    totals = {broker.broker_id: broker.fixed_load for broker in problem.brokers}
    for bundle in problem.bundles:
        destination = assignment[bundle.bundle_id]
        totals[bundle.current_broker] = totals[bundle.current_broker].add(bundle.resident)
        totals[destination] = totals[destination].add(bundle.movable)
    return totals


def placement_objective(problem: PlacementProblem, assignment: dict[str, str]) -> PlacementObjective:
    """Score a complete assignment after hard constraints have been checked."""
    brokers = {broker.broker_id: broker for broker in problem.brokers}
    totals = _totals(problem, assignment)
    values = [value for broker_id in sorted(totals) for value in totals[broker_id].values]
    trigger, target = problem.constraints.trigger, problem.constraints.target
    activated = {
        broker_id for broker_id in assignment.values()
        if brokers[broker_id].state == "warm"
    }
    moved = [bundle for bundle in problem.bundles if assignment[bundle.bundle_id] != bundle.current_broker]
    scarce = sum(
        len(brokers[destination].capabilities - problem.constraints.required_capabilities)
        for destination in assignment.values()
    )
    spread: list[tuple[float, ...]] = []
    for key in problem.constraints.spread_by:
        domain_loads: dict[str, list[float]] = {}
        domain_counts: dict[str, int] = {}
        for bundle in problem.bundles:
            destination = assignment[bundle.bundle_id]
            domain = brokers[destination].domain(key)
            if domain is None:
                continue
            domain_loads.setdefault(domain, []).append(bundle.movable.peak)
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        spread.append(tuple(sorted((math.fsum(loads) for loads in domain_loads.values()), reverse=True)))
        spread.append(tuple(float(value) for value in sorted(domain_counts.values(), reverse=True)))
    return PlacementObjective(
        overloaded_axes=sum(value > trigger + _EPSILON for value in values),
        peak=_normal(max(values, default=0.0)),
        excess=_normal(math.fsum(max(0.0, value - target) for value in values)),
        warm_activations=len(activated),
        moved_bundles=len(moved),
        moved_queue_count=sum(max(1, len(bundle.queue_ids)) for bundle in moved),
        moved_load=_normal(math.fsum(bundle.migration_cost for bundle in moved)),
        scarce_capability_cost=scarce,
        spread=tuple(spread),
        signature=tuple(sorted(assignment.items())),
    )


def _eligible_destination(
    problem: PlacementProblem,
    bundle: PlacementBundle,
    destination: BrokerCandidate,
    assignment: dict[str, str],
    current_totals: dict[str, LoadVector],
    current_counts: dict[str, int],
    current_domain_counts: dict[str, dict[str, int]],
) -> str | None:
    constraints = problem.constraints
    if destination.broker_id == bundle.current_broker:
        return "same-broker"
    if bundle.bundle_id in problem.excluded or bundle.pinned_reason:
        return "bundle-pinned"
    if destination.shard != bundle.shard:
        return "wrong-shard"
    if destination.role not in ("active", "warm") or destination.state not in ("active", "warm"):
        return "destination-not-scaling-capacity"
    if not constraints.required_capabilities <= destination.capabilities:
        return "missing-capability"
    for key, allowed in constraints.allowed_domains.items():
        if destination.domain(key) not in allowed:
            return "failure-domain-not-allowed"
    if constraints.max_bundles_per_broker is not None:
        next_counts = dict(current_counts)
        next_counts[bundle.current_broker] -= 1
        next_counts[destination.broker_id] = next_counts.get(destination.broker_id, 0) + 1
        if any(count > constraints.max_bundles_per_broker for count in next_counts.values()):
            return "broker-bundle-limit"
    brokers = {broker.broker_id: broker for broker in problem.brokers}
    source = brokers[bundle.current_broker]
    for key, limit in constraints.domain_limits.items():
        source_domain = source.domain(key)
        destination_domain = destination.domain(key)
        if source_domain is None or destination_domain is None:
            return "failure-domain-missing"
        destination_after = current_domain_counts[key].get(destination_domain, 0)
        source_domain_after = current_domain_counts[key].get(source_domain, 0)
        if source_domain != destination_domain:
            destination_after += 1
            source_domain_after -= 1
        if destination_after > limit or source_domain_after > limit:
            return "failure-domain-limit"
    active = {
        broker.broker_id for broker in problem.brokers
        if broker.role != "dr" and broker.state == "active"
    }
    if destination.state == "warm":
        active.add(destination.broker_id)
    if len(active) > constraints.max_active_brokers:
        return "active-broker-limit"
    target_after = current_totals[destination.broker_id].add(bundle.movable)
    if not target_after.fits(constraints.target):
        return "destination-capacity"
    source_before = current_totals[bundle.current_broker]
    source_after = source_before.subtract(bundle.movable)
    if all(after >= before - _EPSILON for before, after in zip(
        source_before.values, source_after.values, strict=True
    )):
        return "source-not-relieved"
    return None


def plan_next_move(problem: PlacementProblem) -> PlacementOutcome:
    """Exactly optimize the finite legal one-move neighborhood; never return a stale batch plan."""
    current = _assignment(problem)
    before = placement_objective(problem, current)
    totals = _totals(problem, current)
    brokers = {broker.broker_id: broker for broker in problem.brokers}
    counts: dict[str, int] = {}
    for broker_id in current.values():
        counts[broker_id] = counts.get(broker_id, 0) + 1
    domain_counts: dict[str, dict[str, int]] = {
        key: {} for key in problem.constraints.domain_limits
    }
    for broker_id in current.values():
        broker = brokers[broker_id]
        for key in domain_counts:
            domain = broker.domain(key)
            if domain is not None:
                domain_counts[key][domain] = domain_counts[key].get(domain, 0) + 1
    rejected: dict[str, int] = {}
    options: list[MoveProposal] = []
    for bundle in problem.bundles:
        if totals[bundle.current_broker].peak <= problem.constraints.trigger + _EPSILON:
            continue
        for destination in problem.brokers:
            reason = _eligible_destination(
                problem, bundle, destination, current, totals, counts, domain_counts
            )
            if reason is not None:
                rejected[reason] = rejected.get(reason, 0) + 1
                continue
            assignment = dict(current)
            assignment[bundle.bundle_id] = destination.broker_id
            after = placement_objective(problem, assignment)
            if after >= before:
                rejected["objective-not-improved"] = rejected.get("objective-not-improved", 0) + 1
                continue
            options.append(
                MoveProposal(
                    bundle.bundle_id,
                    bundle.shard,
                    bundle.partition,
                    bundle.current_broker,
                    destination.broker_id,
                    brokers[destination.broker_id].state == "warm",
                    before,
                    after,
                )
            )
    rejection_counts = tuple(sorted(rejected.items()))
    if not options:
        return PlacementOutcome("no-beneficial-move", None, "no legal improving move", rejection_counts)
    proposal = min(
        options,
        key=lambda option: (option.after, option.bundle_id, option.destination),
    )
    return PlacementOutcome(
        "planned",
        MoveProposal(**{**proposal.__dict__, "rejection_counts": rejection_counts}),
        "best legal one-move objective",
        rejection_counts,
    )


def propose_move(
    loads: list[PartitionLoad],
    brokers: list[str],
    *,
    trigger: float,
    target: float,
    excluded: set[tuple[str, int]],
) -> tuple[PartitionLoad, str] | None:
    """Compatibility adapter for the original homogeneous one-shard planner API."""
    if not loads or not brokers:
        return None
    shards = {load.shard for load in loads}
    if len(shards) != 1:
        raise ValueError("propose_move requires loads from exactly one shard")
    by_id = {(load.shard, load.partition): load for load in loads}
    shard = next(iter(shards))
    bundles = tuple(
        PlacementBundle(
            bundle_id=f"{load.shard}/partition:{load.partition}",
            shard=load.shard,
            partition=load.partition,
            current_broker=load.broker_id,
            movable=LoadVector(load.messages, load.bytes, load.connections, 0),
            resident=LoadVector(spool=load.spool),
        )
        for load in sorted(loads, key=lambda item: (item.shard, item.partition))
    )
    candidates = tuple(
        BrokerCandidate(broker_id, shard)
        for broker_id in sorted(set(brokers))
    )
    outcome = plan_next_move(
        PlacementProblem(
            shard,
            bundles,
            candidates,
            PlacementConstraints(trigger, target, len(candidates)),
            frozenset(f"{name}/partition:{partition}" for name, partition in excluded),
        )
    )
    if outcome.proposal is None:
        return None
    proposal = outcome.proposal
    return by_id[(proposal.shard, proposal.partition)], proposal.destination
