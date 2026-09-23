"""Exact, bounded placement oracle and manifest comparison for offline validation only."""

from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..controller.planner import (
    _EPSILON,
    BrokerCandidate,
    LoadVector,
    PlacementBundle,
    PlacementConstraints,
    PlacementObjective,
    PlacementProblem,
    placement_objective,
    plan_next_move,
)


class VectorDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    messages: float = Field(default=0, ge=0)
    bytes: float = Field(default=0, ge=0)
    connections: float = Field(default=0, ge=0)
    spool: float = Field(default=0, ge=0)

    def value(self) -> LoadVector:
        return LoadVector(**self.model_dump())


class BundleDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    id: str = Field(min_length=1)
    partition: int = Field(ge=0)
    current_broker: str = Field(min_length=1)
    movable: VectorDocument
    resident: VectorDocument = Field(default_factory=VectorDocument)
    queue_ids: tuple[str, ...] = ()
    migration_cost: float = Field(default=1, ge=0)
    pinned_reason: str | None = None


class BrokerDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(min_length=1)
    role: str = "active"
    state: str = "active"
    capabilities: frozenset[str] = Field(default_factory=frozenset)
    failure_domains: dict[str, str] = Field(default_factory=dict)
    fixed_load: VectorDocument = Field(default_factory=VectorDocument)


class ConstraintsDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    trigger: float = Field(gt=0, le=1)
    target: float = Field(gt=0, lt=1)
    max_active_brokers: int = Field(ge=1)
    required_capabilities: frozenset[str] = Field(default_factory=frozenset)
    allowed_failure_domains: dict[str, frozenset[str]] = Field(default_factory=dict)
    max_partitions_per_broker: int | None = Field(default=None, ge=1)
    max_partitions_per_domain: dict[str, int] = Field(default_factory=dict)
    spread_by: tuple[str, ...] = ()

    @model_validator(mode="after")
    def ordered_thresholds(self) -> ConstraintsDocument:
        if self.target >= self.trigger:
            raise ValueError("target must be below trigger")
        return self


class PlacementManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: str = "1"
    case_id: str = Field(min_length=1)
    source: dict[str, str]
    shard: str = Field(min_length=1)
    state_limit: int = Field(default=100_000, ge=1, le=1_000_000)
    constraints: ConstraintsDocument
    brokers: list[BrokerDocument] = Field(min_length=1)
    bundles: list[BundleDocument] = Field(min_length=1)
    excluded: frozenset[str] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def synthetic_source(self) -> PlacementManifest:
        if self.schema_version != "1":
            raise ValueError("unsupported placement manifest schema")
        if self.source.get("kind") != "synthetic-normalized":
            raise ValueError("committed placement manifests must be explicitly synthetic-normalized")
        return self

    def problem(self) -> PlacementProblem:
        brokers = tuple(sorted((
            BrokerCandidate(
                item.id,
                self.shard,
                item.role,  # type: ignore[arg-type]
                item.state,
                item.capabilities,
                tuple(sorted(item.failure_domains.items())),
                item.fixed_load.value(),
            )
            for item in self.brokers
        ), key=lambda item: item.broker_id))
        bundles = tuple(sorted((
            PlacementBundle(
                item.id,
                self.shard,
                item.partition,
                item.current_broker,
                item.movable.value(),
                item.resident.value(),
                tuple(sorted(item.queue_ids)),
                item.migration_cost,
                item.pinned_reason,
            )
            for item in self.bundles
        ), key=lambda item: item.bundle_id))
        constraints = self.constraints
        return PlacementProblem(
            self.shard,
            bundles,
            brokers,
            PlacementConstraints(
                constraints.trigger,
                constraints.target,
                constraints.max_active_brokers,
                constraints.required_capabilities,
                tuple(sorted(
                    (key, tuple(sorted(values)))
                    for key, values in constraints.allowed_failure_domains.items()
                )),
                constraints.max_partitions_per_broker,
                tuple(sorted(constraints.max_partitions_per_domain.items())),
                constraints.spread_by,
            ),
            self.excluded,
        )


@dataclass(frozen=True)
class ExactResult:
    assignment: tuple[tuple[str, str], ...] | None
    objective: PlacementObjective | None
    states: int


@dataclass(frozen=True)
class ExactMoveResult:
    bundle_id: str | None
    destination: str | None
    objective: PlacementObjective | None
    states: int


def _valid_assignment(
    problem: PlacementProblem, assignment: dict[str, str], *, require_target_fit: bool = True
) -> bool:
    """Independent full-assignment validator; it does not call production eligibility helpers."""
    brokers = {broker.broker_id: broker for broker in problem.brokers}
    constraints = problem.constraints
    for bundle in problem.bundles:
        destination = brokers.get(assignment.get(bundle.bundle_id, ""))
        if destination is None or destination.shard != bundle.shard:
            return False
        moved = destination.broker_id != bundle.current_broker
        if moved and (bundle.bundle_id in problem.excluded or bundle.pinned_reason):
            return False
        if moved and (
            destination.role not in ("active", "warm")
            or destination.state not in ("active", "warm")
        ):
            return False
        if moved and not constraints.required_capabilities <= destination.capabilities:
            return False
        if moved and any(
            destination.domain(key) not in values
            for key, values in constraints.allowed_domains.items()
        ):
            return False
    counts: dict[str, int] = {}
    for broker_id in assignment.values():
        counts[broker_id] = counts.get(broker_id, 0) + 1
    if constraints.max_bundles_per_broker is not None:
        if any(value > constraints.max_bundles_per_broker for value in counts.values()):
            return False
    for key, limit in constraints.domain_limits.items():
        domains: dict[str, int] = {}
        for broker_id in assignment.values():
            domain = brokers[broker_id].domain(key)
            if domain is None:
                return False
            domains[domain] = domains.get(domain, 0) + 1
        if any(value > limit for value in domains.values()):
            return False
    active = {
        broker.broker_id for broker in problem.brokers
        if broker.role != "dr" and broker.state == "active"
    }
    active.update(broker_id for broker_id in assignment.values() if brokers[broker_id].state == "warm")
    if len(active) > constraints.max_active_brokers:
        return False
    totals = {broker.broker_id: broker.fixed_load for broker in problem.brokers}
    for bundle in problem.bundles:
        totals[bundle.current_broker] = totals[bundle.current_broker].add(bundle.resident)
        broker_id = assignment[bundle.bundle_id]
        totals[broker_id] = totals[broker_id].add(bundle.movable)
    return not require_target_fit or all(total.fits(constraints.target) for total in totals.values())


def _oracle_totals(problem: PlacementProblem, assignment: dict[str, str]) -> dict[str, LoadVector]:
    totals = {broker.broker_id: broker.fixed_load for broker in problem.brokers}
    for bundle in problem.bundles:
        totals[bundle.current_broker] = totals[bundle.current_broker].add(bundle.resident)
        destination = assignment[bundle.bundle_id]
        totals[destination] = totals[destination].add(bundle.movable)
    return totals


def exact_one_step(problem: PlacementProblem) -> ExactMoveResult:
    """Independently enumerate current plus every one-bundle reassignment."""
    current = {bundle.bundle_id: bundle.current_broker for bundle in problem.bundles}
    before = placement_objective(problem, current)
    current_totals = _oracle_totals(problem, current)
    best: tuple[PlacementObjective, str, str] | None = None
    states = 1
    for bundle in problem.bundles:
        if current_totals[bundle.current_broker].peak <= problem.constraints.trigger + _EPSILON:
            continue
        for broker in problem.brokers:
            if broker.broker_id == bundle.current_broker:
                continue
            states += 1
            assignment = dict(current)
            assignment[bundle.bundle_id] = broker.broker_id
            if not _valid_assignment(problem, assignment, require_target_fit=False):
                continue
            next_totals = _oracle_totals(problem, assignment)
            if not next_totals[broker.broker_id].fits(problem.constraints.target):
                continue
            source_before = current_totals[bundle.current_broker]
            source_after = next_totals[bundle.current_broker]
            if all(after >= before - _EPSILON for before, after in zip(
                source_before.values, source_after.values, strict=True
            )):
                continue
            objective = placement_objective(problem, assignment)
            if objective >= before:
                continue
            candidate = objective, bundle.bundle_id, broker.broker_id
            if best is None or candidate < best:
                best = candidate
    if best is None:
        return ExactMoveResult(None, None, None, states)
    return ExactMoveResult(best[1], best[2], best[0], states)


def exact_global(problem: PlacementProblem, state_limit: int = 100_000) -> ExactResult:
    """Enumerate the complete assignment space, refusing rather than truncating large cases."""
    choices: list[tuple[str, ...]] = []
    for bundle in problem.bundles:
        if bundle.bundle_id in problem.excluded or bundle.pinned_reason:
            choices.append((bundle.current_broker,))
        else:
            choices.append(tuple(broker.broker_id for broker in problem.brokers))
    states = math_prod(len(options) for options in choices)
    if states > state_limit:
        raise ValueError(f"placement oracle state space {states} exceeds limit {state_limit}")
    best_assignment = None
    best_objective = None
    ids = [bundle.bundle_id for bundle in problem.bundles]
    for destinations in itertools.product(*choices):
        assignment = dict(zip(ids, destinations, strict=True))
        if not _valid_assignment(problem, assignment):
            continue
        objective = placement_objective(problem, assignment)
        if best_objective is None or objective < best_objective:
            best_assignment = tuple(sorted(assignment.items()))
            best_objective = objective
    return ExactResult(best_assignment, best_objective, states)


def math_prod(values: Any) -> int:
    product = 1
    for value in values:
        product *= int(value)
    return product


def compare_manifest(manifest: PlacementManifest) -> dict[str, Any]:
    problem = manifest.problem()
    production = plan_next_move(problem)
    exact_step = exact_one_step(problem)
    exact = exact_global(problem, manifest.state_limit)
    canonical = json.dumps(manifest.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    proposal = production.proposal
    return {
        "schema_version": "placement-comparison-v1",
        "case_id": manifest.case_id,
        "source": manifest.source,
        "manifest_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
        "planner": {
            "state": production.state,
            "move": (
                {"bundle": proposal.bundle_id, "source": proposal.source,
                 "destination": proposal.destination}
                if proposal else None
            ),
            "objective": asdict(proposal.after) if proposal else None,
            "rejections": dict(production.rejection_counts),
        },
        "exact_one_step": {
            "move": (
                {"bundle": exact_step.bundle_id, "destination": exact_step.destination}
                if exact_step.bundle_id else None
            ),
            "objective": asdict(exact_step.objective) if exact_step.objective else None,
            "states": exact_step.states,
        },
        "exact_global": {
            "assignment": dict(exact.assignment) if exact.assignment else None,
            "objective": asdict(exact.objective) if exact.objective else None,
            "states": exact.states,
        },
    }


def load_manifest(path: str | Path) -> PlacementManifest:
    return PlacementManifest.model_validate_json(Path(path).read_text())
