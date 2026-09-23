"""Differential and adversarial tests for constrained next-move placement."""

from __future__ import annotations

import ast
import itertools
from pathlib import Path

import pytest

from solace_autoscale.controller.planner import (
    BrokerCandidate,
    LoadVector,
    PlacementBundle,
    PlacementConstraints,
    PlacementProblem,
    plan_next_move,
)
from solace_autoscale.simulator.placement import (
    PlacementManifest,
    compare_manifest,
    exact_global,
    exact_one_step,
)


def problem(
    bundles,
    brokers,
    *,
    trigger=.8,
    target=.65,
    max_active=3,
    required=frozenset(),
    allowed=(),
    per_broker=None,
    per_domain=(),
    spread=(),
    excluded=frozenset(),
):
    return PlacementProblem(
        "s",
        tuple(sorted(bundles, key=lambda item: item.bundle_id)),
        tuple(sorted(brokers, key=lambda item: item.broker_id)),
        PlacementConstraints(
            trigger, target, max_active, required, allowed, per_broker, per_domain, spread
        ),
        excluded,
    )


def bundle(name, owner, messages, bytes_=0, *, spool=0, cost=1, pinned=None, queues=()):
    return PlacementBundle(
        name,
        "s",
        int(name.removeprefix("p")),
        owner,
        LoadVector(messages, bytes_),
        LoadVector(spool=spool),
        tuple(sorted(queues)),
        cost,
        pinned,
    )


def broker(name, *, role="active", state=None, capabilities=(), domains=(), fixed=None):
    runtime_state = state or ("active" if role == "dr" else role)
    return BrokerCandidate(
        name,
        "s",
        role,
        runtime_state,
        frozenset(capabilities),
        tuple(sorted(domains)),
        fixed or LoadVector(),
    )


def test_cross_axis_capacity_and_exact_boundary():
    bundles = [bundle("p0", "a", .5, .4), bundle("p1", "a", .4, .3)]
    brokers = [broker("a"), broker("b", fixed=LoadVector(.1, .25))]
    result = plan_next_move(problem(bundles, brokers))
    assert result.proposal is not None
    assert (result.proposal.bundle_id, result.proposal.destination) == ("p1", "b")

    exact = [bundle("p0", "a", .4, .4), bundle("p1", "a", .5, .3)]
    fits = [broker("a"), broker("b", fixed=LoadVector(.25, .25))]
    assert plan_next_move(problem(exact, fits)).proposal.bundle_id == "p0"  # type: ignore[union-attr]
    too_full = [broker("a"), broker("b", fixed=LoadVector(.250000000002, .25))]
    assert plan_next_move(problem(exact, too_full)).proposal is None


def test_move_can_reduce_an_overloaded_axis_even_when_another_axis_ties_the_peak():
    bundles = [bundle("p0", "a", 0, .4), bundle("p1", "a", .9, .5)]
    result = plan_next_move(problem(bundles, [broker("a"), broker("b")]))
    assert result.proposal is not None
    assert result.proposal.bundle_id == "p0"
    assert result.proposal.after.overloaded_axes < result.proposal.before.overloaded_axes


def test_spool_is_source_resident_and_cannot_create_false_relief():
    bundles = [bundle("p0", "a", .1, spool=.8), bundle("p1", "a", .1)]
    result = plan_next_move(problem(bundles, [broker("a"), broker("b")]))
    assert result.proposal is None
    assert result.reason == "no legal improving move"


def test_capability_and_domain_constraints_are_hard():
    bundles = [bundle("p0", "a", .6), bundle("p1", "a", .3)]
    brokers = [
        broker("a", capabilities=("ordinary",), domains=(("zone", "one"),)),
        broker("b", capabilities=("ordinary",), domains=(("zone", "two"),)),
        broker("c", capabilities=("transactions",), domains=(("zone", "three"),)),
    ]
    result = plan_next_move(problem(
        bundles,
        brokers,
        required=frozenset({"transactions"}),
        allowed=(("zone", ("three",)),),
    ))
    assert result.proposal is not None
    assert result.proposal.destination == "c"


def test_dr_is_never_a_candidate_and_warm_ceiling_is_hard():
    bundles = [bundle("p0", "a", .9)]
    blocked = plan_next_move(problem(
        bundles,
        [broker("a"), broker("b", role="warm"), broker("dr", role="dr")],
        max_active=1,
    ))
    assert blocked.proposal is None
    assert {"active-broker-limit", "destination-not-scaling-capacity"} <= set(
        dict(blocked.rejection_counts)
    )


def test_pinned_bundle_and_partition_caps_are_hard():
    bundles = [bundle("p0", "a", .9, pinned="operator-pinned"), bundle("p1", "b", .1)]
    result = plan_next_move(problem(bundles, [broker("a"), broker("b")], per_broker=1))
    assert result.proposal is None
    assert "bundle-pinned" in dict(result.rejection_counts)


def test_input_permutations_have_identical_result():
    bundles = [bundle("p0", "a", .5), bundle("p1", "a", .4), bundle("p2", "a", .2)]
    brokers = [broker("a"), broker("b"), broker("c")]
    expected = None
    for bundle_order in itertools.permutations(bundles):
        for broker_order in itertools.permutations(brokers):
            current = problem(bundle_order, broker_order)
            proposal = plan_next_move(current).proposal
            signature = None if proposal is None else (proposal.bundle_id, proposal.destination, proposal.after)
            expected = signature if expected is None else expected
            assert signature == expected


def test_production_matches_independent_one_step_oracle_and_global_lower_bound():
    case = problem(
        [bundle("p0", "a", .5, .3), bundle("p1", "a", .4, .3), bundle("p2", "b", .2, .1)],
        [broker("a"), broker("b"), broker("c", role="warm")],
    )
    production = plan_next_move(case)
    one_step = exact_one_step(case)
    exact = exact_global(case)
    assert production.proposal is not None
    assert (production.proposal.bundle_id, production.proposal.destination) == (
        one_step.bundle_id, one_step.destination
    )
    assert production.proposal.after == one_step.objective
    assert exact.assignment is not None and exact.objective is not None
    assert production.proposal.after >= exact.objective


def test_one_step_oracle_does_not_move_a_healthy_source():
    case = problem(
        [bundle("p0", "a", .4), bundle("p1", "a", .1)],
        [broker("a"), broker("b")],
    )
    assert plan_next_move(case).proposal is None
    assert exact_one_step(case).bundle_id is None


def test_global_oracle_refuses_unbounded_search():
    case = problem(
        [bundle(f"p{i}", "a", .1) for i in range(8)],
        [broker("a"), broker("b"), broker("c"), broker("d")],
    )
    with pytest.raises(ValueError, match="exceeds limit"):
        exact_global(case, state_limit=100)


def test_manifest_comparison_is_reproducible_and_synthetic_only(tmp_path):
    raw = {
        "case_id": "cross-axis",
        "source": {"kind": "synthetic-normalized"},
        "shard": "s",
        "constraints": {"trigger": .8, "target": .65, "max_active_brokers": 2},
        "brokers": [{"id": "a"}, {"id": "b"}],
        "bundles": [
            {"id": "p0", "partition": 0, "current_broker": "a",
             "movable": {"messages": .9, "bytes": .4}}
        ],
    }
    manifest = PlacementManifest.model_validate(raw)
    assert compare_manifest(manifest) == compare_manifest(manifest)
    raw["source"] = {"kind": "measured"}
    with pytest.raises(ValueError, match="synthetic-normalized"):
        PlacementManifest.model_validate(raw)


def test_planner_and_oracle_are_pure():
    forbidden = {"httpx", "requests", "logging", "sqlite3", "socket", "urllib", "random"}
    root = Path(__file__).resolve().parents[1] / "solace_autoscale"
    for path in (root / "controller" / "planner.py", root / "simulator" / "placement.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(alias.name.split(".")[0] not in forbidden for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert node.module.split(".")[0] not in forbidden
