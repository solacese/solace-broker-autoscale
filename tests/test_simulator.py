"""Simulator validation across the full size × fanout × delivery matrix (§13 Phase 1 gate).

Runs against BOTH the committed synthetic model and — when the real workbook is available under
resources/ — a freshly compiled real model. The real-workbook test is skipped (not failed) when the
workbook is absent, so CI without the proprietary inputs still passes.
"""

from __future__ import annotations

import pytest

from solace_autoscale.capacity.model import load_model
from solace_autoscale.config import Config
from solace_autoscale.simulator.workload import sweep_matrix, validate_model

from .conftest import REPO

WORKBOOK = REPO / "resources" / "Performance" / "Solace-Cloud-Perf-AWS-10.8.1.241-New-Instances-April2025.xlsx"
SERVICE_CLASSES = REPO / "models" / "service-classes.json"


@pytest.fixture
def synthetic():
    return load_model(REPO / "models" / "synthetic-v0.json")


@pytest.fixture
def real_model():
    if not WORKBOOK.exists():
        pytest.skip("real performance workbook not present under resources/")
    openpyxl = pytest.importorskip("openpyxl")  # noqa: F841
    from solace_autoscale.capacity.compile import compile_workbook
    res = compile_workbook(WORKBOOK, SERVICE_CLASSES, compiled_at="1970-01-01T00:00:00Z")
    return res.model


def test_matrix_runs_on_synthetic(synthetic):
    cfg = Config()
    results = sweep_matrix(cfg, synthetic)
    assert len(results) > 0
    # every cell produced a decision object with the model version stamped
    for r in results:
        assert r.decision.model_version == synthetic.model_version


def test_validate_synthetic_model_invariants(synthetic):
    report = validate_model(Config(), synthetic)
    assert report.total_cells > 0
    assert report.ok, "synthetic model validation failures:\n" + "\n".join(report.failures[:20])


def test_validate_real_model_invariants(real_model):
    report = validate_model(Config(), real_model)
    assert report.total_cells > 0
    assert report.ok, "real model validation failures:\n" + "\n".join(report.failures[:20])


def test_real_model_full_matrix_coverage(real_model):
    """The full measured matrix is exercised: every (class, delivery, measured size) is a cell."""
    cfg = Config()
    results = sweep_matrix(cfg, real_model)
    seen = {(r.cell.service_class, r.cell.delivery, r.cell.msg_size_bytes) for r in results}
    expected = set()
    for sc, scc in real_model.service_classes.items():
        for delivery, curve in scc.delivery.items():
            for b in curve.size_buckets:
                expected.add((sc, delivery, b.msg_size_bytes))
    assert seen == expected
