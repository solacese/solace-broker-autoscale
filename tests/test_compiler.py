"""Capacity compiler tests (§13): golden-file against a fixture workbook + failure tests per rule."""

from __future__ import annotations

import json

import pytest

openpyxl = pytest.importorskip("openpyxl")

from solace_autoscale.capacity.compile import CompileError, compile_workbook  # noqa: E402


def _write_sheet(ws, spool_gib, direct_rates, guar_rates,
                 direct_sizes=(100, 1000, 10000), guar_sizes=(512, 1024, 8192)):
    ws["B2"] = "Solace Cloud Test Perf"
    ws["B6"] = "Platform"; ws["C6"] = "aws"
    ws["B11"] = "Spool Disk Size"; ws["C11"] = f"{spool_gib} GiB"
    ws["B22"] = "Message Type"; ws["C22"] = "SMF"
    # Direct table
    ws["B30"] = "Direct Messaging"
    ws["B32"] = "Ingress"; ws["C32"] = "Message Size"
    ws["B33"] = "Fanout"
    for i, s in enumerate(direct_sizes):
        ws.cell(row=33, column=3 + i, value=s)
    ws["B34"] = 1
    for i, r in enumerate(direct_rates):
        ws.cell(row=34, column=3 + i, value=r)
    # add a fanout=2 row (ignored by compiler which reads fanout=1)
    ws["B35"] = 2
    # Guaranteed table
    ws["B40"] = "Guaranteed Messaging"
    ws["B42"] = "Ingress"; ws["C42"] = "Message Size"
    ws["B43"] = "Fanout"
    for i, s in enumerate(guar_sizes):
        ws.cell(row=43, column=3 + i, value=s)
    ws["B44"] = 1
    for i, r in enumerate(guar_rates):
        ws.cell(row=44, column=3 + i, value=r)


def make_workbook(tmp_path, *, direct_rates=(100000, 10000, 1000), guar_rates=(8000, 4000, 800),
                  spool_gib=780, sheet_name="Solace-Cloud-10k"):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name
    _write_sheet(ws, spool_gib, direct_rates, guar_rates)
    p = tmp_path / "perf.xlsx"
    wb.save(p)
    return p


def make_service_classes(tmp_path):
    doc = {"enterprise-10k": {"service_class_id": "ENTERPRISE_10K_HIGHAVAILABILITY",
                              "connections_max": 10000, "spool_bytes_max": 837518622720}}
    p = tmp_path / "sc.json"
    p.write_text(json.dumps(doc))
    return p


def test_golden_compile(tmp_path):
    wb = make_workbook(tmp_path)
    sc = make_service_classes(tmp_path)
    res = compile_workbook(wb, sc, compiled_at="1970-01-01T00:00:00Z")
    m = res.model
    assert "enterprise-10k" in m.service_classes
    scc = m.service_classes["enterprise-10k"]
    assert scc.connections_max == 10000
    direct = {b.msg_size_bytes: b.msg_rate for b in scc.delivery["direct"].size_buckets}
    assert direct == {100: 100000.0, 1000: 10000.0, 10000: 1000.0}
    # byte_rate = msg_rate * size
    b100 = scc.delivery["direct"].size_buckets[0]
    assert b100.byte_rate == 100000.0 * 100
    # model_version is a content hash + compiler schema version
    assert "+cs" in m.model_version
    assert tuple(m.provenance.measured_range.msg_size_bytes) == (100, 10000)


def test_deterministic_model_version(tmp_path):
    wb = make_workbook(tmp_path)
    sc = make_service_classes(tmp_path)
    v1 = compile_workbook(wb, sc, compiled_at="1970-01-01T00:00:00Z").model.model_version
    v2 = compile_workbook(wb, sc, compiled_at="2020-01-01T00:00:00Z").model.model_version
    # version depends on workbook content + compiler schema, NOT on compile time
    assert v1 == v2


def test_fail_on_zero_capacity(tmp_path):
    wb = make_workbook(tmp_path, direct_rates=(100000, 0, 1000))
    sc = make_service_classes(tmp_path)
    with pytest.raises(CompileError):
        compile_workbook(wb, sc, compiled_at="1970-01-01T00:00:00Z")


def test_fail_on_missing_service_class_entry(tmp_path):
    wb = make_workbook(tmp_path)
    doc = {"enterprise-1k": {"service_class_id": "X", "connections_max": 1000,
                             "spool_bytes_max": 1}}  # wrong key
    p = tmp_path / "sc.json"
    p.write_text(json.dumps(doc))
    with pytest.raises(CompileError):
        compile_workbook(wb, p, compiled_at="1970-01-01T00:00:00Z")


def test_fail_on_negative_connections(tmp_path):
    wb = make_workbook(tmp_path)
    doc = {"enterprise-10k": {"service_class_id": "X", "connections_max": -1,
                             "spool_bytes_max": 1}}
    p = tmp_path / "sc.json"
    p.write_text(json.dumps(doc))
    with pytest.raises(CompileError):
        compile_workbook(wb, p, compiled_at="1970-01-01T00:00:00Z")
