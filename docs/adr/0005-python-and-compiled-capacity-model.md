# ADR 0005: Python, and a compiled capacity model

## Status
Accepted.

## Context
Two decisions bundled because they reinforce each other.

**Language.** The tool is a control plane: it reads metrics, runs a pure decision function, writes a
report, and (optionally) calls a REST API. Extreme performance is irrelevant; clarity, typing, and
testability matter, because the decision engine's correctness is the product.

**Capacity data.** The per-broker maxima live in Excel workbooks measured by performance
engineering. Reading Excel at runtime would mean shipping a spreadsheet parser on the hot path,
re-parsing on every run, and - worse - running against numbers that changed under us without a
version trail.

## Decision
- **Python 3.11+.** Pydantic for config and schema validation, standard typing throughout.
- **Compile the Excel to versioned JSON at build time.** `compile.py` reads `performance.xlsx`,
  validates it, and emits `models/<name>.json` whose `model_version` is a content hash of the source
  workbook plus a compiler schema version. The runtime **never reads the Excel**. Every decision
  records the `model_version` that produced it.

## Consequences
- A recommendation is reproducible: the report carries the model version and provenance, so six
  weeks later the exact inputs are recoverable.
- The compiler runs in CI and the generated JSON is committed (except models built from real
  customer data, which are gitignored).
- A synthetic model is explicitly flagged and blocks actuation.
- Interpolation between measured buckets is allowed and recorded; extrapolation beyond the measured
  range is flagged, never silent.
