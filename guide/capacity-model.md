# Capacity model

This document describes (a) what the input performance workbooks actually contain, (b) the
normalised capacity-model JSON schema the compiler emits, and (c) the open questions the
workbooks alone cannot answer.

## 1. What the source workbooks contain

The workbooks under `resources/Performance/` (Solace Cloud results) share a stable shape.
Each **sheet is a service class**: `Solace-Cloud-250`, `-1k`, `-5k`, `-10k`, `-50k`, `-100k`.
These map to the Mission Control `ServiceClassId` enum
(`ENTERPRISE_250_*`, … `ENTERPRISE_100K_*`).

Each sheet has:

- A **metadata block** (rows ~6-25): `Platform`, `Instance Type`, `Memory`, `Volume Type`,
  `Spool Disk Size` (e.g. `65 GiB` → `1300 GiB`), `Broker Mode` (`HA`), client encryption,
  windows, `Message Type` (`SMF`), `API` (`CCSMP`).
- A **Direct Messaging** table and one or more **Guaranteed Messaging** tables. Each table is a
  matrix: **rows = fanout** (1, 2, 5, 10, 50), **columns = message size in bytes**, split into an
  **Ingress** half and an **Egress** half. Values are **messages per second**.
  - Direct size buckets (bytes): `100, 1024, 2048, 10240, 20480, 51200`
  - Guaranteed size buckets (bytes): `512, 1024, 2048, 8192, 65536, 204800`

### How we read a per-broker maximum from this

The **fanout = 1, Ingress** row gives the sustained single-stream max message rate at each size
bucket. That is the `msg_rate` capacity per size bucket. `byte_rate` capacity is derived as
`msg_rate * msg_size` for that bucket. Higher-fanout rows describe egress amplification, not a
different ingress ceiling; the decision engine models fanout from live metrics (§5.1), so the
compiler records the fanout=1 ingress figures as the capacity curve and keeps the egress figures
for validation only.

When a sheet has multiple Guaranteed tables (different consumer/persistence configs), the compiler
takes the **most conservative** (lowest) ingress figure per (size, fanout) cell so the model never
over-promises. This is recorded in provenance.

## 2. What the workbooks do NOT contain - resolved via the service-class API

The workbooks measure **throughput** only. They do not contain connection limits or spool
capacity. Those are authoritative in the Mission Control `ServiceClass` schema:

- `vpnConnections` - max client connections for the class → capacity axis `connections`.
- `vpnMaxSpoolSize` - max message spool size → capacity axis `spool_bytes`.

The compiler therefore fuses two sources into one model: **throughput from the workbook**,
**connections + spool from a service-class table** (checked in as `models/service-classes.json`,
themselves sourced from `getServiceClasses`, or supplied at compile time). Every value records its
origin in provenance.

## 3. Normalised capacity-model JSON schema (v1)

```jsonc
{
  "schema_version": "1",
  "model_version": "<sha256(workbook)>+cs1",   // content hash + compiler schema version
  "provenance": {
    "source_filename": "Solace-Cloud-Perf-AWS-....xlsx",
    "source_sha256": "…",
    "compiled_at": "2026-09-01T00:00:00Z",      // injected by caller, not read from a clock in the engine
    "compiler_version": "1",
    "row_count": 123,
    "platform": "aws",
    "measured_range": {
      "msg_size_bytes": [100, 204800],
      "fanout": [1, 50]
    },
    "notes": ["guaranteed table reduced to conservative min across N tables", "..."]
  },
  "synthetic": false,                            // true blocks all actuation (§10)
  "service_classes": {
    "enterprise-10k": {
      "service_class_id": "ENTERPRISE_10K_HIGHAVAILABILITY",
      "connections_max": 10000,                  // from ServiceClass.vpnConnections
      "spool_bytes_max": 838868336640,           // from ServiceClass.vpnMaxSpoolSize (GB→bytes)
      "delivery": {
        "direct":     { "size_buckets": [ {"msg_size_bytes":100,"msg_rate":319394,"byte_rate":31939400}, ... ] },
        "guaranteed": { "size_buckets": [ {"msg_size_bytes":512,"msg_rate":18779,"byte_rate":9614848}, ... ] }
      }
    }
  }
}
```

`byte_rate = msg_rate * msg_size_bytes`. Buckets are sorted ascending by size. Lookup interpolates
linearly between the two bracketing buckets and records `interpolated: true` on the result; a query
outside `[first, last]` is flagged as **model extrapolation** (§5.6) rather than extrapolated
silently.

## 4. Validation on compile (§6)

- Monotonicity: `msg_rate` is non-increasing as message size increases (larger messages → fewer
  msg/s). `byte_rate` is generally non-decreasing but not required to be strictly monotonic;
  violations are warned, not fatal, unless a rate is zero.
- No missing buckets: every declared size bucket present for every enabled delivery mode.
- No zero or negative capacities anywhere. Fatal.
- Fail the build (non-zero exit) on any fatal violation; never emit a broken model.

## 5. Synthetic placeholder

`models/synthetic-v0.json` ships with obviously fabricated round numbers, `"synthetic": true`, and
a `"WARNING"` field. The report prints a banner when running against it, and a synthetic model
hard-blocks all actuation.
