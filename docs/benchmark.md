# Measuring your own brokers

`solace-autoscale` ships the capacity-model **schema and harness**, not measured numbers. The
performance figures are yours to measure against your own service classes, message sizes, and
delivery modes. This directory explains how.

## What the compiler expects

`solace-autoscale compile` reads a workbook with **one sheet per service class**, each sheet
containing:

- a metadata block with at least `Platform` and `Spool Disk Size`;
- a **Direct Messaging** table and one or more **Guaranteed Messaging** tables, each a matrix of
  **rows = Fanout** and **columns = Message Size (bytes)**, split into Ingress and Egress halves,
  values in **messages/second**.

The compiler reads the **fanout = 1, Ingress** row as the sustained per-broker maximum at each size
bucket. See `docs/capacity-model.md` for the exact shape and the normalised output schema.

## Producing the numbers

1. Provision one broker of each service class you care about.
2. For each (message size, delivery mode), run a sustained-throughput test that finds the highest
   ingress rate the broker holds without falling behind (spool growth stays flat for guaranteed,
   discards stay zero for direct). Tools: `sdkperf` (Solace), or your own load harness.
3. Record msg/s at fanout = 1 for each size bucket into the sheet. Repeat per class.
4. Add connection and spool ceilings to `models/service-classes.json` from
   `GET /api/v2/missionControl/serviceClasses` (`vpnConnections`, `vpnMaxSpoolSize`).
5. `solace-autoscale compile --workbook yours.xlsx --service-classes models/service-classes.json
   --out models/yours.json`.

## Trust it only after the simulator and accuracy pass

- `solace-autoscale simulate --config your.yaml` validates the model across the full matrix.
- After running against real load, `solace-autoscale accuracy` reports predicted-vs-actual error.
  Treat a consistently optimistic bucket as a reason to re-measure, not to ship.

Compiled models built from **real customer data are gitignored** and must never be committed. Only
`models/synthetic-v0.json` (obviously fake) is checked in.
