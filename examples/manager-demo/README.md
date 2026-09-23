# Payments keep moving

A two-minute manager presentation backed by a real local failure test: two Solace brokers, Go publishers/subscribers, native topic fanout, durable storage, and the production partition planner and migration engine.

## Run it

Prerequisites: Docker running, Go 1.26, Python 3.11+, and the controller dependencies. From the repository root:

```bash
python3 -m venv .venv
.venv/bin/pip install -e './scaling-controller[dev,service]'
./scripts/manager-demo.sh
```

The launcher creates two **disposable, loopback-only** brokers, builds the Go application, runs the scenario and removes its containers on exit. It refuses to reuse existing demo containers. Ports 18081/18082, 15556/15557 and 15672/15673 must be free. It does not call Solace Cloud, use a customer workbook, or touch your existing fleet. Initial image download/startup can take a few minutes.

Open the printed `state/manager-demo-TIMESTAMP/index.html`. It works offline and can be shared as one HTML file. Click **Replay real run**, drag the timeline or jump to **Result**. Counts and phases come from the just-completed test; missing measurements are shown as “—”. The timestamped directory also contains the JSON report and private demo runtime state. Share the HTML/report, not the database directory.

## What to say

**0:00 — The problem.** “A burst of payments overloads a broker. Adding a server is only half the problem: applications must find it, subscriptions must follow, and we must not abandon payments during the handover.”

**0:20 — The policy.** Show the seven-line YAML. “We keep an account's events together. Ledger and audit each get their own native subscription. The application publishes a topic; it doesn't pick a broker.” The demo uses an audit filter for `created` events; all generated events are `created`, so both groups see all 112 payments.

**0:40 — The handover.** “The planner sees broker counters and selects a partition that fits on the warm broker. The Go subscriber discovers the preparing queue. The controller fences the old path, waits for drain plus grace, and records the new owner.”

**1:00 — The failure.** Pause on **Publisher killed**. “There are 24 accepted payments still waiting on disk. We send SIGKILL—no graceful shutdown—then restart with the same outbox while the migration is in progress.”

**1:20 — The proof.** Jump to Result. “112 accepted IDs. 112 in ledger. 112 in audit. Zero pending. Account order checked in both groups. If an uncertain receipt causes a duplicate, the business transaction deduplicates it.”

**1:40 — The value.** “This supports payment bursts, growth across independent use cases, and isolating a busy workload from quieter ones. We automate placement and recovery so application code stays simple.”

## Be precise about the result

This uses real messages, real SEMP counters, the production planner, and the production migration engine. The demo driver deliberately sets a reduced capacity budget to 83.3% of the measured burst, so the displayed pressure is 120%. This makes the scenario repeatable on a laptop. It is **not** an actual broker capacity measurement and does not run the full production sustained-window policy or Cloud replenishment loop. No production model guard is disabled.

The test checks native fanout, destination consumer binding, ingress rejection, persisted local acceptance, publisher SIGKILL/restart, resumed migration state, complete ID reconciliation, and per-account ordering. It does not prove every failure phase, disk loss, a total broker outage, HA failover, cost savings, scale-in or production SLOs.

The initial consumer restart also verifies durable subscriber registration and business deduplication state. Warm means provisioned and available, not powered off or free. Existing backlog stays on its original broker until processed. One indivisible hot account and a slow business handler are not magically fixed by adding brokers.

For engineering, the same scenario runs in CI as `test_integration_go_managed.py`, with the Go race detector enabled. The managed Go client also supports an explicit `--groups` list for isolated qualification workloads; the presentation keeps its default `ledger,audit` groups. The [Go API guide](../../guide/go-messaging.md) explains delivery limits and error handling.
