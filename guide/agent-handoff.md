# Start here: feature-aware placement work

Continue the existing controller and managed clients. Inspect the current HEAD and working tree before editing.

## Current implementation

- Python control plane: `scaling-controller/solace_autoscale/controller/`.
- Placement baseline: `controller/planner.py` (`propose_move`).
- Safe handover: `controller/migration.py` (`MigrationEngine`), with durable ownership in the controller store.
- Native Python messaging: `scaling-controller/solace_autoscale_client/`.
- Managed Go AMQP client: `shim/messaging/`; protocol adapter: `shim/transport/amqp/`.
- Small business policy: `examples/simple/payments.yaml`; environment settings: `examples/simple/connection.yaml`.
- Maintained manager demo: `examples/manager-demo/`, launched with `./scripts/manager-demo.sh`.

The managed clients use recorded partition ownership. Do not replace this with per-message round robin or independent topology hashing. The older Go rules/spine tools are a separate path.

## Next objective

Research and empirically validate placement when workloads combine broker features, especially transactions, disaster recovery, replication, persistence, fanout, replay and tracing. Establish compatibility and indivisible transaction/ordering boundaries before optimizing placement. Distinguish HA, DR and warm scaling capacity.

Compare deterministic placement and constrained optimization baselines before introducing machine learning. A predictive model may estimate performance and uncertainty; hard correctness constraints must remain enforced outside it. Unknown feature combinations need conservative behavior, not invented capacity multipliers. Keep the business YAML small.

Preserve migration ordering: prepare an empty destination, bind consumers, fence the source, observe stored and unacknowledged messages drained through the required grace period, commit ownership atomically, then activate the destination. After ownership commit, recover forward. Transactions must never be split or migrated unsafely.

## Local checks

From the repository root, create a virtual environment if needed:

```sh
python3 -m venv .venv
.venv/bin/pip install -e './scaling-controller[dev,service]'
```

Run Python checks from `scaling-controller/`:

```sh
../.venv/bin/ruff check solace_autoscale tests solace_autoscale_client
../.venv/bin/mypy solace_autoscale
../.venv/bin/pytest -q -m 'not integration'
```

Run Go checks from `shim/` with the Go version required by `go.mod`:

```sh
go vet ./...
go build ./...
go test -race ./...
```

See `.github/workflows/ci.yml` for live-broker integration setup. SEMP HTTP readiness alone is insufficient: `scripts/wait-solace-queues.py` verifies queue readiness for CI.

## Evidence and limits

The baseline at `f74f2a2` passed all six CI jobs in [run 35633737895](https://github.com/solacese/solace-broker-autoscale/actions/runs/35633737895). The manager demo reconciled 112 payments in both subscriber groups, including 24 buffered across a publisher SIGKILL. Its reduced capacity budget demonstrates behavior; it is not a performance benchmark.

Read [production readiness](production-readiness.md), [measured profiles](measured-profiles.md), and [Go messaging](go-messaging.md) before changing guarantees. Cloud provisioning qualification, real workload capacity, feature interactions and multi-host controller HA remain production gates.

## Data and workspace boundaries

Never commit credentials, customer workbooks, raw measurements, generated customer capacity models, or durable message state. Local ignored inputs may exist and must be preserved; a fresh clone requires separately supplied benchmark data. Synthetic fixtures must remain clearly labelled and cannot qualify production capacity.

The September 22 cleanup moved the old untracked `demo/`, `improvements.md`, and duplicate-named examples into an archive outside the repository. The maintained demo is `examples/manager-demo/`. Do not revive archived review instructions without checking whether the issue still exists.
