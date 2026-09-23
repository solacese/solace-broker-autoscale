# Solace Broker Autoscale

**Feature-aware load balancing for managed guaranteed messaging across independent Solace brokers.**

This community project routes business-topic messages to stable partition owners, watches real broker and queue load, and moves complete partition bundles to qualified spare brokers when one owner is persistently overloaded. It is not per-message round robin: all messages for one business key keep the same partition and owner until a fenced, drained handover changes that owner.

> **v1 customer alpha:** ready for a bounded trial on dedicated, isolated non-production brokers using the managed Python SMF path. It is not qualified for unattended production payments. Start with the [customer-alpha runbook](guide/customer-alpha.md) and its acceptance boundary.

[Customer-alpha runbook](guide/customer-alpha.md) · [Architecture](guide/architecture.md) · [Feature qualification](guide/feature-qualification-matrix-2026-09-23.md) · [Production gates](guide/production-readiness.md)

## What it does

For a topic such as `payments/account-42/created`:

1. The application policy names `account` as the field to keep together.
2. The Python SMF or Go AMQP client hashes `account-42` into one fixed partition.
3. The assignment service returns that partition's recorded broker owner.
4. The client publishes one message directly to that broker. Solace topic subscriptions fan it out to one durable queue per subscriber group, such as `ledger` and `audit`.
5. The controller measures queue-copy rates, bytes, spool, connections, and complete VPN totals. It selects a legal partition bundle that relieves the source and fits the destination.
6. During a move, every subscriber-group queue for that partition moves as one unit: prepare the destination, bind consumers, fence old writes, drain the source, atomically change ownership, enable the destination, and retry buffered publications.

```text
business topic + key
        │
        ▼
fixed partition ── durable owner record ──► selected broker
        │                                      │
        │                           native topic subscriptions
        │                                      ▼
        └────────────────────────► ledger queue + audit queue
                                   (one indivisible bundle)
```

Hashing supplies stable partition identity. Runtime load, capacity, feature constraints, affinity, failure domains, broker roles, and explicit pins determine each recorded owner and later moves. A lease only caches an owner; it never authorizes moving a durable queue.

## Small policy, explicit operator contract

The application-facing policy is seven lines:

```yaml
version: 1
connection: connection.yaml
workloads:
  payments:
    topic: payments/{account}/{event}
    keep_together: account
    subscribers: [ledger, audit]
```

This is [`examples/simple/payments.yaml`](examples/simple/payments.yaml). The referenced [`connection.yaml`](examples/simple/connection.yaml) keeps broker inventory, measured capacity, persistent state, and safety settings out of application code. Its endpoints and capacity-model path are placeholders; replace them before running an alpha.

Inspect the compiled policy without contacting a broker:

```bash
python3 -m venv .venv-alpha
.venv-alpha/bin/pip install -e './scaling-controller[compile,service,smf]'
.venv-alpha/bin/solace-autoscale explain --json \
  --config examples/simple/payments.yaml \
  --topic payments/account-42/created
```

The output must show guaranteed, at-least-once delivery, 128 partitions, Cloud creation disabled, and no transaction, DR, replay, or tracing requirement.

## Feature-aware means declared constraints, not runtime feature detection

The controller deliberately separates operator declarations from live observations:

| Source | Used for |
|---|---|
| Application/operator YAML | Transaction mode, DR replication, replay, tracing, required capability labels, allowed failure domains, role, fixed load, partition pins, affinity, and broker limits. |
| Persisted controller state | Routing contract, partition count, subscriber groups, owners, migrations, and feature/placement contracts. Incompatible changes fail closed. |
| SEMP at runtime | Queue depth, unacknowledged messages, bound consumers, ingress state, spool counters, VPN ingress/egress, connections, and spool pressure. |

SEMP does **not** automatically discover whether an application is using transactions, XA, replay, tracing, or DR. Those facts and broker capability/domain labels must be declared and attested by the operator. The controller checks them before planning and again before recording a migration intent.

The current conservative policy is:

- Managed guaranteed baseline (`transactions: none`, `replication: none`, `replay: false`, `tracing: false`) may move.
- Local transactions, XA, DR replication, replay, and tracing pin the complete shard until that feature's managed-migration behavior is qualified.
- Explicitly pinned partitions, recently moved partitions, source-resident backlog, and partitions that cannot fit an eligible destination do not move.
- DR inventory rows are protection capacity, never active or warm scaling capacity.
- Capability/domain-constrained optimization is planner-tested, but combined-feature optimization remains experimental until exercised on matching real services.

Native broker transaction, XA, replay, tracing, and HA tests prove those broker semantics separately. They do not prove that the autoscaler can migrate those workloads safely.

## Delivery and ordering contract

- `publish()` means the event is durably accepted into that publisher's local outbox. `flush()` waits for broker acceptance, not subscriber completion.
- The managed clients keep at most one unconfirmed head per partition while independent partitions progress concurrently.
- Sequential publications from one outbox retain order within a partition. There is no global order across partitions or independent publisher processes.
- Delivery is **at least once**. A lost broker or consumer acknowledgement can cause redelivery.
- Every consumer must atomically store the event ID and its business effect so retries are harmless. Exactly-once delivery is not claimed.
- Stored backlog remains on the source and must drain; the controller does not copy it to the destination.
- Before owner commit, a timed-out move can roll back only after proving the destination empty. After commit, restart recovery proceeds forward to the new owner.

## Supported customer-alpha path

Use the [customer-alpha runbook](guide/customer-alpha.md) for exact setup, start, verification, demonstration, acceptance, diagnostics, and shutdown steps. The supported starting boundary is:

- managed Python SMF client;
- dedicated existing active and warm non-production brokers;
- guaranteed messages and controller-managed ordinary queues;
- homogeneous provider, exact broker version, service class, and HA deployment mode matching a customer-compiled measured profile;
- one single-host controller with persistent SQLite state;
- one persistent local outbox directory per publisher process;
- Cloud creation disabled;
- application-level event-ID deduplication.

The managed Go AMQP path follows the same business policy and recorded owners, but every eligible owner must also expose an `amqp` endpoint. The controller validates SMF for queue management; it does not infer AMQP eligibility. More importantly, durability-preserving Go batching and throughput qualification remain open, so Go is not the recommended first alpha path.

## Evidence available now

The 23 September 2026 branch evidence includes:

- **455 Python unit tests** (4 optional skips), Ruff, strict mypy, Go build/vet, and the full Go race suite.
- Real local automatic activation from **1 to 4 independent Standard brokers** using SEMP telemetry and `Controller.tick()`: 1,752 accepted originals, 3,504 two-group deliveries, zero missing IDs, duplicate IDs, or partition-order violations.
- Static 2/3/4-broker routing: 48 originals reached each of two groups through publisher/subscriber restart, with ownership and queue-copy counters checked.
- Managed Python SMF and Go AMQP handover/restart evidence, plus real AMQP release/redelivery.
- Separate native JMS transaction/XA/replay/tracing tests and one three-node Standard HA failover.
- Prior bounded Cloud functional runs on Enterprise 5K/10K HA services; these were not saturation tests, and all exact-ID services were deleted.

The automatic thresholds were deliberately invented and reduced to trigger short functional tests. These runs do **not** calibrate broker capacity, establish a production SLO, or qualify combined advanced features. See the [sanitized evidence](examples/qualification-evidence/2026-09-23/) and [automatic scale-out record](guide/automatic-scaleout-evidence-2026-09-23.md).

## Known release boundaries

- No saturation or soak result and no calibrated customer capacity model. Compile one from measurements matching the exact deployment.
- The measured Go durable outbox path sustained only 61–68 4 KiB enqueue-plus-confirmed-delete cycles/s on the tested filesystem. Durability-preserving batching or another qualified store remains a production gate.
- No managed migration of transactions/XA, replay, tracing, DR, or their combinations.
- No multi-host controller HA, disk-loss recovery, automatic scale-in/deletion, backlog copying, or arbitrary existing-queue adoption.
- Cloud provisioning is off in the alpha path. Automatic replenishment cannot infer capability or failure-domain facts; constrained fleets must pre-inventory qualified warm services.
- Python SMF and Go AMQP support are functional results, not protocol-specific production throughput claims.

This is suitable for a controlled customer alpha with explicit acceptance criteria, not a production-rate payment-service claim.

## Repository map

- [`scaling-controller/`](scaling-controller/) — Python controller, assignment service, constrained planner, SEMP queue management, native SMF client, and durable outbox.
- [`shim/`](shim/) — managed Go AMQP client plus the older portable rules/event-spine path.
- [`examples/`](examples/) — short policies, inventories, demos, and sanitized qualification evidence.
- [`guide/`](guide/) — architecture, routing, capacity, operations, safety, and evidence.
- [`deploy/terraform/`](deploy/terraform/) — operator-run broker configuration example; the controller does not run Terraform.

## Development verification

```bash
cd scaling-controller
python -m pip install -e '.[dev]'
python -m pytest -m "not integration" -q
ruff check solace_autoscale solace_autoscale_client tests
mypy solace_autoscale

cd ../shim
go test -race ./...
go vet ./...
go build ./...
```

Live-broker tests require separately owned local brokers and optional protocol clients. Do not treat generic unit CI as Cloud or capacity qualification.

Community project · Apache 2.0 · Not an officially supported Solace product.
