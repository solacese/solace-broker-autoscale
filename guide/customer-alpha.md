# v1 customer-alpha runbook

Status: ready for a bounded trial on dedicated, isolated non-production brokers. Not qualified for unattended production payments.

This runbook uses the managed **Python SMF** client because that is the smallest tested end-to-end path. The Go AMQP client is also functionally tested, but its durability-preserving batching and production throughput work remains open.

## 1. Acceptance boundary

Use this alpha only when all of the following are true:

- Existing active and warm brokers are dedicated to this workload and isolated from production.
- Messages use guaranteed delivery and controller-managed ordinary queues.
- The inventory has one row per independent HA service. Do not list an HA standby node as another capacity unit.
- Provider, exact broker version, service class, and `deployment_mode: HA` are homogeneous and match a customer-compiled measured profile.
- Cloud provisioning remains disabled.
- Transactions are `none`, DR replication is `none`, replay is `false`, and tracing is `false`.
- The controller and assignment service share one persistent local SQLite database on one host.
- Every publisher process has its own persistent local outbox directory.
- Each subscriber atomically deduplicates event IDs with its business update.

Do not use this alpha to establish capacity. The checked-in reduced-threshold results are functional evidence only. Combined capability/domain placement is experimental; transaction/XA, DR, replay, and tracing workloads remain pinned during managed migration.

## 2. Prerequisites

You need:

- Python 3.11 or later.
- Two or more existing non-production Solace services: at least one `active` and one `warm`.
- An HTTPS SEMP origin and SEMP credentials for each service. The identity needs access to inspect the VPN and create/patch this controller's managed queues and client profile.
- One application user, `autoscale-app` by default, configured with the same credential on every eligible service.
- An SMF TLS URI (`tcps://...`) for every eligible service and trusted CA configuration where required.
- Customer workload measurements and actual deployed HA VPN limits for the exact broker generation and service class.
- Persistent local storage for controller state, publisher outboxes, and subscriber deduplication/business state. Do not use a network filesystem for the reference outbox.

Create a clean environment. Do not reuse a stale editable-install environment:

```bash
git clone https://github.com/solacese/solace-broker-autoscale.git
cd solace-broker-autoscale
python3 -m venv .venv-alpha
.venv-alpha/bin/pip install -e './scaling-controller[compile,service,smf]'
```

## 3. Use the checked-in minimal policy

`examples/simple/payments.yaml` is the exact minimal business policy:

```yaml
version: 1
connection: connection.yaml
workloads:
  payments:
    topic: payments/{account}/{event}
    keep_together: account
    subscribers: [ledger, audit]
```

It references `examples/simple/connection.yaml`, which supplies the operator settings. The checked-in profile is a template, not runnable as-is: replace its missing capacity-model path and the placeholder inventory endpoints before starting.

Copy the two templates to a customer-owned configuration location, then update the operator profile:

```bash
mkdir -p customer-alpha models state
cp examples/simple/payments.yaml customer-alpha/payments.yaml
cp examples/simple/connection.yaml customer-alpha/connection.yaml
cp examples/measured/payments-fleet.yaml customer-alpha/payments-fleet.yaml
```

Replace `customer-alpha/connection.yaml` with this complete baseline. Paths are relative to this file:

```yaml
inventory: payments-fleet.yaml
fleet:
  service_class: enterprise-1k
capacity:
  model: ../models/customer-alpha.json
  scenario: worst
  fanout: 1
  message_size_hint: 1024
metrics:
  source: semp
  scrape_interval: 10s
  staleness_limit: 30s
assignment:
  store: ../state/payments.db
automation:
  fleet_id: payments
  poll_interval: 10s
  shards:
    payments:
      features:
        transactions: none
        replication: none
        replay: false
        tracing: false
policy:
  scale_up_window: 30s
  cooldown: 15m
actuation:
  kill_switch_file: ../state/payments.halt
provisioning:
  enabled: false
  client_username: autoscale-app
```

This is the checked-in `examples/simple/connection.yaml` baseline with only the customer-local paths made explicit. The seven-line policy compiles the remaining guaranteed-routing, 128-partition, automatic scale-up, and static-group settings.

In `customer-alpha/payments-fleet.yaml`, replace every placeholder. Each active/warm row needs:

```yaml
provider: aws                         # match the measured profile
broker_version: 10.8.1.241           # exact running version
service_class: enterprise-1k          # match connection.yaml and the profile
deployment_mode: HA
placement:
  shards:
    payments:
      required_capabilities: [guaranteed]
      spread_by: [zone]
brokers:
  - broker_id: payments-a
    shard: payments
    role: active
    capabilities: [guaranteed]
    failure_domains: {zone: zone-a}
    msg_vpn: payments
    base_url: https://broker-a.example:943
    username_env: PAYMENTS_A_SEMP_USERNAME
    password_env: PAYMENTS_A_SEMP_PASSWORD
    endpoints:
      smf: tcps://broker-a.example:55443
  - broker_id: payments-b
    shard: payments
    role: warm
    capabilities: [guaranteed]
    failure_domains: {zone: zone-b}
    msg_vpn: payments
    base_url: https://broker-b.example:943
    username_env: PAYMENTS_B_SEMP_USERNAME
    password_env: PAYMENTS_B_SEMP_PASSWORD
    endpoints:
      smf: tcps://broker-b.example:55443
```

These capability, domain, and role fields are operator declarations. SEMP discovers live load and queue state; it does not discover application transaction, DR, replay, tracing, capability, or failure-domain facts.

If testing the Go client later, every eligible row also needs an `amqp: amqps://...` endpoint. The current controller validates SMF for queue management but does not automatically enforce AMQP destination eligibility.

## 4. Compile a matching capacity model

Do not use `models/synthetic-v0.json` or the reduced automatic-test profile for an alpha threshold. Import customer measurements and compile against actual deployed HA VPN limits:

```bash
.venv-alpha/bin/solace-autoscale profiles import \
  --directory /path/to/customer-workbooks

.venv-alpha/bin/solace-autoscale profiles list

.venv-alpha/bin/solace-autoscale profiles inspect \
  resources/performance/catalog/PROFILE.json

.venv-alpha/bin/solace-autoscale profiles compile \
  --catalog resources/performance/catalog/PROFILE.json \
  --service-classes /path/to/actual-deployed-ha-limits.json \
  --limits-source "Customer alpha deployed VPN limits, YYYY-MM-DD" \
  --out models/customer-alpha.json
```

The complete baseline already points `customer-alpha/connection.yaml` at `../models/customer-alpha.json`. The checked-in `examples/measured/ha-planning-limits.json` is planning-only and is not sufficient for actuation.

## 5. Validate without touching brokers

Run the offline policy check:

```bash
.venv-alpha/bin/solace-autoscale explain --json \
  --config customer-alpha/payments.yaml \
  --topic payments/account-42/created
```

Confirm the output shows:

- `cloud_creation: false`;
- `delivery: guaranteed, asynchronous broker receipts, at least once`;
- `partitions_per_shard: 128`;
- payment features `transactions: none`, `replication: none`, `replay: false`, `tracing: false`;
- the expected route, business key, and partition.

Review the inventory and model together. A provider, version, service-class, deployment-mode, capability, or domain mismatch must fail closed. Do not use `run --once` as a read-only probe: controller startup can bootstrap managed queues and alter scoped broker configuration.

## 6. Supply secrets

Use a secret manager; do not put values in YAML or logs.

```bash
export PAYMENTS_A_SEMP_USERNAME='...'
export PAYMENTS_A_SEMP_PASSWORD='...'
export PAYMENTS_B_SEMP_USERNAME='...'
export PAYMENTS_B_SEMP_PASSWORD='...'
export SOLACE_ASSIGNMENT_API_KEY='...'
export SOLACE_USERNAME='autoscale-app'
export SOLACE_PASSWORD='...'
```

The server reads `SOLACE_ASSIGNMENT_API_KEY`. The sample application reads `CONTROLLER_API_KEY`; set it to the same value:

```bash
export CONTROLLER_URL='http://127.0.0.1:8099'
export CONTROLLER_API_KEY="$SOLACE_ASSIGNMENT_API_KEY"
```

Bind the assignment service to loopback for this alpha. If you later expose it beyond loopback, terminate TLS at trusted ingress and keep bearer authentication enabled.

## 7. Start and verify

Start the assignment service:

```bash
.venv-alpha/bin/solace-autoscale serve \
  --config customer-alpha/payments.yaml \
  --host 127.0.0.1 \
  --port 8099
```

In a second supervised process, start the controller with the same policy and database:

```bash
.venv-alpha/bin/solace-autoscale run \
  --config customer-alpha/payments.yaml
```

The short policy supplies its inventory through `connection.yaml`; `--inventory` is optional here.

Check the service:

```bash
curl -fsS http://127.0.0.1:8099/healthz
curl -fsS http://127.0.0.1:8099/readyz
curl -fsS \
  -H "Authorization: Bearer ${SOLACE_ASSIGNMENT_API_KEY}" \
  http://127.0.0.1:8099/messaging/config
curl -fsS \
  -H "Authorization: Bearer ${SOLACE_ASSIGNMENT_API_KEY}" \
  'http://127.0.0.1:8099/partitions?shard=payments&group=ledger'
```

Expect both configured groups to become ready, all 128 partitions to have owners, and every partition phase to be `active` before starting the customer test.

## 8. Run a smoke demonstration

Start one process per subscriber group, each with separate persistent state:

```bash
.venv-alpha/bin/python examples/payments/native_app.py consume \
  --state ./state/alpha-ledger \
  --group ledger
```

```bash
.venv-alpha/bin/python examples/payments/native_app.py consume \
  --state ./state/alpha-audit \
  --group audit
```

Publish from a third process:

```bash
.venv-alpha/bin/python examples/payments/native_app.py publish \
  --state ./state/alpha-publisher \
  --event-id alpha-payment-001
```

The publisher must report broker acknowledgement and both consumers must print `alpha-payment-001`. This proves connectivity and fanout only. The sample handler prints messages; it is not a durable business deduplicator.

For a customer acceptance test, replace the print handler with a test sink that atomically records `event_id`, business key, per-key sequence, and business effect under a uniqueness constraint.

## 9. Customer acceptance test

Before sending load, record:

- Git commit SHA and the complete `explain --json` output;
- capacity-model digest, source workbook/cells, deployed VPN limits, provider, exact broker version, service class, and deployment mode;
- client/protocol/TLS versions;
- payload distribution, fanout, account skew, partition count, group count, and number of publishers;
- explicit pass/fail limits for throughput, latency, migration pause, recovery time, RPO, and acceptable duplicate transport deliveries.

Run at least these bounded cases:

1. Baseline delivery without migration.
2. One planned active-to-warm partition migration under representative traffic.
3. Publisher process termination and restart with the same outbox directory.
4. Consumer termination and restart with the same business/deduplication store.
5. Controller termination in pre-commit and post-commit handover phases, then restart with the same database.
6. Fresh post-cutover messages whose keys cover the new owner.

Accept only when every case shows:

- every locally accepted event ID has one durable business effect in every expected group;
- zero missing accepted IDs;
- no duplicate business effects (transport redeliveries may occur and must deduplicate harmlessly);
- per-business-key order preserved for the tested producer contract;
- publisher outboxes return to zero;
- source ready, unacknowledged, and spool state drain before owner commit;
- the destination receives explicit post-cutover traffic;
- migration transitions are `preparing → fencing → draining → activating → complete`;
- no unexplained `feature-pinned`, `capacity-shortfall`, `retry`, or `no-decision` state.

The committed 1→4 local result—1,752 accepted originals and 3,504 deliveries with zero reconciled issues—is a functional comparison point only. Its invented thresholds cannot serve as customer capacity evidence.

## 10. Diagnostics

The controller emits JSON states including `observing`, `migration-planned`, `migrating`, `limited`, `capacity-shortfall`, `feature-pinned`, `no-decision`, `retry`, and `halted`. Preserve logs, but redact URLs and never collect secret values.

Inspect `/partitions` for current owner, source/target locations, and phase. In application instrumentation, expose `MessagingClient.status()`:

- `pending` — locally accepted events not yet broker-acknowledged;
- `policy_error` — assignment authorization or routing-contract failure;
- `delivery_error` — broker delivery failure;
- `publishing_paused` — policy rejection has paused delivery.

With `run` and `serve` stopped, inspect unresolved migration state read-only:

```bash
sqlite3 state/payments.db \
  "SELECT shard,partition,source,target,phase,detail
   FROM migrations
   WHERE phase NOT IN ('complete','rolled-back');"

sqlite3 state/payments.db \
  "SELECT ts,event,detail
   FROM controller_events
   ORDER BY id DESC
   LIMIT 50;"
```

Common responses:

- `feature-pinned`: correct the declaration only if it is factually wrong; otherwise do not migrate that shard.
- `capacity-shortfall`: use a finer business key or a larger measured tier; another same-size broker cannot split one indivisible key.
- destination consumer missing: keep/restart the consumer and let the persisted phase continue.
- outbox full: stop upstream acceptance and preserve the outbox; never delete it to clear pressure.
- stale/missing SEMP data: fix monitoring access; do not infer an empty queue.

## 11. Stop, rollback, and recovery

For a planned stop:

1. Pause upstream publication.
2. Call `flush()` and wait until each publisher reports zero pending events.
3. Reconcile subscriber business effects.
4. Confirm every `/partitions` record has phase `active` and the unresolved-migration query is empty.
5. Stop publishers and consumers gracefully.
6. Stop `run`, then stop `serve`.
7. Back up the stopped controller database, publisher state directories, and subscriber business/deduplication stores as one recovery set using your normal backup tooling.

Do not delete assignment state, outboxes, managed queues, or broker services as rollback.

If a handover is in progress, leave or restart `run` with the same database and let recovery finish. Before owner commit, timeout recovery can reopen the source only after proving the destination empty. After owner commit, recovery is forward-only. There is no supported manual reverse-migration command.

Do not use the kill-switch file as a normal stop: it halts further work immediately and can leave a fenced partition paused. To prevent new moves for a restarted alpha while allowing a recorded handover to finish, set `scaling: {mode: paused}` in the short policy, restart `serve` and `run` with the same state, and observe completion.

## 12. What this alpha does not claim

- Production-rate payment throughput, saturation, soak, or latency SLOs.
- Calibrated Cloud capacity from repository fixtures or generic CI.
- Exactly-once delivery or global ordering.
- Safe managed migration for transaction/XA, DR, replay, tracing, HA interactions, or combined advanced features.
- Automatic runtime discovery of those features.
- Multi-host controller HA or disk-loss recovery.
- Automatic scale-in/deletion, backlog copying, or adoption of arbitrary existing queues.
- Production-ready Go throughput until durability-preserving batching or an alternative qualified store is implemented and re-tested.

Use [production readiness](production-readiness.md) as the gate list before expanding beyond this isolated alpha.
