# Solace Broker Autoscale (`solace-broker-autoscale`)

**Scale the brokers. Move the workload. Keep related payments together.**

This project helps you plan and operate a horizontally scaled Solace Cloud fleet. It combines measured performance profiles, stable business-key routing and an unattended controller that moves managed queue partitions to available capacity.

[Explore the project](https://solacese.github.io/solace-broker-autoscale/) · [Routing explained](guide/routing.md) · [Measured profiles](guide/measured-profiles.md) · [Configuration](guide/configuration.md)

> Community project, Apache 2.0. Managed SMF and Go AMQP queue handover are tested on two real local brokers, including publisher rejection, durable retry and restart. Optional Cloud creation is implemented and mock-tested. Production Cloud rollout, workload performance validation and multi-host controller HA remain outstanding.

For development and the next feature-aware placement phase, start with the [agent handoff](guide/agent-handoff.md).

## Get the repository

The repository is **[solacese/solace-broker-autoscale](https://github.com/solacese/solace-broker-autoscale)**. Use `solace-broker-autoscale` as the checkout folder:

```bash
git clone https://github.com/solacese/solace-broker-autoscale.git
cd solace-broker-autoscale
```

The installed command is still named `solace-autoscale`, and the Python package is `solace_autoscale`. These are names within this project, not separate repositories. Run the root-level setup and demo commands below from this checkout.

## Repository layout and supported paths

- [scaling-controller/](scaling-controller/): Python controller, capacity planning, native SMF client, durable outbox and managed queue migration.
- [shim/](shim/): Go managed AMQP client with a durable outbox, topic routing, subscription discovery and migration recovery; also retains the portable rules and event-spine tools.
- [guide/](guide/): documentation and the GitHub Pages site.
- [examples/](examples/): short application policies and operator profiles.

Use the Python native SMF client or the [managed Go AMQP client](guide/go-messaging.md) with the same business YAML and recorded queue ownership. The older Go rules/spine path remains separate. [Production readiness](guide/production-readiness.md) lists qualification still needed.

## Show it working

[Before/after broker split examples](guide/broker-split-examples.md): payment bursts, gradual growth, and bandwidth-heavy traffic, with runnable planner inputs and a Cloud qualification plan.

Run `./scripts/manager-demo.sh` for a real two-broker payment burst and publisher crash/recovery demo. It produces an offline HTML presentation with a replay, seven-line policy and reconciled counts. [Setup and two-minute presenter notes](examples/manager-demo/README.md).

The local scenario recovered **112 accepted payments in both ledger and audit**, including **24 buffered publications across SIGKILL and migration**, with account order checked. The reduced demo capacity is explicitly labelled; this is functional evidence, not a benchmark.

## Start with a small application policy

Use [the short payments YAML](examples/simple/payments.yaml) to choose topics, ordering keys, subscriber groups and broker limits. Keep environment setup in [one operator connection file](examples/simple/connection.yaml). [The simple guide](guide/simple-policy.md) explains each choice and the two service commands.

```bash
solace-autoscale explain --config examples/simple/payments.yaml --topic payments/account-42/created
```

**Publishing is asynchronous pub/sub.** The application saves locally and continues. Broker receipts arrive in the background; subscribers process independently. No subscriber reply is required. Guaranteed delivery keeps transport receipts so a payment is never removed from the local buffer merely because it was sent.

## What you can do

| Need | What the repository provides |
|---|---|
| Size a real workload | Import measured Cloud workbooks and keep provider, broker generation, tier, fanout and replay/tracing conditions separate. Trace the answer back to spreadsheet cells. |
| See which limit is reached | Evaluate message rate, incoming/outgoing bytes, connections and stored messages. Include headroom, sustained windows and cooldown policy. |
| Plan growth and cost | Explore traffic multipliers and broker counts. Add your own prices to estimate costs; committed and elastic billing behave differently. |
| Observe the fleet | Collect every active service over SEMP. Refuse incomplete snapshots and show per-broker load alongside totals. |
| Keep related messages together | Route by a shared business key into fixed partitions. Use stable or weighted rendezvous hashing; preserve durable queue ownership. |
| Connect existing applications | Return ordinary SMF, AMQP, MQTT and REST endpoints. The Python key router caches partition locations; messages travel directly to brokers. |
| Explore shard boundaries | Use Event Portal exports to propose traffic groups. The application still supplies its routing key. |
| Scale while unattended | Detect sustained overload, activate warm brokers, select fitting partitions, fence and drain queues, switch ownership, and recover after restart. Optionally replenish warm capacity through Solace Cloud. |

## Start with your performance profiles

Python 3.11 or later:

```bash
pip install -e './scaling-controller[compile,service]'
solace-autoscale profiles import --directory /path/to/workbooks
solace-autoscale profiles list
solace-autoscale profiles inspect resources/performance/catalog/PROFILE.json
```

Compile one matching provider/version generation with explicit HA service limits:

```bash
solace-autoscale profiles compile \
  --catalog resources/performance/catalog/PROFILE.json \
  --service-classes examples/measured/ha-planning-limits.json \
  --limits-source "Planning defaults; replace with actual deployed VPN limits" \
  --out models/my-profile.json

solace-autoscale plan --model models/my-profile.json \
  --service-class enterprise-1k --message-size 1024 \
  --messages 10000 --fanout 5 --scenario streaming
```

The example limits file is explicitly for planning. The supplied Cloud test layouts describe HA services, not standalone brokers. Disk size is not the VPN spool limit. Real workbooks, catalogs and models remain local and ignored by Git; the public repository contains invented test data.

For an existing metrics window:

```bash
solace-autoscale recommend --config your-config.yaml --metrics metrics.json
solace-autoscale whatif --config your-config.yaml --metrics metrics.json --multipliers 1,2,4
```

For a fleet, edit [the inventory example](examples/measured/fleet.yaml), set its SEMP credential environment variables, and run:

```bash
solace-autoscale monitor-fleet --config your-config.yaml --inventory your-fleet.yaml
```

These commands are read-only. A fresh monitor gathers a full observation window before recommending a change. See [measured profiles](guide/measured-profiles.md) for coverage, assumptions and the complete workflow.

## Publish and subscribe through one API

The native Python shim handles broker selection, connections, durable buffering and subscription discovery. Solace matches topics and fans out to durable group queues. Your application uses business topics:

```python
client.subscribe(group="ledger", handler=commit_payment)
client.publish("payments/account-42/created", {"amount": 25}, event_id="payment-123")
```

YAML chooses business-key, full-topic or whole-family dispatch. It declares subscriber groups and per-shard scaling thresholds. In this example, it extracts `account-42` as the ordering key. The controller moves its partition through preparation, fencing, drainage and cutover; the shim follows automatically. `publish()` confirms local persistence, and subscriber handlers must deduplicate event IDs. Different subscription groups receive independent copies; replicas in one group share exclusive queues.

[Native messaging guide](guide/native-messaging.md) · [Complete YAML](examples/measured/payments-native.yaml) · [Runnable application](examples/payments/native_app.py)

## How messages are spread

The original resolver balanced **client placement counts**. A busy client could still dominate a broker, and unrelated client IDs could send a publisher and its consumer to different brokers.

For lower-level integrations that supply their own business key, use:

```yaml
assignment:
  store: ./assignment.db
  routing: partitioned        # client | partitioned
  strategy: rendezvous        # least-placements | rendezvous
  partitions: 128             # Fixed per shard; changing this requires migration.
  lease_seconds: 300
  broker_weights: {}          # Optional: {broker-a: 1, broker-b: 2}
```

A key such as `order-123` maps to a fixed partition with a recorded broker owner. Publishers and consumers agree on that owner. The managed controller creates partition queues; the Python managed adapters buffer rejected messages and pre-bind destination consumers. Generic routing integrations retain responsibility for their own queue conventions.

```python
from solace_autoscale_client import KeyRouter, Resolver

router = KeyRouter(
    Resolver("https://assign.example.com", api_key=assignment_api_key),
    shard="orders", client_id="publisher-1", partitions=128,
    mode="guaranteed", protocol="smf",
)
location = router.resolve_key("order-123")
# Use location.broker_id to reuse a connection from your application's pool.
# Publish to the topic for location.partition_id on that broker.
```

The key router performs local hashing and caches locations for a lease, avoiding a lookup for every message. It is included under `scaling-controller`; see [client setup](guide/native-messaging.md).

## Scale automatically through a burst

The controller measures each managed partition, finds a move that relieves the hot broker and fits on the destination, then performs the handover. Hashing chooses initial owners; measured load chooses migrations.

**Prepare destination → bind consumer → fence old writes → drain and wait → change owner → retry buffered messages.**

A grace period never bypasses outstanding messages or acknowledgments. The old broker rejects stale publishers. The durable outbox retains rejected/uncertain messages; consumers must atomically deduplicate event IDs with their business changes. Other partitions continue during the handover.

```bash
# First edit the examples, compile a matching real profile, and set credential environment variables.
pip install -e './scaling-controller[compile,service]'
pip install -e 'scaling-controller[smf]'
solace-autoscale serve --config examples/measured/payments-automatic.yaml
# In a second process:
solace-autoscale run --config examples/measured/payments-automatic.yaml \
  --inventory examples/measured/payments-fleet.yaml
```

Keep managed consumers running so they can bind new destinations. Enable `provisioning.enabled` to replenish warm services automatically, with an explicit region, exact version, token and spending ceiling. The example leaves Cloud creation disabled until those are configured.

[Automatic scaling: walkthrough, YAML and recovery](guide/automatic-scaling.md) · [Feature-aware placement boundary](guide/feature-aware-placement.md) · [Validation results](guide/automatic-scaling-validation.md) · [Full config](examples/measured/payments-automatic.yaml) · [Publisher/consumer example](examples/payments/worker.py)

## Choose the workload conditions

```yaml
capacity:
  model: models/my-profile.json
  scenario: worst             # worst | streaming | unspooling | replay | tracing
  fanout: 5                   # Design floor; observed higher fanout takes precedence.
workload:
  delivery: guaranteed        # direct | guaranteed | mixed
  bottleneck: auto             # auto | messages | bytes | spool | connections
fleet:
  service_class: enterprise-1k
  min_brokers: 1              # Per-shard bounds
  max_brokers: 8
actuation:
  mode: recommend
  dry_run: true
```

Replay and tracing were benchmarked separately; their combined cost is not measured. Intermediate size/fanout estimates are labeled. Workloads outside measured coverage produce an explicit refusal, not invented throughput.

## Commands

| Command | Purpose |
|---|---|
| `profiles import/list/inspect/compile` | Build and inspect a local measured profile catalog. |
| `plan` | Size a hypothetical workload without a live broker; show source cells and limits. |
| `recommend`, `whatif` | Evaluate a metrics window and growth scenarios. |
| `monitor`, `monitor-fleet` | Observe one broker or a complete inventoried fleet over SEMP. |
| `serve` | Run assignment and managed partition discovery using the YAML policy. |
| `run` | Run automatic managed queue handover and optional Cloud warm-pool replenishment. |
| `simulate` | Exercise the legacy model/decision matrix. Measured profiles have separate coverage tests. |
| `placement-compare` | Compare the constrained next-move planner with a bounded exact synthetic oracle. |
| `adopt-feature-contract` | Explicitly attest feature and placement constraints for pre-contract durable state. |
| `accuracy` | Report recorded calibration evidence; normal traffic alone does not establish saturation. |
| `shard-advise` | Suggest traffic groups from an Event Portal export. |
| `compile` | Legacy fanout-one compiler. Prefer `profiles compile` for the supported Cloud workbooks. |

## Operational boundaries

- SEMP and static collection are implemented. Prometheus and Cloud-API collectors remain placeholders.
- Assignment transactions support threads/processes on **one host** sharing local SQLite. Multi-host HA needs a shared durable backend.
- Assignment authentication uses `SOLACE_ASSIGNMENT_API_KEY`; non-loopback startup requires it. Use TLS ingress. The key is service-wide, not tenant-level authorization.
- Guaranteed placements do not silently change broker or delivery mode. Routing/partition-count changes are rejected against an existing store.
- Automatic movement supports dedicated, homogeneous fleets with managed guaranteed SMF queues. Arbitrary existing queues, multi-protocol migration, automatic scale-in/deletion and backlog copying are not implemented.
- A single hot key still needs a finer ordering key or larger tier. Slow consumers can delay a drain; a timeout never discards payments.
- Protocol adapters do not prove measured throughput for every protocol. Validate the actual workload and message-ordering requirements before production use.

## Documentation and development

[Automatic scaling](guide/automatic-scaling.md) · [Routing](guide/routing.md) · [Profiles](guide/measured-profiles.md) · [Architecture](guide/architecture.md) · [Metrics](guide/metrics.md) · [Safety](guide/safety.md) · [Configuration](guide/configuration.md) · [Client integration](guide/client-integration.md) · [Design decisions](guide/adr/)

```bash
cd scaling-controller
pip install -e '.[dev]'
pytest -q
ruff check solace_autoscale tests solace_autoscale_client
mypy solace_autoscale
```

Live-broker tests use separate protocol clients and a local broker: install `.[integration]` and run `pytest -m integration`. Tests for private measured workbooks run locally; customer performance data is not required by CI.
