# Use measured Cloud performance profiles

The profile workflow preserves the test conditions that change broker capacity: cloud provider, broker version, infrastructure generation, service tier, message size, fanout and guaranteed-message scenario. It retains both ingress and egress measurements and their original sheet/cell addresses.

## Import and inspect

```bash
pip install -e './scaling-controller[compile,service]'
solace-autoscale profiles import --directory /path/to/your/workbooks
solace-autoscale profiles list
solace-autoscale profiles inspect resources/performance/catalog/PROFILE.json
```

Four supported Cloud workbook layouts cover AWS, Azure and GCP, including separate AWS generations. Older software-broker lab workbooks are indexed as reference-only: different hardware and protocols cannot silently become Cloud capacity. The source PDF explains the streaming, unspooling, replay and tracing test setups.

Real catalogs go under ignored `resources/`; real compiled models go under ignored `models/`. Tests use invented fixture values. No Excel formulas, macros or external links are executed. Missing cached formula results, missing direction values and duplicate measurements fail validation.

## Compile one generation

```bash
solace-autoscale profiles compile \
  --catalog resources/performance/catalog/PROFILE.json \
  --service-classes examples/measured/ha-planning-limits.json \
  --limits-source "Planning assumptions; replace with deployed HA VPN limits" \
  --out models/my-profile.json
```

The supplied Cloud benchmarks describe **HA** brokers. Standalone service-class IDs are rejected. Physical disk size in a spreadsheet is not the VPN spool limit. Supply actual connection and spool ceilings in bytes from your deployment when making operational decisions.

The example limits file uses decimal-GB planning defaults from [Solace's message-spool documentation](https://docs.solace.com/Cloud/Configure-Message-Spools.htm), checked on 2026-09-17. It is an example, not a discovery of your deployed limits. Older services and expanded spools can differ. The model records the limits source and a hash; changing limits changes the model version.

## Plan a workload without a live broker

```bash
solace-autoscale plan --model models/my-profile.json \
  --service-class enterprise-1k --delivery guaranteed \
  --scenario streaming --message-size 1024 --fanout 5 \
  --messages 10000 --connections 100 --spool-gib 5 \
  --utilization 0.75 --max-brokers 8
```

The JSON answer includes the required broker count, capacity on each direction, a ceiling warning, and source cells. Counts are active HA services, not primary-plus-standby nodes. This is a capacity estimate for an evenly partitionable workload; consult [routing](routing.md) before assuming that another broker can accept existing traffic.

## Runtime YAML options

```yaml
capacity:
  model: models/my-profile.json
  scenario: worst       # worst | streaming | unspooling | replay | tracing
  fanout: 5             # Design floor; observed higher fanout takes precedence.
```

| Scenario | Workload represented |
|---|---|
| `streaming` | Guaranteed messages delivered to consumers; replay and tracing disabled in the benchmark. |
| `unspooling` | Streaming alongside a pre-spooled slow queue being read from disk. |
| `replay` | Streaming/unspooling with replay enabled. |
| `tracing` | Streaming/unspooling with tracing enabled. |
| `worst` | Conservative envelope of all four guaranteed scenarios, where all cover the requested workload. |

`workload.delivery: direct` uses direct measurements. `mixed` uses the conservative envelope of direct and the selected guaranteed scenario. Replay and tracing **together** were not measured; selecting `worst` does not establish their combined performance. A measured replay or tracing profile is capacity evidence only: automatic movement remains pinned until the corresponding broker-local state and handover behavior are qualified. See [feature-aware placement](feature-aware-placement.md).

At an exact size/fanout, the measured directions are retained. Between points, the runtime uses the lower message and byte ceilings of bracketing observations and labels the result as estimated. Outside measured coverage, it refuses a recommendation. An absent fanout row is not zero capacity and is not guessed.

Incoming and outgoing byte limits are checked independently. A fanout-one throughput measurement does not imply that adding incoming and outgoing bytes should halve the benchmark's measured message rate.

The profiles measure SMF/CCSMP over TLS in a particular HA test environment. Protocol adapters supporting AMQP/MQTT/REST do not turn these into measured performance profiles for those protocols. Validate actual customer workloads, payload distributions, fanout, security settings and latency objectives.

## Observe the entire fleet

```bash
solace-autoscale monitor-fleet --config examples/measured/aws.yaml \
  --inventory examples/measured/fleet.yaml
```

Replace example hosts and set the named SEMP credential environment variables first. The inventory declares one active service endpoint per HA group and must match the model's provider/version/tier. Credentials never go into the inventory/report. All endpoints must report successfully for a snapshot to be used. Per-broker measurements are included so an overloaded broker is visible alongside fleet totals.

The monitor is read-only. It accumulates real history; `--once` is a connectivity/snapshot check and does not fabricate a sustained scale-up window. Missing samples or a restart require the history window to fill again. Min/max counts apply per shard.

## What validation proves

Unit tests exercise malformed imports, exact source values, direction limits, fanout, scenario selection, interpolation, unsupported workload refusal and model identity. Local validation checks supplied workbook cells against compiled models. These prove data handling and calculation behavior. They do not constitute a sustained-load benchmark of your deployed fleet or a production SLA.

Normal offered traffic is not an observed saturation ceiling. Accuracy calibration now requires independent saturation confirmation; routine monitoring cannot certify its own predictions by assuming that a busy broker is full.
