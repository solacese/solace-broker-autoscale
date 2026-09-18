# Configuration

For managed native messaging, start with the [short application policy](simple-policy.md). The reference below remains available for operator tuning.

For the implemented unattended managed-queue workflow, use [Automatic scaling](automatic-scaling.md), including its complete YAML and recovery behavior.

Loaded from YAML, validated with Pydantic. **Unknown keys are rejected** (fail loudly). Durations
accept `30s`, `3m`, `45m`, `1h`, or a bare number (seconds). See `config.example.yaml`.

## Behavioural rules tied to config

- `billing.model: committed` **suppresses all scale-down** recommendations and warns that a warm
  pool is billed idle capacity with no offsetting saving. The report says this plainly.
- `topology.mode: mesh` (or `hybrid`) activates cross-broker amplification in the demand calculation
  (§5.3): inter-broker link byte rates are added to the bytes axis. Retained spool copies must
  already be included in observed `spool_used`; rates are never added to stored byte counts.
- `actuation.mode: recommend` means the actuator is **never constructed**. `scale-up-only` and
  `full` construct it; `dry_run` still gates actual calls.
- `actuation.require_confirmation: true` (the default) is **enforced by the safety gate**: a real
  (non-dry-run) operation is refused - and audited as `refused` - unless the caller confirmed it by
  setting `Operation.approved=True`. The gate never prompts, so it stays deterministic and testable;
  the caller (e.g. a CLI) collects the operator's confirmation and passes the result in. `dry_run`
  operations issue nothing and are exempt. Set it `false` to allow unattended actuation.
- `workload.bottleneck: auto` computes all four axes and reports the binding one. Setting it to a
  specific axis forces that axis as binding.
- `policy.headroom.mode: derived` computes safe thresholds (§5.7) and treats the configured per-axis
  values as **ceilings**: a derived value may be more conservative, never less. `fixed` uses the
  configured values as-is.
- `policy.scale_up_window: auto` computes the window as `max(5 × scrape_interval,
  minutes_to_capacity)` (§5.8). A configured window below `3 × scrape_interval` is rejected.

## Every setting

### `fleet`
| Key | Default | Meaning |
|---|---|---|
| `provider` | `solace-cloud` | Only value supported (ADR 0001). |
| `service_class` | `enterprise-10k` | Key into the capacity model. |
| `min_brokers` / `max_brokers` | 1 / 8 | Clamp on the recommendation. |

### `topology`
`mode` (`sharded`\|`mesh`\|`hybrid`), `shard_key`, `shards[]` (`name`, `match`).

### `workload`
`delivery` (`direct`\|`guaranteed`\|`mixed`), `bottleneck` (`auto`\|axis).

### `protocols`
Per protocol `{enabled, port?}`. These are integration metadata. The decision engine currently
uses an aggregate connection count; it does not enforce separate protocol limits.

### `metrics`
`source` (`prometheus`\|`cloud-api`\|`semp`\|`static`), `scrape_interval`, `staleness_limit` (refuse
to decide on older data), `endpoint`, `static_path`.

### `policy`
`headroom.{mode,messages,bytes,spool,connections,safety_factor}`, `scale_down_at`,
`scale_up_window`, `scale_down_window`, `cooldown`, `warm_pool`.

### `billing`
`model` (`committed`\|`elastic`).

### `actuation`
`mode`, `dry_run`, `require_confirmation` (enforced - see the behavioural rule above),
`max_ops_in_flight`, `max_ops_per_hour`, `kill_switch_file`. Defaults are maximally safe.

### `capacity`
`model` - path to the compiled JSON.

### `accuracy`
`record`, `store` (SQLite path; never committed).

## Implementation scope

`recommend` and `monitor` report advice. `monitor-fleet` reads a fleet over SEMP. The opt-in
`run` controller performs managed queue migration and optional Cloud warm-pool provisioning.
Prometheus and Cloud API metrics collectors, DNS updates and Terraform execution remain outside
that path. Static windows must contain fleet totals.
Timing values must be finite and positive (cooldown may be zero).
See [the simple guide](autoscaler-simple-guide.md) for the production work still required.

## Measured profile selection

`capacity.scenario`: `worst` (default), `streaming`, `unspooling`, `replay`, `tracing`.
`capacity.fanout`: design fanout floor, at least 1; higher observed fanout wins.
`capacity.message_size_hint`: optional positive payload size used for an idle schema-2 workload.
Provider/version are selected by the model file and checked against the declared fleet inventory.
See [measured profiles](measured-profiles.md).

## Assignment routing (implemented)

The `assignment` block is consumed by `serve`. `store` defaults to `./assignment.db`;
`routing` is `client` (default) or `partitioned`; `strategy` is `least-placements` (default) or
`rendezvous`; `partitions` defaults to 128; `lease_seconds` defaults to 300;
`broker_weights` maps broker IDs to positive relative capacities. Partitioned routing requires
an application business key or explicit partition number. Guaranteed ownership survives lease
expiry; changing the stored routing/partition-count contract requires migration.
See [routing choices and examples](routing.md) and [the YAML example](../examples/measured/routing.yaml).

## Native topic messaging

Set `messaging.enabled: true` and configure `routes` with `pattern`, `shard` and zero-based
`key_levels`. The shim extracts the business key; apps call publish/subscribe. See
[native messaging](native-messaging.md) for all options, durable group semantics and migration requirements.

Native routes also accept `dispatch: by-key | by-topic | single`. Only `by-key` uses `key_levels`.
`messaging.subscriptions` declares durable `{group, topics}` definitions;
`messaging.allow_dynamic_groups: false` restricts registration to YAML-declared groups.
`automation.shards.<name>` overrides `enabled`, `trigger_utilization`, `target_utilization`,
`scale_up_window` and `cooldown` for an inventoried shard. Omitted values inherit globals.
These settings take effect on process restart; routing contracts cannot change silently.
