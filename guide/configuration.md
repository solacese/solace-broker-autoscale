# Configuration

Loaded from YAML, validated with Pydantic. **Unknown keys are rejected** (fail loudly). Durations
accept `30s`, `3m`, `45m`, `1h`, or a bare number (seconds). See `config.example.yaml`.

## Behavioural rules tied to config

- `billing.model: committed` **suppresses all scale-down** recommendations and warns that a warm
  pool is billed idle capacity with no offsetting saving. The report says this plainly.
- `topology.mode: mesh` (or `hybrid`) activates cross-broker amplification in the demand calculation
  (§5.3): inter-broker link bytes are added to the bytes axis, and for guaranteed delivery to spool
  pressure too. This makes mesh look bad for large payloads - deliberately.
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
| `service_class` | `enterprise-10k` | Friendly label **or** a raw Mission Control `ServiceClassId` (e.g. `ENTERPRISE_10K_HIGHAVAILABILITY`). Validated at load; a typo fails loudly. Also the key into the capacity model. See [ADR 0010](adr/0010-mission-control-identifier-alignment.md). |
| `min_brokers` / `max_brokers` | 1 / 8 | Clamp on the recommendation. |

Friendly labels: `developer`, `enterprise-{250,1k,5k,10k,50k,100k,200k}`, and a `-ha` suffix for the
high-availability variant (e.g. `enterprise-10k-ha`). Each resolves to a canonical `ServiceClassId`.

### `topology`
`mode` (`sharded`\|`mesh`\|`hybrid`), `shard_key`, `shards[]` (`name`, `match`).

### `workload`
`delivery` (`direct`\|`guaranteed`\|`mixed`), `bottleneck` (`auto`\|axis).

### `protocols`
Per protocol `{enabled, port?}`. Ports are read from broker config, never hardcoded. The decision
engine reads this because connection limits are counted per protocol.

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

### `cloud`
Solace Cloud (Mission Control) connection. `region` (`us`\|`au`\|`eu`\|`sg`\|`us-static-ip`) picks the
control-plane base URL; `base_url` overrides it explicitly when set; `datacenter_id` is the
`datacenterId` required by createService; `idempotency_header` (default `Idempotency-Key`);
`timeout`. **The API token is never in config** — it is read from the environment/secret store, so it
can't leak into the hashed config or an audit record. See
[ADR 0010](adr/0010-mission-control-identifier-alignment.md).

### `capacity`
`model` - path to the compiled JSON.

### `accuracy`
`record`, `store` (SQLite path; never committed).
