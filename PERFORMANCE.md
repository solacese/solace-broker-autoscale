# Performance and capacity

The controller needs a measured capacity model for the exact broker environment it operates. The checked-in `models/synthetic-v0.json` is only a schema/test fixture and the reconciliation runtime rejects it.

## Calibrate the deployment

Measure sustained throughput for each provider, broker version, HA service tier, payload size, fanout, and guaranteed-message scenario you intend to run. Find a stable plateau: offered traffic alone is not a capacity result. During a run, require flat guaranteed-message backlog, zero unexpected discards, bounded latency, and enough duration/repetitions to expose throttling and storage effects.

Import workbooks locally, then compile them with the deployed VPN limits:

```bash
.venv/bin/solace-autoscale profiles import \
  --directory /private/path/to/workbooks \
  --out /private/path/to/catalog

.venv/bin/solace-autoscale profiles compile \
  --catalog /private/path/to/catalog/PROFILE.json \
  --service-classes /private/path/to/ha-limits.json \
  --limits-source "deployed VPN limits checked YYYY-MM-DD" \
  --out models/customer-profile.json
```

The limits file supplies, per tier, `service_class_id`, `connections_max`, and `spool_bytes_max`. For example:

```json
{
  "enterprise-1k": {
    "service_class_id": "ENTERPRISE_1K_HIGHAVAILABILITY",
    "connections_max": 1000,
    "spool_bytes_max": 100000000000
  }
}
```

Replace both numeric limits with the deployed HA VPN values; the numbers above are placeholders, not product defaults. The key must match a tier in the imported catalog, and `service_class_id` must be that tier's `*_HIGHAVAILABILITY` ID. Keep source workbooks, catalogs, limits, and customer models private. The compiler preserves source hashes/cells and refuses missing or malformed measurements. Runtime lookup does not extrapolate outside measured size/fanout coverage; between points it uses a conservative bracketing envelope.

## Declare what was measured

`capacity.scenario` distinguishes `streaming`, `unspooling`, `replay`, `tracing`, and the conservative `worst` envelope. Replay and tracing measured separately do not prove their combined behavior. Set `capacity.fanout` to the design floor; observed extra copies still add pressure.

Inventory and configuration must match the model's provider, broker version, tier, and HA mode. Declare placement facts rather than inferring them:

- broker `capabilities` and `failure_domains` in inventory;
- per-shard required capabilities/domain constraints in inventory;
- `automation.shards.<name>.features` for local/XA transactions, disaster recovery, replay, and tracing.

Advanced features are pinned when their migration safety is not established. A broker feature working by itself is not evidence that controller-managed handover preserves its state.

## Managed-client limits

The managed publisher accepts a message only after a local durable outbox commit, then removes it after broker confirmation. Subscribers acknowledge only after the handler succeeds. Delivery is at least once, so the handler must atomically store the event ID with its business effect.

A bounded 2026-09-23 macOS/arm64 bbolt profile with 4 KiB records measured roughly 118–122 durable enqueues/s and 61–68 complete enqueue-plus-confirmed-delete cycles/s. Disabling sync was much faster but destroys the durability guarantee and is not supported. These are storage-specific diagnostics, not broker capacity or an SLA. Re-measure the complete publisher, broker, fanout, subscriber, and business-datastore path on target hardware.

Before production, test publisher/subscriber/controller crashes in every migration phase, disk-full behavior, ACK uncertainty, poison messages, broker failover, state backup/restore, long soak, and saturation. This alpha has no multi-host controller HA, automatic broker deletion, or disk-loss recovery.
