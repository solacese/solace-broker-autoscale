# Automatic scaling for payment bursts

The `run` command continuously measures managed queue traffic, chooses a partition that fits on another broker, and hands it over automatically. It uses spare brokers first. With Cloud provisioning enabled, it also creates replacement spare capacity. No approval prompt interrupts the running loop.

This is an implemented path for **guaranteed messages on managed SMF queues**, with a Python publisher/consumer integration. The controller is a single process with persistent local SQLite state. The two-broker handover has been tested against real local Solace brokers; Cloud creation is covered with mocked API tests, not a production Cloud rollout.

## A payment burst, step by step

Suppose broker A reaches 90% of its measured capacity. Broker B is a warm spare. One partition accounts for 30% of a broker's capacity. These numbers illustrate the decision; they are not performance claims.

1. After the configured observation window, the controller finds that moving this partition leaves A at roughly 60% and B at 30%. It evaluates message rate, bytes and stored-message pressure, rather than counting clients.
2. It records the migration in SQLite and creates the same managed queue on B with incoming messages disabled.
3. The worker discovers B through `/partitions` and binds a consumer there. The controller waits for that consumer.
4. It disables incoming messages on A's queue. The assignment API pauses new publisher resolutions for that partition. A stale publisher also receives a broker rejection, so a cached address cannot bypass the handover.
5. Publishers retain rejected or uncertain messages in their durable outbox. Other partitions continue. The original consumer finishes A's queued and unacknowledged messages.
6. After the grace period **and** continuously observed empty state, the controller atomically changes the owner to B, verifies the source remains drained, and opens B to new messages.
7. Publishers resolve B and retry their buffered messages. The controller measures again before selecting another move. Optional Cloud provisioning replenishes the warm pool.

A grace period is a minimum wait, not permission to abandon messages. This implementation drains the old queue in place; it does not copy its backlog to the new broker. A slow or failed business consumer can therefore delay migration.

## YAML choices

Use [the complete automatic configuration](../examples/measured/payments-automatic.yaml) and [the matching inventory](../examples/measured/payments-fleet.yaml). Paths are relative to the working directory.

```yaml
assignment:
  store: ./assignment.db
  routing: partitioned
  strategy: rendezvous
  partitions: 128
  lease_seconds: 30
  broker_weights: {}
automation:
  enabled: true
  fleet_id: payments
  poll_interval: 10s
  trigger_utilization: 0.80
  target_utilization: 0.65
  migration_grace: 30s
  empty_settle: 10s
  migration_timeout: 15m
  max_parallel_migrations: 1
  max_migrations_per_hour: 12
  min_target_consumers: 1
  queue_spool_mb: 1000
policy:
  scale_up_window: 30s
  cooldown: 15m
  warm_pool: 1
actuation:
  mode: scale-up-only
  dry_run: false
  require_confirmation: false
```

| Setting | What it changes |
|---|---|
| `assignment.strategy` | Initial ownership: `least-placements` balances counts; `rendezvous` gives stable hash winners. Neither uses per-message round robin. |
| `assignment.partitions` | Fixed migration units per shard. More units allow finer balancing, at the cost of queues, consumer flows and telemetry. Never change this against existing state. |
| `broker_weights` | Relative hash weights for initial placement. Automatic moves use measured load instead. The automated controller requires one matching broker tier/generation. |
| `trigger_utilization`, `target_utilization` | Start after sustained overload; choose a move that fits below the target on the destination and improves the worst broker pressure. |
| `migration_grace`, `empty_settle` | Minimum handover wait and continuously empty observation period. Both must pass. |
| `migration_timeout` | Before ownership changes, a stalled handover safely reopens the original queue after verifying the destination is empty. After ownership changes, recovery completes forward. |
| `max_migrations_per_hour`, `cooldown` | Bound movement and prevent repeated movement of the same partition. Only one migration runs at a time. |
| `min_target_consumers` | Require bound destination consumers before ownership changes. For exclusive queues, extra bindings are standby consumers. |
| `warm_pool` | Desired already-created spares per shard. Replenished when `provisioning.enabled` is true and fresh telemetry is available. |
| `fleet.max_brokers` | Per-shard ceiling including active and warm services for Cloud creation. Warm services cost money. |
| `queue_spool_mb` | Quota for each new managed queue. The actual VPN spool limit still caps their combined usage. |

The automatic planner uses `capacity.scenario`, `capacity.fanout`, `capacity.message_size_hint`, and the imported profile. It uses its own trigger/target settings; `policy.headroom` belongs to the recommendation commands. Queue counter deltas give average message size per partition and interval. Use conservative scenarios and verify representative message-size distributions. `message_size_hint` supplies the design size for idle queues with backlog.

Before load scoring, the controller applies the [feature-aware placement boundary](feature-aware-placement.md). Advanced operator profiles can declare transactions, DR replication, replay and tracing under `automation.shards.<name>.features`. Unqualified features return `feature-pinned`; they do not activate a warm service or start a migration. Inventory `role: dr` identifies protection capacity and is never considered active/warm scaling capacity.

## Run the three parts

1. Compile a profile matching the actual provider, broker version, HA service class and deployed VPN limits. The example limits are planning defaults; replace them before actuation.
2. Inventory dedicated active/warm services, their SMF endpoints and SEMP credential environment-variable names. Use one entry per HA service, never a standby node. Configure the application credential on the initial services.
3. Install `pip install -e './scaling-controller[compile,service]'` and `pip install -e 'scaling-controller[smf]'`. Set the SEMP variables, `SOLACE_ASSIGNMENT_API_KEY` and application credentials using your secret manager.
4. Start the assignment service and controller with **the same config and local database**:

```bash
solace-autoscale serve --config examples/measured/payments-automatic.yaml
solace-autoscale run --config examples/measured/payments-automatic.yaml \
  --inventory examples/measured/payments-fleet.yaml
```

5. Run [the managed application example](../examples/payments/worker.py). Start its consumer mode continuously, and integrate its publisher pattern into the payment ingress application. Run the controller, assignment service and workers under a process supervisor that restarts them. Keep their local databases on persistent disks. The fleet namespace, partition contract and broker identities must remain stable across restarts.

Only the controller creates the `autoscale/<fleet-id>/<shard-hash>/pN` queues. Publishers use the returned `queue_name`; existing arbitrary topics/queues are not adopted automatically. Use a dedicated fleet: unrelated traffic and connection pressure are not accounted for by this partition planner. Read-only fleet monitoring covers broader broker metrics.

## Creating more brokers automatically

Set `provisioning.enabled: true`, choose your Cloud datacenter, and pin the exact available broker version including revision. It must match the imported measurements. Set `SOLACE_CLOUD_TOKEN` and `SOLACE_AUTOSCALE_CLIENT_PASSWORD` in the controller environment; do not put secrets in YAML.

```yaml
provisioning:
  enabled: true
  token_env: SOLACE_CLOUD_TOKEN
  api_base_url: https://api.solace.cloud
  datacenter_id: YOUR_DATACENTER_ID
  broker_version: 10.8.1.241-0 # Example format; check availability and actual revision.
  endpoint_index: 0
  client_username: autoscale-app
  client_password_env: SOLACE_AUTOSCALE_CLIENT_PASSWORD
  readiness_timeout: 30m
```

Choose the Cloud API URL for your organization home: `https://api.solace.cloud` (US), `https://api.solacecloud.eu` (EU), `https://api.solacecloud.com.au` (Australia), or `https://api.solacecloud.sg` (Singapore). This is separate from the broker datacenter.

The controller records a unique service name before issuing a request, reconciles that exact name after uncertain results, checks returned service identity, region, version, tier and mate-link encryption, waits for Cloud and SEMP readiness, configures the managed application username, and adds the service as warm capacity. It never saves Cloud/SEMP passwords into the assignment database or returns them to publishers. Choose a reachable private endpoint with `endpoint_index`. The API token needs permission to create and inspect services; SEMP credentials need permission to manage the scoped queues and application username.

Creating a Cloud service takes time. Warm capacity and bounded upstream buffering cover that delay. Reaching a broker limit, lacking a fitting partition, or failing readiness produces an explicit state in the JSON output. Connect those states to your existing monitoring. No email or paging integration is included.

## Failure and recovery

| Event | Automatic behavior |
|---|---|
| Controller restart | Reopen durable state, recover known Cloud services, resume the recorded migration phase. A local process lock prevents a second controller writer. |
| Missing/stale/invalid queue telemetry | Stop planning new moves; never interpret an unknown queue as empty. Reset the empty-settle period after a telemetry gap. |
| Stale publisher address | Old broker rejects writes after the fence. The outbox retains and retries them. |
| Missing destination consumer | Wait without switching ownership; on pre-commit timeout, recover toward the source. |
| Lost publish acknowledgment | Retain and retry; duplicates are possible. |
| Outbox full | Apply upstream backpressure. Never evict accepted payments to make space. |
| Failed consumer business operation | Retry without acknowledging or overtaking that message on its partition. A poison message requires an application policy. |
| Kill-switch file | Halt further controller/provisioning work. Remove it to resume the persisted phase. A fenced partition can remain paused while halted. |
| One indivisible hot partition | Report capacity shortfall. More same-size brokers cannot split its ordering requirement. Use a finer business key or a larger, validated tier. |

**Delivery is at least once.** Persist the event ID and the business effect atomically in the consumer's database. The example ledger demonstrates this using a unique event ID. A local file lock enforces one publisher process per outbox. Local publisher buffers must use separate files per publisher process; their survival depends on persistent storage. Key locality does not create a total order across independently concurrent producers.

This release automates scale-out and partition handover. It does not delete brokers, automatically scale in, copy unprocessed backlogs, provide multi-host controller HA, or prove production throughput for the reference adapters. Test your real Cloud networking, permissions, connection/queue limits, application processing and sustained peak workload before enabling it on customer traffic.

Broker fencing uses [Solace's queue rejection settings](https://docs.solace.com/Messaging/Guaranteed-Msg/Configuring-Queues.htm); a disabled ingress must reject, not acknowledge and discard. Managed SMF publishing uses the documented [reserved queue destination](https://docs.solace.com/Messaging/Reserved-Topics.htm).
