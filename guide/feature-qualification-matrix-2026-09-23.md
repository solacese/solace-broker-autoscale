# Feature qualification matrix — 23 September 2026

This matrix separates product support, empirical evidence, and automatic-migration policy. “Tested” means the exact listed environment and client were exercised; it is not a capacity or production certification.

| Feature / behavior | Current placement contract | Evidence in this branch | Status / remaining gate |
|---|---|---|---|
| Guaranteed messaging | Partition owner is durable; source is fenced and drained before atomic owner commit | Local Standard brokers, managed Python SMF and Go AMQP; 2/3/4-broker topology and two-broker migration tests | **Supported and tested locally**; Cloud capacity remains unqualified |
| Direct messaging | Assignment API can resolve direct clients; automatic queue handover is guaranteed-only | Local SMF/AMQP/MQTT/REST protocol integration | **Supported routing, not migration-qualified** |
| Fanout | All subscriber-group queues remain an indivisible partition bundle | Two wildcard groups on 2/3/4 local brokers; 48 IDs/group across client restart, zero duplicate IDs | **Supported and tested locally** |
| Wildcard and overlapping subscriptions | Solace subscriptions fan out to each matching group; telemetry uses copy-rate bounds | `payments/>` plus `payments/*/created` live test; identical and heterogeneous filter attribution regressions | **Supported**; ambiguous filters retain lower/upper bounds, not a fabricated exact publication rate |
| Local transactions | Transaction boundary cannot be inferred from queue depth | [Native qualification](native-feature-evidence.md) committed 5 persistent messages, hid 4 rolled-back publications, redelivered 3 rolled-back consumptions in order, and exposed 0 messages after a client halted with 4 uncommitted publications | **Native broker semantics tested separately; managed migration remains pinned**, including transactions in flight during handover |
| XA | In-doubt branches are outside the queue-drain proof | [Native qualification](native-feature-evidence.md) observed `XA_OK`, committed 3 IDs, hid a rolled-back branch, then recovered 1 matching prepared branch after broker restart and committed its 2 IDs | **Native broker semantics tested separately; managed migration and HA/DR interaction remain pinned** |
| Replay | Ordinary queue replay and application-managed partition queues are distinct from native Solace partitioned queues | [Native qualification](native-feature-evidence.md) replayed 5 persistent IDs to an ordinary exclusive queue, then received 3 newly published live IDs; all 8 arrived with 0 duplicates | **Native ordinary-queue semantics tested separately; managed migration pinned** until replay-active/catch-up state and overlap are qualified |
| Distributed tracing | Migration must preserve trace completeness across cutover | Official OpenTelemetry Collector `0.100.0` reconciled all 12 exact native JMS message IDs across 24 send/receive spans | **Native path tested; migration-specific span completeness remains pinned** |
| HA | One HA service is one active capacity unit; mate is not a scale-out broker | Cloud 5K/10K functional evidence plus a local three-node Standard HA group; after primary stop, 8 persistent IDs were consumed from the active backup in order | **Bounded HA failover tested**; transactions/XA in flight, failback, saturation and managed-migration interaction remain unqualified |
| DR sync/async | DR peers are protection capacity and excluded from placement | No two-site message-VPN replication pair was configured | **NOT TESTED; pinned** pending promotion/failback and feature-interaction trials |
| Slow consumer / backlog | Source-resident backlog blocks ownership commit; no backlog copy | Local slow-consumer workload plus real drain/unacked tests | **Supported locally**; long soak, expiry/DMQ, and capacity limits remain open |
| Ordering / business affinity | Business key maps to one partition; partition is indivisible | 2/3/4-broker live tests preserve per-partition order across publisher/subscriber restart | **Supported and tested locally** |
| Retry / dedup | Transport is at-least-once; application transaction must deduplicate event ID | Real AMQP release/redelivery and durable publisher restart; workload reconciles unique IDs per group | **Retry tested; exactly-once is not claimed** |
| Heterogeneous feature requirements | Deterministic constrained optimizer filters capability/domain/role/pin constraints | Unit/oracle tests and explicit infeasibility reasons | **Planner-tested; feature combinations need matching real services** |
| Bursts | Open-loop multiprocess generator records configured and achieved rate separately | Local Standard: 256 B steady, 4 KiB fanout, 64 KiB burst, slow consumer | **Functional evidence only; no saturation capacity claim** |
| 2 / 3 / 4 brokers | Deterministic owners and one active queue per partition | Real local Standard brokers, 3 preassigned static topologies | **Static routing tested locally**; automatic sequential 1→4 scale-out evidence is still pending and must not be inferred from the static runs |
| Controller / publisher / subscriber restart | Durable state resumes; pre-commit recovers or rolls back, post-commit recovers forward | Real client restart and migration tests; phase-fault unit matrix | **Supported locally**; multi-host controller HA remains unavailable |

## Local measured workload

Environment: one local `solace/solace-pubsub-standard` image digest `sha256:05f80ec7bd38…`, Go race-enabled AMQP client, four application-managed partitions. Raw reports are retained under ignored `state/claude-directed-qualification/concurrent-local-v2/`; sanitized aggregates are committed under [`examples/qualification-evidence/2026-09-23/`](../examples/qualification-evidence/2026-09-23/).

| Case | Payload | Fanout | Configured rate | Achieved injection | Accepted | Deliveries | Loss / duplicates / order violations | End-to-end p99 |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| small steady | 256 B | 1 | 200 msg/s | 200.8 msg/s | 200 | 200 | 0 / 0 / 0 | 4,243 ms |
| medium fanout | 4 KiB | 2 | 500 msg/s | 121.3 msg/s | 200 | 400 | 0 / 0 / 0 | 5,705 ms |
| large burst | 64 KiB | 2 | unpaced | 70.3 msg/s | 100 | 200 | 0 / 0 / 0 | 2,662 ms |
| slow consumer | 1 KiB | 2 | 500 msg/s | 503.7 msg/s | 100 | 200 | 0 / 0 / 0 | 4,035 ms |

The 4 KiB case missed its configured schedule by 1,251 ms. The corrected harness distinguishes stdin offer injection, durable acceptance completion, broker flush and business-handler completion; these below-saturation observations still are not broker capacity. The [router performance review](router-performance-review-2026-09-23.md) isolates synchronous bbolt persistence as the dominant local limit: a durability-preserving 4 KiB enqueue-plus-confirmed-delete cycle sustained only **61–68 messages/s** in the bounded filesystem profile. That environment-specific result is a high-priority throughput gate, not a portable product ceiling.

## Telemetry attribution

Group queue cumulative counters count delivery copies, not necessarily original publications. For identical group filters, copy count divided by group count is exact. For heterogeneous or overlapping filters, the controller now retains a lower bound (`copies / groups`) and upper bound (`copies`). Destination-fit checks use the upper bound; source-relief checks use the lower bound. Live VPN ingress, egress, connection, and spool totals are normalized against the matching measured profile; pressure not accounted for by partition lower bounds remains broker-resident residual load.

This is deliberately conservative: it may decline a beneficial move, but it does not invent precise partition ingress from ambiguous subscription copies.

## Native feature evidence and boundaries

The compact sanitized aggregate is [`native-features.json`](../examples/qualification-evidence/2026-09-23/native-features.json); the [narrative record](native-feature-evidence.md) documents the setup and exact limits. Local transactions, XA including prepared-branch restart recovery, ordinary-queue replay, native distributed tracing and one three-node Standard HA failover all passed their bounded assertions. Early tracing and HA setup failures were harness defects that were corrected and are retained in the aggregate as superseded attempts, not current product blockers. DR remains **NOT TESTED**.

These native tests do not lift the autoscaler's migration pins. The deterministic constrained optimizer respects declared feature, domain, role and pin requirements, but the functional runs do not provide an empirical capacity surface for DR-plus-transaction, XA, replay or tracing interactions. Qualification requires matching broker class/version, client version, measured performance profile and feature combination; the local TLS and Cloud demos establish a baseline, not customer-production equivalence.
