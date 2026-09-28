# Claude Code handoff — Solace Workload Balancer

Use this file as the actionable prompt for a fresh Claude Code session. Continue from the current working tree; do not restart architecture design or discard WIP.

## 1. Guardrails and product intent

Build a lean Go workload-balancing layer for Solace PubSub+ that routes related application messages directly to one of three data brokers while a separate logical Broker 0 carries authoritative control traffic.

User preferences and constraints:

- Production core stays Go: controller, publisher/subscriber shims, customer library.
- Customer code owns both business hashing and rendezvous score hashing.
- Use generic `events-a`, `events-b`, `entity_id`; no airline/flight/baggage terminology.
- SDKPerf is optional **TEST ENV ONLY**, not the long-term/production publisher or subscriber.
- Keep explanations and code practical, bounded, reversible, and honest.
- Never expose passwords, tokens, keys, credential files, or real credential-bearing commands.
- Keep the existing AWS host and all three Solace brokers running; do not delete or replace infrastructure.
- Keep the public graph minimal: Publisher shim → Broker A/B/C → Subscriber shim.
- Before live deployment, pause and drain safely; never purge queues/outboxes or blindly retry `ack_uncertain` records.
- User authorizes a normal non-force push to `origin/main` only after all WIP is complete, tested, safely deployed, and verified. Never push unfinished WIP.

No claims of production readiness, exactly-once, global ordering, autonomous Cloud provisioning, ML-based scaling, or completed live handover qualification.

## 2. Repository and exact status

| Item | Value |
|---|---|
| Local repo | `/Users/raphaelcaillon/Documents/github/solace-workload-balancer` |
| GitHub remote (name differs) | `https://github.com/solacese/solace-broker-autoscale.git` |
| Branch used for implementation | `feat/customer-ready-implementation` |
| Implementation baseline before the README/recovery design update | `25b0db0e3db0ac49e6cc722c3a9d5aaae6e552d4` |
| Baseline title | `Finish live workload dashboard and SDKPerf controls` |
| Previous | `7a5499d871dfee0c72058737ebd9abbce4957be4` — shutdown drain and SDKPerf guidance |
| Last verified baseline CI | success, run `36445372656`, SHA `25b0db0`, https://github.com/solacese/solace-broker-autoscale/actions/runs/36445372656 |
| Date observed | 2026-09-28 |

The implementation/dashboard WIP described later in this handoff was completed, committed, live-tested, and pushed before the current README/recovery design update began. Do not expect the old uncommitted file list. Use `git status`, `git log`, and GitHub Actions for current state; the publication target remains `origin/main`.

Optional historical transcript (handoff is self-contained):

```text
/Users/raphaelcaillon/.claude/projects/-Users-raphaelcaillon-Documents-github-solace-workload-balancer/5ed9c820-2187-4717-a8ca-c0f8d7e3bf87.jsonl
```

## 3. Exact current architecture

### 3.1 Components and protocols

| Layer | Code / behavior |
|---|---|
| Controller | `cmd/controller`, `controller`, `runtime/production.go`; persisted versioned membership is authoritative |
| Publisher | `cmd/publisher`, `shim/publisher`; durable bbolt outbox, async AMQP dispatch |
| Subscriber | `cmd/subscriber`, `shim/subscriber`; validates routing metadata, serializes local key lanes, ACKs after app success |
| Customer boundary | `customer/customer.go`; group, business hash, score hash, algorithm ID |
| Broker 0 | `broker0/orchestrator.go`, `broker0/snapshot_rpc.go`; control request/reply, updates, commands |
| Control contract | `control/snapshot.go`, `control/bootstrap.go`, `control/reconciler.go` |
| AMQP | `integration/amqp.go`, `integration/amqp_pool.go`, `runtime/amqp.go`; AMQP 1.0 TLS for control and data |
| SEMP | `semp`, `runtime/resources.go`, `runtime/drain_monitor.go`; exact resources, fencing, current telemetry |
| Durable state | `outbox`, controller JSON state; bounded and fail-closed |

Three physical data brokers A/B/C are active. Logical Broker 0 is cohosted on physical Broker A using separate namespaced resources. Broker A is still an eligible data broker: payloads go directly from publisher shim to selected A/B/C and never through the controller.

### 3.2 Authoritative bootstrap/reconnect

No HTTP discovery, LVQ, active browsing, or controller call per business message. Participants:

1. attach durable update and participant-scoped reply receivers;
2. request a complete snapshot over Broker 0 with unique correlation;
3. validate namespace/group/role/participant, correlation, revision/epoch, algorithm/library, broker descriptors/resources;
4. reconcile updates buffered during request/reply;
5. remain fail-closed until valid authority is installed.

Key APIs: `broker0.SnapshotRequester.RequestSnapshot`, `broker0.SnapshotRequestServer`, `broker0.Orchestrator`, `control.Reconciler`, `shim/publisher.(*Publisher).ApplyMembership`, `shim/subscriber.(*Shim).ApplySnapshot`.

Revision/epoch regression and conflicting equal revisions are rejected. Late stale replies are accepted/discarded from exclusive reply queues rather than released forever. Reconnect revokes authority until a fresh snapshot succeeds. Membership is then locally cached.

### 3.3 Customer hashing and placement

Actual interfaces:

```go
type CustomerLibrary interface {
    GetScalingGroup(MessageView) (string, error)
    GetBusinessHash(MessageView) (BusinessHash, error)
}
type RoutingHashPolicy interface {
    GetRendezvousScore(ScoreInput) (RoutingScore, error)
    RoutingAlgorithm() string
}
```

`MessageView` is borrowed/read-only (topic, headers, payload, event ID); mutable borrowed data must not be retained/mutated. `BusinessHash` and `RoutingScore` are opaque 32-byte wire containers, not a semantic requirement that every alternative be SHA-256.

Default `customer.EntityCustomerLibrary` supports Events A/B, hashes group + `entity_id`, preserves byte-exact canonical SHA-256 vectors, and identifies `swlb-rendezvous-v1`. `routing.RendezvousBroker` is hash-neutral: ask policy for each stable broker-ID score, choose highest unsigned 32-byte score, tie-break lexicographically smaller broker ID. Default score input is length-prefixed algorithm ID, group, raw business hash, stable broker ID. Endpoint, membership order, and epoch are excluded.

Controller treats algorithm IDs as validated opaque identifiers; publisher/subscriber handshakes must match. Alternatives are valid only with a new coordinated contract/migration after durable records resolve. Never hot-swap meaning under one ID.

### 3.4 Queues, durability, ordering

- AMQP v1 uses native Solace durable **exclusive** queues, one serialized consumer per broker/group/consumer-set/epoch binding.
- These are neither Kafka partitions nor Solace native partitioned queues; partitioned configuration is rejected.
- Namespaced managed resources are adopted only if exact configuration matches; never adopt/delete arbitrary incompatible queues.
- Broker 0 durable control resources are pre-provisioned/operator-owned.
- Production controller manages exact SEMP resources/policy but explicitly does not act as a qualified autonomous Cloud-service provisioner.

Publisher `Accept`/`AcceptBatch` means durable local outbox receipt, **not broker ACK**. Records persist event/payload, group/hash/contracts, routing algorithm, exact broker ID/endpoint/epoch/destination, attempts/state. Positive AMQP settlement deletes; definitive rejection stays retryable; transport ambiguity becomes `ack_uncertain`. Such a record blocks its local ordering lane and may be retried explicitly via `Publisher.RetryAckUncertain` only if exact route still matches; retry may duplicate. This is at-least-once, not exactly-once.

Subscriber `Shim.Handle` recomputes group/hash, validates metadata, serializes local group+hash lane, calls app, then ACKs. Failure retains/blocks the lane. Same-key affinity and local lane serialization do not create global order across keys, publishers, failover, or restarts. Bounded pools/caches/outboxes/dial reservations/sender links are implemented; active leases prevent idle eviction.

### 3.5 Membership transition

Implemented sequence:

```text
PREPARE -> PAUSE -> FENCE -> DRAIN -> COMMIT -> ACTIVATE
```

Prepare target consumers/resources and readiness; pause/quiesce publishers; fence source ingress (including retained brokers where required); require continuously known zero queued/unacked/in-progress; durably commit; activate new epoch. Missing telemetry is unknown, never zero. Pre-commit recovery may roll back; post-commit moves forward. Stale-client fencing, subscriber readiness, drains, and uncertain publications must reconcile.

Unit/broker-free coverage exists, but the live fleet has **not** completed full fault-injected handover qualification. DMR is separate broker-domain routing and does not replace this application placement/fencing/drain contract.

Capacity profiles/SEMP management exist but only support honest configured/dedicated-fleet decisions. Unrelated broker traffic distorts fleet-wide telemetry; no ML or arbitrary fleet claims.

## 4. Live infrastructure (no secrets)

| Resource | Value |
|---|---|
| Public UI | https://workload.sol-se-emea.com |
| AWS | Lightsail `swlb-live-20260928-121042`, static IP `3.93.230.1`, `us-east-1a` |
| Bundle/cost | `medium_3_0`, 2 vCPU/4 GB/80 GB, observed USD 24/month base excluding Solace |
| Runtime | Ubuntu 24.04, `swlb.service`, Caddy HTTPS |
| Namespace | `swlb-neutral-20260928` |
| Normal load | 100 msg/s total across Events A/B |
| Broker A | `da4bhp70ft3`, `swlb-20260928-121042-a-control-data-1k` |
| Broker B | `t95ezd27d6e`, `swlb-20260928-121042-b-data-1k` |
| Broker C | `avfk5i15jtm`, `swlb-20260928-121042-c-data-1k` |
| Broker tier | all Enterprise 1K Standalone; existing EKS US East environment |

Keep all running. Do not touch unrelated services/world domain. Deletion is not authorized.

Private ignored operations only:

```text
.local/live-neutral-20260928/OPERATIONS.md
.local/live-neutral-20260928/
.local/live-20260928-121042/
.local/live-20260928-121042/lightsail-default.pem
```

Use the operations guide for SSH/config/journals. Never print private files. No AWS credentials belong on VM. Temporary provisioning tokens were removed; long-lived runtime secrets remain private/ignored.

### Last read-only public snapshot

At `2026-09-28T14:27:26Z`: target 100/s; accepted 136,539; unique delivered 136,539; unresolved 1 ordinary in-flight; current backlog 1; duplicates 0; ordering diagnostic 1; all five supervised processes running with zero dashboard-process restarts; both groups ACTIVE epoch 1 on A+B+C; persisted SDKPerf proof 100/100/100.

This HTTP snapshot supports current demo behavior only; it does **not independently prove which commit/binary is deployed**. The completed dashboard/SDKPerf assets were later committed in `25b0db0`, deployed, and live-tested as recorded in section 8.

The one ordering diagnostic’s cause is **unconfirmed**. Restart/metric namespace interaction is a hypothesis, not established evidence. Keep it visible and investigate; do not reset counters to explain it away. SDKPerf-versus-synthetic metric namespaces remain a task.

## 5. SDKPerf test-only architecture/status

Installed official tool: SDKPerf Java/JCSMP 10.30.2. It is SMF/JCSMP, not AMQP.

Managed path:

```text
SDKPerf producer (SMF)
 -> dedicated Broker A ingress queue/topic
 -> optional Go sdkperf-adapter
 -> EXISTING supervised Go publisher shim
 -> AMQP selected A/B/C
 -> EXISTING supervised Go subscriber shim
 -> adapter -> durable Broker A result queue
 -> SDKPerf consumer (SMF)
```

Resources: `swlb-neutral-20260928.sdkperf.ingress`, topic `swlb-neutral-20260928/sdkperf/ingress`, `swlb-neutral-20260928.sdkperf.results`, topic prefix `swlb-neutral-20260928/sdkperf/results/>`.

Choices/evidence:

- Reuse existing shims; never start duplicate identities/outboxes.
- SDKPerf/Java/adapter/test queues are absent from normal Go startup.
- Adapter preserves payload bytes; generated IDs/sequence are test-only, not restart-stable order/exactly-once proof.
- Pass requires real SDKPerf producer exit + reported transmitted count, native published/delivered counts, and real SDKPerf consumer unique run-correlated count.
- Stop/error kills whole child process groups; output/logs are bounded/redacted.
- Managed 100-message proof passed twice; hardened run: producer transmitted 100, native publish/delivery 100/100, consumer unique 100, exit 0, no orphan process, normal 100/s restored.
- Direct SDKPerf examples use a dedicated topic and **bypass shims**; never label them end-to-end.

## 6. Resolved incidents that govern operations

| Incident | Resolution / invariant |
|---|---|
| Wrong backlog | `spooledMsgCount` is lifetime cumulative. Current depth is `GET .../queues/{q}/msgs?count=1` → `meta.count`; track unacked separately; errors stay unknown. |
| Missing endpoint | Durable assignment now persists exact `BrokerEndpoint`, broker, epoch, destination. |
| Duplicate SDKPerf participants | Removed; harness reuses supervised shims. |
| False SDKPerf pass | Adapter sender ACK is insufficient; verify real producer stats and consumer unique count. |
| Blocked Events B lane | Two historical + one diagnostic `ack_uncertain` records blocked 566 ready records. Paused, proved SEMP zero, inspected bbolt stopped, verified/retried exact routes only, purged nothing, drained/re-inspected empty. Added 15s graceful shutdown drain in `7a5499d`; resumed 100/s without growing deficit. |

Never hide problems by resetting metrics/deleting durable state.

## 7. Current task: five-family live operations board + flexible SDKPerf

Latest user direction supersedes the previous dashboard-only framing: make the page visually compelling around five indivisible ordered event families A–E on Brokers 1/2/3 while completing the existing SDKPerf WIP. The selected design uses all five families inside existing scaling group `events-a`; `events-b` remains ACTIVE and intentionally idle during the family demo. Stable keys are selected using the real current customer hash/rendezvous implementation, not fake assignments: A=`family-key-00`→broker-b/Broker 2, B=`family-key-01`→broker-c/Broker 3, C=`family-key-02`→broker-b/Broker 2, D=`family-key-03`→broker-a/Broker 1, E=`family-key-04`→broker-a/Broker 1. This is 2–2–1 families and, at equal 20/s rates under the 100/s default, an honest 40/40/20 traffic split. Do not round-robin messages within a family or claim perfect thirds.

Each family uses one stable `entity_id`, explicit `event_source=family-demo`, `event_family`, and monotonically increasing `family_sequence`. Display the broker from the actual durable receipt; compare it to the documented expected mapping and surface mismatches. Track delivered family-local sequences by run ID, gaps, duplicates, and ordering anomalies; SDKPerf must not affect synthetic family ordering. Keep the graph to Publisher shim → Brokers 1/2/3 (internal IDs remain broker-a/b/c, Broker 1 subtly hosts Broker 0) → Subscriber shim. Use validated family palette A `#1D4ED8`, B `#C2410C`, C `#047857`, D `#7E22CE`, E `#A16207`, direct labels and reduced-motion support.


### Required UI

Explain every current element inline plus compact “How to read this page”:

- target vs recent accepted/delivered msg/s;
- accepted is local durable receipt, not broker ACK;
- current SEMP backlog, not lifetime counter;
- recent dashboard p95 latency limitations;
- bounded observation-window order/duplicate diagnostics, not exactly-once;
- cumulative assignment counts vs current broker rates;
- Broker A hosts Broker 0 and data;
- groups, process names/restarts, membership phase/epoch/brokers;
- SDKPerf test-only bridge/source/sink;
- recent events are sanitized bounded display, not audit log;
- honest healthy/degraded/paused/error/stale state;
- last SDKPerf timestamp/run ID/duration and actual producer/native/consumer counts.

Keep graph only shims + A/B/C.

### Exactly four editable managed templates

| Template | Messages | Rate | Payload |
|---|---:|---:|---:|
| Quick check | 100 | 25/s | 256 B |
| Steady | 1,000 | 50/s | 256 B |
| Burst | 1,000 | 100/s | 256 B |
| Larger messages | 100 | 25/s | 8,192 B |

Authenticated edits: group A/B, count, rate, payload bytes, entity cardinality, timeout. Server caps: messages 1–1000, rate 1–100/s, payload 1–8192, entities 1–1000, timeout 10–300s and at least `ceil(messages/rate)+20`; strict integers (no bool/float/nonfinite/malformed). Inputs must reach actual producer/adapter argv. One execution globally. Pause managed test, preserve prior synthetic rate, restore after success/failure/launch error/stop.

### Restricted SDKPerf terminal

Accept normal syntax, e.g.:

```text
sdkperf_java.sh -mn=100 -mr=25 -msa=256 -l
```

Mode is selected separately; never invent `SDKPerf managed` as native syntax. Managed mode injects fixed private broker/VPN/credentials/destinations and executes configured official SDKPerf via harness, `shell=False`.

Allow bounded: `-mn`, `-mr`, `-msa`, `-mt=persistent`, `-soe`, `-psm`, `-l`, `-ped=0..30`, `-lb=1..4096`, `-lg=0..1000`, `-psv=1..50`. Info: `-v`, `-h`, `-?`, `-hm`, `-he`.

Reject shell operators/substitution/redirection, arbitrary executables/hosts/credentials/destinations/files, `-cip/-cu/-cp/-cpf`, `-ptl/-pql/-stl/-sql/-sdl`, `-pfl/-pal/-mdd`, `-epl/-cpl`, JVM/system/debug/profile/JAAS/truststore/keystore, naming/retry controls. Explain rejection. Direct mode may remain fixed dedicated-topic examples only and must say bypasses shims.

After unlock only: editor, mode, redacted bounded output, run ID, exit status, timestamps, stop/copy/clear/help. Run and output/status endpoints must require cookie auth + same-origin/custom header + body cap. Keep one execution lock, process-group cleanup, output cap, secret redaction; public API never includes terminal output.

Also isolate SDKPerf ordering diagnostics from synthetic ordering. Current code parses every event ID and keys only `(group, entity_id)`; use source namespace/run-aware key or exclude SDKPerf from synthetic order checks. The observed ordering error remains unexplained.

## 8. Completion status (2026-09-28)

The dashboard/SDKPerf work was reviewed, corrected, locally tested, safely deployed after a zero-queue/zero-unacked/zero-outbox drain, and live-tested. Final repository verification passed before commit.

Verified live results:

- five family-local sequences at 20/s each, no current family gaps/order errors/duplicates, with durable-receipt placement A/C→Broker 2, B→Broker 3, D/E→Broker 1;
- quick 100×25/s×256 B, steady 1,000×50/s×256 B, burst 1,000×100/s×256 B, and large 100×25/s×8,192 B managed SDKPerf paths; the final large run independently proved producer/native publish/native delivery/sink `100/100/100/100`;
- managed normal SDKPerf syntax with `-l`, native `-v`, request authentication/header enforcement, injection/limit rejection, rate restoration, early cancellation, and child-process cleanup;
- desktop and 390 px mobile browser QA with no horizontal overflow, animation, or console warnings. Final screenshots are ignored under `.local/final-solace-workload-balancer-{desktop,mobile}.png`.

The UI uses the current public Solace logo asset and brand green on a compact white technical header. This is branding for the demo, not a production-readiness claim.

| File | Completed role |
|---|---|
| `tools/dashboard.html` / `.css` / `.js` | responsive technical UI, explanations, four templates, restricted console, direct examples |
| `tools/solace-logo.svg` | official current public Solace logo asset |
| `tools/sdkperf_control.py` | normal SDKPerf parser, strict bounds, blocked categories, argv/redaction |
| `tools/live_dashboard.py` / `live_state.py` | static assets, public metrics, family evidence, authenticated controls, global SDKPerf lifecycle |
| `tools/sdkperf_harness.py` | bounded parameters/logs, streaming run evidence, cancellation, child-group cleanup |
| `tools/sdkperf-adapter` | test bridge with current-run delivery validation |

Reviewer issues resolved include the shared execution lock, launch/failure/cancel rate restoration, managed-console ownership and state, private-output authentication, configured caps, producer/native/sink evidence separation, streaming evidence independent of bounded presentation logs, cancellation during readiness/version/final evidence phases, process-group cleanup, and SDKPerf/synthetic metric isolation.

## 9. Definition of done

### Local review/tests

Cover: exact templates/defaults; strict bounds/nonfinite/duration; injection and all blocked categories; normal syntax; supported flags reaching real producer argv; `--producer-arg=<flag>`; payload/entity adapter values; auth on run/stop/private status/output; origin/header/body cap; redaction/output cap; global lock; rate restoration on error/cancel; process-group cleanup; public API secrecy; ordering namespace isolation.

Commands:

```bash
cd /Users/raphaelcaillon/Documents/github/solace-workload-balancer
make test-python
make test
make vet
make build
make integration
make ci   # final: race + Python + vet + build + integration
```

Avoid repeating expensive suites after pass unless code changes invalidate them.

### Safe deployment and live acceptance

1. Authenticated rate → 0; wait unresolved 0.
2. Prove each application queue `meta.count==0`, unacked 0.
3. Inspect outboxes consistently (stop only if needed). If uncertain records exist, investigate exact route; never purge/blind retry.
4. Preserve controller/outboxes/queues/credentials; deploy only relevant assets; restart with 15s drain.
5. Run all four templates and verify real producer/native/consumer counts and actual payload/entity settings.
6. Run a custom normal-syntax managed command with a supported extra flag; run `-v` or help and verify redacted bounded output/exit status.
7. Cancel a run; prove child cleanup and prior-rate restoration.
8. Prove unauth/private output 401, injection/limits reject, no secrets in HTML/API/log/evidence/screenshots, no orphan JVM/SDKPerf/adapter/harness.
9. Browser QA desktop/mobile: explanations, four editable templates, console hidden until unlock, commands/copy/help/output, direct-vs-managed clarity, no overflow/console errors, minimal graph.
10. Restore 100 msg/s; observe no growing unresolved, low current depth, A/B/C ACTIVE, five Go processes healthy.

Then update README briefly, scan staged files (exclude `.local`, keys, credentials, downloads, SDKPerf distributions, logs, caches/screenshots/pycache/binaries), fetch origin, preserve remote changes, commit intended work, normal non-force push `origin/main`, watch/fix CI, leave clean tree. Do not push unfinished WIP.

## 10. Code map and longer-term priorities

| Area | Files |
|---|---|
| Customer/routing | `customer/customer.go`, `customer/contract_test.go`, `routing/membership.go` |
| Control/Broker 0 | `control/{snapshot,bootstrap,reconciler}.go`, `broker0/{orchestrator,snapshot_rpc}.go` |
| Controller/SEMP | `controller`, `runtime/{production,resources,drain_monitor}.go`, `semp` |
| Publisher/outbox | `shim/publisher`, `outbox`, `integration/{amqp,amqp_pool}.go` |
| Subscriber | `shim/subscriber`, `runtime/{participant,participant_assembly}.go` |
| Commands | `cmd/controller`, `cmd/publisher`, `cmd/subscriber`, `cmd/internal/participantio` |
| SDKPerf baseline | `tools/live_dashboard.py`, `live_state.py`, `sdkperf_harness.py`, `sdkperf-adapter`, `sdkperf-provision` |
| Completed dashboard/SDKPerf work | `dashboard.{html,css,js}`, `sdkperf_control.py`, control tests, dashboard/harness/Makefile |

After this task only: live fault-injected handover qualification; stale-client/readiness/drain proof; explicit uncertain-record operator workflow; restart/durability stress; multi-publisher ordering characterization; DR/transaction semantics; exact service-class capacity benchmarks and unrelated-traffic sensitivity; bounded durable observability/recovery runbook. No new feature expansion or ML claims before these fundamentals.

Final truth constraints remain: not production-qualified; no full live handover proof; no exactly-once/global order; uncertain retry can duplicate; SDKPerf validates this test environment only; direct SDKPerf bypasses shims; autonomous Cloud provisioning and DMR are not provided by this product.
