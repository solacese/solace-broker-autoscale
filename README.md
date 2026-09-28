# Solace Workload Balancer

A Go workload-balancing layer that routes related application messages directly to one of several Solace PubSub+ data brokers. A separate logical Solace service, **Broker 0**, carries control traffic between the controller and publisher/subscriber shims; business payloads never pass through the controller.

> **Status:** The AMQP 1.0 data and control paths, durable publisher outbox, asynchronous dispatcher, exclusive-queue subscribers, rendezvous placement, and pause/fence/drain transition state machine are implemented. Local CI and a live three-service plumbing demo have passed, including all four managed SDKPerf presets. The optional live membership-transition harness and fault-injected recovery qualification have **not** run. This is not a production-readiness, exactly-once, uninterrupted-failover, or 100,000-message/second claim.

## What is implemented

```text
application --NDJSON--> Go publisher shim --AMQP 1.0--> selected data broker
                                                            |
application <--NDJSON-- Go subscriber shim <--AMQP 1.0------+

                     controller <--> Broker 0 (AMQP 1.0 control)
                     controller ----> data-broker SEMP
```

- The Go publisher and subscriber use `github.com/Azure/go-amqp` to publish and consume persistent AMQP 1.0 messages. Positive settlement removes a publisher outbox record; rejected/released settlement is retryable; timeout, disconnect, or an unknown disposition becomes `ack_uncertain`.
- The `cmd/publisher` and `cmd/subscriber` application interface is strict newline-delimited JSON over standard input/output. It is **not** an AMQP server, a Solace API implementation, or a drop-in replacement for a Solace SDK.
- The publisher durably records messages in bbolt before dispatch. The production dispatcher concurrently submits independent ordering-lane heads, bounds unresolved native sends, and batches durable in-progress and completion mutations. `AcceptBatch` is available in Go, but the command interface accepts one NDJSON record at a time.
- Each durable data queue is exclusive, with one serialized AMQP consumer for each broker/group/consumer-set/epoch binding. Native Solace partitioned queues are not used and are rejected by v1 configuration validation.
- Connections and sender links are opened lazily and reused through bounded pools. Active leases prevent idle eviction while a native operation is outstanding.
- The controller persists authoritative snapshots and transition checkpoints before publishing control actions. SEMP manages exact queue/topic resources, applies ingress fences, and supplies current drain telemetry.

### Broker 0

Broker 0 is a **logical control service**, not a data-routing hop. It carries durable, identity-scoped snapshot requests/replies, membership updates, commands, acknowledgements, registrations, readiness, and telemetry. Bootstrap uses correlated request/reply with participant-exclusive durable reply queues; it does not use an LVQ and the controller is not called for each business message.

The current demo cohosts Broker 0 resources on data broker A. That is economical plumbing evidence, not failure isolation. A real deployment should place the logical control service on an appropriately HA Solace service and, where practical, in a failure domain independent from the data brokers. Broker 0 HA protects control availability; it does not make arbitrary data queues on independent brokers interchangeable.

## Customer routing contract

The infrastructure-independent customer boundary is:

```go
type CustomerLibrary interface {
    GetScalingGroup(MessageView) (string, error)
    GetBusinessHash(MessageView) (BusinessHash, error)
}

type RoutingHashPolicy interface {
    RoutingAlgorithm() string
    GetRendezvousScore(ScoreInput) (RoutingScore, error)
}
```

The customer library owns the scaling group, business hash, rendezvous score function, and algorithm identifier. `BusinessHash` and `RoutingScore` are neutral 32-byte wire containers; a custom implementation may canonically expand or pad another deterministic hash into those containers.

The default `swlb-rendezvous-v1` score for each immutable logical broker ID in the current membership is:

```text
SHA256(
  u32be(len("swlb-rendezvous-v1")) || "swlb-rendezvous-v1" ||
  u32be(len(group_utf8))            || group_utf8            ||
  u32be(32)                         || raw_business_hash      ||
  u32be(len(broker_id_utf8))        || broker_id_utf8
)
```

The highest unsigned 256-bit score wins; ties select the lexicographically smaller broker ID. Membership order, endpoint, and epoch are not score inputs, so logical broker IDs must remain stable when an endpoint changes. Shims cache the authoritative membership and route locally; there is no per-message controller decision.

Changing hash semantics or reusing an identifier with new meaning is unsafe. A new implementation/identifier requires a coordinated migration after old durable records are resolved and the previous epoch is drained.

### Affinity and ordering boundaries

The durable ordering key is `scaling group + customer business hash`, not a dashboard label or event family. The live demo intentionally assigns one stable `entity_id` to each visual family, so each displayed family is one ordering key; a different application may place many keys in one family.

Only one head per local ordering lane is submitted at a time. This preserves local same-key sequencing while independent keys can progress concurrently. Consequences:

- one hot key maps to one broker and one serialized lane, so it cannot usefully consume all broker capacity without splitting the key and changing the ordering contract;
- rendezvous placement reduces movement when stable membership changes, but does not replace fencing and draining;
- ordering is not global across keys, publisher processes, failover, or restarts, and concurrent producers do not gain a total order merely because they compute the same broker;
- retrying an uncertain publish or redelivering after an application failure can produce duplicates.

## Authority, startup, and reconnect

Each shim:

1. attaches its durable update receiver and participant-exclusive reply receiver;
2. sends a uniquely correlated snapshot request through Broker 0;
3. validates transport and JSON correlation, namespace, group, role, participant, algorithm, revision, epoch, and resource scope;
4. reconciles complete updates buffered during request/reply;
5. enables routing only after authoritative state is installed.

The controller's atomic local JSON file is the current durable authority. It is persisted with file and directory synchronization before state becomes visible or control publication proceeds. Reconnect revokes local authority until bootstrap succeeds again. Revision/epoch regression, conflicting equal revisions, changed contracts, stale replies, and incorrectly scoped control messages fail closed.

This is restartable **single-controller** state, not replicated consensus and not a stale-leader fence. The proposed HA control design below is not implemented.

## Membership changes

The implemented per-group sequence is:

```text
PREPARE -> PAUSE -> FENCE -> DRAIN -> COMMIT -> ACTIVATE
```

1. Prepare target queues/consumers and obtain readiness.
2. Pause publishers and wait until native attempts, durable in-progress records, and `ack_uncertain` records are resolved.
3. Fence source-epoch ingress, including retained brokers where required.
4. Require continuously known zero queued, stored, unacknowledged, and in-progress work for the configured grace period. Missing or stale telemetry is unknown, never zero.
5. Persist the commit point, re-verify the fence, enable and verify target ingress, publish ACTIVE, and collect participant acknowledgements.

Controller operations use deterministic IDs and durable intent so restart replay is idempotent. Pre-commit rollback is permitted only with proof that source membership is intact, the target was not activated, and any attempted fence is reversible. Once COMMIT is durably reached, recovery moves forward only.

These mechanisms are implemented and covered by broker-free tests. A real membership transition under active live traffic, process crashes, stale publishers, and network partitions remains unqualified.

## Application interface

Requirements: Go `1.26.4`, existing broker services, AMQPS connectivity, SEMP access for the controller, and pre-provisioned Broker 0 resources and ACLs. No Java helper is required for normal operation.

Start from [`config.example.yaml`](config.example.yaml); it contains generic endpoints and environment-variable names, not credentials.

```bash
make validate
make build

./bin/controller -config config.example.yaml
./bin/subscriber -config config.example.yaml -participant events-a-subscriber-1
./bin/publisher -config config.example.yaml -participant events-a-publisher-1
```

Publisher input is one NDJSON object per line:

```json
{"event_id":"events-a-event-1","topic":"synthetic/events","headers":{"scaling-group":"events-a","entity_id":"entity-001","event_type":"updated","sequence":"1"},"payload_base64":"e30="}
```

A publisher receipt confirms only local durable outbox acceptance, not broker acknowledgement. The subscriber emits an NDJSON delivery and waits for a matching `ack`, `retry`, `reject`, or `release` directive. In the current command adapter, `reject` and the legacy-named `release` directive both call AMQP `RejectMessage` and, after successful settlement, unblock the local key; neither emits an AMQP `Released` outcome. `ack` means application handling returned success and AMQP acceptance succeeded; this still does not provide exactly-once processing.

## Verification and evidence boundaries

```bash
make test
make test-python
make test-race
make vet
make build
make integration
# all local CI checks:
make ci
```

Local tests cover fixed rendezvous vectors, order/endpoint invariance, membership add/remove movement, distribution, a **100,000-key placement simulation**, request/reply races, correlation/scope rejection, reconnect fail-closed behavior, persistence ordering, transition checkpoints, AMQP settlement mapping, durable outbox behavior, async dispatch concurrency/batching, and bounded pools. The 100,000-key test checks placement behavior; it is not 100,000 msg/s performance proof.

### Live plumbing evidence

The public read-only dashboard is <https://workload.sol-se-emea.com>.

On 2026-09-28, the live demo used three Solace Cloud Enterprise 1K Standalone services, with logical Broker 0 cohosted on data broker A. Five stable event-family keys were routed through the real customer hash and rendezvous implementation at a combined synthetic target of 100 messages/second. The four managed SDKPerf presets passed:

| Preset | Messages | Rate | Payload |
|---|---:|---:|---:|
| Quick check | 100 | 25/s | 256 B |
| Steady | 1,000 | 50/s | 256 B |
| Burst | 1,000 | 100/s | 256 B |
| Larger messages | 100 | 25/s | 8,192 B |

The final larger-message run independently recorded producer/native-publish/native-delivery/sink counts of `100/100/100/100`. Rate controls are bounded to 0–500 synthetic messages/second, and the demo circuit breaker pauses submissions at 1,000 current queued/unacknowledged messages.

This proves interoperability and the currently deployed plumbing at demo scale. It does **not** prove 100K throughput, a broker or controller failover, a membership change, recovery from uncertain acknowledgements, or production capacity.

### SDKPerf is test-only

SDKPerf Java 10.30.2 uses JCSMP/SMF, not AMQP. The optional managed test path is:

```text
SDKPerf producer (SMF) -> dedicated Broker A test queue -> Go test adapter
  -> existing Go publisher shim -> AMQP 1.0 data brokers -> existing Go subscriber shim
  -> Go test adapter -> dedicated durable result queue -> SDKPerf consumer (SMF)
```

The adapter preserves source payload bytes and verifies run-correlated results. This demonstrates SMF-to-existing-Go-shims-to-SMF interoperability. It does **not** make the NDJSON command interface an SMF/AMQP server, establish Solace SDK API compatibility, or add Java/SDKPerf to the production path. Direct SDKPerf examples shown by the dashboard bypass the shims and are labeled accordingly.

### Optional transition harness still unrun

`integration/separate_process_test.go` is an opt-in harness that launches separate controller, publisher, and subscriber processes against existing, pre-provisioned services. It does not create or delete infrastructure. Its planned scenario observes `A -> A+B -> A+B+C -> A+B` for one group while another remains stable.

It has **not** been run for the current implementation. Its current traffic submission also does not cover every transition phase continuously, and application-written ACK commands are not independent broker-observed settlement evidence. Full qualification must add the failure cases listed below.

## Proposed fail-safe failover and automatic recovery — not implemented

The safe design separates two fundamentally different events:

1. **Broker-service HA under one logical broker ID.** An HA Solace service can fail over its active node while retaining the same logical service, durable queues, and identity. Shims reconnect to the same logical broker and resume from durable state. This should be the primary automatic data-plane recovery mechanism.
2. **Reassignment to an independent broker.** This creates a new queue and routing membership. If the source broker is unreachable, its queue depth, unacknowledged deliveries, and publisher outcomes are unknown. The system cannot safely assume that source is drained. Strict zero-loss plus per-key ordering therefore requires waiting for source recovery. An operator may explicitly choose a documented business RPO/RTO compromise, but the system must never invisibly purge, replay, or reroute unknown work.

### Proposed control-plane architecture

- Run multiple controller replicas but permit exactly one active leader. Store a replicated transition journal in a quorum system that supports linearizable compare-and-swap. Every mutation includes a monotonically increasing controller **term** and group **epoch**.
- Acquire leadership by CAS, durably append intent before side effects, then append observed broker/participant proof. Replays use deterministic operation IDs and resume from the journal.
- Enforce stale-leader exclusion at a proposed management guard in front of SEMP; stock SEMP is not claimed to understand this system's terms. The guard must be the only network/credential path to managed SEMP resources, reject stale terms, serialize and drain every admitted prior-term operation (including asynchronously forwarded requests), then re-observe broker state before granting a new term. A term check before asynchronous forwarding is not atomic fencing, and direct SEMP access would bypass it. Term-scoped credentials are an alternative only if qualification proves revocation closes existing authenticated sessions and prevents or drains every already-admitted prior-term operation. A lease value, request header, or simple credential rotation is insufficient. If this fence cannot be proven, automatic promotion must stop and require an operator.
- Place Broker 0 on an HA logical control service. Do not use an arbitrary data broker failover as a substitute for control-plane consensus, and do not infer leadership from Broker 0 connectivity alone.
- Give participants a signed or otherwise authenticated authority record containing term, epoch, revision, and expiry. The current and default behavior remains fail closed after authority expires. Any proposed cached-authority continuation must also prove stale-controller exclusion, broker-enforced source-ingress fencing, and coordinated participant pause/readiness before a membership change; otherwise cached state may route only under the unchanged membership and no availability exception is safe.
- Replicate controller state and define backup/restore objectives. Existing bbolt publisher outboxes are durable only on their local disk. Node loss can therefore lose accepted-but-unacknowledged records unless that disk is synchronously replicated or the application retains its own source of truth; document the resulting RPO explicitly.

### Proposed recovery rules

- **Before COMMIT:** rollback only when the journal and fresh broker proof establish that source membership is intact, target ingress was never active, and every attempted source fence can be reversed. Otherwise remain paused and escalate.
- **At or after COMMIT:** move forward only. Re-verify the source fence under the current controller term, enable/verify target ingress, republish idempotent commands, and wait for exact participant acknowledgements.
- **ACK uncertain:** never blindly retry or route to another broker. A scoped operator/API action may retry only the exact still-current broker/endpoint/epoch/destination; it can duplicate. Applications needing stronger outcomes require a business event ID plus an idempotent or transactional consumer/deduplication store.
- **Backpressure:** bound reconnect and publish retries with exponential delay/jitter, cap outboxes by count/bytes, stop admissions before disk exhaustion, and apply per-group cooldown after recovery. Slow consumers remain a capacity/poison-message issue, not a reason to bypass ordering.
- **Retry/release versus quarantine:** AMQP `Released` makes a delivery eligible for redelivery; it is not discard or DLQ placement and does not guarantee that the key progresses. Keep the lane blocked while retry/redelivery remains possible. Only a business-authorized terminal reject, quarantine/DLQ move, or explicit skip may unblock the key, and each changes that key's business-order semantics. Record that decision as an explicit policy and audit event.

### Proposed fault decision matrix

| Fault/evidence | Automatic action | Stop or escalate when |
|---|---|---|
| HA node loss, same logical broker/service and durable queue | reconnect with bounded backoff; bootstrap fresh authority; resume exact outbox routes | service identity, queue durability, or ACK state cannot be verified |
| Broker 0 unavailable | retain durable local state; fail closed when authority expires | no fresh authenticated authority or control service loses quorum |
| Controller process crash | elected leader loads replicated journal and resumes the idempotent phase | leadership term cannot be broker-side fenced or journal quorum is unavailable |
| Stale controller or controller network partition | management guard rejects stale terms, drains admitted prior-term operations, re-observes broker state, then grants the new term | direct SEMP bypass exists, existing sessions/prior operations are not fenced, both sides can mutate brokers, or clocks/leases are the only protection |
| Data broker unreachable before drain proof | pause affected group and wait for recovery | queue/unacked state remains unknown; independent-broker promotion needs explicit RPO approval |
| Failure after durable COMMIT | current leader proceeds forward and re-verifies each side effect | source fence or target ingress cannot be freshly verified |
| Publisher crash with durable disk intact | reopen outbox; resolve exact-route records; do not reroute uncertain sends | disk is lost/corrupt/full or route is no longer current |
| ACK timeout/disconnect | mark `ack_uncertain`; block that ordering lane | no authoritative late result; operator decides exact-route retry versus business reconciliation |
| Slow/poisoned consumer | backpressure group; bounded retry; explicit reject/DLQ policy | drain deadline expires, backlog grows, or preserving order conflicts with DLQ action |
| Disk full on controller/outbox | stop new admissions and control mutations; alert before reserve is exhausted | durable journal/outbox write or fsync cannot complete |

### Proposed implementation and acceptance phases

1. **Define invariants and policy:** choose RPO/RTO per fault class; specify logical broker identity, queue durability, term/epoch rules, cached-authority policy, exact-route uncertain-ACK workflow, DLQ ordering impact, retry ceilings, cooldown, and operator approvals.
2. **Replicate control state:** replace the single local JSON authority with a quorum journal/CAS store; add snapshots, compaction, backups, schema migration, and deterministic replay tests.
3. **Add real fencing and leadership:** implement term acquisition plus the exclusive SEMP management guard described above. Test already-admitted asynchronous requests and existing sessions, prove the prior term is drained and broker state re-observed before promotion, and prove that a partitioned old leader cannot change ingress or publish an accepted newer-revision command. Fail closed if any bypass or fencing gap remains.
4. **Harden participants and durability:** authenticate authority records, add bounded reconnect state machines, expose `ack_uncertain` reconciliation, define local-disk replication/RPO, reserve disk space, and add explicit operator/audit APIs. Keep the current default fail-closed behavior; allow cached authority only for unchanged membership unless source-ingress fencing and participant coordination for a change have been proven.
5. **Implement broker recovery policy:** automatically reconnect only within the same HA logical broker. For independent-broker reassignment, require fresh source drain/fence evidence or an explicit recorded business-RPO override.
6. **Qualify under active traffic:** inject controller crashes before/after every persisted phase checkpoint, stale leaders, Broker 0 partitions, publisher/subscriber restarts, data-broker loss, slow consumers, disk-full conditions, and uncertain ACKs. Run traffic continuously throughout transitions; verify no stale ingress, bounded backpressure/retries, expected duplicate bounds, per-key order under the declared policy, journal replay, and operator stop conditions.
7. **Promote cautiously:** canary one group, retain manual abort, measure recovery against the declared RTO/RPO, and qualify the exact Solace service class, client version, network, payload mix, and multipublisher topology before any production claim.

## Current non-goals and limitations

No claim is made for exactly-once delivery, global ordering, automatic independent-broker failover, controller HA, zero-loss recovery from an unreachable source, selective per-key draining, unlimited hot-key scaling, native SMF/MQTT adapters, autonomous Solace Cloud provisioning, ML-based scaling, 100K msg/s, or production readiness.

## Solace references

- [Using AMQP 1.0](https://docs.solace.com/API/AMQP/Using-AMQP.htm)
- [AMQP 1.0 messaging management](https://docs.solace.com/Services/Managing-AMQP-Messaging.htm)
- [Guaranteed messages and durable endpoints](https://docs.solace.com/Messaging/Guaranteed-Msg/Guaranteed-Messages.htm)
- [SDKPerf downloads](https://products.solace.com/download/SDKPERF_JAVA)
