# Solace Workload Balancer

A Go implementation that routes related application messages directly to one of several Solace PubSub+ data brokers. A separate Solace service, **Broker 0**, carries control traffic between the controller and publisher/subscriber shims.

> **Status:** AMQP 1.0 is the implemented v1 data and control transport. Local unit, race, adapter, vet, and build checks are provided. The system is not production-qualified, exactly-once, or an uninterrupted-handover solution. A live three-service separate-process run remains required.

## Architecture

```text
application -> publisher shim -> selected data broker -> subscriber shim -> application
                         controller <-> Broker 0
                         controller -> data-broker SEMP
```

Business payloads never pass through the controller. The customer library remains infrastructure-independent:

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

The customer library owns both hash choices and their algorithm identifier. `BusinessHash` and `RoutingScore` are neutral 32-byte wire containers: a custom implementation may canonically expand or pad MurmurHash, xxHash, or another deterministic value into that container. The generic router only requests and compares scores. Changing a hash implementation or identifier is a coordinated migration after old durable records are drained, never a live hot-swap.

## Routing contract

The v1 algorithm identifier is `swlb-rendezvous-v1`. For every immutable logical broker ID in the current membership:

```text
score = SHA256(
  u32be(len("swlb-rendezvous-v1")) || "swlb-rendezvous-v1" ||
  u32be(len(group_utf8))            || group_utf8            ||
  u32be(32)                         || raw_business_hash      ||
  u32be(len(broker_id_utf8))        || broker_id_utf8
)
```

The broker with the highest full unsigned 256-bit score wins; equal scores use the lexicographically smaller broker ID. Endpoint, membership order, and epoch are not score inputs. Broker IDs must therefore be stable across endpoint changes.

Snapshots carry algorithm, customer hash contract, library version, revision, epoch, phase, ordered membership, credential-free broker ID/AMQP endpoint descriptors, and exact managed resources. Endpoints select connections but are not score inputs; endpoint updates preserve broker placement and create a new connection generation while existing in-events-a leases finish. Credentials stay local and are resolved by stable broker ID. Unsupported or changed contracts fail closed. Durable outbox records persist the algorithm and exact endpoint/epoch/broker/destination. An ACK-uncertain record is never silently rerouted; an operator retry is allowed only while that exact route remains current.

### Migration from modulo routing

Snapshot schema v3 requires a non-empty, bounded routing-algorithm identifier and complete broker descriptors. The default library uses `swlb-rendezvous-v1`; custom identifiers remain opaque to the controller and must match the publisher/subscriber library handshake. Existing modulo snapshots and legacy outbox records without a matching algorithm are rejected rather than reinterpreted. Upgrade requires pausing publishers, resolving all ACK-uncertain sends, draining and fencing the old epoch, deploying the controller and shims together, then activating a newly persisted snapshot. This is a coordinated migration, not an in-place algorithm flag change.

## Startup and reconnect

Each shim:

1. attaches its durable update receiver and participant-exclusive reply receiver;
2. sends a durable, uniquely correlated snapshot request on Broker 0;
3. validates transport and JSON correlation, namespace, group, role, participant, algorithm, revision, and resource scope;
4. reconciles complete updates buffered during request/reply;
5. enables routing only after authoritative state is installed.

The controller's atomic local state is authoritative and is persisted before control publication. Request topics, request queues, and reply queues are participant/group/role scoped; arbitrary reply-to addresses are rejected. Timeouts use bounded exponential backoff. Late responses from timed-out attempts are accepted and discarded from the participant-exclusive reply queue rather than endlessly released. Reconnect revokes local authority until a fresh response succeeds.

## Membership changes

Rendezvous reduces key movement but does not remove the safety protocol. v1 pauses the whole affected group:

```text
PREPARE -> PAUSE -> FENCE -> DRAIN -> COMMIT -> ACTIVATE
```

The controller prepares target exclusive queues, waits for subscriber readiness, pauses and quiesces publishers, fences all source-epoch ingress (including brokers retained in the target membership), and requires continuously zero queued plus unacknowledged/in-progress work for the configured grace period. Missing metrics are unknown, never zero. Pre-commit recovery may roll back; after commit recovery only moves forward. Other groups remain independent.

## AMQP v1 limits

- Data and Broker 0 control use AMQP 1.0 with durable messages and explicit settlement.
- Data queues are durable **exclusive** queues with one serialized consumer per broker/group/consumer-set/epoch binding.
- Native Solace partitioned queues are not supported by this AMQP v1 runtime and are rejected by configuration validation.
- Publisher connections are opened lazily through a bounded pool. Connection generations and sender links are reused while valid; active leases prevent idle eviction.
- Publisher outboxes are bbolt-backed and bounded by message count and bytes. Full outboxes apply backpressure.
- SEMP monitoring and retry loops are bounded by configured intervals, freshness, transition deadlines, and exponential retry caps.
- Native SMF and MQTT adapters are future work. MQTT acknowledgement and recovery semantics must be designed and qualified separately rather than mapped to AMQP settlement.

## Configure and run

Requirements: Go `1.26.4`, three existing broker services with logical Broker 0 cohosted on data broker A, AMQPS connectivity, SEMP access for the controller, and pre-provisioned queues/topics/ACLs. No Java helper is required.

Start from [`config.example.yaml`](config.example.yaml). It uses exclusive queues only and environment-variable names rather than secrets.

```bash
make validate
make build

./bin/controller -config config.example.yaml
./bin/subscriber -config config.example.yaml -participant events-a-subscriber-1
./bin/publisher -config config.example.yaml -participant events-a-publisher-1
```

The publisher and subscriber command protocols are newline-delimited JSON. Example input:

```json
{"event_id":"events-a-event-1","topic":"synthetic/events","headers":{"scaling-group":"events-a","entity_id":"UA","event_type":"123","timestamp":"2026-09-25","sequence":"ORD-LAX"},"payload_base64":"e30="}
```

A publisher receipt confirms only local durable outbox acceptance, not broker acknowledgement. Subscriber `ack` means application processing completed and AMQP acceptance succeeded; retry/reject paths do not provide exactly-once processing.

## Verification

```bash
make test
make test-race
make vet
make build
make integration
# or all local checks:
make ci
```

The local tests cover fixed rendezvous vectors, order/endpoint invariance, add/remove movement, distribution, a 100,000-key 200→201 simulation, request/reply bootstrap races, correlation/scope rejection, reconnect fail-closed behavior, controller persistence ordering, transition safety, AMQP settlement mapping, durable outbox behavior, and bounded pool concurrency.

## Live smoke harness and qualification gap

`integration/separate_process_test.go` is an opt-in repository-owned smoke harness that launches one controller plus Events A and Events B publisher/subscriber processes. It requires already-provisioned resources and does not create or delete infrastructure. The opt-in repository-owned smoke harness needs exactly:

- three AMQP broker services (`A`, `B`, `C`), with logical Broker 0 cohosted on service `A` under separate namespaced control resources;
- SEMP access to those three data brokers;
- participant-scoped durable request, reply, update, command, registration, acknowledgement, and telemetry queues/topics;
- exclusive epoch data queues for Events A and Events B;
- distinct controller, publisher, subscriber, and observer credentials/ACLs.

The smoke scenario requires an authorized policy stimulus to drive Events A `A -> A+B -> A+B+C -> A+B`. It validates exact ACTIVE memberships, stable Events B membership plus completion of the initial 20 Events B deliveries, durable publisher receipts, submitted event-ID completeness, per-group ordering, application ACK commands written to subscriber stdin, and separately reports at-least-once duplicates. It has **not** been run in this change. Traffic is submitted only once before transition observation, not continuously throughout every phase, and an ACK command written to subscriber stdin is not independent broker-observed settlement proof. It does not force process restarts or prove stale-publisher rejection, fence NACKs, readiness timing, or continuous-zero drain evidence; those remain full live qualification gates. No paid infrastructure was provisioned and no prior spending authorization was reused.

## Non-goals and limitations

No global ordering across publisher processes, exactly-once delivery, automatic weight adjustment, selective per-key draining, infinite scaling, uninterrupted publication during handover, automatic cloud provisioning, or production-readiness claim is made.

## Running live demo

Public read-only dashboard: **https://workload.sol-se-emea.com**

The demo uses three Solace Cloud Enterprise 1K Standalone services. Logical Broker 0 control resources are cohosted on data broker A. The generator runs generic synthetic `events-a` and `events-b` records at a combined target of 100 messages/second; the broker-depth circuit breaker pauses submissions at 1,000 current queued/unacknowledged messages.

Private operational files are ignored under `.local/live-neutral-20260928/`. The dashboard admin password remains only in `.local/live-20260928-121042/admin-token`.

```bash
# AWS host status
ssh -i .local/live-20260928-121042/lightsail-default.pem ubuntu@3.93.230.1 \
  'systemctl status swlb caddy --no-pager'

# Stop or start the complete workload
ssh -i .local/live-20260928-121042/lightsail-default.pem ubuntu@3.93.230.1 \
  'sudo systemctl stop swlb'
ssh -i .local/live-20260928-121042/lightsail-default.pem ubuntu@3.93.230.1 \
  'sudo systemctl start swlb'
```

Rate controls are available on the same HTTPS page after login and are bounded to 0–500 messages/second total. Current queue depth comes from SEMP queue-message collection `meta.count`; lifetime `spooledMsgCount` is not used as backlog. AWS baseline is $24/month for the Lightsail instance, excluding Solace Cloud contract costs. This remains a live plumbing demo, not production qualification; no real membership transition has been exercised.

### Optional SDKPerf test path

SDKPerf is **test-environment tooling only**. The normal controller, publisher, subscriber, and customer library remain native Go and have no JVM, SDKPerf, adapter, or extra-ingress dependency. The official SDKPerf Java 10.30.2 package is JCSMP/SMF, not AMQP. The optional test path is therefore:

```text
SDKPerf producer (SMF) -> dedicated Broker A test queue -> Go test adapter
  -> already-running native publisher shim -> AMQP data brokers -> already-running native subscriber shim
  -> Go test adapter -> dedicated durable result queue -> SDKPerf consumer (SMF)
```

`tools/sdkperf_harness.py` bounds message count, rate, and duration; verifies the real SDKPerf producer exit status, native shim counts, and unique run-correlated messages printed by the real SDKPerf consumer; redacts credentials; and terminates complete child process groups. `tools/sdkperf-adapter` preserves source payload bytes and is intentionally absent from default builds and startup. Its generated test IDs and in-memory sequence do not provide exactly-once or restart-stable ordering guarantees.

The dashboard exposes only fixed authenticated start/stop actions and accepts no command text. Its primary button runs the complete managed end-to-end test without a terminal. Expandable CLI examples use shell-safe environment references and distinguish a direct-broker SDKPerf smoke test—which bypasses the shims—from the managed ingress/result commands that require the Go adapter. Test credentials remain in the private service environment and never appear in public status, HTML, or evidence. Official download: <https://products.solace.com/download/SDKPERF_JAVA>.
