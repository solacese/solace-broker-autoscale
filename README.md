# Solace Workload Balancer

`solace-workload-balancer` routes related application messages to one Solace PubSub+ data service while a separate Solace service, **Broker 0**, distributes versioned membership and coordinates membership changes.

The customer defines the business ordering key. The publisher shim hashes that key, selects one broker from an ordered membership, writes the message to a bounded local outbox, and publishes directly to that broker. The controller is not in the application data path.

> **Status:** this repository contains production-mode controller, publisher, and subscriber processes; native SMF messaging; JCSMP-based non-destructive Broker 0 browsing; SEMP queue management, monitoring, and fencing; and an opt-in Solace Cloud qualification runner. It is **not yet qualified as production-ready**. See [Evidence and remaining limits](#evidence-and-remaining-limits).

## What it solves

The project adds broker-level placement above Solace-native queues without creating a Kafka-style log or an application-managed partition layer:

```text
publisher application
  -> publisher shim
  -> selected data broker
  -> subscriber shim
  -> subscriber application

controller <-> Broker 0 <-> publisher/subscriber participants
controller -> SEMP on data brokers
```

- Application payloads go directly to data brokers, never through Broker 0 or the controller.
- Broker 0 carries retained membership, live update hints, transition commands, registrations, and acknowledgements.
- The controller samples broker and group queue pressure through SEMP, applies measured-capacity headroom, sustained-pressure, cooldown, and transition limits, manages epoch-specific data queues, and runs a group-scoped `PREPARE -> PAUSE/FENCE -> DRAIN -> COMMIT -> ACTIVE` transition. Telemetry failures fail closed and are retried with bounded backoff rather than authorizing a scaling decision.
- Scaling groups have independent membership, epoch, queue policy, participants, and transition state. A Flight transition does not pause Baggage.
- This is not DMR: DMR joins broker fabrics; this project deliberately selects one broker for each business key and coordinates changes to that affinity.

## Customer routing API

Application code implements:

```go
type CustomerLibrary interface {
    GetScalingGroup(MessageView) (string, error)
    GetBusinessHash(MessageView) ([32]byte, error)
}

type MessageView struct {
    Topic   string
    Headers map[string]string
    Payload any
    EventID string
}
```

`MessageView` is borrowed. `Headers`, `Payload`, and objects reachable through `Payload` must not be mutated or retained after either callback returns. The implementation must be deterministic, local, broker-independent, and versioned consistently across publishers and subscribers. An error rejects publication before durable acceptance.

The current commands wire the repository's `AirlineCustomerLibrary`; replacing it requires embedding a different Go implementation and rebuilding. It is not a dynamic plugin interface.

### Included contracts

The supplied library requires a `scaling-group` header and supports:

| Group | Hash contract | Required headers |
|---|---|---|
| `flight-operations` | `flight-operations-v1` | `carrier`, `flight-number`, `departure-date`, `leg-id` |
| `baggage-tracking` | `baggage-journey-v1` | `carrier`, `bag-journey-id` |

Each non-empty, NUL-free component—including the contract name—is encoded as a 4-byte unsigned big-endian UTF-8 byte length followed by the original UTF-8 bytes. The implementation does not trim, case-fold, normalize Unicode, parse dates, or otherwise canonicalize values; the customer schema must supply canonical values. The encoded components are hashed with SHA-256.

The publisher interprets the complete digest as an unsigned big-endian integer:

```text
index = business_hash mod len(ordered_broker_ids)
destination = ordered_broker_ids[index]
```

Membership order is authoritative. Clients do not sort, trim, deduplicate, or shuffle it. Because this is modulo routing rather than consistent hashing, a membership-size change can remap many keys; the coordinated pause-and-drain transition prevents old- and new-epoch traffic from overlapping.

## Ordering and delivery contract

The implementation provides a bounded ordering guarantee, not a global sequencer:

- Within one publisher participant and one scaling group/business hash, the bbolt outbox exposes only the earliest accepted record for publication. A pending, in-flight, or ACK-uncertain head blocks later records for that key while unrelated keys continue.
- The chosen broker and epoch are persisted with the accepted record; routing is not recomputed under a later library version.
- The subscriber serializes handler invocation and acknowledgement by scaling group/business hash. A handler or acknowledgement failure blocks that key until explicit retry or release.
- Membership changes pause the affected group's publishers, fence old-epoch ingress, drain old queues, commit the new membership, enable target ingress, and then resume. Publication is not uninterrupted during a transition.

Limits of that contract:

- Independent publisher processes using the same hash do not acquire a global publication order.
- An uncertain broker acknowledgement is retained for operator-directed retry; retry can duplicate a message and block a transition until resolved.
- There is no durable subscriber deduplication or exactly-once processing. Applications must make `event_id` handling idempotent.
- Correct order still depends on the application publishing events in the intended order.

### Flight partitioned vs. Baggage exclusive

The example config intentionally demonstrates both supported queue modes:

- **Flight Operations:** a non-exclusive native partitioned queue with 12 partitions. The publisher sets Solace `JMSXGroupID`/`QueuePartitionKey` to the full lowercase business-hash hex value, and the subscriber opens 12 consumers per broker/epoch binding. This permits different flight keys to progress concurrently while one key remains assigned to a queue partition.
- **Baggage Tracking:** an exclusive queue with one consumer per broker/epoch binding and no partition key. Broker-level modulo routing still spreads different bag journeys across the ordered broker membership.

Epoch-specific ingress topics and queues provide the broker-side fence. The original application topic is carried in message metadata and restored for the subscriber application.

## Prerequisites

- Go **1.26.4** as declared by `go.mod`.
- Maven and Java 8 or newer to build the JCSMP LVQ browser.
- One Broker 0 service and the configured data brokers. The example uses three data brokers.
- Trusted TLS connectivity to each SMF and SEMP endpoint.
- Broker 0 resources and ACLs provisioned before any process starts.
- Writable paths for controller state and per-publisher bbolt outboxes.

The implementation uses Solace Go messaging API `v1.10.1` for persistent SMF publish/receive and JCSMP `10.30.2` only for non-destructive LVQ browse. Set these optional overrides when needed:

```text
SWLB_JCSMP_BROWSER_JAR   alternate shaded helper JAR
SWLB_JAVA_EXECUTABLE    alternate Java executable
SWLB_JAVA_TRUST_STORE   readable Java trust-store file
SSL_CERT_DIR            native Go trust-store directory used by qualification
```

## Configure

Start from [`config.example.yaml`](config.example.yaml). Replace all example endpoints and illustrative capacity numbers. Capacity profiles must describe measurements for the exact service class, broker build, and workload; the values in the example are not Solace-published limits.

Each scaling group must set `policy.max_concurrent_changes: 1`, matching the controller's one-active-transition-per-group state model. This does not serialize unrelated groups: independent groups may transition concurrently, with runtime-wide concurrency bounded by the number of configured groups.

The example references these exact environment variables:

```text
# Controller -> Broker 0 SMF and SEMP
SOLACE_CONTROL_USERNAME
SOLACE_CONTROL_PASSWORD
SOLACE_CONTROL_SEMP_USERNAME
SOLACE_CONTROL_SEMP_PASSWORD

# Publisher/subscriber -> data-broker SMF; shared by the example
SOLACE_DATA_USERNAME
SOLACE_DATA_PASSWORD

# Controller -> data-broker SEMP; shared by the example
SOLACE_DATA_SEMP_USERNAME
SOLACE_DATA_SEMP_PASSWORD

# Participant-specific Broker 0 SMF credentials
SOLACE_FLIGHT_PUBLISHER_CONTROL_USERNAME
SOLACE_FLIGHT_PUBLISHER_CONTROL_PASSWORD
SOLACE_BAGGAGE_PUBLISHER_CONTROL_USERNAME
SOLACE_BAGGAGE_PUBLISHER_CONTROL_PASSWORD
SOLACE_FLIGHT_SUBSCRIBER_CONTROL_USERNAME
SOLACE_FLIGHT_SUBSCRIBER_CONTROL_PASSWORD
SOLACE_BAGGAGE_SUBSCRIBER_CONTROL_USERNAME
SOLACE_BAGGAGE_SUBSCRIBER_CONTROL_PASSWORD
SOLACE_WORKLOAD_OBSERVER_CONTROL_USERNAME
SOLACE_WORKLOAD_OBSERVER_CONTROL_PASSWORD

# Named by the disabled production-cloud section
SOLACE_CLOUD_TOKEN
```

The production controller currently requires `cloud.enabled: false`; it manages existing data services but does not provision or delete services. The separate qualification CLI uses `SOLACE_CLOUD_JWT`, described below, rather than `SOLACE_CLOUD_TOKEN`.

Validate without resolving credentials or opening network connections:

```bash
make validate

go run ./cmd/publisher -config config.example.yaml \
  -participant flight-publisher-1 -validate-only

go run ./cmd/subscriber -config config.example.yaml \
  -participant flight-subscriber-1 -validate-only
```

## Broker provisioning and ACLs

### Broker 0 must be pre-provisioned

The runtime binds existing Broker 0 resources and uses `DoNotCreateMissingResources`; it does not create queues, subscriptions, users, passwords, or ACL exceptions there. Names are derived from `namespace` (`swlb` below), not provisioned by the descriptive `control.resources` fields.

For every group and applicable participant, provision durable resources with these deterministic names:

| Purpose | Topic | Queue |
|---|---|---|
| Retained membership | `swlb/control/<group>/snapshot` | `swlb.ctl.<group>.membership` |
| Live update hint | `swlb/control/<group>/updates` | `swlb.ctl.<group>.updates.<participant>` |
| Command | `swlb/control/<group>/commands/<role>/<participant>` | `swlb.ctl.<group>.cmd.<role>.<participant>` |
| Registration | `swlb/control/<group>/registrations/<role>/<participant>` | `swlb.ctl.<group>.reg.<role>.<participant>` |
| Acknowledgement | `swlb/control/<group>/acknowledgements/<role>/<participant>` | `swlb.ctl.<group>.ack.<role>.<participant>` |
| Telemetry | `swlb/control/<group>/telemetry/<role>/<participant>` | `swlb.ctl.<group>.tel.<role>.<participant>` |

Use roles `publisher`, `subscriber`, `broker`, and `observer` as applicable. Subscribe each queue to its exact topic. Configure the membership queue as the group LVQ used for non-destructive browse and retain only the latest complete snapshot. Participants subscribe first, browse second, reconcile buffered updates by revision/epoch, and periodically rebrowse.

ACLs are part of the safety boundary:

- The **controller principal** publishes snapshots, update hints, and exact participant command topics; consumes the exact registration, acknowledgement, and telemetry queues; and browses membership LVQs.
- Each **publisher or subscriber principal** browses membership for only its assigned groups, consumes only its own update and command queues, and publishes only its own exact registration and acknowledgement topics.
- Each **broker/observer principal** publishes only its exact telemetry topic.
- Production validation requires each Broker 0 participant username to equal its configured principal and requires distinct participant credential-variable pairs. Do not authorize by trusting a message `SenderID` field.

### Data brokers

The controller's SEMP principal must be allowed to create, compare, monitor, fence/unfence, subscribe, and delete only the namespace-scoped epoch queues it manages. On startup it creates or verifies the retained current epoch, replays durable source/target ingress intent after an interrupted transition, prepares proposed-epoch queues with ingress disabled, and deletes only exact old-epoch queues backed by completed-transition fence and drain evidence and no remaining reference. Existing queues are adopted only when their complete managed configuration matches. Publisher data credentials need publish access to epoch ingress topics; subscriber data credentials need consume access to the corresponding queues.

The configured dead-message queues are referenced but are not created by runtime assembly; provision them separately. Do not point the controller at arbitrary existing customer queues.

## Build and run

```bash
make build
```

This writes `bin/controller`, `bin/publisher`, and `bin/subscriber`.

After Broker 0 provisioning and environment setup, start the controller and participants in separate processes:

```bash
./bin/controller -config config.example.yaml

./bin/subscriber -config config.example.yaml \
  -participant flight-subscriber-1

./bin/publisher -config config.example.yaml \
  -participant flight-publisher-1
```

Use `baggage-subscriber-1` and `baggage-publisher-1` for the Baggage group. Logs go to stderr; the publisher/subscriber application protocol uses stdin/stdout.

## Publisher NDJSON protocol

The publisher reads one JSON object per line from stdin. Records are limited to 1 MiB, unknown fields and trailing JSON are rejected, and `payload_base64` uses strict standard Base64. `event_id` and `topic` are required. Application headers beginning with `swlb.` and `JMSXGroupID` are reserved.

Flight input:

```json
{"event_id":"flight-event-1","topic":"airline/flight/status","headers":{"scaling-group":"flight-operations","carrier":"UA","flight-number":"123","departure-date":"2026-09-25","leg-id":"ORD-LAX"},"payload_base64":"eyJzdGF0dXMiOiJib2FyZGluZyJ9"}
```

Baggage input:

```json
{"event_id":"bag-event-1","topic":"airline/baggage/status","headers":{"scaling-group":"baggage-tracking","carrier":"UA","bag-journey-id":"BAG-0001"},"payload_base64":"eyJzdGF0dXMiOiJsb2FkZWQifQ=="}
```

For each durable local acceptance, stdout receives a receipt. Assuming the example's active epoch 1 membership `[broker-a, broker-b, broker-c]`, the Flight example hashes to index 2:

```json
{"event_id":"flight-event-1","durably_accepted":true,"state":"ready","group":"flight-operations","hash":"4a91f3ee485b3a4687e0badf310d2964217732ba358a84f2a6a689e0872f9dc0","epoch":1,"broker":"broker-c"}
```

`durably_accepted` means committed to the local bbolt outbox, not acknowledged by a broker. During a pause or before valid membership, an accepted message may be returned as `state:"unassigned"` without `epoch` or `broker`; it remains durable and blocked until safe assignment. Duplicate event IDs and a full outbox are rejected.

## Subscriber NDJSON protocol

The subscriber writes each delivery to stdout, then waits for an application outcome on stdin:

```json
{"delivery_id":"delivery-1","event_id":"flight-event-1","topic":"airline/flight/status","headers":{"carrier":"UA"},"payload_base64":"eyJzdGF0dXMiOiJib2FyZGluZyJ9"}
```

Reply with one exact, case-sensitive outcome:

```json
{"delivery_id":"delivery-1","outcome":"ack"}
```

- `ack` confirms durable application processing and acknowledges the native message.
- `retry` retains the native message, keeps that business key blocked, and presents the same message again with a new `delivery_id`.
- `reject` or `release` terminally settles the native message as rejected and unblocks the key.

Unknown fields, duplicate fields, trailing JSON, unknown delivery IDs, unsupported outcomes, and records over 1 MiB are rejected without affecting another pending delivery. Later messages for a poisoned business key cannot overtake it.

## Local verification

No local target contacts Solace Cloud or creates broker resources.

```bash
make test          # Go unit tests
make test-race     # Go tests with race detector
make vet           # static analysis
make integration   # Java helper + local native-adapter capability checks
make check         # test + vet + integration
make ci            # race + vet + integration
```

`make integration` is intentionally local: it builds/tests the JCSMP helper and tests adapter wiring with fakes. It does not claim real-broker qualification and is not designed to fail as a placeholder.

## Opt-in Solace Cloud qualification

Cloud qualification is separate from normal runtime and never runs from `make check` or `make ci`.

Create `.env` with a raw JWT (without a `Bearer ` prefix):

```dotenv
SOLACE_CLOUD_JWT=<raw-jwt>
```

The token must include a non-empty `sub` claim and permit the required list operations; `--run` and `--cleanup` also require exact service create/read/delete and operation-polling access.

```bash
make qualify-preflight  # read-only: credentials, catalog, release, and quota
make qualify-cloud      # creates, exercises, then deletes exactly four services
make cleanup-cloud      # retry exact-ID cleanup from the private journal
```

Defaults are:

- API: `https://api.solace.cloud`
- credential file: `.env`
- JWT variable: `SOLACE_CLOUD_JWT`
- private journal: `.qualify-cloud/journal.json`
- preflight/run timeout: 30 minutes
- cleanup timeout: 10 minutes

Override them with `--base-url`, `--env`, `--jwt-var`, `--journal`, `--timeout`, and `--cleanup-timeout` on `go run ./cmd/qualify-cloud ...`.

Preflight resolves the fixed qualification plan without creating services: Broker release `10.26.0.8894-14` in `gke-gcp-us-east4-a`, one `ENTERPRISE_250_HIGHAVAILABILITY` Broker 0, and three `ENTERPRISE_5K_STANDALONE` data brokers. The run journals exact service IDs before lifecycle operations and always attempts independent-timeout cleanup. If cleanup is incomplete, the private journal is retained for `make cleanup-cloud`.

The strict run checks partitioned-queue configuration, definitive fence NACKs, three-broker data flow, drain telemetry, two independent non-destructive Broker 0 browses, durable command/ack routing, and Flight transitions `A -> A+B -> A+B+C -> A+B` with restarts, stale-publisher rejection, final retained state, and Baggage isolation. No scenario may be skipped, and queue plus service cleanup must complete for an overall pass.

## Evidence and remaining limits

### Latest completed Cloud evidence

The latest completed historical run reported:

- **133,120 messages expected, accepted, delivered, unique, and positively acknowledged**;
- **0 missing, duplicate, out-of-order, or wrong-broker deliveries**;
- **1,572.53 messages/second**, reported as a diagnostic achieved rate rather than a qualification threshold or capacity claim;
- **PASS** for independent non-destructive Broker 0 browse;
- **PASS** for Broker 0 command/ack routing; and
- **FAIL** for drain telemetry and the dependent transition scenario because that older run used the then-known wrong SEMP queue-counter shape.

The raw result is not committed to this repository, and the historical run predates the current qualification settings. The messaging, browse, and command/ack results are useful evidence, but the overall run did not pass: it did not prove drain or a completed membership transition. Throughput is now report-only—there is no throughput pass threshold—and the measured rate is not a supported throughput ceiling. The code now reads the Cloud-available `collections.msgs.count` queue count and uses message count plus unacknowledged/in-progress acknowledgements as the authoritative drain predicate, with spool bytes diagnostic only. That fix passes local SEMP/runtime/qualification tests but still requires a fresh strict Cloud rerun before production-readiness can be considered.

### Still unqualified or intentionally limited

- No completed strict Cloud run currently proves all seven scenarios together. The latest command/ack probe passed, but the corrected drain path and dependent full transition remain pending a Cloud rerun. The qualification transition driver uses real Broker 0 publication/browse, native data traffic, and SEMP queue operations, but its controller, publisher shim, subscriber shim, and participant acknowledgements are coordinated in-process; it does not prove a transition among separately deployed `cmd/controller`, `cmd/publisher`, and `cmd/subscriber` processes.
- No exactly-once delivery, durable subscriber deduplication, or global ordering across publisher processes is provided.
- No uninterrupted publishing during membership changes is claimed.
- Production controller Cloud provisioning is disabled. Operators provide an existing broker inventory; only the explicit qualification CLI creates and deletes Cloud services.
- Production SEMP capacity policy is wired for eligible existing brokers and fail-closes on missing, stale, partial, or failed telemetry. Automatic scale-in is not wired because no proof-bearing scale-in choice is populated; policy-driven scale-out is implemented but remains covered only by local tests until a complete Cloud transition passes.
- Sustained-pressure policy state is restored across controller restart from a private atomic sidecar derived from `controller_state`; persistence failure stops policy actions before a recommendation is applied. Configuration requires one active transition per group; independent groups may transition concurrently.
- SEMP collection failures reset pressure evidence and retry with bounded backoff. Errors while evaluating or applying an otherwise valid snapshot still stop the controller.
- ACK-uncertain publications require operator action and may duplicate on retry.
- The JCSMP browser helper is a separate long-running Java process; cancellation or malformed helper output fails it, and runtime does not automatically restart it.
- Broker 0 resources, identities, passwords, subscriptions, and ACL exceptions are not provisioned by production runtime. The qualification run reuses a service credential for managed test queues and does not prove production identity isolation.
- Dead-message queues are not auto-provisioned.
- Capacity values in the example and qualification safety caps are not broker limits. The recorded evidence does not qualify other regions, releases, service classes, topologies, payload distributions, longer durations, sustained maximum load, or HA/failure-domain capacity.
