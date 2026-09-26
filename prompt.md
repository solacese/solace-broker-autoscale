# Build Solace Workload Balancer

## Objective

Create **`solace-workload-balancer`**.

Build a system that distributes application messaging workloads across multiple Solace brokers while preserving the intended business ordering boundaries.

The architecture consists of:

- A local customer-provided routing library.
- A publisher shim embedded in publisher applications.
- A subscriber shim embedded in subscriber applications.
- Multiple Solace data brokers.
- A separate Solace control service called **Broker 0**.
- One controller that observes workloads, manages broker capacity, and coordinates membership changes through Broker 0.

This is a fresh implementation. Do not modify, rename, or delete the existing `solace-broker-autoscale` repository.

Do not copy its previous application-managed partition architecture into the new project.

## 1. Core design principles

### Solace-native messaging

We are using Solace, not implementing Kafka-style partitions or an application-level distributed log.

The customer library identifies messages that must stay together. The publisher shim consistently sends those messages, in publication order, to one data broker. Solace delivers them using an appropriate native queue and consumer configuration.

Native Solace partitioned queues may be used to distribute consumption within a broker when appropriate. They are distinct from our broker-selection calculation.

Do not introduce an application-managed virtual-partition layer.

### Business logic belongs in the customer library

The infrastructure must not contain airline-specific routing logic.

The customer library determines:

1. Which scaling group a message belongs to.
2. Which business identity determines its ordering group.

The infrastructure implements routing, reliable publication, subscription management, membership distribution, scaling, and coordinated handover.

### Broker 0 is the control channel

The controller does not expose an HTTP assignment API to publisher or subscriber shims.

All membership distribution, transition notifications, participant readiness, and coordination between controller and shims use Solace messaging through Broker 0.

A local health or metrics endpoint is acceptable. It must not become an alternative routing control plane.

### Direct application traffic

Application messages travel directly:

```text
Publisher application
    → publisher shim
    → selected data broker
    → subscriber shim
    → subscriber application
```

Application payloads do not pass through the controller or Broker 0.

### Explicit boundaries

Do not claim infinite scaling, exactly-once processing, or production readiness without evidence.

Do not implement a business sequencer. The ordering contract assumes that applications publish events in the intended order. Independently concurrent publishers do not gain a global business order merely by generating the same hash.

## 2. Reference deployment

Implement and test the design using:

- **Broker 0:** one logical control service, preferably deployed with Solace HA.
- **Broker A, Broker B, Broker C:** three independent data services.
- **Scaling group 1:** Flight Operations.
- **Scaling group 2:** Baggage Tracking.
- Publisher and subscriber applications for both groups.

An HA service counts as one routing destination. Its primary, backup, and monitoring nodes must not appear as separate entries in the broker list.

The two scaling groups may share data brokers, but they have independent:

- Membership lists.
- Membership epochs.
- Queues and subscriptions.
- Routing-library contracts.
- Scaling policies.
- Handover state.
- Participant readiness.

Changing Flight Operations membership must not pause Baggage Tracking. Shared broker-capacity effects must still be observed.

## 3. Implementation approach

Prefer a compact Go implementation for the controller and shims, with shared protocol types.

Use the official native Solace Go messaging API where its capabilities support the design. Verify SDK and broker-version compatibility before selecting queue types and APIs.

Do not assume an AMQP subscriber can consume from a Solace native partitioned queue. Verify the supported transport for every feature used.

The architecture and routing contract should be language-independent, but v1 only needs one complete, tested implementation. Do not claim that a library written in any language can plug into a Go process automatically.

Keep dependencies and abstractions minimal. Avoid speculative support for multiple languages, transports, databases, or cloud providers.

## 4. Customer-provided library

Provide an interface equivalent to:

```go
type CustomerLibrary interface {
    GetScalingGroup(message MessageView) (string, error)
    GetBusinessHash(message MessageView) ([32]byte, error)
}
```

The function names are part of the intended public API. Adapt the exact Go types as needed.

### Input

Pass a borrowed, read-only view of the whole application message:

- Original topic.
- Payload.
- Headers/properties.
- Event identifier.
- Other relevant application metadata.

Avoid unnecessary payload copying or parsing. Make object lifetime and concurrency rules explicit.

The customer function must not mutate or retain borrowed data beyond its documented lifetime.

### Behavior

The library must be:

- Deterministic.
- Local and in-process.
- Fast enough for the intended publisher rate.
- Free of per-message network or database calls.
- Explicitly versioned.

Library failures must reject a publication before durable acceptance, with a useful error.

### Flight Operations example

Group by a stable flight-instance identity:

```text
Operating carrier
+ flight number
+ scheduled departure date
+ flight leg identifier
```

For example:

```text
UA / 123 / 2026-09-25 / ORD–LAX
```

A delay, gate change, and boarding event for that flight produce the same hash.

Do not include the event type, revised departure time, or other mutable fields in this identity.

Use a unique operational leg identifier when airport pair alone is insufficient.

### Baggage example

Group by a stable bag-journey identifier.

A scan, loading event, and arrival event for the same bag journey produce the same hash.

### Hash contract

Specify:

- Canonical field representation.
- Character encoding.
- Normalization rules.
- Missing-field behavior.
- Unambiguous field encoding.
- SHA-256 digest representation.
- Conversion to an unsigned integer.
- Version identifier.

Provide shared test vectors.

Do not use Go’s randomized map iteration, process-dependent hashes, or ambiguous concatenation.

An incompatible library upgrade must not silently change routing beneath live traffic.

## 5. Broker-selection calculation

The publisher shim receives an ordered broker list for the selected scaling group.

Example:

```text
Flight Operations, epoch 12:
[A, B, C]
```

The shim computes:

```text
index = unsigned_integer(business_hash) modulo broker_count
destination = ordered_brokers[index]
```

Interpret the full digest consistently. Specify byte order.

The customer library does not choose a broker. It supplies the scaling group and business hash.

### Membership stability

List order is significant.

Do not independently sort, shuffle, deduplicate, or reorder a received list in each client.

Use stable broker IDs separately from endpoint URLs.

Appending, removing, or reordering brokers can change destinations for many hashes. Every such routing change must use the coordinated membership-transition protocol.

Do not silently substitute rendezvous hashing or consistent hashing in v1. The requested algorithm is modulo over a versioned ordered list.

## 6. Broker 0: retained membership and control messaging

Broker 0 holds a last-value queue for each scaling group.

Each LVQ retains one complete membership snapshot, not a sequence of incremental edits.

An illustrative snapshot:

```yaml
schema_version: 1
scaling_group: flight-operations
revision: 42
phase: ACTIVE
active_epoch: 12
hash_contract: flight-operations-v1
algorithm: sha256-unsigned-big-endian-modulo
brokers:
  - id: broker-a
    endpoints:
      smf: tcps://broker-a.example:55443
    message_vpn: flight-test
  - id: broker-b
    endpoints:
      smf: tcps://broker-b.example:55443
    message_vpn: flight-test
```

Finalize a typed, versioned schema before implementing its consumers.

During a transition, the snapshot must distinguish current membership from proposed membership. A PREPARE notification must never cause publishers to switch immediately.

Include enough information to reconcile:

- Current and proposed epochs.
- Ordered memberships.
- Transition ID and phase.
- Hash contract.
- Relevant destination/queue information.
- Participant requirements or references to them.

Never include passwords or private keys in membership documents.

### LVQ access

An LVQ is not a broadcast mechanism by itself.

Shims must retrieve the retained snapshot non-destructively. Do not consume and acknowledge the shared retained membership message.

Use separate control topics for live notifications and coordination.

Bootstrap safely:

1. Subscribe to control updates.
2. Browse the retained snapshot.
3. Reconcile buffered updates using revisions and epochs.
4. Start application traffic only after obtaining valid ACTIVE state.

Repeat reconciliation after reconnect and periodically to recover from missed notifications.

### Control-message delivery

Membership-change events are hints to reconcile authoritative state.

Transition commands, readiness acknowledgements, and participant registration must have explicit reliable-delivery and retry semantics. Use suitable durable queues where required.

Commands and acknowledgements must be idempotent and scoped to a group, transition, epoch, and participant.

An LVQ cannot replace the controller’s durable transition history or participant-acknowledgement records.

## 7. Publisher shim

Expose a small application API for ordered, guaranteed publication.

For each message:

1. Validate the message.
2. Call the customer library.
3. Find the selected group’s cached membership.
4. Calculate the destination using modulo.
5. Persist the accepted message in a durable local outbox.
6. Publish directly to the selected data broker.
7. Remove it from the outbox only after positive broker acknowledgement.

Attach the business hash, scaling group, routing epoch, and event ID using appropriate Solace message properties.

If native partitioned queues are selected, explicitly set the native Solace partition key. An arbitrary custom hash property is not sufficient.

Preserve the original application topic and metadata. Document any internal topic mapping needed for fencing.

### Ordering and retries

Within the supported publisher ordering contract:

- Later messages for the same business key must not overtake earlier pending messages.
- Reconnects, retries, batching, and concurrency must respect that constraint.
- Independent keys may progress concurrently.
- Uncertain broker acknowledgements may cause duplicates.
- Do not promise exactly-once delivery.

Separate durable local acceptance from broker acknowledgement in the API.

Provide bounded outbox capacity and explicit backpressure. Never silently drop accepted messages.

Persist sufficient routing metadata to recover without recomputing a business hash under a different library version.

Specify how buffered messages are assigned to the new epoch after a completed handover without breaking their per-key order.

### Membership failures

Do not silently switch a key to another broker because its current broker is unreachable.

Before obtaining valid membership, fail closed or buffer within a documented bound.

Define behavior during Broker 0 outages and stale membership. A cached list must not become permission to bypass a transition fence.

## 8. Data brokers and native queues

Each data service hosts the queues required by the scaling groups and subscriber groups assigned to it.

Use native Solace facilities rather than implementing an application message router inside the broker.

Select and document the queue/consumer configuration that satisfies the ordering contract:

- An exclusive queue with suitably serialized processing is a valid simple option.
- Native partitioned queues may allow parallel consumption across business keys when supported and qualified.

If several independent subscriber groups need the same events, preserve pub/sub fanout semantics. Do not confuse independent subscriptions with competing workers.

Do not adopt or mutate arbitrary pre-existing customer queues.

Use an explicit managed namespace and narrowly scoped permissions.

### Critical fencing requirement

A stale publisher must not be able to continue publishing under an old membership after the new membership becomes active.

An epoch header alone does not enforce this.

Design an enforceable broker-side fence using supported Solace mechanisms. Prove its behavior with real-broker tests.

Pay special attention to brokers that remain in both memberships:

```text
Old: [A, B]
New: [A, B, C]
```

Simply reopening the same old destinations could allow stale publishers to resume using the old modulo result.

A possible implementation uses epoch-specific internal destinations and subscriptions, leaving the old epoch fenced while opening the new one. Evaluate and document the actual mechanism before claiming correctness.

Preserve application-facing topics if internal destination mapping is used.

## 9. Subscriber shim

The subscriber shim also uses Broker 0 for discovery and transition coordination.

It must:

- Obtain valid group membership.
- Discover and bind the appropriate native queues.
- Maintain connections across the group’s relevant brokers.
- Prepare destination consumers before handover.
- Continue draining source queues during the transition.
- Report readiness through Broker 0.
- Deliver the original application topic, payload, and headers.
- Acknowledge only after successful application processing.

Application handlers must not accidentally introduce out-of-order concurrent processing for the same business key.

The customer library may optionally validate the message’s grouping/hash contract on receipt. It must not independently reroute consumed messages.

Explain application-level idempotency using event IDs. Do not add a pretend exactly-once layer.

Define poison-message behavior. Do not silently skip a failed message and continue later events for the same key while claiming strict ordering.

## 10. Controller

The controller has five responsibilities:

1. Observe data-broker and workload pressure.
2. Apply capacity and feature-placement policies.
3. Request appropriate capacity changes through Solace Cloud.
4. Coordinate safe scaling-group membership transitions.
5. Publish authoritative membership state through Broker 0.

It does not hash every application message or make per-message routing decisions.

### Observability

Use SEMP measurements and native broker events where appropriate.

Broker events can trigger a measurement refresh. Do not describe SEMP itself as a streaming subscription.

Observe the resource dimensions needed for sound decisions, including throughput, backlog, spool, connections, and telemetry freshness.

Distinguish broker saturation from slow downstream application processing.

Account for shared broker pressure across scaling groups.

### Capacity decisions

Use explicit measured capacity profiles and declared feature constraints.

Do not relabel measurements from another broker version as validated capacity.

Support:

- Minimum and maximum broker counts.
- Warm capacity.
- Sustained-pressure windows.
- Headroom targets.
- Cooldowns.
- Bounded concurrent transitions.
- Safe refusal when no eligible destination exists.

Changing broker membership is only one step of scaling; readiness and handover must finish before traffic uses new capacity.

### Cloud operations

Use a narrow provider interface and a Solace Cloud implementation.

Provisioning must be idempotent and recoverable after uncertain API responses.

Track the exact resources created by this system. Never delete resources solely because their names resemble a test prefix.

Cloud credentials belong only to the controller, never the shims.

Default examples must not incur cloud charges. Paid provisioning requires explicit operator enablement and resource/cost constraints.

Scale-in is not “remove URL, then delete broker.” Drain and ownership checks must complete, and no other scaling group may still depend on the service.

## 11. Coordinated membership transition

For v1, implement a group-wide pause-and-drain protocol.

Do not claim uninterrupted publishing during membership changes.

Example:

```text
Flight Operations:
  ACTIVE epoch 12: [A, B]
  Proposed epoch 13: [A, B, C]

Illustrative hash 5:
  Old destination: 5 % 2 = 1 → B
  New destination: 5 % 3 = 2 → C

Baggage Tracking:
  Remains on its existing membership and epoch.
```

### Prepare

- Prepare the new capacity and destinations.
- Keep proposed-epoch ingress disabled.
- Persist the transition.
- Publish PREPARE state through Broker 0.
- Have subscribers bind destination queues while retaining source bindings.

### Establish readiness

- Define the required participant set explicitly.
- Record readiness acknowledgements with participant identity and epoch.
- Verify broker-side consumer readiness as appropriate.
- Do not infer readiness from elapsed time alone.

### Pause and fence

- Request publisher pause through Broker 0.
- Buffer new messages durably.
- Establish broker-side fences against old-epoch writes.
- Resolve or retain in-flight publications according to acknowledgement outcomes.

Safety must not depend on simultaneous notification delivery.

### Drain

- Drain all affected old-group destinations.
- Verify queued messages, stored messages, and unacknowledged deliveries.
- Require continuous drain evidence and the configured grace interval.
- Treat missing, stale, or failed telemetry as unknown—not zero.

If an unavailable broker prevents proving drain, stop and follow an explicit recovery procedure. Do not route around it and claim preserved order.

### Commit and activate

- Recheck destination readiness and source fences.
- Durably commit the new membership.
- Publish the committed state through Broker 0.
- Enable the appropriate new-epoch destinations.
- Resume buffered publication in the supported per-key order.
- Keep old-epoch destinations fenced.
- Retain recovery evidence until cleanup is safe.

The controller’s durable commit and publication to Broker 0 are separate operations. Make their reconciliation idempotent; do not assume an atomic transaction spanning both.

### Recovery

Before commit, rollback is permitted only when the old ownership and safe destination state are provable.

After commit, recover forward. Do not casually revert to an old membership.

Controller restart, Broker 0 restart, duplicate commands, stale acknowledgements, and reconnects must preserve these rules.

## 12. Configuration and repository shape

Keep the repository small and understandable.

A suggested structure:

```text
README.md
go.mod
go.sum
config.example.yaml
cmd/controller/
controller/
control/
shim/publisher/
shim/subscriber/
customer/
examples/
tests/
```

Adjust only where it clearly simplifies implementation.

Provide one concise configuration example covering:

- Broker 0 connectivity.
- Existing data-broker inventory.
- Two scaling groups.
- Customer-library contract versions.
- Queue/consumer choices.
- Capacity and scaling policy.
- Handover timings.
- Persistent state paths.
- Cloud provisioning disabled by default.

Use environment variables or secret references for credentials.

Do not add a large collection of demos, guides, deployment frameworks, or overlapping configuration formats.

Provide two small runnable examples: one publisher and one subscriber. Include Flight Operations and Baggage Tracking customer functions without building a separate demonstration framework.

## 13. Verification

Build tests around invariants and failures, not only happy-path examples.

Required coverage:

- Deterministic grouping and hash test vectors.
- Full-digest modulo calculation.
- Stable membership ordering.
- Invalid and incompatible membership rejection.
- Independent scaling-group behavior.
- LVQ non-destructive reads.
- Startup/reconnect update reconciliation.
- Durable outbox recovery and bounded storage.
- Broker ACK uncertainty and duplicate delivery.
- Ordered retry behavior for the same key.
- Destination readiness before source handover.
- Source fencing before ownership changes.
- No commit with outstanding unacknowledged messages.
- Stale publishers remaining blocked after new activation.
- Controller restart at every transition boundary.
- Duplicate and out-of-order control messages.
- Broker 0 outage and recovery.
- Poison messages, disk-full behavior, and cancellation.
- Isolation of unrelated groups and queues.

Provide real Solace integration tests for behavior that mocks cannot prove, especially LVQs, queue access semantics, partition-key mapping, acknowledgements, and ingress fencing.

Never silently skip a failed qualification and report the system as production-ready.

Benchmark the customer callback separately from the complete publishing path. Measure durable-storage, serialization, connection, and broker costs rather than assuming hashing is the bottleneck.

## 14. Delivery expectations

Implement a working vertical slice before expanding scope:

1. Two groups, static membership, three existing data brokers.
2. Broker 0 retained membership and update reconciliation.
3. Native publication and consumption with customer grouping.
4. Durable retries and bounded backpressure.
5. One fully tested membership transition.
6. Restart and failure recovery.
7. Automated capacity decisions and optional cloud provisioning.

Keep the README clear enough for a customer to understand:

- What the customer library controls.
- How the hash selects one broker.
- What Broker 0 retains.
- How subscribers discover destinations.
- What happens when membership changes.
- How this differs from DMR.
- What is implemented, tested, and still unqualified.

Do not substitute a diagram or a large design document for working code and evidence.

At completion, report:

- Implemented behavior.
- Commands to install and run the small examples.
- Unit and real-broker test results.
- Measured performance and test conditions.
- Exact remaining operational limitations.
- Any cloud resources created and their cleanup status.

The central requirement is:

**The customer library defines which messages must remain ordered together. The publisher shim sends those messages in order to one selected Solace broker. Broker 0 distributes versioned membership, and the controller coordinates capacity changes so a membership transition does not break that broker-affinity contract.**