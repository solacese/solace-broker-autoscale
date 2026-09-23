# Automatic scaling validation — 17 September 2026

The managed queue controller and reference SMF integration are implemented and locally validated. This is not evidence of a completed production Solace Cloud rollout.

## Checks completed

| Check | Result |
|---|---|
| Unit and regression suite | 418 passed; four optional local/workbook tests skipped; eight live protocol tests excluded from this unit run. |
| Real two-broker migration | Passed using two isolated local Solace Docker brokers and the native SMF client. |
| Ruff | Clean for source, tests, Python adapters and payment example. |
| Mypy | Clean across 58 source files. |
| Supplied Cloud profile data | All 2,142 paired observations matched 4,284 original cached spreadsheet cell values; source hashes verified across four workbooks and 15 tier profiles. |
| Exact runtime capacity queries | All 2,142 exact queries matched their source directions, fanout and cell references. |
| Example configuration | Every measured-example configuration and inventory parsed successfully. |
| Python client packaging | Adapter editable installation and command help checked; both package imports verified. |
| Landing page | Desktop and 390px mobile visually inspected; light/dark theme, routing, six-stage handover and YAML copy exercised. No JavaScript errors or page-level horizontal overflow. |

The live migration test starts with ten payments accepted on the original broker and an application handler that deliberately pauses processing. It prepares a consumer on the destination, fences source ingress, observes rejection of a stale publisher, and retains that payment in a durable outbox. After the original payments complete in order, it resumes migration from persisted state, changes ownership, retries the buffered payment, and verifies all eleven event IDs are delivered in sequence. The old queue is empty and remains fenced.

Additional tests cover pending acknowledgments, unavailable telemetry, continuous-empty timing, missing destination consumers, pre-commit rollback, forward recovery after commit, measured load selection, warm activation, assignment concurrency, persistent routing contracts, outbox recovery and backpressure, and independent-partition progress when another partition is rejected.

Cloud lifecycle tests use mocks. They cover persistent unique-name retries after an uncertain create result, bounded exact-ID cleanup, backoff, kill-switch refusal, service identity/region/class/version mismatch rejection, and successful SEMP readiness attachment. The response shape was checked against the official [Mission Control service API](https://api.solace.dev/cloud/reference/getservice) and its embedded OpenAPI schemas. Regional API configuration follows the [official API base URLs](https://api.solace.dev/cloud/reference/using-the-v2-rest-apis-for-pubsub-cloud).

A real Cloud qualification on 22–23 September 2026 found no Enterprise 100K quota, then used the largest available two-service tier: Enterprise 5K HA. Its managed fanout/migration run reconciled 112 accepted events in both groups, with zero duplicates and preserved per-account order through publisher SIGKILL/restart. The largest available single-service tier, Enterprise 10K HA, subsequently completed four bounded AMQP-over-TLS cases (256 B steady, 4 KiB fanout, 64 KiB burst and a slow consumer); every accepted ID reached every expected group with zero duplicates and zero per-account order violations. The sequential client path did not seek saturation and does not establish broker throughput. All exact-ID resources were deleted. See [the qualification record](cloud-qualification-2026-09-22.md).

## Remaining rollout requirements

- Validate real Cloud token permissions, selected endpoint reachability, available exact broker revision and application credentials in the intended environment.
- Replace planning-default VPN limits with the actual deployed limits before actuation. The genuine throughput data does not establish those configuration limits.
- Test sustained representative traffic, payload distributions and failure scenarios. The reference synchronous publisher and thread-per-flow consumer have not been performance qualified for the benchmark throughput.
- Use a dedicated homogeneous managed fleet; unmanaged traffic and connection pressure are outside this partition planner. Provide upstream backpressure, persistent local disks and transactional business idempotency.
- Configure external monitoring of JSON states such as `capacity-shortfall`, `feature-pinned`, `no-decision`, `retry` and `limited`.
- Multi-host controller HA, automatic broker deletion/scale-in, arbitrary queue adoption and backlog copying remain outside this implementation.

Private raw Cloud journals and connection bundles remain ignored under `state/`; customer data and credentials are not committed. Public examples use illustrative loads and invented fixtures. The qualification created and deleted only its exact journaled test-service IDs and did not modify existing services.

## Native topic API — 2026-09-18

Added `MessagingClient.publish()` / `subscribe()`, topic-level key extraction, durable group
registration, owner-prefixed SMF publication and native queue subscriptions. The controller fences
and drains every group queue before changing ownership.

- Unit suite: 262 passed, 2 skipped (legacy simulator workbook fixtures absent), 6 integration tests deselected.
- Real local two-broker suite: native topic handover and legacy direct-queue handover passed.
- Native coverage: wildcard selectivity, broker fanout, overlapping subscriptions, exclusive group
  replicas, no-subscription NACK, profile/subscription reconciliation, fenced publisher buffering,
  source drainage and controller recovery during a move.
- Unit coverage also includes partial group fence failure, readiness, immutable groups/contracts,
  cached topic-key extraction and publisher outbox recovery.
- Ruff clean; mypy clean across 59 source files. Complete native YAML parses successfully.
- Project page checked at desktop and mobile sizes, with no page overflow or JavaScript errors.

This is protocol/correctness evidence from disposable local brokers. It does not qualify production
Cloud deployment, DMR interoperability or throughput. Cloud provisioning remains mock-tested.
