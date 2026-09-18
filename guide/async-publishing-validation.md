# Asynchronous publishing validation — 18 September 2026

The managed native shim saves locally, then submits persistent SMF messages asynchronously. Broker receipts complete delivery attempts; subscribers are independent. The message property requesting an immediate broker receipt avoids the idle ACK batching timer without blocking the application.

## Evidence from this checkout

- Unit/regression suite: **286 passed, 2 skipped, 6 integration tests deselected**. The two skips require an unavailable performance workbook fixture.
- Two real local Solace broker tests: **2 passed in 7.77 seconds**. This is total functional-suite duration, not a throughput benchmark.
- Ruff passed across source, tests, adapters and payment examples.
- Mypy passed for 60 controller/source files.
- The short example policy compiles and the actual explain command reports account-42's payments partition.
- The website's copyable YAML exactly matches the validated short example; HTML IDs are unique. No new browser visual qualification or website publication was performed.

The real native test covers independent ledger/audit subscriptions, competing ledger replicas, no-subscription rejection, slow-handler retry, destination preparation, source ingress fencing, durable publication retry, recovered migration, ordered cutover, and 128 additional ordered publications while policy refresh is unavailable. Irrelevant inventory subscriptions do not bind payments receivers. The second real test exercises the legacy managed-queue migration path.

Focused regression tests cover independent asynchronous partitions, retaining a rejected head, refreshed routing after rejection, late receipt isolation, restart retention, stale ownership despite a long lease, additive topic contracts, scoped groups, simple policy validation and avoiding fictional backlog transfer.

## Important findings resolved during validation

The SDK requires the receipt listener to be installed after publisher startup. Sending one ordered message at a time also interacts badly with the broker's default ACK batching timer: the first burst test exposed roughly one confirmed publication per second for that partition. The shim now uses Solace's supported ACK Immediately property; the expanded real test then passed.

An interrupted shutdown can produce an SDK incomplete-delivery exception. The shim now handles that expected condition, disconnects and leaves unconfirmed publications on disk.

## Remaining qualification

No production Cloud provisioning, sustained capacity benchmark, disk-loss resilience or multi-host controller failover was tested. Same-partition throughput still depends on broker receipt round-trip latency and durable disk performance. Multiple publishers sharing a business key do not acquire a global application transaction order.

Native group-copy byte counters are no longer multiplied by fanout a second time. Original-publication rate is still conservatively approximated from queue copies; profile/fanout calibration and management polling at scale need qualification. Stored backlog stays on its source until consumed.

The short policy simplifies configuration; it does not remove the need for a measured profile, prepared inventory, persistent storage and idempotent business handlers.

## Publication snapshot

The isolated Git-index export passed **284 tests, with 4 skipped and 6 integration tests deselected**. The additional skips are optional local-demo checks because that directory is deliberately excluded from publication. Lint and type checking passed again. A secret scan found only the disposable CI broker's documented admin/admin basic-auth fixture.

This update is published on codex/autoscaler-reliability. The newer main branch contains a separate repository reorganization and Go shim implementation; integration with that work is not represented as complete.
