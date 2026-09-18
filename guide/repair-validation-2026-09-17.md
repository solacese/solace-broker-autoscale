# Initial repair validation — 17 September 2026

Historical checkpoint before the managed controller work. See [the later automatic scaling validation](automatic-scaling-validation.md) for the current result.

- Baseline: 137 unit tests passed; 2 workbook tests skipped; 4 integration tests deselected.
- Final: **169 unit/regression tests passed**; the same 2 workbook tests skipped.
- **4 live protocol integration tests passed:** REST, AMQP, MQTT and SMF.
- Ruff passed for src, tests and Python adapters. Mypy passed across 44 source files.
- Synthetic simulator: **1,200 matrix cells**, all invariants passed.
- Quickstart recommendation: 1 → 2 brokers; what-if 1× → 2, 2× → 4, 4× → ceiling warning.
- Live local SEMP: confirmed spool values are already bytes using the broker OpenAPI schema.
- Live pagination: traversed the temporary broker's queue collection with two entries per page,
  including separate child-flow count requests (150 read requests), and recovered the pending message.
- An existing Cloud broker with stored messages and an active connection correctly failed the empty-broker deletion precondition. Customer telemetry is omitted here.
- The old Cloud monitor-proxy URL returned HTTP 404; direct SEMP with explicitly supplied management
  credentials replaced it. Cloud bearer credentials and broker credentials use separate HTTP clients.
- No production broker was created, changed or deleted. The temporary local Docker broker was removed.

The live tests exercise messaging and monitoring, not automatic cloud provisioning or migration.
The managed handover controller was added after this checkpoint; production Cloud validation remains outstanding.
All existing local changes were preserved. Work is on `codex/autoscaler-reliability`, not pushed.
