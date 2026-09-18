# Safety (§10)

The actuator is the only component that can damage a running system. These rules are enforced in
`actuator/safety.py` **before any operation is issued**. They are not optional and not configurable
away.

## The actuator is off by default

- `actuation.mode` defaults to `recommend`. In recommend mode the actuator is **never constructed** -
  `build_actuator` returns `None` (ADR 0004). There is no object to misuse.
- Building the actuator (Phase 4) does not change the default. Flipping to `scale-up-only`/`full`
  requires an explicit config edit by the user.
- Even constructed, `dry_run: true` (default) makes every operation log-what-it-would-do and return
  without issuing.

## Guardrails (each has a test proving refusal)

| # | Rule | Enforced by |
|---|---|---|
| 1 | `dry_run: true` logs and returns without issuing | `_dry_run` branch |
| 2 | Refuse all actuation when the capacity model is synthetic | `_check_model_not_synthetic` |
| 3 | Refuse when metrics are stale beyond `staleness_limit` | `_check_metrics_fresh` |
| 4 | Refuse to delete a broker with non-zero queue depth, bound consumers, active flows, or spooled messages - checked LIVE immediately before the call | `_check_safe_to_delete` |
| 5 | Never scale below `min_brokers` or above `max_brokers` | `_check_bounds` |
| 6 | Honour `max_ops_in_flight` and `max_ops_per_hour` | `_check_rate_limits` |
| 7 | Check `kill_switch_file` before every operation; if present, halt and log | `_check_kill_switch` |
| 8 | Write the audit record BEFORE issuing (decision id, model version, config hash, full request body) | `AuditLog` + intent phase |
| 9 | Idempotency key on every mutation; server support and caller reconciliation are also required | `_check_idempotency_key` + client header |
| 10 | When `require_confirmation` is set, refuse a real (non-dry-run) operation the caller did not confirm (`Operation.approved`) | `_check_confirmation` |

Mode is also checked: `scale-up-only` refuses deletes; `recommend` should never reach the gate at
all (no actuator exists).

`require_confirmation` (rule 10) keeps the gate deterministic: it never prompts. The caller collects
the operator's confirmation out of band and passes it in as `Operation.approved`; a missing
confirmation is refused and audited exactly like any other guardrail. Dry-run operations issue
nothing and are exempt.

TLS certificate verification is **on by default** wherever the tool connects to a broker
(`SempCollector(..., verify=True)`). The `monitor` CLI exposes an explicit `--insecure` flag to
disable it for a broker with a self-signed certificate you trust; using it prints a stderr warning
naming the host.

## Drain state machine

`ACTIVE → DRAINING → DRAINED → DELETING → GONE`, with `STUCK` for stalls.

- Entering `DRAINING` blocks new assignments (the assignment store's `DRAINING` state).
- `DRAINED` requires zero queue depth AND zero bound consumers held **continuously** for a settle
  period. A transient dip to zero resets the timer; a `DRAINED` broker that refills falls back to
  `DRAINING`.
- Only `DRAINED` may transition to `DELETING`.
- A drain that does not reach `DRAINED` within the stall timeout goes to `STUCK`, which requires
  operator intervention and **never** auto-resolves into deletion.
- With large messages and guaranteed delivery a drain can take a long time. That is correct
  behaviour, not a bug to optimise away - a slow-but-progressing drain stays `DRAINING`, not `STUCK`.

## Warm pool

`policy.warm_pool` is a planning assumption for recommendations and a desired spare count for
`run` with `provisioning.enabled`. The automatic controller reconciles unique Cloud names,
waits for readiness, and replenishes spares within the active-plus-warm broker ceiling.
It does not currently learn readiness durations automatically. See [automatic scaling](automatic-scaling.md).

Managed queue handover uses a separate persisted phase machine from broker deletion above.
It requires broker-enforced ingress rejection, zero queued/unacknowledged/spooled messages,
a continuous empty interval and bound destination consumers. It never deletes either broker.

## Terraform and direct SEMP

`deploy/terraform/` contains configuration templates. The CLI does not execute Terraform.
Pre-delete observations require an explicit `SempConnection` per service, with broker management
credentials separate from the Cloud API token. The previously assumed Cloud monitor proxy is not
used. Every queue/topic-endpoint page is inspected, missing counters refuse deletion, and remaining
client connections also block it. Publishers must be quiesced; inspection is not an atomic lock.

## Testing

Every guardrail has a test that proves the operation is refused (`tests/test_actuator_safety.py`).
The drain state machine is tested including the stall→`STUCK` path and the refusal to delete from
any state but `DRAINED` (`tests/test_drain.py`). **No test issues a real Cloud API call** - a
`FakeCloud` records calls instead.
