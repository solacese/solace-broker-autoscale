# Safety (§10)

The actuator is the only component that can damage a running system. These rules are enforced in
`actuator/safety.py` **before any operation is issued**. They are not optional and not configurable
away.

## The actuator is off by default

- `actuation.mode` defaults to `recommend`. In recommend mode the actuator is **never constructed** —
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
| 4 | Refuse to delete a broker with non-zero queue depth, bound consumers, active flows, or spooled messages — checked LIVE immediately before the call | `_check_safe_to_delete` |
| 5 | Never scale below `min_brokers` or above `max_brokers` | `_check_bounds` |
| 6 | Honour `max_ops_in_flight` and `max_ops_per_hour` | `_check_rate_limits` |
| 7 | Check `kill_switch_file` before every operation; if present, halt and log | `_check_kill_switch` |
| 8 | Write the audit record BEFORE issuing (decision id, model version, config hash, full request body) | `AuditLog` + intent phase |
| 9 | Idempotency key on every Cloud API call so a retry cannot double-provision | `_check_idempotency_key` + client header |

Mode is also checked: `scale-up-only` refuses deletes; `recommend` should never reach the gate at
all (no actuator exists).

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
  behaviour, not a bug to optimise away — a slow-but-progressing drain stays `DRAINING`, not `STUCK`.

## Warm pool

Maintain `policy.warm_pool` pre-provisioned idle brokers so activation is seconds. Actual
provisioning duration is recorded on every create and fed back into `minutes_to_capacity` (§5.7),
replacing the assumption with a measured value. Warm brokers are billed idle capacity, so their cost
is reported on every run.

## Terraform for identical config

`deploy/terraform/` templates the broker **configuration** (VPN, queues, ACLs, client profiles, DMR
links) so a new broker matches its peers. The actuator triggers `apply` for config rather than
hand-rolling SEMPv2 calls, keeping its blast radius to provisioning only.

## Testing

Every guardrail has a test that proves the operation is refused (`tests/test_actuator_safety.py`).
The drain state machine is tested including the stall→`STUCK` path and the refusal to delete from
any state but `DRAINED` (`tests/test_drain.py`). **No test issues a real Cloud API call** — a
`FakeCloud` records calls instead.
