# ADR 0004: Recommendation mode is the default and stays that way

## Status
Accepted.

## Context
The actuator can delete a customer's broker. A tool that can do that and defaults to doing it
automatically is a liability. The credibility of the capacity model is unproven until prediction
accuracy has been tracked over real workloads (§7); acting on an unproven model is how you delete
the wrong broker.

## Decision
`actuation.mode` defaults to `recommend` and **stays** `recommend`. In `recommend` mode the actuator
is **never constructed** — not instantiated, not injected, not held behind a dry-run flag. The tool
produces a report and a machine-readable recommendation; a human acts.

Building the actuator (Phase 4) does **not** change this default. Changing it requires an explicit
config edit by the user, made deliberately, in their own YAML. The tool will refuse to flip the
default on the user's behalf in any later turn.

## Consequences
- `recommend` → no actuator object exists in the process. `scale-up-only` and `full` construct it.
- `dry_run: true` is a second, independent safety: even a constructed actuator logs the operation it
  *would* issue and returns, until `dry_run` is explicitly set false.
- A synthetic capacity model hard-blocks actuation regardless of mode (ADR 0005, §10).
- The safety layer (`safety.py`) enforces every guardrail *before* any operation, so even
  misconfiguration cannot bypass the checks.
