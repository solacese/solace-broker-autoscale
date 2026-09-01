# ADR 0006: Headroom is derived from reaction time, not chosen

## Status
Accepted.

## Context
A utilisation threshold ("scale at 75%") is usually picked by feel. But the *right* threshold is not
a preference — it is whatever leaves enough spare capacity to absorb the load that arrives **while
new capacity is still being added**. If a broker takes 12 minutes to provision and load is growing
5% per minute, a 75% threshold is unsafe: by the time the new broker is live, demand has grown past
100% and messages have already been dropped or spooled to exhaustion.

## Decision
Derive a safe headroom per axis from observed growth and measured provisioning time:

```
safe_headroom = 1 - (peak_growth_rate_per_min * minutes_to_capacity * safety_factor)
effective_threshold = min(configured, safe_headroom)     # never raise the configured value
```

- `peak_growth_rate_per_min`: the 95th percentile of one-minute-over-one-minute growth in the
  binding axis, from metric history.
- `minutes_to_capacity`: measured from the actuator audit log once Phase 4 exists. Before that,
  `fleet.warm_pool > 0 ? 1.0 : 12.0`, **labelled as an assumption** in the output.
- `safety_factor`: default 1.5.

The configured per-axis headroom values are treated as **ceilings**: a derived value may be more
conservative (lower), never less. When the derived value is lower, the report shows both numbers and
the inputs that produced them, prominently — this is one of the most useful things the tool outputs.

## Consequences
- Headroom is explainable and reproducible, not a magic constant.
- A warm pool (fast activation) mathematically justifies a higher threshold; a cold fleet forces a
  conservative one. The config surfaces the trade rather than hiding it.
- `policy.headroom.mode: fixed` opts out and uses the configured values as-is, for operators who
  insist. `derived` is the default.
