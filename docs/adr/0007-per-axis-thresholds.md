# ADR 0007: Per-axis thresholds and ratio-to-threshold comparison

## Status
Accepted.

## Context
A broker's capacity is not a single number. It has four distinct axes — message rate, byte rate,
connection count, spool bytes — each with its own failure mode and its own safe operating fraction.
Spool filling to 100% is catastrophic and slow to recover; connection count hitting the class limit
is a hard wall; byte rate saturating a link degrades gracefully. Treating them with one global
threshold either wastes capacity on the forgiving axes or runs the dangerous ones too hot.

## Decision
Each axis has its **own** headroom threshold (`policy.headroom.{messages,bytes,spool,connections}`),
and the binding axis is chosen by **ratio-to-its-own-threshold**, not raw utilisation:

```
binding_axis = argmax_axis ( demand_ratio[axis] / effective_threshold[axis] )
```

Spool defaults to the most conservative threshold (0.60) because its failure mode is the worst;
connections to the most permissive (0.85) because the limit is hard but the approach is not
destructive.

## Consequences
- The engine computes all four axes every run (`workload.bottleneck: auto`) and reports the binding
  one, so an operator sees *why* a shard needs brokers, not just that it does.
- Because comparison is ratio-to-threshold, an axis at 62% against a 0.60 spool ceiling correctly
  outranks an axis at 80% against a 0.85 connections ceiling.
- Derived headroom (ADR 0006) is applied per axis, so a fast-growing axis can pull its own threshold
  down without dragging the others with it.
