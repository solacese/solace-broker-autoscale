# ADR 0008: KEDA external scaler as an optional adapter, not the core

## Status
Accepted. Refines [ADR 0001](0001-solace-cloud-only.md), which flatly ruled out Kubernetes.

## Context
ADR 0001 rejected Kubernetes outright to keep the actuator, safety model, and config schema from
multiplying across environments. That rejection was too strong. Kubernetes users reasonably ask
whether this tool can participate in a KEDA-driven autoscaling setup, and the honest answer is "yes,
as an adapter, later" — not "no, never".

The obvious Kubernetes-native shape is a [KEDA external scaler](https://keda.sh/docs/2.20/concepts/external-scalers/):
a gRPC service KEDA polls, which drives a HorizontalPodAutoscaler (HPA). The question is whether the
*core* of this tool should become that scaler, or whether the scaler should be a thin adapter on top
of an unchanged, Kubernetes-independent core.

## Decision
The **core stays standalone and Kubernetes-independent.** A KEDA external scaler (gRPC) may be added
in a later phase as an **optional adapter** that calls the existing decision engine and translates
its output to KEDA's interface. It is not a mode of the core, not a dependency of the core, and not
required to use the tool. **This ADR does not implement the adapter.**

## Reasoning
The valuable part to record is *why* the adapter cannot be the core:

- **HPA's interface is a scalar replica count.** The engine's primary product is a per-shard report:
  binding axis, effective-versus-configured headroom, hot-shard detection, model provenance, and the
  warnings that explain *why*. None of that survives flattening to a single integer. The
  recommendation phase — the reasoning, not the number — is the product; an adapter that emits only a
  count discards it. The core must therefore keep producing the full report, and the adapter is a
  lossy projection of it, not a replacement for it.
- **HPA assumes scale-in is a decrement that succeeds.** Removing a broker is not: a drain can take
  hours and can legitimately terminate in `STUCK`, requiring a human. HPA has no vocabulary for "this
  scale-in is in progress and may need intervention". The safety layer (drain controller, pre-delete
  emptiness checks, audit trail) has to live in the core, outside HPA's model of the world.
- **Requiring a cluster and a CRD blocks the highest-value use case.** That use case is an engineer
  running a capacity report in front of an architect — no cluster, no CRD, no operator. Making
  Kubernetes a prerequisite would gate the tool's most important moment behind infrastructure that
  moment does not need.
- **What KEDA does buy, later.** Catalog distribution (KEDA's scaler catalog is a discovery channel),
  and a clean composition story: one loop scales *consumers within a broker* (the KEDA Solace
  scaler) while this tool's loop scales *brokers underneath them*. That composition is worth
  supporting — as an adapter, once the two-loop interaction (see `docs/architecture.md`) is handled
  with asymmetric windows and cooldowns.

## Consequences
- The decision engine, config schema, and safety layer gain nothing Kubernetes-specific; they remain
  usable with no cluster present.
- A future `adapters/keda/` external scaler can wrap the engine without changing it. It will have to
  own the lossy count projection and document what report detail it drops.
- The two independent control loops (KEDA consumers, this tool's brokers) must be composed
  deliberately to avoid oscillation; the mitigations are documented in `docs/architecture.md`.
- ADR 0001's "Solace Cloud managed services only" still holds for the *actuator*: the adapter drives
  the same Cloud-API actuator through the same safety gate, it does not add a self-managed broker
  lifecycle.
