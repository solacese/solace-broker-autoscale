# ADR 0010: Mission Control identifiers are a single source of truth

## Status
Accepted. Refines [ADR 0001](0001-solace-cloud-only.md) (Solace Cloud only) by pinning *which*
Solace Cloud identifiers we accept and send, and supports the actuator described in §10.

## Context
The tool is a Solace Cloud extension: it provisions and resizes Mission Control event broker
services. Every value it sends to that API — the service class, the region it talks to, the status
strings it interprets while awaiting an async operation — is defined by the Mission Control REST
API, not by us. Before this decision those identifiers leaked into the code as free-form strings:
`fleet.service_class` was a label like `enterprise-10k` that nothing validated, the client only knew
the US base URL, and operation status was compared as bare strings wherever a poller would live.

A free-form identifier fails in the worst place — a Cloud API round-trip, or worse, a silent
mis-read of a status — instead of at config load. And a US-only base URL makes the tool unusable for
the AU/EU/SG control planes that the same API serves.

## Decision
`solace_autoscale/cloud/enums.py` is the **single source of truth** for Solace Cloud identifiers,
mirrored verbatim from the authoritative Mission Control OpenAPI (version 2.0.0). It is the Cloud
analogue of `capacity/schema.py`: pure data and validation, no I/O. It defines:

- `ServiceClassId` — the 15 real tiers (`DEVELOPER`, and `ENTERPRISE_{250,1K,5K,10K,50K,100K,200K}_
  {STANDALONE,HIGHAVAILABILITY}`).
- `OperationStatus` (`PENDING/INPROGRESS/SUCCEEDED/FAILED`), `ServiceCreationState`
  (`…/COMPLETED/…` — note *not* `SUCCEEDED`), `ServiceAdminState`
  (`INITIAL/START/STOP/DESTROY`).
- `Region` → base-URL map for US / AU / EU / SG and the static-ip variant.
- `resolve_service_class()`, accepting a friendly label (`enterprise-10k`) *or* a raw
  `ServiceClassId`, and raising with the valid options on a typo.

`fleet.service_class` keeps its friendly-label form (so existing config and cost tables keyed by the
label keep working) but is validated at load through `resolve_service_class`, and exposes
`service_class_id` for the canonical id to send. A `cloud:` config block selects the region (or an
explicit `base_url` override) plus `datacenter_id`, `idempotency_header`, and `timeout`.

**The API token is never in config.** It is read from the environment/secret store by the caller, so
it cannot leak into the hashed config or an audit record (§10, and the no-secrets rule).

## Consequences
- A bad service class or region fails at config load with a message listing valid options, not in a
  Cloud API call.
- Non-US control planes are reachable by setting `cloud.region`.
- A future operation poller compares against `OperationStatus.SUCCEEDED` / `.is_terminal()` and
  against `ServiceCreationState.COMPLETED`; an unexpected status value raises rather than reading as
  "not done", so spec drift is loud.
- The client gains `getService` and a service-scoped operation path (the id-only
  `multiResourceOperations` path stays as a fallback), matching how the spec documents them.

## What this ADR deliberately does NOT do
- It does **not** fetch `GET /serviceClasses` or `GET /datacenters` at runtime to build the capacity
  model. The compiled-model design ([ADR 0005](0005-python-and-compiled-capacity-model.md)) stands;
  those discovery endpoints exist and are noted here as available for a future compile-time helper,
  not wired into the runtime.
- It does **not** implement the `CloudApiCollector` monitoring source. No real monitoring payload
  sample has been supplied, and we do not invent field names (§1/§14); it stays a documented stub
  that points operators to `metrics.source: semp` or `static`.
