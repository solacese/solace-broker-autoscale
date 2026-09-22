# Enterprise 100K Cloud qualification — 22 September 2026

## Outcome

The authorized preflight completed against the Solace Cloud US control plane. No workload benchmark or two-service split was executed because the first service creation request was rejected before allocation by the organization's service-class limit.

This is a useful operational finding, not a successful Cloud qualification and not a production-readiness claim.

## Scope and accounting

- Region: `eks-us-east-1a`
- Requested service class: `ENTERPRISE_100K_HIGHAVAILABILITY`
- Requested broker version: `10.26.0.8894-14`
- Planned independent services: 2
- Created services: 0
- Deleted services: 0
- Qualification services remaining after verification: 0
- Billing basis: the user explicitly attested that this is an internal non-billable account.
- API-visible EUR rate: unavailable. The billing API returned no usage products and exposes PCU usage rather than a currency rate.
- Actual monetary charge: unavailable; no service was allocated.

The first request initially exposed a missing explicit endpoint-port requirement in the harness and was rejected with HTTP 400 before allocation. After that request was corrected and covered by tests, the Cloud API rejected the service with `event broker limits for service class 'enterprise-100k-tera' reached`. Exact-name inventory checks after both attempts found no created qualification service.

## Verified preflight facts

- The target datacenter was available and listed `ENTERPRISE_100K_HIGHAVAILABILITY`.
- Version `10.26.0.8894-14` was available, compatible with the datacenter, and marked recommended.
- The service collection was readable.
- Existing services were not modified.
- Existing service records advertised deletion as an allowed action, but this did not prove the token could create a new 100K service.
- Mission Control documents no non-mutating create-permission or quota reservation endpoint; the create request was therefore the first authoritative write-path check.

## Harness improvements

The new bounded lifecycle harness:

- validates the exact names, region, class, version, HA/deletion settings, count and deadline;
- journals create intent, idempotency key, operation ID and exact service ID to an ignored `0600` file;
- reconciles uncertain create responses by exact requested name only;
- polls asynchronous operations with a deadline;
- deletes only exact IDs recorded in the run journal;
- gives cleanup an independent deadline and verifies each service is absent;
- redacts service IDs, hosts, credentials, authorization values and payloads from shareable output.

Run read-only preflight:

```bash
PYTHONPATH=scaling-controller .venv/bin/python scripts/cloud-qualification.py --preflight-only
```

Creation remains explicit with `--create`. The current runner deliberately refuses to leave services running until the workload phase is configured, and invokes cleanup in `finally`.

## Customer-facing before/after status

The three checked-in examples remain **synthetic normalized planner illustrations**:

- Payment burst: A 95% → A 50%, B 45%.
- Gradual growth: A 90% → A 55%, B 35%.
- Byte-heavy traffic: A 50%/95% messages/bytes and B 45%/4% → A 30%/35%, B 65%/64%.

They match the exact one-step oracle. They are not Enterprise 100K measurements. There is no credible Cloud before/after chart from this run because no 100K service could be allocated.

## Remaining blockers and next experiment

1. Raise or free the organization's Enterprise 100K service quota without touching existing services.
2. Re-run the same exact-name preflight and bounded lifecycle.
3. Only after both independent HA services are ready, run single-service payload/fanout/consumer baselines and the managed two-service handover/failure matrix.
4. Keep transaction, replay, tracing and DR migration pinned. Two ordinary HA services are not a DR pair.
5. Record generator CPU/network saturation separately so a client or WAN ceiling is never presented as broker capacity.
