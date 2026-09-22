# Enterprise 100K Cloud qualification — 22 September 2026

## Outcome

The authorized preflight completed against the Solace Cloud US control plane. Enterprise 100K HA had no available account quota, so the largest feasible tests were one Enterprise 10K HA service and two independent Enterprise 5K HA services.

The two-service 5K run completed the managed topic/fanout/migration/crash-recovery path. A separate 10K service reached readiness, but the first baseline attempt exposed missing Python-client trust-store configuration and produced no workload measurements. A corrected 10K retry was rejected because the single 10K quota slot had not yet been released by the control plane even though the prior delete operation had succeeded. This is measured qualification evidence, not a production-readiness claim.

## Scope and accounting

- Region: `eks-us-east-1a`
- Requested maximum: `ENTERPRISE_100K_HIGHAVAILABILITY`
- Largest provisioned single service: `ENTERPRISE_10K_HIGHAVAILABILITY`
- Largest two-service split: `ENTERPRISE_5K_HIGHAVAILABILITY`
- Broker version: `10.26.0.8894-14`
- Successful 5K split run: 2 services created and deleted
- 10K baseline attempts: 1 service created and deleted; immediate corrected retry rejected by quota accounting
- Qualification services remaining after verification: 0
- Billing basis: the user explicitly attested that this is an internal non-billable account.
- API-visible EUR rate: unavailable. The billing API returned no usage products and exposes PCU usage rather than a currency rate.
- Actual monetary charge: unavailable from the API. The account was attested non-billable; elapsed service lifetimes and exact IDs remain in ignored private journals.

The initial 100K request exposed a missing explicit endpoint-port requirement and was rejected before allocation. After correction, 100K was rejected with `event broker limits for service class 'enterprise-100k-tera' reached`. Account limits showed one 10K slot and six 5K slots, so the test used the largest feasible classes without touching existing services.

The 5K split run accepted 112 events, delivered all 112 to both ledger and audit, reported zero duplicates and preserved per-account order. It reached destination binding, broker-enforced source fencing, publisher SIGKILL, same-outbox restart, source drain, ownership commit and destination activation in 35.5 seconds. A repeated 5K run exposed that the local 45-second timeout was too short for Cloud drain; it rolled back without changing ownership, and cleanup deleted both services.

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

1. Raise or free the organization's Enterprise 100K quota before claiming 100K behavior.
2. Allow the deleted 10K quota slot to settle, then run the corrected TLS baseline matrix; the current run provides no 10K throughput numbers.
3. Expand the 5K matrix with sustained rate ramps and explicit destination/network interruption now that the core Cloud handover is proven.
4. Keep transaction, replay, tracing and DR migration pinned. Two ordinary HA services are not a DR pair.
5. Record generator CPU/network saturation separately so a client or WAN ceiling is never presented as broker capacity.
