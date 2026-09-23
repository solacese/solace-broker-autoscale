# Enterprise Cloud qualification — 22–23 September 2026

## Outcome

The authorized preflight completed against the Solace Cloud US control plane. Enterprise 100K HA had no available account quota, so the largest feasible tests were one Enterprise 10K HA service and two independent Enterprise 5K HA services.

The two-service 5K run completed the managed topic/fanout/migration/crash-recovery path. A separate 10K service completed a bounded AMQP-over-TLS matrix covering small steady messages, two-group fanout, 64 KiB bursts and intentionally slow consumers. Every accepted event reached every expected group with zero duplicates and zero per-account ordering violations. These are functional and client-path observations, not saturation measurements or production-readiness claims.

## Scope and accounting

- Region: `eks-us-east-1a`
- Requested maximum: `ENTERPRISE_100K_HIGHAVAILABILITY`
- Largest provisioned single service: `ENTERPRISE_10K_HIGHAVAILABILITY`
- Largest two-service split: `ENTERPRISE_5K_HIGHAVAILABILITY`
- Broker version: `10.26.0.8894-14`
- Successful 5K split run: 2 services created and deleted
- 10K lifecycle: 5 services allocated and deleted across harness diagnosis and the final run; 2 intervening creates were rejected without allocation while quota release was pending
- Successful 10K bounded matrix: the final service completed four workload cases and was deleted
- Qualification services remaining after verification: 0; the 10K quota also reported `inUse: 0`
- Billing basis: the user explicitly attested that this is an internal non-billable account.
- API-visible EUR rate: unavailable. The billing API returned no usage products and exposes PCU usage rather than a currency rate.
- Actual monetary charge: unavailable from the API. The account was attested non-billable; elapsed service lifetimes and exact IDs remain in ignored private journals.

The initial 100K request exposed a missing explicit endpoint-port requirement and was rejected before allocation. After correction, 100K was rejected with `event broker limits for service class 'enterprise-100k-tera' reached`. Account limits showed one 10K slot and six 5K slots, so the test used the largest feasible classes without touching existing services.

The 5K split run accepted 112 events, delivered all 112 to both ledger and audit, reported zero duplicates and preserved per-account order. It reached destination binding, broker-enforced source fencing, publisher SIGKILL, same-outbox restart, source drain, ownership commit and destination activation in 35.5 seconds. A repeated 5K run exposed that the local 45-second timeout was too short for Cloud drain; it rolled back without changing ownership, and cleanup deleted both services.

The 10K matrix used the managed Go AMQP client over TLS. The numbers below are wall-clock observations from a single remote client and include startup, durable local-outbox dispatch, network transit, broker handling and consumer processing. The sequential harness did not seek saturation, so these values must not be interpreted as Enterprise 10K capacity.

| Case | Payload | Fanout | Accepted | Processed per group | Loss / duplicates / order violations | Acceptance p50 / p95 / p99 | Processing p50 / p95 / p99 |
|---|---:|---:|---:|---|---|---|---|
| Small steady | 256 B | 1 | 200 | 200 | 0 / 0 / 0 | 8.6 / 18.5 / 22.6 ms | 11.00 / 19.01 / 19.74 s |
| Medium fanout | 4 KiB | 2 | 200 | 200, 200 | 0 / 0 / 0 | 8.5 / 18.1 / 29.9 ms | 11.57 / 20.08 / 21.52 s |
| Large burst | 64 KiB | 2 | 100 | 100, 100 | 0 / 0 / 0 | 10.1 / 17.3 / 38.9 ms | 7.09 / 12.47 / 12.86 s |
| Slow consumer | 1 KiB | 2 | 100 | 100, 100 | 0 / 0 / 0 | 8.1 / 11.0 / 12.1 ms | 7.02 / 12.57 / 13.05 s |

The long processing percentiles are client-path end-to-end measurements and were dominated by the reference client’s bounded sequential dispatch behavior; the run did not identify a broker bottleneck. The slow-consumer case added 20 ms in each handler and still reconciled every event. SEMP snapshots showed zero residual spool after every case.

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

They match the exact one-step oracle. They are not Enterprise 100K measurements. The 10K run supplies bounded client-path observations, not a capacity-derived before/after chart.

## Remaining blockers and next experiment

1. Raise or free the organization's Enterprise 100K quota before claiming 100K behavior.
2. Replace the sequential reference publisher with a controlled concurrent generator before attempting a sustained 10K throughput or saturation claim; record generator CPU and network saturation separately.
3. Expand the 5K matrix with sustained rate ramps and explicit destination/network interruption now that the core Cloud handover is proven.
4. Keep transaction, replay, tracing and DR migration pinned. Two ordinary HA services are not a DR pair.
5. Define workload-specific SLOs before a longer soak so the next run has explicit pass/fail thresholds rather than retrospective interpretation.
