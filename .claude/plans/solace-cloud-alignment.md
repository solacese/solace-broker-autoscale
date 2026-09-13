# Solace Cloud alignment: be a faithful extension of Mission Control

## Goal

Active `/goal`: "explore more features of the solace cloud … read the faq, datasheets, apis,
concepts and implement all necessary changes so we are the perfect extension of it."

Concretely: close the gaps between our config/client and the **authoritative Mission Control
OpenAPI v2.0.0** (`/tmp/mc.json`, title "MISSION CONTROL", 50 paths) so every Solace-facing
identifier we accept, send, or interpret matches the real API — validated, not free-form.

## What is already correct (verified — do NOT touch)

- `update_message_spool` body `{"messageSpoolSizeInGB": size_gb}` — matches spec.
- `DEFAULT_BASE = "https://api.solace.cloud"` — correct US base.
- createService path `POST /api/v2/missionControl/eventBrokerServices`, returns `data.id` (202 op).
- `capacity/schema.py::ServiceClassCapacity.service_class_id` already documents the real
  `ServiceClassId` and source fields (`vpnConnections`, `vpnMaxSpoolSize`).
- Async op model: create/delete return an Operation with `status` in
  `PENDING/INPROGRESS/SUCCEEDED/FAILED` (confirmed in spec schemas `Operation`,
  `MultiResourceOperation`).

## Evidence-based gaps (each verified against code + spec)

1. **`ServiceClassId` is not codified or validated.** `fleet.service_class` is a free string
   defaulting to `"enterprise-10k"` (a friendly label). Tests already put real ids
   (`ENTERPRISE_10K_HIGHAVAILABILITY`) into `request_body`, so a label→id mapping exists only
   implicitly. Nothing rejects a typo like `enterprise-9k`.

2. **No Cloud connection config block.** There is no `base_url`, `region`, `api_token`, or
   `datacenter_id` anywhere in `config.py`. The client is hand-constructed and supports only the
   US base; AU/EU/SG regional bases from the spec are unreachable via config.

3. **Operation status enum is never expressed.** `get_operation`/`get_broker_state` exist on the
   client but no module names the real `Operation.status` / `ServiceCreationState` /
   `ServiceAdminState` enums, so any future poller would hand-type the strings.

4. **`get_operation` uses the multi-resource path with an admitted-uncertain docstring.** The
   spec has both `.../{serviceId}/operations/{operationId}` (service-scoped) and
   `.../multiResourceOperations/{operationId}`. We should support the service-scoped path (which
   the spec documents first) and keep multi-resource as the id-only fallback.

## Approach — one small, reversible module + surgical wiring

Introduce a single **`solace_autoscale/cloud/enums.py`** as the one source of truth for
Solace-Cloud identifiers (mirrors how `capacity/schema.py` is the source of truth for the model).
Everything else references it. No behavior change to the green paths above.

### Commit 1 — codify the Mission Control enums (pure data, no I/O)
- New `solace_autoscale/cloud/__init__.py` and `solace_autoscale/cloud/enums.py`:
  - `ServiceClassId(StrEnum)` — all 15 values from the spec.
  - `OperationStatus(StrEnum)` = PENDING/INPROGRESS/SUCCEEDED/FAILED; helpers
    `is_terminal()`, `is_success()`.
  - `ServiceCreationState`, `ServiceAdminState` StrEnums.
  - `Region(StrEnum)` → base URL map: US `api.solace.cloud`, AU `api.solacecloud.com.au`,
    EU `api.solacecloud.eu`, SG `api.solacecloud.sg`; plus `STATIC_IP` variant. `base_url()`.
  - `SERVICE_CLASS_ALIASES: dict[str,ServiceClassId]` mapping friendly labels
    (`enterprise-10k`, `enterprise-10k-ha`, `developer`, …) → canonical id, and
    `resolve_service_class(value) -> ServiceClassId` accepting either a label or a raw id and
    raising a clear `ValueError` listing valid options on miss.
- Tests `tests/test_cloud_enums.py`: round-trip aliases, reject unknown, terminal/success logic,
  region→base mapping, all 15 ids present.

### Commit 2 — validate `fleet.service_class` and add optional Cloud connection config
- `config.py`:
  - `FleetConfig`: add a `@field_validator("service_class")` that calls
    `resolve_service_class` so a bad class fails loudly at load (extra='forbid' spirit), while
    still accepting friendly labels. Keep the field a `str` (back-compat) but guarantee it
    resolves. Add a `service_class_id` computed property returning the canonical `ServiceClassId`.
  - New `CloudConfig(_Base)`: `region: Region = US`, `base_url: str | None = None`
    (explicit override wins over region), `datacenter_id: str | None = None`,
    `idempotency_header: str = "Idempotency-Key"`, `timeout: float = 30s`. Token stays out of
    config (env/secret only — never in a hashed config file; matches the no-secrets rule).
  - Wire `cloud: CloudConfig` into `Config`.
- `SolaceCloudClient`: accept `base_url` from `CloudConfig.effective_base_url()` (region or
  override); no signature break (still defaults to US).
- Tests: config accepts labels + raw ids, rejects `enterprise-9k`; region resolves to base;
  explicit `base_url` overrides region.

### Commit 3 — align the operation client surface with the spec
- `solace_cloud.py`:
  - Add `get_service(service_id)` → `GET /api/v2/missionControl/eventBrokerServices/{id}`
    (spec `getService`, returns the Service with `serviceConnectionEndpoints`, `creationState`,
    `adminState`) — this is the natural, spec-first way to read endpoints/state.
  - Split operation polling: `get_service_operation(service_id, operation_id)` (service-scoped,
    spec-documented-first) and keep `get_operation(operation_id)` as the multi-resource fallback,
    fixing the docstring to state exactly which path each uses and when.
  - Add tiny typed helpers that parse `status`/`creationState`/`adminState` via the enums from
    commit 1 (so callers compare against `OperationStatus.SUCCEEDED`, not a bare string).
- Update the client module docstring's endpoint list to include `getService` and both operation
  paths; keep "verified against Mission Control OpenAPI v2.0.0".
- Tests: mock httpx transport asserting exact URLs + enum parsing of a sample Operation payload
  (payload shapes taken from the spec schema, not invented field values).

### Commit 4 — docs + coherence sweep + gates
- New ADR `guide/adr/0010-mission-control-identifier-alignment.md`: records that
  `cloud/enums.py` is the single source of truth for Solace-Cloud identifiers, why the token
  stays out of config, and the region/base-URL model.
- Update `guide/` references (config docs, any service-class mentions) to note friendly labels
  resolve to canonical `ServiceClassId` and list the regions.
- Run full gates: `ruff check`, `mypy solace_autoscale`, `pytest` (expect 200 + new tests green);
  Go gate unaffected but run `gofmt -l` / `go build ./... ` / `go vet ./...` / `go test ./...`
  in `shim/` to prove no regression.

## Explicit non-goals (guard against scope creep / inventing schema)

- **Do NOT implement `CloudApiCollector`.** Still no real monitoring payload sample; it stays a
  documented stub (§1/§14 — do not invent field names).
- **Do NOT auto-fetch `GET /serviceClasses` or `/datacenters` at runtime** to build the capacity
  model. The compiled-model design (ADR 0005) is deliberate; discovery endpoints can be a later,
  separate `compile`-time helper. Note them in the ADR as available, don't wire them now.
- No change to the messageSpool body, base default, or createService path (all verified correct).
- No secrets in config or logs; no force-push; no destructive git.

## Verification per commit

- Commit 1/2/3: `python3 -m pytest tests/test_cloud_enums.py tests/test_config.py
  tests/test_actuator_safety.py -q` first, then broaden.
- Final: full `ruff`/`mypy`/`pytest` + `shim/` Go gate. Land on `build/solace-autoscale`
  (current branch), commit per-step with the `Co-Authored-By: Claude Opus 4.8` trailer.

## Ordering

Commit 1 → 2 → 3 → 4. Each is independently revertible and leaves the tree green.
