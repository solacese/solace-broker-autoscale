# Fixes batch — plan

Scope agreed with user:
- configsync: **focus on additive, ignore mirror.** No delete fence, no threshold guard, no changes to mirror-mode delete behaviour. Keep the truncation fix because it makes *additive* correct too.
- Rule engine portability: **export/import spec + docs only.** No new native adapters this batch.
- Plus the two data-path safety fixes on the publisher shim.

Four changes, each small, reversible, and covered by tests. No changes to pure modules' behaviour (`decision/`, `dispatch/rules.py`, `configsync/diff.py`).

---

## Fix 1 — configsync snapshot truncation (correctness)

**Problem.** `SempConfigClient.snapshot()` fetches each collection with a hardcoded `?count=100`
(`semp_config.py` lines 89 and 99). SEMPv2 returns at most that many objects and a paging cursor at
`meta.paging.nextPageUri`. If a broker has more than 100 of any kind (queues, subscriptions, etc.),
the tool silently sees only the first 100.

Why it matters even in additive mode: additive creates/updates whatever the source has that the
target lacks. If `snapshot()` under-reads the **source**, additive never learns about the objects it
didn't fetch, so it fails to replicate them — a broker comes up missing queues/subscriptions and
nobody is told. This is a silent correctness bug, not just a mirror-mode risk.

**Change.** Add a private paging helper and route both collection reads through it.

- New method `_get_all(path) -> list[dict]` in `SempConfigClient`:
  - GET the path; collect `resp["data"]`.
  - Follow `resp["meta"]["paging"]["nextPageUri"]` until absent/empty.
  - `nextPageUri` is an absolute URL; call it directly (strip to path+query against `self._base`
    if it comes back absolute, else use as-is). Guard against a self-referential cursor loop with a
    hard page cap (e.g. 10_000 pages) that raises `ConfigSyncError` — defensive, never expected.
  - Keep `?count=100` as the page size (fine as a page size; the bug was treating it as a limit).
- `snapshot()`: replace the two `self._get(...).get("data", [])` sites (lines 89, 99) with
  `self._get_all(...)`.

**Files.** `src/solace_autoscale/configsync/semp_config.py` only. `diff.py`/`model.py` untouched.

**Tests** (`tests/test_configsync_client.py`, MockTransport handler):
- `test_snapshot_pages_through_all_objects`: handler returns page 1 with `nextPageUri` +
  101 objects across two pages; assert the snapshot contains all of them.
- `test_snapshot_single_page_no_cursor`: no `nextPageUri` → one request, existing behaviour intact
  (regression guard).

---

## Fix 2 — publisher shim: retry on transient send failure (reliability)

**Problem.** `_ProtonSender.send()` does a single `send(msg, timeout=10)` (`dispatch.py` line 221)
with no retry. One transient broker blip drops the message with an exception.

**Change.** Wrap the `send` in a bounded retry with backoff, inside `_ProtonSender.send`:
- Retry on the proton send exception only (import proton's error type lazily alongside `Message`).
- Config: `max_retries` (default 3) + simple exponential backoff (0.1s, 0.2s, 0.4s), values as
  constructor args on `_ProtonSender` with defaults so nothing else changes.
- On final failure re-raise wrapped in `DispatchError` so callers see one exception type.
- Keep it synchronous; matches the existing blocking-connection design.

Note: this does not attempt reconnect/failover (that's a bigger change and out of scope for this
batch) — only re-attempts the send on the existing connection. Documented as such in the docstring.

**Files.** `adapters/python/solace_autoscale_client/dispatch.py` only.

**Tests** (`tests/test_shim_dispatch.py`, fake sender via `sender_factory`):
- `test_sender_retries_then_succeeds`: fake `send` raises twice then succeeds; assert 3 calls, no
  raise. (Test targets a small retry wrapper; if retry lives only in `_ProtonSender` which needs
  proton, extract the retry loop into a tiny pure helper `_send_with_retry(fn, attempts, backoff)`
  so it's testable without proton. Prefer the helper.)
- `test_sender_gives_up_after_max`: always raises → raises `DispatchError` after `max_retries`.
- Use a monkeypatched `sleep` (inject a no-op sleeper) so tests don't actually wait.

---

## Fix 3 — publisher shim: cache connections per (broker, credentials) (robustness)

**Problem.** `PublisherShim._conns` is keyed by `broker_id` alone (`dispatch.py` lines 72, 92-98).
If the same broker id is ever resolved with different credentials/URI, the first cached connection is
reused with the wrong auth. Low-likelihood today (one user/password per shim) but a latent footgun.

**Change.** Key the cache by `(broker_id, uri, user)`:
- `_conns: dict[tuple[str, str, str | None], Any]`.
- `_sender_for` builds the key from `assignment.broker_id`, the resolved `uri`, and `self.user`.
- `close()` unchanged (iterates values).

This is a 3-line change and purely internal; no public API change.

**Files.** `adapters/python/solace_autoscale_client/dispatch.py` only.

**Tests** (`tests/test_shim_dispatch.py`):
- `test_conn_cache_reuses_same_endpoint`: two publishes to the same broker → `sender_factory`
  called once.
- `test_conn_cache_separates_by_uri`: monkeypatch resolver to return two URIs for two brokers →
  factory called twice (guard against over-merging).

Also removes the redundant double-resolve in `publish()` (lines 103-104 re-run `decide()` +
`resolve()` that `plan_message()` already did): have `plan_message` return the resolved `Assignment`
alongside the `OutboundMessage`, and `publish` reuse it. Small, removes wasted work + a
double-evaluation of the pure rules. Covered by existing publish tests + the cache tests above.

---

## Fix 4 — rule engine: portable spec export/import + docs (portability groundwork)

**Problem.** Rules only exist as in-Python objects, so no other language can consume them and there's
no written spec. User wants the groundwork only: a documented JSON round-trip, no native adapters.

**Change.**
- Add `to_spec(plan) -> dict` and `from_spec(dict) -> DispatchPlan` in `dispatch/` (new module
  `dispatch/spec.py`, pure, no I/O), round-tripping the existing rule model (topic pattern, payload
  predicates, broker, address/key templates, default_broker). Reuse existing dataclasses; this is
  serialization only, engine semantics unchanged.
- Add a JSON Schema-ish description + prose in `docs/` (or a `SPEC.md` under `dispatch/`) documenting
  the format and the evaluation semantics that any adapter must honour: Solace wildcards (`*` one
  level, `>` rest), AND across predicates, first-match-wins, else default_broker. Lowercase "shim",
  no em dashes, per repo style.
- Version field in the spec (`"version": 1`) so future adapters can gate.

**Files.** `src/solace_autoscale/dispatch/spec.py` (new); `src/solace_autoscale/dispatch/__init__.py`
(export `to_spec`/`from_spec`); one docs/spec markdown file.

**Tests** (`tests/test_dispatch_rules.py` or new `tests/test_dispatch_spec.py`):
- `test_spec_roundtrip`: build a plan with several rules incl. payload predicates + templates →
  `from_spec(to_spec(plan))` produces a plan that `.decide()`s identically on a battery of
  `(topic, payload)` inputs.
- `test_spec_is_json_serialisable`: `json.dumps(to_spec(plan))` succeeds (no non-JSON types).
- Add to purity-guard's covered modules so `spec.py` stays pure.

---

## Verification gate (run after each fix, then all at end)

```
.venv/bin/ruff check src/ tests/ adapters/python/ dns/
.venv/bin/mypy src/solace_autoscale
.venv/bin/pytest -m "not integration" -q
```
Integration suite (`-m integration`) is proton/broker-gated; run if runnable, else note it.

## Order & commits
1. Fix 1 (truncation) — isolated, highest value.
2. Fixes 3 then 2 (same file; do cache+dedupe, then retry).
3. Fix 4 (new module + docs).
One commit per fix, conventional messages, `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
Branch is already `build/solace-autoscale` (PR #1); push updates the existing PR. No force-push.

## Explicitly OUT of scope (per user)
- Mirror mode: no delete fence, no threshold guard, no behaviour change. Left as-is.
- No native (Java/Go/Node) adapters this batch.
- No reconnect/failover in the shim (only same-connection send retry).
