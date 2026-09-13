# Go shim + full repo reorg — plan

Three approved decisions:
- **Full Go shim, real AMQP.** Port the whole shim to Go: pure rule engine (topic + payload),
  portable-spec loader, fail-open resolver, publisher + listener, real AMQP transport
  (`github.com/Azure/go-amqp`) behind a `Transport` interface so tests run offline with an in-memory
  fake. Runnable CLI demo. Full `go test`.
- **Full reorg to product folders.** Top level becomes intuitive product folders.
- **Go replaces the Python shim.** Remove the Python publisher/listener/dispatch data path. Keep the
  pure Python rule engine (controller still needs it for `dispatch-test`) and keep the Tier-1
  client helpers (resolver + per-protocol adapters) — those are not the shim data path.

---

## Target top-level layout

```
/shim                     Go shim (new home, real AMQP data path)
  go.mod  go.sum
  cmd/shim/main.go        runnable CLI: route a message, or run pub/sub demo
  rules/                  pure engine: topic match, payload predicates, templates, spec loader
  resolve/                fail-open resolver (broker name -> endpoint), HTTP + cache
  dispatch/               PublisherShim, ListenerShim, Transport interface
  transport/amqp/         go-amqp implementation of Transport
  transport/memory/       in-memory Transport for tests/demo
  README.md
/scaling-controller       the Python control plane (moved from src/solace_autoscale)
  solace_autoscale/       the package (decision, actuator, metrics, configsync, dispatch, ...)
  tests/                  the Python tests (moved from /tests)
  pyproject.toml          moved; packaging paths updated
/guide                    all docs (moved from /docs): architecture, safety, rule-spec, adr/, site
/deploy                   unchanged (terraform)
/examples                 config.example.yaml + a sample rule spec + a runnable walkthrough
/models  /resources  /scripts   unchanged (data/tooling the controller reads)
README.md                 rewritten top-level: two products (shim, controller) + folder map
```

Rationale: a newcomer sees `/shim` (the thing in their app) and `/scaling-controller` (the thing
that runs the fleet) immediately, with `/guide` for docs and `/examples` to copy from.

### Moves (use `git mv` to preserve history)
- `src/solace_autoscale/` -> `scaling-controller/solace_autoscale/`
- `tests/` -> `scaling-controller/tests/`
- `pyproject.toml` -> `scaling-controller/pyproject.toml` (packaging + pythonpath updated)
- `docs/` -> `guide/`
- `config.example.yaml` -> `examples/config.example.yaml`
- Delete `adapters/` after salvaging: the Tier-1 client helpers (`resolver.py`, `adapters.py`,
  `smf_wrapper.py`, `__init__.py`) move to `scaling-controller/solace_autoscale_client/` as a
  second installed package (they are client-side helpers, still Python, still tested by
  `test_client_adapters.py`). The Python **shim** files (`dispatch.py`) and the Java stub are
  removed — replaced by Go.

Note: keeping the controller Python code under a nested `solace_autoscale/` package name keeps every
internal `from solace_autoscale... import` working unchanged; only the `where`/`pythonpath` roots in
packaging + CI move. This bounds the churn to config, not 50 source files.

---

## The Go shim (the real work)

Module: `github.com/solacese/solace-broker-autoscale/shim` (Go 1.26, already installed).

### `rules/` — pure engine (port of dispatch/rules.py + payload.py + spec.py)
- `topic.go`: `TopicMatches(pattern, topic)` — Solace `*` (one level) and `>` (rest, must be final).
- `payload.go`: JSON decode + dotted-path get (objects + list indices) + `EvalPredicate`. Operators
  ported 1:1: eq ne in nin gt gte lt lte exists missing prefix contains regex raw_size_gt
  raw_size_lt raw_prefix. Missing-field semantics identical (false for all but `missing`;
  type-mismatch compare is false, not an error).
- `template.go`: `RenderTemplate` — `{topic}` + `{dotted.path}`, absent -> empty, collapse `.{2,}`
  -> `.` and trim, matching Python exactly.
- `rules.go`: `Rule`, `Match`, `Route`, `Decision`, `Evaluate(topic, payload, rules, default)` —
  first-match-wins, else default, else error. `TargetBrokers`.
- `spec.go`: `FromSpec([]byte) (*Plan, error)` / `ToSpec(*Plan) ([]byte, error)` reading the SAME
  JSON spec documented in guide/rule-spec.md (version 1). This is the interop proof: the Python
  `to_spec` output loads in Go and decides identically.
- Reasonable defaults: empty topic pattern defaults to `>`; nil payload handled.

### `resolve/` — fail-open resolver (port of resolver.py)
- `Assignment{BrokerID, MsgVPN, State, LeaseSeconds, Endpoints map[string]string}`.
- `Resolver.Resolve(shard, clientID, mode, protocol)` — HTTP GET assignment service, cache on
  success, return cache on failure, error only when uncached. `http.Client` with timeout; an
  injectable `fetch` func seam for tests (mirrors Python `_opener`).

### `dispatch/` — the two shims (port of dispatch.py)
- `Transport` interface: `Sender(uri string) (Sender, error)`; `Sender.Send(address string, body
  []byte, props map[string]any, groupID string) error`; `Receiver`/`Subscribe` symmetrically.
- `PublisherShim{Resolver, Plan, Shard}` with `Publish(topic, payload)` -> decides, resolves,
  caches sender per (brokerID, uri), sends with the partition key as AMQP group-id + a
  `saas_partition_key` application-property. Bounded retry with backoff (carry over fix #2), context
  aware.
- `ListenerShim` subscribes across `Plan.Targets`, `Demux` re-runs rules, sets `Consistent`.
- Connection cache keyed by (brokerID, uri) — carries over fix #3.

### `transport/amqp/` — real AMQP via go-amqp
- Implements `Transport` with `github.com/Azure/go-amqp`: dial, session, sender/receiver, set
  `Message.Properties.GroupID` and `ApplicationProperties["saas_partition_key"]`. Timeouts +
  context. This is the only package that imports the network lib; everything above is pure or
  interface-typed, so `go test ./...` runs with the in-memory transport and needs no broker.

### `transport/memory/` — in-memory Transport
- A channel-backed fake so the demo and tests exercise publisher->listener end to end offline.

### `cmd/shim/main.go` — runnable CLI (intuitive, good defaults)
- `shim route --spec rules.json --topic orders/eu/new --payload '{...}'` -> prints chosen broker,
  key, address, rule (offline; mirrors the controller's `dispatch-test`).
- `shim demo --spec rules.json` -> spins the in-memory transport, publishes a few sample messages,
  shows the listener demuxing them by key and flagging drift. Zero external deps to run.
- Defaults: spec path `./rules.json`, resolver URL optional (route/demo work without one).

### Tests (`go test ./...`)
- rules: topic-match table, every operator, template collapse, spec round-trip, and a golden test
  that loads a spec file also used by the Python round-trip test (cross-language interop).
- dispatch: publisher routes+caches+retries (fake transport), listener demux consistent/inconsistent.
- resolve: cache hit, fail-open on error, error when uncached.

---

## Python side changes (controller)
- Remove `adapters/python/.../dispatch.py` and its unit test `tests/test_shim_dispatch.py`, and the
  shim integration test `tests/test_integration_shim.py` (data path is Go now). Keep
  `test_client_adapters.py` (resolver/adapters still Python) and `test_integration_broker.py`.
- Move salvaged client helpers to `scaling-controller/solace_autoscale_client/`; add to packaging as
  a second top-level package. Update the 2 lines in `pyproject.toml` (`where`, package list) and
  `pythonpath`.
- Keep the pure Python `solace_autoscale/dispatch/` (rules/payload/spec) — `dispatch-test` CLI and
  its tests stay green; it is now the *reference* the Go engine mirrors.

## CI (.github/workflows/ci.yml)
- Python job: paths become `scaling-controller/` (`working-directory`), lint/type/test unchanged
  otherwise. `ruff check solace_autoscale tests solace_autoscale_client`.
- New `go` job: `actions/setup-go@v5`, `go vet ./...`, `go build ./...`, `go test ./...` in `shim/`.
- pages.yml: doc site source path `docs/` -> `guide/`.

## Docs / guide
- `guide/rule-spec.md`: add a short "Go shim" section (both engines read this spec).
- New `shim/README.md`: build, run `shim route` / `shim demo`, wire real AMQP, the Transport seam.
- Rewrite top-level `README.md`: lead with the two products, the folder map, quick-starts for each.
  No em dashes; lowercase "shim"; "the shim wraps the app's existing messaging client".

## Verification gate
- Go: `cd shim && go vet ./... && go build ./... && go test ./...`
- Python: `cd scaling-controller && ruff check . && mypy solace_autoscale && pytest -m "not integration" -q`
- Coherence pass: `rg` for stale paths (`adapters/python`, `src/solace_autoscale`, `docs/`) across
  README, guide, CI, pyproject; fix every reference. Confirm `git mv` preserved history.

## Order of work (commits)
1. Reorg moves (git mv controller/tests/docs/config; fix packaging + pythonpath + CI paths; salvage
   client helpers; remove Python shim + its 2 tests). Verify Python gate green. One commit.
2. Go module scaffold + `rules/` + spec + tests (interop with Python spec). One commit.
3. Go `resolve/` + `dispatch/` + `transport/memory` + tests. One commit.
4. Go `transport/amqp` + `cmd/shim` CLI + shim/README. One commit.
5. README rewrite + guide cross-links + CI go job + final coherence sweep. One commit.
Push to PR #1 branch after gates pass. No force-push.

## Out of scope / risks
- No Java shim (removed the stub; Go is the native shim now).
- go-amqp adds a dependency to `shim/` only; pinned in go.mod. If offline module download is
  blocked in this environment, the amqp package will be authored but `go test` will run against the
  in-memory transport (pure + interface packages need no network lib); I will flag if `go get`
  cannot fetch go-amqp and leave that one package unbuilt with a clear TODO rather than fake it.
```
