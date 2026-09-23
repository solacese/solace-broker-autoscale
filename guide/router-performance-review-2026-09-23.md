# Router performance review — 2026-09-23

## Scope and conclusion

This review separates the `scripts/cloud-workload.py` measurements from the Go shim's durable publisher and subscriber costs. Initial profiling used the ignored `state/claude-directed-qualification/concurrent-local-v2` report and a temporary copy of `shim/`. The corrected measurement harness was subsequently validated on one disposable local broker, as recorded below. No Cloud service was created.

The dominant local bottleneck is synchronous storage, not JSON serialization or the 50 ms delivery ticker:

- Every accepted `Publish` is one synchronous bbolt write transaction (`shim/messaging/outbox.go:80-119`). bbolt 1.5.0 performs a data `fdatasync` and a metadata `fdatasync` for a normal commit.
- Every broker-confirmed publication is removed by another synchronous bbolt write transaction (`shim/messaging/outbox.go:140-169`). Enqueue and deletion share bbolt's single writer.
- Every subscriber callback in the qualification executable commits the event ID and payload through another synchronous bbolt transaction before acknowledging the broker (`shim/cmd/payments-demo/main.go:100-123`). All partition/group handlers in that process share that database writer.
- On the measured filesystem, a 4 KiB synchronous enqueue or delete took about 8 ms median. A durability-preserving enqueue-plus-delete cycle sustained only 62–68 messages/s in isolation. This explains the previously observed approximately 38–53 messages/s per synchronous publisher and why four publisher processes reached only 121 messages/s at 4 KiB when competing for the same storage device.

The current evidence proves correctness under a bounded workload (no observed loss, duplicates, or per-key ordering violations). It does **not** establish production capacity, a broker saturation point, or a stable end-to-end latency SLO.

## Inputs inspected

- `scripts/cloud-workload.py`
- `shim/messaging/outbox.go`
- `shim/messaging/client.go`
- `shim/messaging/routing.go`
- `shim/messaging/subscribe.go`
- `shim/cmd/payments-demo/main.go`
- `shim/transport/amqp/amqp.go`
- `state/claude-directed-qualification/concurrent-local-v2/results/report.json`
- The completed bbolt outbox files under `state/claude-directed-qualification/concurrent-local-v2/results/*/publisher-*`
- The preceding ignored `state/claude-directed-qualification/concurrent-local/results/report.json`, to understand the earlier synchronous harness result

## Measurement semantics and artifacts

### What `publish_seconds` measures

The v2 harness writes JSON lines to four publisher subprocesses without waiting for each acceptance (`scripts/cloud-workload.py:274-295`). It sets `publish_finished` immediately after the last stdin write and only then drains all `accepted` responses (`scripts/cloud-workload.py:296-303`). Consequently:

- `achieved_offer_messages_per_second` is the rate at which the Python harness offered commands into subprocess pipes.
- It is **not** durable acceptance throughput, broker-confirmed throughput, or subscriber throughput.
- Pipe backpressure can still expose a slow publisher. That is what happened in `medium-fanout`: the requested 500 messages/s became 121.27 messages/s with 1,251 ms of schedule lag. The metric is useful as an overload symptom, but should not be named or interpreted as completed throughput.

The older report sent one command and waited for one acceptance at a time. Its `offered_messages_per_second` therefore included the per-message durable commit and process round trip and reported 37.86–52.82 messages/s. That number was not broker capacity either; it was a serialized request/accept loop.

### What acceptance latency measures

`accepted_at` is recorded before the Python process writes to the child's stdin, and `observed_at` is recorded by the stdout reader after decoding the child's response. Thus acceptance latency includes:

1. Python scheduling and pipe wait,
2. Go stdin scanning and JSON decoding,
3. routing and payload JSON serialization,
4. waiting for bbolt's single writer,
5. the synchronous enqueue commit,
6. Go stdout encoding and the Python reader thread.

It is a valid harness-to-durable-acceptance latency, but not a direct timing of `Client.Publish`. Under offered load it includes queueing. This explains the v2 p99 values near one second even though an isolated durable enqueue has a roughly 8–14 ms p99: many commands were already queued in each process.

### What processing latency measures

Processing latency starts at the same pre-stdin timestamp and ends when the subscriber's `processed` JSON line is observed. It includes all publisher queueing above, durable enqueue, assignment/connection startup when applicable, outbox dispatch, broker persistence and delivery, subscriber handler work, a synchronous subscriber ledger commit, and stdout scheduling. The demo emits `processed` inside the handler before the handler returns, so this metric explicitly does **not** include the subsequent broker ACK settlement (`shim/cmd/payments-demo/main.go:109-123`, `shim/messaging/subscribe.go:195-211`).

It does not include pre-case queue preparation or subscriber-process startup, because timestamps start later. However, `subscribed` only means `Client.Subscribe` registered the desired subscription. Actual receiver creation occurs later in the control loop (`shim/messaging/subscribe.go:78-142`), which polls every 100 ms in the qualification executable. Early messages can therefore wait durably in broker queues while receiver flows bind.

The reported multi-second p99 values are predominantly backlog/drain measurements, not the no-queue service time of one message:

| Case | Offered/achieved | Acceptance p99 | Processing p99 | Main interpretation |
|---|---:|---:|---:|---|
| small steady, 256 B, fanout 1 | 200 / 200.79 msg/s | 1.096 s | 4.243 s | Offered load exceeds one shared disk's synchronous transaction capacity once publisher deletes and subscriber commits are included. |
| medium fanout, 4 KiB, fanout 2 | 500 / 121.27 msg/s | 0.927 s | 5.705 s | Pipe backpressure exposes the enqueue commit ceiling; two subscriber groups double durable ledger transactions. |
| large burst, 64 KiB, fanout 2 | unpaced / 70.25 msg/s | 0.181 s | 2.662 s | Short burst plus larger writes; still a drain result rather than steady-state saturation. |
| slow consumer, 1 KiB, fanout 2 | 500 / 503.72 msg/s | 1.100 s | 4.035 s | The harness offers rapidly, then measures accumulated publisher/subscriber backlog; the explicit 20 ms handler delay is only one contributor. |

`elapsed_seconds` is even broader: it includes setup, SEMP sampling, receiver startup, publish, flush, reconciliation, the quiet period, and shutdown (`scripts/cloud-workload.py:231-340`). It must not be used as throughput or message latency.

The legacy `broker_before` and `broker_after` rate fields are point samples around short bursts. For example, the 4 KiB case reports only 5 ingress messages/s after the run despite processing 200 publications. They do not integrate traffic over the run and cannot identify the saturation component. New runs also capture monotonic VPN data message/byte counters and their exact sampling window; if a broker does not expose those counters, the report marks them unsupported rather than substituting rates.

## Bounded local microbenchmark

### Method

The original investigation used a temporary shim copy and package-local test against temporary bbolt files on macOS 27.0, arm64, Go 1.26.4. The production-method portion is now reproducible from the repository:

```text
go -C shim test ./messaging -run '^$' \
  -bench '^BenchmarkOutboxDurability$' -benchtime=100x -count=3 -benchmem
```

The benchmark calls the production `outbox.enqueue` and `outbox.accepted` methods with normal synchronous bbolt commits. The sync-attribution control is deliberately separate and opt-in:

```text
go -C shim test ./messaging -run '^$' \
  -bench '^BenchmarkOutboxNoSyncDiagnostic$' -benchtime=1000x -count=1 -benchmem
```

Three original repetitions covered routing/serialization, synchronous enqueue, and the `DB.NoSync=true` attribution control. A final diagnostic run also measured a subscriber-like single-key bbolt update, synchronous deletion, a complete enqueue-plus-delete cycle, and grouped enqueue transactions. Individual operations were sequential; the benchmark intentionally excluded broker/network effects. Total benchmark time stayed below one minute.

`NoSync` results are an attribution control only. They do not preserve crash durability and are not a production recommendation. A compact machine-readable normal-sync run is preserved at `examples/qualification-evidence/2026-09-23/local-outbox-durability-profile.json`; it contains only synthetic benchmark results and environment metadata.

### Results

Ranges below are across three runs unless marked single run.

| Operation | Payload | Result |
|---|---:|---:|
| Route + payload JSON serialization | 256 B | 0.79–1.54 µs p50; 0.49–1.00 million ops/s |
| Route + payload JSON serialization | 4 KiB | 2.63–2.71 µs p50; 305k–324k ops/s |
| Route + payload JSON serialization | 64 KiB | 33.2–33.6 µs p50; 26.6k–27.2k ops/s |
| Durable enqueue, normal sync | 256 B | 7.0–8.0 ms p50; 105–142 ops/s |
| Durable enqueue, normal sync | 4 KiB | 8.0 ms p50; 118–122 ops/s |
| Durable enqueue, normal sync | 64 KiB | 9.0 ms p50; 104–108 ops/s |
| Enqueue, `NoSync=true` control | 4 KiB | 58–71 µs p50; 8.7k–10.0k ops/s |
| Subscriber-like bbolt update, normal sync | 4 KiB | 8.0–10.0 ms p50; 99–123 ops/s (single runs) |
| Broker-confirmed outbox delete, normal sync | 4 KiB | about 8.0 ms p50; 120–131 ops/s |
| Enqueue + confirmed delete, normal sync | 4 KiB | 14.9–16.1 ms p50; 61–68 messages/s |

Removing sync increased 4 KiB enqueue throughput by roughly 70–84 times; route plus serialization was over 300,000 operations/s. This isolates synchronous commit latency as the dominant local enqueue cost. Payload serialization is not material at the tested sizes. The 64 KiB no-sync case still exceeded 1,400 operations/s, more than an order of magnitude above the durable result.

A diagnostic group-commit transaction using the same bbolt data layout produced these single-run results:

| Records per durable transaction | Transaction time p50 | Effective record rate |
|---:|---:|---:|
| 10 | 11.1 ms | about 800 records/s |
| 50 | 14.0 ms | about 3,226 records/s |
| 100 | 19.0 ms | about 5,262 records/s |

This is evidence that transaction-level group commit can remove most per-message sync overhead. The diagnostic helper omitted some production duplicate/capacity checks, so these are directional bounds, not product benchmarks.

## Concrete implementation findings

### 1. Each message pays for two synchronous outbox commits

`Publish` calls `box.enqueue` while holding `Client.mu` (`shim/messaging/client.go:165-185`). `enqueue` uses one bbolt `Update` (`shim/messaging/outbox.go:80-119`). After AMQP confirms broker persistence, `send` calls `box.accepted`, which uses another `Update` (`shim/messaging/client.go:323-370`, `shim/messaging/outbox.go:140-169`). bbolt serializes writers, so enqueue and cleanup compete even though delivery uses goroutines.

This design gives clear per-call durability and durable cleanup, but throughput scales with storage commit latency, not CPU. More publisher processes can overlap userspace work, yet their fsyncs still contend on the same device. Increasing `MaxInflight` cannot fix the enqueue ceiling and can increase write contention.

### 2. The subscriber qualification path has the same commit ceiling

Each delivered copy calls `db.Update`, persists the event ID and full payload, then emits `processed`; only after the handler returns does the shim ACK (`shim/cmd/payments-demo/main.go:100-123`, `shim/messaging/subscribe.go:195-223`). With fanout two, 200 publications require 400 ledger commits. The isolated ledger-shaped test measured about 99–123 commits/s. Four partitions and multiple receiver goroutines do not remove the shared bbolt writer bottleneck.

This behavior is appropriate for demonstrating transactional deduplication, but it makes the current `processing_latency_ms` primarily a benchmark of synchronous demo-ledger persistence under backlog.

### 3. The 50 ms delivery ticker is not the loaded-path bottleneck

The delivery loop wakes on every successful enqueue and on every worker completion, in addition to the 50 ms ticker (`shim/messaging/client.go:371-433`). Under backlog, a completion immediately enables scheduling the next lane head. The ticker can add up to about 50 ms when idle notifications coalesce or race, but it cannot explain sustained 8 ms transaction cost or multi-second p99 values. `Flush` similarly polls at 20 ms (`shim/messaging/client.go:208-233`), affecting completion observation rather than durable acceptance.

### 4. Dispatch is deliberately one in-flight message per lane

`heads` returns only the first record from each lane, and `busy[lane]` prevents concurrent sends within a lane (`shim/messaging/outbox.go:121-139`, `shim/messaging/client.go:411-433`). This preserves ordering. With four partitions, publisher dispatch has at most four active lanes even though `MaxInflight` defaults to 128. That is a real concurrency bound, but the local storage profile shows the write transaction cost dominates before this limit can be evaluated cleanly.

### 5. JSON is copied repeatedly, but it is secondary here

The payload is marshalled in `publication`; `enqueue` marshals the record once for the ID identity and again after assigning a sequence; `send` marshals a broker envelope (`shim/messaging/routing.go:193-200`, `shim/messaging/outbox.go:83-112`, `shim/messaging/client.go:352-356`). This creates allocation and byte-copy overhead, especially at 64 KiB, but measured serialization was tens of microseconds rather than milliseconds. Optimize it after durable commit batching, not before.

### 6. `MaxOutboxBytes` is not a physical disk bound

Capacity accounting adds only `len(identity)` even though enqueue stores both the identity value in `ids` and the sequence-bearing record in a lane, plus bbolt page/freelist overhead (`shim/messaging/outbox.go:83-118`). The completed 4 KiB publisher databases are each 1 MiB for only 50 events before drain, and the 64 KiB databases are 4–8 MiB. This means the option is a logical payload-ish limit, not a maximum database size. The name/documentation should say so, or accounting should include both values and a documented storage-overhead allowance.

## Recommendations

1. **Add durability-preserving group commit for enqueue.** A single writer goroutine should collect a small bounded batch (for example, up to N messages or a sub-millisecond deadline), write all records in one bbolt transaction, and complete each `Publish` only after that transaction commits. This preserves the current acceptance guarantee while amortizing sync cost. Bound both batch size and wait time, expose metrics, and retain immediate rejection for invalid/full records.
2. **Batch confirmed deletions separately.** Broker-confirmed records may be removed in one transaction. A crash before that commit only causes duplicate resend, which the documented subscriber contract already requires handlers to deduplicate. Preserve per-lane order and never remove an unconfirmed record.
3. **Do not enable bbolt `NoSync` in production.** It removes the exact crash-durability guarantee the outbox is intended to provide.
4. **Separate publisher and subscriber storage metrics.** Instrument enqueue queue wait, enqueue transaction duration, broker-send/settlement duration, delete transaction duration, handler duration, ledger transaction duration, and queue depth. Histograms should distinguish service time from time waiting behind the single writer.
5. **Correct qualification metric names and add completion windows.** Keep `offer_seconds`/`achieved_offer_rate`; add `durable_accept_seconds` and durable acceptance throughput based on first-send to final accepted response; add `broker_drain_seconds` based on the final accepted response to all publisher flushes; add `consumer_drain_seconds` based on final flush to reconciliation. Keep end-to-end per-message latency, but label it as queueing-inclusive.
6. **Gate subscriber readiness on bound flows.** Do not emit `subscribed` immediately after registration. Wait until expected receiver flows are bound, or expose an explicit ready status and have the harness wait for it. This removes receiver startup from early-message latency without weakening durability.
7. **Use interval telemetry for broker attribution.** Sample or query counters over the active publish/drain window. The current before/after instantaneous rates cannot prove a broker limit.
8. **After group commit, run a saturation matrix.** Vary publisher process count, partition count, batch size/deadline, payload size, and fanout. Use enough messages for a steady plateau, report confidence/repetitions, and keep durability and ordering assertions enabled.
9. **Clarify outbox capacity semantics.** Either rename the setting to indicate logical queued bytes or enforce/report actual on-disk usage. Alert on both logical pending bytes/rows and database file size.

## Readiness limits

- **Ready for correctness demonstrations:** bounded durable acceptance, restart recovery, broker-confirmed deletion, at-least-once subscriber processing, fanout, and per-key ordering have test or observed evidence.
- **Not ready for a production throughput claim:** the runs contain only 40–200 messages per case and do not reach a measured steady-state plateau. The corrected harness separates completion windows, but the earlier results combine storage, process-pipe, broker, and subscriber stages.
- **Not ready for a latency SLO:** the corrected smoke gates receiver readiness and reports separate completion windows. Its short, queueing-inclusive latency sample does not establish sustained-load percentiles or a customer latency SLO.
- **Current practical limit on the measured storage:** expect roughly 100–125 durable bbolt transactions/s per database in isolation and roughly 60–68 messages/s when each message causes both enqueue and deletion commits. Shared-device contention and subscriber ledger commits can reduce this further. These numbers are environment-specific, not portable product limits.
- **Fanout amplifies sink durability cost:** every additional subscriber group causes another durable business-ledger commit in this harness. Capacity planning must state whether the target is publications/s or delivered copies/s.

## Implemented qualification corrections

The bounded follow-up implements the measurement and reproducibility corrections without changing publisher concurrency:

- The legacy `publish_seconds` and `achieved_offer_messages_per_second` fields remain compatibility aliases, explicitly defined as stdin offer injection. `measurement_windows` now adds first-injection-to-last-durable-accept, first-injection-to-final-flush, and first-injection-to-last-business-handler-output durations and rates, plus the post-accept flush interval.
- Timed workload collection starts only after `QueueManager.status` reports at least one bound consumer for every configured group on every partition, with a bounded timeout.
- `broker_cumulative_counters` records before/after/delta values for VPN data-message and data-byte counters over a stated sampling window. When the broker omits or invalidates those counters, the report records `supported: false` and `null` values rather than manufacturing a rate.
- `shim/messaging/outbox_benchmark_test.go` makes the production-method normal-sync profile rerunnable. Its separate NoSync benchmark is labeled an unsafe diagnostic, not a setting.
- `MaxOutboxBytes` is documented in the Go API as serialized logical pending-record accounting, not a bbolt file-size quota.

A future production group-commit implementation remains intentionally out of scope until it has dedicated crash-recovery and ordering qualification.

### Observed corrected-harness smoke

A real local Standard broker smoke on 2026-09-23 used 40 synthetic 4 KiB publications at a configured 20 messages/s, fanout two, four publishers, and four partitions. Timing began only after all eight receiver flows were visible through SEMP. The run accepted 40 publications and observed exactly 80 business-handler completions with zero loss, duplicates, or ordering violations. Offer injection, final durable acceptance, final broker flush, and final handler completion took 1.955 s, 1.965 s, 1.986 s, and 1.999 s respectively. Cumulative VPN counters over the 3.320 s sampling window reported exactly 40 data messages received and 80 transmitted. Sanitized evidence is in `examples/qualification-evidence/2026-09-23/local-workload-v2-smoke.json`; this is a correctness and metric-consistency smoke, not a capacity result.
