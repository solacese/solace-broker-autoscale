# Native broker feature evidence

Executed on 2026-09-23 with disposable `solace/solace-pubsub-standard` containers. Phase one used one broker for transactions/XA/replay; phase two used one broker plus the official OpenTelemetry Collector for tracing, then a separate three-node Standard HA group. These are semantic checks with small synthetic message sets, not capacity benchmarks.

## Environment and isolation

- Broker: PubSub+ Standard `10.26.0.8799`
- Java client: official Maven artifact `com.solacesystems:sol-jms:10.27.2`
- Bound ports: SEMP `127.0.0.1:28081`, SMF `127.0.0.1:25555`, AMQP `127.0.0.1:25672`
- Queue type under test: ordinary durable **exclusive** queue, matching the application-level partition queue model. No native partitioned-queue restriction was assumed.
- The runner checks port availability, creates one uniquely named `codex-native-qual-*` container, tracks its exact ID, and removes only that ID in an exit trap. The completed run confirmed removal and all three ports were free afterward.
- Credentials are generated per run, passed only through process environment or authenticated requests, redacted from the command ledger, and stored nowhere in Git.

Run with:

```bash
./scripts/qualify-native-features.sh
./scripts/qualify-native-features-phase2.sh trace-only
./scripts/qualify-native-features-phase2.sh ha-only
```

Phase-one machine-readable evidence is `state/claude-directed-qualification/native/phase1-report-20260923-101249.json`. Tracing evidence is under `state/claude-directed-qualification/native/phase2-20260923-102934/`, including `collector/traces.json` and `trace-summary.json`. Final HA evidence is under `state/claude-directed-qualification/native/phase2-20260923-104758/`, including pre/post-failover SEMP XML/JSON and the eight-ID publish/consume records. These ignored reports contain sanitized commands, versions, statuses, message IDs, elapsed milliseconds, and cleanup status.

## Executed results

| Capability | Status | Executed assertion |
|---|---|---|
| Local JMS transaction | PASS | Five persistent publications were invisible before `Session.commit()` and then received in ID order. Four publications followed by `Session.rollback()` produced no visible message. |
| Rolled-back consumption | PASS | Three committed messages were consumed in a transacted session, rolled back, redelivered with matching per-message IDs and redelivery evidence, then committed. |
| Client crash with open transaction | PASS | A separate JVM published four persistent messages in an uncommitted transaction and terminated via `Runtime.halt(73)`; a fresh connection observed zero messages. |
| XA prepare/commit/rollback | PASS | The official JMS XA API returned `XA_OK`; three committed IDs were visible and the separately prepared/rolled-back branch exposed zero messages. This is distinct from the local JMS transaction test. |
| XA broker restart recovery | PASS | A two-message XA branch was prepared, the owned broker container was restarted, a fresh `XAResource.recover()` returned the matching XID, two-phase commit completed, and both IDs were received. |
| Ordinary queue replay | PASS | A bounded 100 MB replay log and topic filter were configured with SEMP v2. Five persistent topic messages were logged and replayed to an ordinary exclusive queue; after the consumer received the first replayed message, three new live messages were published. All eight IDs arrived with zero duplicates. Observed order was replay IDs 0–4 followed by live IDs 0–2. The run did not independently observe broker replay-active state at live ingress, so it proves replay followed by new live publications, not overlap or a universal ordering guarantee. |
| Native distributed tracing | PASS | A telemetry profile, generated receiver profiles/queue, receiver ACL, trace filter, and SMF subscription were enabled. Official `otel/opentelemetry-collector-contrib:0.100.0` used its Solace AMQP receiver and file exporter. Structural JSON parsing found all 12 exact message IDs across 24 spans with operations `(anonymous) send` and `(topic) receive`; this is an actual span assertion, not only a configuration probe. |
| Three-node Standard HA topology | PASS | Two message-routing nodes and one monitor node formed a documented developer HA group. Authenticated SEMP v1 XML showed configuration enabled, redundancy `Up`, ADB link/hello `true`, Primary `Local Active`, Backup `Mate Active`, with config-sync and message-spool state captured. |
| HA failover durability | PASS | Eight persistent IDs were published to an exclusive queue on the active primary. After stopping that exact owned primary, SEMP v1 showed the Backup became `Local Active`; expected degraded state was redundancy `Down`, ADB link/hello `false`, while message spool was `AD-Active` and disk `Ready`. All eight IDs were consumed from the backup in order. |
| DR replication | NOT TESTED | HA is not a DR pair. A distinct two-site message-VPN replication setup requires remote VPN bridge authentication, replicated-topic configuration, active/standby roles, and promotion testing. The bounded phase ended after the real HA failover; no unsupported/license or fake URL-switch claim is made. |

Phase one final aggregate was **10 PASS, 0 FAIL, 2 BLOCKED** before tracing/HA were exercised. Phase-two evidence supersedes those two provisional blockers: tracing produced **4 PASS**, and the final HA-only run produced **2 PASS, 0 FAIL, 1 NOT TESTED** (DR).

## Replay configuration actually exercised

The runner used the broker's SEMP v2 schema, including its exact replay topic-filter field:

```text
POST  /SEMP/v2/config/msgVpns/default/replayLogs
      {replayLogName:<unique>, maxSpoolUsage:100}
PATCH /SEMP/v2/config/msgVpns/default/replayLogs/<log>
      {ingressEnabled:true, egressEnabled:true, topicFilterEnabled:true}
POST  /SEMP/v2/config/msgVpns/default/replayLogs/<log>/topicFilterSubscriptions
      {topicFilterSubscription:<topic>}
POST  /SEMP/v2/config/msgVpns/default/queues/<ordinary-exclusive-queue>/subscriptions
      {subscriptionTopic:<topic>}
PUT   /SEMP/v2/action/msgVpns/default/queues/<ordinary-exclusive-queue>/startReplay
      {replayLogName:<log>}
```

## Evidence boundaries

- Message counts and latencies document bounded behavior only; they do not establish throughput or capacity.
- Replay ordering is recorded exactly as observed. The test proves ID completeness and reports duplicates; it does not generalize the observed replay-before-live sequence into a broker-wide contract.
- XA restart recovery proves an in-doubt branch survived a restart of this single broker container. It does not qualify HA failover or DR replication.
- Native tracing is qualified for this bounded path: the official collector reconciled exact application IDs to native send/receive spans. The measured span count is not a capacity result.
- HA failover is qualified for persistent ordinary-queue messages in this three-node local Standard topology. This does not qualify transaction-in-flight behavior across HA failover or DR.
- DR replication remains unqualified and should stay pinned until a separate two-site message-VPN replication topology is configured and promotion/failback semantics are executed.

Official references used for implementation:

- [Solace JMS API: obtaining a connection factory](https://docs.solace.com/API/Solace-JMS-API/Obtaining-Connection-Fac.htm)
- [Solace JMS API transactions](https://docs.solace.com/API/Solace-JMS-API/Using-Local-Transactions.htm)
- [Message replay configuration](https://docs.solace.com/Features/Replay/Msg-Replay-Config.htm)
- [Message replay playback](https://docs.solace.com/Features/Replay/Msg-Replay-Playback.htm)
- [Distributed tracing setup overview](https://docs.solace.com/Features/Distributed-Tracing/Distributed-Tracing-Setup-Overview.htm)
- [Solace OpenTelemetry Receiver](https://docs.solace.com/Features/Distributed-Tracing/Distributed-Tracing-Receiver.htm)
- [Configuring HA groups](https://docs.solace.com/Features/HA-Redundancy/Configuring-HA-Groups.htm)
- [Software broker HA redundancy](https://docs.solace.com/Features/HA-Redundancy/SW-Broker-Redundancy-and-Fault-Tolerance.htm)
