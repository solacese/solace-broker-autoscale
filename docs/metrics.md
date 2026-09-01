# Metrics field mapping

The decision engine consumes a **normalised** `MetricSample` (§5.1). Collectors map a broker metrics
source onto it. Field names below are **not guessed** — they were captured from a live Solace
PubSub+ broker (`solace/solace-pubsub-standard`, SEMP version `broker/10.26.x`) and are saved as
fixtures under `tests/fixtures/semp/`.

## Normalised `MetricSample`

| Normalised field       | Unit    |
|------------------------|---------|
| `ingress_msg_rate`     | msg/s   |
| `egress_msg_rate`      | msg/s   |
| `ingress_byte_rate`    | bytes/s |
| `egress_byte_rate`     | bytes/s |
| `avg_msg_size`         | bytes   |
| `connection_count`     | count   |
| `spool_used`           | bytes   |
| `current_brokers`      | count   |

## SEMPv2 monitor mapping (`metrics.source: semp`)

Source object: `GET /SEMP/v2/monitor/msgVpns/{vpn}` (verified field names):

| Normalised            | SEMPv2 monitor field                          | Notes |
|-----------------------|-----------------------------------------------|-------|
| `ingress_msg_rate`    | `averageRxMsgRate`                            | msg/s, VPN rx |
| `egress_msg_rate`     | `averageTxMsgRate`                            | msg/s, VPN tx |
| `ingress_byte_rate`   | `averageRxByteRate`                           | bytes/s |
| `egress_byte_rate`    | `averageTxByteRate`                           | bytes/s |
| `spool_used`          | `msgSpoolUsage` × 1_048_576                   | field is in **MB** → convert to bytes |
| `avg_msg_size`        | derived: `averageRxByteRate / averageRxMsgRate` | falls back to `egress` or 0 when rx rate is 0 |

Connection count has **no scalar field** on the VPN monitor object (only `maxConnectionCount` and
per-service `service*MaxConnectionCount`). The live count is read from the clients collection:
`GET /SEMP/v2/monitor/msgVpns/{vpn}/clients?count=1` → `meta.count`.

`current_brokers` is supplied by the fleet inventory (the assignment store / actuator), not the
broker — a single broker does not know how many peers serve its shard.

### Absent fields → open questions, never invented

Per §1/§14, a needed field that is genuinely absent is listed here rather than given a plausible
name:

- **Per-protocol live connection counts.** The VPN monitor exposes per-service *maxes*
  (`serviceSmfMaxConnectionCount`, …) but not per-service *live* counts. If the decision engine's
  per-protocol connection accounting (§9.4) needs live per-protocol counts, they must be derived by
  grouping the clients collection by `clientProfileName`/protocol, or read from a source that
  provides them. **OPEN QUESTION** until confirmed against the target broker/version.
- **Cloud monitoring API field names.** The Solace Cloud managed monitoring API payload was not
  supplied as a sample. The `cloud-api` collector is therefore a documented stub that raises
  `NotImplementedError` with the list of fields it needs, rather than guessing. Provide a sample to
  implement it.
- **Prometheus exporter metric names.** No exporter dump was supplied. The `prometheus` collector is
  a documented stub for the same reason.

## Static collector (`metrics.source: static`)

Reads pre-captured `MetricSample` windows from a JSON file (`metrics.static_path`). Needs no
field-name mapping and is used by tests and offline analysis. This is the collector used in CI.
