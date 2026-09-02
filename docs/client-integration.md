# Client integration (§9)

Clients reach brokers **directly** - there is no proxy in the data path (ADR 0002). Steering is
out-of-band, in three tiers. Pick per application.

## The assignment service

`GET /assignment?shard=&client_id=&protocol=&mode=direct|guaranteed` returns a **per-protocol
endpoint map**, not a single host/port - every protocol listens on a different port and expects a
different URI form, so returning one endpoint forces the caller to reconstruct the rest and get it
wrong.

```json
{
  "broker_id": "shard-a-03",
  "msg_vpn": "acme-prod",
  "state": "active",
  "lease_seconds": 300,
  "endpoints": {
    "smf":  "tcps://shard-a-03.example.com:55443",
    "amqp": "amqps://shard-a-03.example.com:5671",
    "mqtt": "ssl://shard-a-03.example.com:8883",
    "rest": "https://shard-a-03.example.com:9443",
    "web":  "wss://shard-a-03.example.com:443"
  }
}
```

- Ports are read from broker configuration, never hardcoded. They differ per service and customer.
- The service returns a **location, never a credential**. Authentication stays with your existing
  mechanism.
- Guaranteed placement is sticky and durable, persisted, and survives service restart and lease
  expiry (the queue outlives the connection). A `DRAINING` broker keeps serving existing placements
  but takes no new ones.
- Store: SQLite by default; Postgres for multi-instance. Multi-instance uses **optimistic locking**
  (a version compare-and-set on placement writes) rather than leader election, because placement
  writes are low-rate and idempotent.
- Health `/healthz` and readiness `/readyz` endpoints.

## Three tiers

### Tier 0 - DNS, zero client change
The updater maintains one DNS record per shard (`shard-a.brokers.example.com`) pointing at the
shard's ACTIVE brokers. Clients connect to the shard name; brokers are invisible. Any protocol, any
library, no code.

**Limits (documented, not incidental):** DNS resolves per *shard*, not per client, so it cannot
express sticky placement for a specific guaranteed consumer. TTL bounds reassignment propagation,
and many clients cache DNS for the process lifetime. **Use Tier 0 for direct messaging and
publishers; do not use it for durable consumers.**

### Tier 1 - resolver + thin adapters (default for AMQP, MQTT)
A small helper per language calls the assignment service and returns a connection URI / factory
config; your app uses its own unmodified client (Qpid JMS/Proton, Paho, any HTTP client). ~20 lines
per protocol, nothing on the message path. Qpid JMS failover URI lists are built for the warm path.

### Tier 2 - full SDK wrapper (only SMF, Solace JMS)
Wraps connection creation, caches the assignment, re-looks-up on reconnect, honours a reassignment
signal. **Guaranteed consumers are never silently reassigned** - a reassignment signal applies to
direct clients and publishers only; the wrapper refuses it for a guaranteed consumer so the app
migrates deliberately. Do **not** build a Tier 2 wrapper for AMQP or MQTT.

## Protocol coverage (verified against a real broker)

| Protocol | Client | Tier | Integration test |
|---|---|---|---|
| SMF | Solace PubSub+ Messaging API | 2 | `test_smf_guaranteed_publish` - persistent publish lands on a durable queue |
| AMQP 1.0 | Qpid Proton / Qpid JMS / rhea | 1 | `test_amqp_send` |
| MQTT 3.1.1 | Paho | 1 | `test_mqtt_pubsub` - pub/sub round trip |
| MQTT 5 | Paho | 1 | See note below |
| REST | any HTTP client | 1 | `test_rest_publish` |
| JMS | Solace JMS or Qpid JMS | 1 or 2 | via SMF (Solace JMS) / AMQP (Qpid JMS) adapters |

Run: `pytest -m integration` against a local broker (see `tests/test_integration_broker.py`).

### MQTT 5 CONNACK Server Reference - verify per broker
MQTT 5 has a standard server-redirect: the broker can return a **Server Reference** property in the
CONNACK (or DISCONNECT), and a compliant client follows it with no adapter at all. Whether Solace
emits it depends on broker version/config, so **verify against your broker rather than assuming**.
If present, MQTT 5 clients need no Tier-1 adapter for reassignment; if absent, use the Tier-1
adapter as for MQTT 3.1.1. This is an **open question** to confirm on your target broker.

## Cross-cutting (§9.4)

- **TLS and hostnames.** Every broker in a shard needs a certificate valid for the name the client
  connects to. Two supported options, plan this before the first pilot:
  1. a **wildcard** certificate covering all broker hostnames in the shard's zone, or
  2. **SAN** entries per broker hostname.
  The integration tests use plaintext ports for simplicity; production uses the TLS ports in the
  endpoint map.
- **Failure behaviour - never fail closed.** When the assignment service is unreachable, the
  resolver returns the **cached** assignment. The assignment service being down must not take an
  application down. Only when there is no cache AND the service is down does the resolver error.
- **Guaranteed consumers are never silently reassigned.** Reassignment signals apply to direct-mode
  clients and publishers only.
