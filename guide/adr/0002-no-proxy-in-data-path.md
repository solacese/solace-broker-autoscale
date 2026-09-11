# ADR 0002: No proxy in the data path

## Status
Accepted.

## Context
A horizontal fleet needs clients to reach the right broker. One tempting design is a message proxy
that terminates client connections and forwards to brokers. That would centralise routing and make
reassignment invisible to clients.

It would also put a stateful, latency-adding, single-point-of-failure component on the **data path**
of a messaging system whose entire value is throughput and delivery guarantees. A proxy would have
to re-implement flow control, guaranteed-delivery acknowledgement, and per-protocol semantics for
SMF, AMQP, MQTT, and REST - correctly - or it silently breaks delivery.

## Decision
**No proxy in the data path.** Client traffic goes directly to brokers. Every component in this
repository is **control plane**: it decides, recommends, assigns, and actuates, but never carries a
client message.

## Consequences
- Client steering is done out-of-band: DNS (Tier 0), a resolver returning a URI (Tier 1), or an SDK
  wrapper that caches an assignment (Tier 2). See ADR 0003 and §9.
- The assignment service returns a **location**, never a connection it owns and never a credential.
- We accept that reassignment is not instantaneous (DNS TTL, client reconnect) in exchange for not
  owning the data path. This is the correct trade for a messaging system.
- Guaranteed consumers get sticky/durable placement because their queue lives on one broker; the
  control plane records that placement rather than routing around it.
