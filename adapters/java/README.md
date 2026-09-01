# Java client adapters

Mirrors the Python tiers (see `docs/client-integration.md`):

- **Tier 1 resolver** (`Resolver.java`): calls `GET /assignment`, caches the result, fails open to
  the cache when the service is unreachable. Returns an `Assignment` with the per-protocol endpoint
  map. Never handles credentials.
- **Qpid JMS adapter**: builds an AMQP `ConnectionFactory` (or a failover URI list) from the `amqp`
  endpoint; the application uses the standard Qpid JMS client.
- **Paho adapter**: builds MQTT connect options from the `mqtt` endpoint.
- **Solace JMS wrapper (Tier 2)**: wraps the Solace JMS `ConnectionFactory`, caches the assignment,
  re-resolves on reconnect, and refuses to silently reassign a guaranteed consumer.

This directory ships the interface and wiring; drop it into your build (Maven/Gradle) alongside your
existing messaging client. The reference implementation lives in `Resolver.java` and
`QpidJmsAdapter.java`.
