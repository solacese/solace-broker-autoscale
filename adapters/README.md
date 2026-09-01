# Client adapters

Tier-1 resolvers and thin per-protocol adapters, and the Tier-2 SDK wrappers. See
`docs/client-integration.md` for the tiering model and cross-cutting requirements.

- `python/` — `solace_autoscale_client`: `Resolver` (fail-open cache), per-protocol adapters
  (`amqp_uri`, `mqtt_config`, `rest_target`, `smf_host`), and the Tier-2 `SmfClient` wrapper.
- `java/` — resolver + Qpid JMS, Paho, and Solace JMS wiring (skeleton; mirrors the Python tiers).

None of these carry a message on the data path, and none vend credentials.

## Python quick use

```python
from solace_autoscale_client import Resolver, amqp_uri, mqtt_config

r = Resolver(base_url="https://assign.example.com")
a = r.resolve(shard="shard-a", client_id="orders-consumer", mode="guaranteed", protocol="amqp")
uri = amqp_uri(a)          # feed to your unmodified Qpid JMS / Proton client
# ... connect with YOUR client library and YOUR credentials ...
```

If the assignment service is unreachable, `resolve` returns the last cached assignment rather than
failing — the control plane being down must never take your application down.
