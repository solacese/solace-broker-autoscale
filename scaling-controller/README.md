# scaling-controller

The control plane for solace-broker-autoscale: it watches how busy your Solace brokers are, decides
when the fleet needs more or fewer of them, and (only when you turn scaling on) acts on that
decision. It also replicates broker configuration across the fleet and hands clients their broker
assignment.

This is the Python control plane and managed native SMF client. The separate Go AMQP shim under
[`../shim`](../shim), reads the portable topic/payload rule spec this package can emit. Managed queue migration uses the Python SMF client and its durable outbox.

## Install

```
pip install -e '.[dev]'
```

## Use it

```
# How many brokers does this recorded workload window need now?
solace-autoscale recommend --config ../examples/config.example.yaml --metrics ../examples/orders.json

# Play a workload forward and see when it would need to scale.
solace-autoscale simulate --config ../examples/config.example.yaml

# Show how the smart shim would route one message (offline).
solace-autoscale dispatch-test --config ../examples/config.example.yaml \
  --topic orders/eu/new --payload '{"priority":"high","region":"EU"}'
```

## Layout

- `solace_autoscale/` the package: `decision`, `capacity`, `metrics`, `actuator`, `assignment`,
  `configsync`, `dispatch` (the pure rule engine the Go shim mirrors), `simulator`, `report`, `dns`.
- `solace_autoscale_client/` Tier-1 client helpers: the fail-open `Resolver` and per-protocol
  endpoint adapters, plus the native SMF publish/subscribe client and durable outbox.
- `tests/` unit tests (default) and live-broker integration tests (`-m integration`).

## Verify

```
ruff check .
mypy solace_autoscale
pytest -m "not integration" -q
```

See [`../guide`](../guide) for architecture, safety, the capacity model, and the rule spec.
