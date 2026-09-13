# scaling-controller

The control plane for solace-broker-autoscale: it watches how busy your Solace brokers are, decides
when the fleet needs more or fewer of them, and (only when you turn scaling on) acts on that
decision. It also replicates broker configuration across the fleet and hands clients their broker
assignment.

This is the Python half of the project. The per-message data path is the native Go shim under
[`../shim`](../shim), which reads the same portable rule spec this package can emit.

## Install

```
pip install -e '.[dev]'
```

## Use it

```
# How many brokers does this workload need now?
solace-autoscale recommend --config ../examples/config.example.yaml

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
  endpoint adapters. These never carry a message and never vend credentials.
- `tests/` unit tests (default) and live-broker integration tests (`-m integration`).

## Verify

```
ruff check .
mypy solace_autoscale
pytest -m "not integration" -q
```

See [`../guide`](../guide) for architecture, safety, the capacity model, and the rule spec.
