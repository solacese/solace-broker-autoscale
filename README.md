# Solace Broker Autoscale

A community alpha control plane for spreading guaranteed messaging partitions across existing Solace brokers.

```text
Message path:
  publisher -> publisher shim -> Solace brokers -> subscriber shim -> subscriber

Control plane:
  publisher/subscriber shims -> assignment HTTP API (SQLite ownership)
  controller reconciliation -> existing Solace brokers (SEMP)
```

The controller is not a message proxy. A logical partition is a stable bucket of related messages with exactly one recorded broker owner at a time. It is an application-routing concept—not a broker's native partition and not the primary/standby nodes inside an HA service. The controller prepares managed queues, fences old writes, drains, then commits an owner handover. Publisher and subscriber “shims” are embedded client APIs, not separate services. Python uses native SMF; Go uses AMQP 1.0. Both preserve the managed ownership and migration contract.

## Status and safety

Use this only for a supervised trial on dedicated, isolated, non-production brokers. The example configuration:

- requires an explicit inventory and measured capacity model;
- gives the controller sole ownership of generated queues and its client profile; incompatible pre-existing managed names are rejected rather than adopted;
- disables Cloud provisioning and contains no broker deletion path;
- declares advanced features off so unqualified state is not moved.

The controller runtime intentionally rejects `dry_run: true`, recommendation mode, and the synthetic model. Starting it is therefore an explicit broker-configuration action, not a read-only preview. `run-controller.sh` starts the assignment API and reconciliation process against the same SQLite file; it never starts or tears down brokers.

## Install and broker-free practice

Requirements: Python 3.11+; Go is optional unless using the managed Go client.

```bash
./install.sh
.venv/bin/python examples/customer_library.py
```

The exercise runs without Docker, credentials, or a broker. It proves that two events for one tenant/order return the same SHA-256 routing result while another order differs. The customer function selects only a stable business key/digest; it cannot choose a broker.

## Configure existing brokers

1. Copy `config.example.yaml` and `inventory.example.yaml` as shown below.
2. Replace every placeholder. Inventory one active endpoint per HA service—never its standby nodes separately.
3. On **every listed broker/VPN**, create or enable the application username `autoscale-app`, set the same operator-chosen sample password, and grant the authentication/ACL permissions needed to publish managed topics and consume the controller-managed queues. Existing brokers do not get this username automatically.
4. Set SEMP credentials through the environment names referenced by inventory. The controller uses them to make scoped queue and managed client-profile changes.
5. Build `models/customer-profile.json` from measurements matching provider, broker version, HA tier, payloads, fanout, and features. See [PERFORMANCE.md](PERFORMANCE.md).
6. Keep `.state/controller.db`, publisher state, and subscriber business/deduplication state on persistent local storage and back them up together.

```bash
cp config.example.yaml customer.yaml
cp inventory.example.yaml customer-inventory.yaml
# customer.yaml already references customer-inventory.yaml
export SOLACE_SEMP_USERNAME='REPLACE_ME'
export SOLACE_SEMP_PASSWORD='REPLACE_ME'
./run-controller.sh customer.yaml customer-inventory.yaml
```

The API binds to `127.0.0.1:8099`. Set `SOLACE_ASSIGNMENT_API_KEY` before exposing it beyond loopback; clients pass the same value without the `Bearer` prefix.

## Publish and subscribe

The following walkthrough needs the configured controller and existing broker endpoints. In one terminal:

```bash
export CONTROLLER_URL='http://127.0.0.1:8099'
export SOLACE_CLIENT_USERNAME='autoscale-app'
export SOLACE_CLIENT_PASSWORD='REPLACE_WITH_THE_PASSWORD_CONFIGURED_ON_EVERY_BROKER'
.venv/bin/python examples/subscriber.py
```

After it reports ready, publish from another terminal with the same environment:

```bash
.venv/bin/python examples/publisher.py
```

`publish()` returns after durable local acceptance; `flush()` waits for broker confirmation. Preserve the publisher state directory across restart. Subscriber delivery is at least once: replace the example `print` with one business-database transaction that stores the event ID and applies the business effect before the handler returns.

### Managed APIs

Python:

```python
from solace_autoscale_client import MessagingClient

client.publish("orders/acme/created", payload, event_id=event_id, headers=headers)
client.subscribe(group="processor", handler=process)
```

The managed Go API remains at `github.com/solacese/solace-broker-autoscale/shim/messaging`. This evaluator-based example also requires registering an equivalent `order-lifecycle@1.0.0` Go customer evaluator before opening the client.

## Guarantees and limits

- Guaranteed delivery is at least once; deduplicate event IDs in the business transaction.
- The local publisher outbox serializes each partition, but this alpha does not claim end-to-end ordering or exactly-once processing.
- Stored backlog drains on the source; it is not copied to the destination.
- Routing contracts and partition counts cannot be rewritten underneath durable state.
- Transactions/XA, replay, tracing, and disaster recovery require matching placement declarations and migration evidence; unsupported combinations remain pinned.
- There is no multi-host controller HA, automatic scale-in/broker deletion, arbitrary existing-queue adoption, or disk-loss recovery.
- Capacity is deployment-specific. The synthetic model is never valid for operations.

## Repository

- `scaling-controller/solace_autoscale/` — controller, assignment API, migration, SEMP, placement, and capacity logic.
- `scaling-controller/solace_autoscale_client/` — managed Python publisher/subscriber API.
- `shim/messaging/` — managed Go publisher/subscriber API; `shim/transport/amqp/` is its wire transport.
- `examples/` — one customer routing library plus minimal publisher/subscriber usage.
- `PERFORMANCE.md` — calibration method, feature declarations, measured storage diagnostic, and release limits.
- `install.sh`, `run-controller.sh` — dependency installation and controller startup.

Community project · Apache 2.0 · Not an officially supported Solace product.
