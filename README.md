# Solace Broker Autoscale

A community alpha control plane for placing guaranteed-message partitions across existing Solace brokers.

> **Status:** supervised pilot only. Use dedicated, isolated, non-production brokers. This is not an officially supported Solace product.

```text
Message path: publisher -> managed Python/Go client -> Solace broker -> managed client -> subscriber
Control plane: managed clients -> assignment API -> SQLite
               controller -> SQLite and inventoried brokers through SEMP
```

The controller is not a message proxy. Customer code chooses a stable business key; the local client maps it to a logical partition and asks the assignment API for that partition's broker. The controller owns only its generated queues and client profile, observes them through SEMP, and performs controlled handover between inventoried brokers.

## Quick start

Requirements: Python 3.11+; Go is optional for the managed Go client.

```bash
./install.sh
.venv/bin/python examples/customer_library.py
```

This broker-free exercise validates the customer routing function: events for one tenant/order produce one stable SHA-256 result, while another order differs.

For a supervised broker trial:

```bash
cp config.example.yaml customer.yaml
cp inventory.example.yaml customer-inventory.yaml
# Replace every placeholder and build models/customer-profile.json from measurements.
export SOLACE_SEMP_USERNAME='REPLACE_ME'
export SOLACE_SEMP_PASSWORD='REPLACE_ME'
./run-controller.sh customer.yaml customer-inventory.yaml
```

`run-controller.sh` starts the loopback assignment API and controller against the same SQLite file. `/healthz` reports that the API process is alive. `/readyz` reports only persisted bootstrap completion: every configured managed partition and subscriber group was marked ready. It does **not** prove that the controller is running or that brokers, SEMP, publishers, or subscribers are currently healthy. Starting the controller is a broker-configuration action, not a dry run.

On every inventoried broker/VPN, first create or enable the application username configured as `provisioning.client_username` (the example uses `autoscale-app`) with permissions to publish managed topics and consume controller-managed queues. Inventory one active endpoint per HA service, never standby nodes as separate brokers.

## Customer library

`examples/customer_library.py` is the application-owned routing contract. Register the same evaluator name, version, and result type in every publisher and subscriber deployment. The function selects only stable business data; it cannot select a broker.

Python:

```python
from solace_autoscale_client import MessagingClient

client.publish("orders/acme/created", payload, event_id=event_id, headers=headers)
client.subscribe(group="processor", handler=process)
```

The Go API is `github.com/solacese/solace-broker-autoscale/shim/messaging`; register an equivalent evaluator before opening the client.

To exercise configured brokers, start the subscriber in terminal 1:

```bash
export CONTROLLER_URL='http://127.0.0.1:8099'
export SOLACE_CLIENT_USERNAME='autoscale-app'
export SOLACE_CLIENT_PASSWORD='REPLACE_WITH_THE_PASSWORD_CONFIGURED_ON_EVERY_BROKER'
# Also export SOLACE_ASSIGNMENT_API_KEY if the API uses one.
.venv/bin/python examples/subscriber.py
```

After it reports ready, export the same variables and publish from terminal 2:

```bash
.venv/bin/python examples/publisher.py
```

`publish()` returns after durable local acceptance; `flush()` waits for broker confirmation. Keep publisher state on persistent local storage. Subscriber delivery is at least once: atomically store the event ID with the business effect before the handler returns.

## Handover and DMR

[Dynamic Message Routing (DMR)](https://docs.solace.com/Features/DMR/DMR-Overview.htm) propagates subscription interest through a broker network. It is not fleet-wide work sharing for one logical workload: matching subscriptions on multiple nodes can each attract messages. This project does not replace DMR and has not qualified DMR coexistence.

For a managed partition, handover is prepare → fence → drain → commit → activate:

1. Prepare an ingress-disabled target and wait for its consumers.
2. Fence source ingress so stale publishers are rejected and retain work in their durable outbox.
3. Require continuously empty ready, stored, and unacknowledged state through the configured grace window.
4. Atomically commit the owner in SQLite, then activate target ingress.

Before commit, timeout recovery restores the source only when an empty fenced target proves rollback safe. After commit, recovery moves forward to target activation. Notifications are hints; HTTP/SQLite state and broker fences are authoritative.

## Operational boundaries

- Keep `.state/controller.db`, publisher outboxes, and subscriber business/deduplication state on persistent local storage; back up and restore them as one operational set.
- The assignment API binds to `127.0.0.1:8099`. Non-loopback binding is rejected without `SOLACE_ASSIGNMENT_API_KEY`; use authenticated TLS termination before any network exposure.
- Use a measured model matching provider, broker version, HA tier, payload, fanout, and feature scenario. The checked-in synthetic model is rejected by the controller. See [PERFORMANCE.md](PERFORMANCE.md).
- Routing contracts, partition counts, broker identity, and recorded placement facts fail closed when changed beneath durable state.
- Cloud provisioning is disabled in the customer example. The managed runtime has no broker deletion or automatic scale-in path.
- Transactions/XA, replay, tracing, disaster recovery, and DMR interoperability are not qualified here; unsupported migration combinations remain pinned.
- There is no multi-host controller HA, distributed state store, disk-loss recovery, exactly-once guarantee, or end-to-end ordering guarantee.
- Production qualification still requires target-hardware capacity tests, crash/failover and disk-full testing, backup/restore drills, poison-message handling, long soak, saturation, security review, and an operated recovery runbook.

## Repository

- `scaling-controller/solace_autoscale/` — controller, assignment API, migration, SEMP, placement, and capacity logic
- `scaling-controller/solace_autoscale_client/` — managed Python publisher/subscriber library
- `shim/messaging/` — managed Go publisher/subscriber library; `shim/transport/amqp/` is its transport
- `examples/` — customer routing library plus minimal publisher and subscriber
- `PERFORMANCE.md` — measurement and qualification requirements
- `install.sh`, `run-controller.sh` — installation and supervised startup

Community project · Apache 2.0 · Not an officially supported Solace product.
