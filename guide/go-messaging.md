# Go applications: publish topics, follow the controller

Use `shim/messaging` for automatic managed queue migration. It uses the same YAML and recorded partition owners as the Python client, with Solace's native AMQP 1.0 transport. It never hashes live broker membership to move an existing queue.

```go
client, err := messaging.Open(ctx, messaging.Options{
    ControllerURL: "https://controller.example.net",
    APIKey: os.Getenv("AUTOSCALE_API_KEY"),
    OutboxPath: "/var/lib/my-app/payments.outbox.db",
    Credentials: credentialsForBroker, // local secret lookup by broker ID
})
if err != nil { return err }
defer client.Close()

err = client.Subscribe(ctx, "ledger", commitPayment)
if err != nil { return err }
err = client.Publish("payments/account-42/created", payment, "payment-123")
```

Import `github.com/solacese/solace-broker-autoscale/shim/messaging`. A credentials function returns `(username, password string, err error)`. A handler has signature `func(context.Context, messaging.Message) error`; the message contains `Topic`, JSON `Payload`, and `EventID`. Handle every returned error. The [runnable demo application](../shim/cmd/payments-demo/main.go) includes a durable, transactional deduplication example.

## What acceptance means

`Publish` returns after a synced bbolt transaction on the local disk. It does not wait for a broker receipt or a subscriber reply. Background workers send persistent messages; only a positive broker settlement removes an outbox entry. Negative or uncertain outcomes retain it and refresh the controller assignment before retrying.

The outbox permits one writer process. Restart with the **same file on a persistent local volume**. Each publisher replica needs its own file. Keep that directory private; it contains payloads. A full buffer rejects new work explicitly. Defaults: 100,000 pending records, 100 MB of serialized logical records, 1 MiB per payload and 128 concurrent partitions. Database pages, indexes and file high-water size need additional disk space; the bound is not a filesystem quota. Do not use a network filesystem or delete the outbox to clear an error.

Only one head per partition is in flight. A failed head cannot be overtaken by later events from that publisher; independent partitions continue. Concurrent publisher processes do not establish global business sequence numbers. If several publishers write one account, enforce that business ordering in the application.

A repeated ID with identical content is idempotent while pending. A conflicting pending ID is rejected. Once delivered, the outbox does not keep an unbounded history: subscribers must deduplicate IDs atomically with the business update. Delivery is **at least once**, including when a broker accepted an event but its receipt was lost.

## Native subscriptions and migration

Declare groups in YAML. `Subscribe` registers the group and binds asynchronously; inspect `Status().Receivers` for successfully opened local links. Publisher readiness is based on controller-provisioned durable queues, so consumers can be offline without preventing durable publication.

The client polls `/partitions` and pre-binds the destination alongside the source. The controller waits for consumers, fences old ingress, drains stored and unacknowledged work, observes the grace period, commits ownership, then enables the destination. Source backlog is consumed on the source, never copied to the destination.

Receiver links share an AMQP connection/session per endpoint; a queue does not need a separate TCP connection. One credit per link limits prefetch. The client caps managed flows at 1,024. Each flow processes one handler at a time. Handlers must respect cancellation and have bounded execution; `Close` waits for them. A handler error or panic retries the same head. Invalid envelopes block that queue and report an operational error; they are never silently acknowledged. Connection failures trigger re-discovery/rebinding. Tune broker link, queue and client-profile limits for the actual workload.

## Failure and policy behavior

- `Status` reports pending work, policy rejection, delivery failures, last subscription error and currently bound receivers. Error messages omit endpoint credentials. Subscription errors describe the last observed failure and may remain after recovery.
- Existing assignment leases avoid a control-plane request per event. A send failure discards that cached owner. Temporary control-plane outages permit known assignments for at most five minutes; after that publishing pauses and accepted work stays on disk.
- Authentication/validation failures and incompatible policy changes pause delivery. HTTP redirects are refused. Broker TLS uses normal certificate verification.
- Fleet, partition count and existing routes are preserved in the outbox contract. Non-overlapping added routes can be adopted on restart. Changing/removing existing routes is refused.
- `Flush(ctx)` optionally waits for broker acceptance, not business processing. `Close()` preserves unsent work. Disk or host loss needs an external recovery plan; a local outbox is not replicated storage.

For managed AMQP, inventory entries need an `amqp` URI (`amqps://...` in a TLS environment) as well as the controller's SMF endpoint. SEMP must configure the managed client profile to allow guaranteed send/receive and reject unroutable publications. Only use transports whose successful `Send` means positive durable broker settlement; the default AMQP implementation meets that contract. The in-memory transport is not a durability substitute.

## Kept for existing users

The `dispatch`, `rules`, `resolve` and `spine` packages remain available for manually routed applications and offline demonstrations. Their `AsyncPublisher` remains memory-only. Do not mix their topology ownership with `messaging` for one managed workload.

Go-to-Python and Python-to-Go envelope delivery is covered by a real-broker integration test; binary and string body encodings are both supported.

See [the manager demo](../examples/manager-demo/README.md) and [production qualification gates](production-readiness.md). Passing the local failure scenario is functional evidence, not a production throughput or availability certificate.
