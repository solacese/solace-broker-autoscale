// Package dispatch is the data path of the smart shim: it wraps the application's own messaging
// client rather than proxying it. A PublisherShim applies the rule engine per message to pick a
// target broker, partition key, and address, then hands the message to a Transport for that broker.
// A ListenerShim subscribes across every broker the rules can target and re-runs the same rules so a
// consumer sees a coherent per-key stream.
//
// The Transport interface is the seam between the shim logic and the wire. The real implementation
// (transport/amqp) speaks AMQP 1.0; an in-memory implementation (transport/memory) lets the whole
// shim be tested offline. The shim stamps the partition key as the AMQP group-id and also as a
// "saas_partition_key" application property, so brokers and listeners can both read it.
package dispatch

import "context"

// PartitionKeyProperty is the application-property name the shim stamps with the partition key, in
// addition to the AMQP group-id. Listeners read it back to demultiplex per-key streams.
const PartitionKeyProperty = "saas_partition_key"

// Message is one message crossing the shim. Properties carries application properties; the shim adds
// the partition key under PartitionKeyProperty and sets GroupID from the same value.
type Message struct {
	Address    string            // topic / address to publish to
	Body       []byte            // opaque payload
	GroupID    string            // AMQP group-id (the partition key); "" for none
	Properties map[string]string // application properties
}

// Sender publishes messages to one broker. Send must be safe for concurrent use.
type Sender interface {
	// Send publishes one message. It should return an error the shim can retry on transient failure.
	Send(ctx context.Context, msg Message) error
	// Close releases the underlying connection.
	Close() error
}

// Receiver delivers messages from one broker. Receive blocks until a message arrives, the context is
// cancelled, or the link fails.
type Receiver interface {
	Receive(ctx context.Context) (Message, error)
	Close() error
}

// Transport opens senders and receivers against brokers addressed by a connection URI. A single
// Transport backs a whole shim; the shim caches the senders it opens (see connCache).
type Transport interface {
	// Sender returns a Sender for the broker reachable at uri. Implementations may return a fresh
	// sender each call; the shim is responsible for caching.
	Sender(ctx context.Context, uri string) (Sender, error)
	// Receiver returns a Receiver bound to source (a topic or queue) on the broker at uri.
	Receiver(ctx context.Context, uri, source string) (Receiver, error)
}
