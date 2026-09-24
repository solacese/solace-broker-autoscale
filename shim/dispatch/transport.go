// Package dispatch defines the transport seam used by the managed Go messaging client.
// The AMQP implementation lives in transport/amqp; applications may inject a test transport.
package dispatch

import "context"

// Message is one message sent or received by the managed client transport.
type Message struct {
	Address    string            // topic / address to publish to
	Body       []byte            // opaque payload
	GroupID    string            // AMQP group-id (the partition key); "" for none
	Properties map[string]string // application properties
	// Ack and Release are set by reliable transports. Applications settle only after processing.
	Ack     func(context.Context) error
	Release func(context.Context) error
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
