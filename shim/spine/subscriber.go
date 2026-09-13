// Package spine is the Go shim's client of the event spine (ADR 0009). It subscribes to the reserved
// control topics on the same brokers that carry data, turns each retained topology snapshot into a
// topology.Topology, and applies it to the resolver - so routing becomes event-driven: a scale event
// reaches the shim as a push, not on the next poll.
//
// It also carries the ordering machinery that makes a scale event safe. When a partition key's owner
// changes at a generation, messages must not reorder: the publisher keeps sending a moved key to the
// old broker until in-flight traffic drains, then cuts to the new broker stamping the generation
// (Fencer); the listener releases a key's messages in generation order, holding a newer generation
// until the older one for that key has drained (Reorderer). Both are pure and clock-injected.
package spine

import (
	"context"
	"fmt"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
	"github.com/solacese/solace-broker-autoscale/shim/topology"
)

// TopologySink applies a parsed topology snapshot. *resolve.Resolver satisfies it via Apply, but the
// narrow interface keeps this package testable without the resolver.
type TopologySink interface {
	Apply(t *topology.Topology) bool
}

// ControlSource is the reserved topic the spine publishes topology snapshots on for a shard, matching
// the Python topology_topic(). The shim subscribes to these (or a wildcard over them) for control.
func ControlSource(shard string) string {
	return "_autoscale/shard/" + shard + "/topology"
}

// WildcardTopologySource subscribes to every shard's topology topic in one receiver, using the Solace
// single-level wildcard '*', so a shim following many shards opens one control receiver per broker.
// This matches the topic wildcard syntax the rule engine and both transports use ('*' single level,
// '>' multi level) - not MQTT '+'.
const WildcardTopologySource = "_autoscale/shard/*/topology"

// Subscriber consumes control messages from one broker's spine and applies each snapshot to the sink.
// It is deliberately minimal: parse, apply (generation-win happens in the sink), repeat. If the bus
// is unreachable the subscriber simply delivers nothing and the sink keeps serving its last applied
// topology - fail-open, exactly like the HTTP resolver.
type Subscriber struct {
	tport dispatch.Transport
	sink  TopologySink
	// OnApply, if set, is called after each snapshot with (shard, gen, applied). For observability
	// and tests; never required for correctness.
	OnApply func(shard string, gen int, applied bool)
	// OnError, if set, is called for a snapshot that failed to parse. Malformed control is skipped,
	// never fatal: a bad event must not take the data path down.
	OnError func(err error)
}

// NewSubscriber builds a spine subscriber over a transport and a sink.
func NewSubscriber(tport dispatch.Transport, sink TopologySink) *Subscriber {
	return &Subscriber{tport: tport, sink: sink}
}

// Run subscribes to source (typically WildcardTopologySource) on the broker at uri and applies every
// snapshot until ctx is cancelled or the link fails. It blocks; run it in a goroutine per broker.
func (s *Subscriber) Run(ctx context.Context, uri, source string) error {
	recv, err := s.tport.Receiver(ctx, uri, source)
	if err != nil {
		return fmt.Errorf("spine subscribe %s on %s: %w", source, uri, err)
	}
	defer recv.Close()

	for {
		msg, err := recv.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown
			}
			return fmt.Errorf("spine receive: %w", err)
		}
		s.handle(msg)
	}
}

func (s *Subscriber) handle(msg dispatch.Message) {
	t, err := topology.LoadTopology(msg.Body)
	if err != nil {
		if s.OnError != nil {
			s.OnError(err)
		}
		return
	}
	applied := s.sink.Apply(t)
	if s.OnApply != nil {
		s.OnApply(t.Shard, t.Gen(), applied)
	}
}
