package dispatch

import (
	"context"
	"fmt"
	"sync"

	"github.com/solacese/solace-broker-autoscale/shim/rules"
)

// Delivery is one received message plus what the shim recovered about it. GroupID and the rule are
// read back from the wire (the group-id / PartitionKeyProperty the publisher stamped) so a consumer
// can process a coherent per-key stream without re-deciding, while Rule/Address re-derived from the
// plan let a consumer confirm the message landed where the rules say it should.
type Delivery struct {
	Broker  string
	Source  string
	Message Message
	GroupID string // partition key recovered from group-id / saas_partition_key
}

// ListenerShim subscribes across every broker the rules can target and fans their messages into one
// channel. It is the consumer-side mirror of the PublisherShim: because both sides share the same
// plan, the set of brokers to cover is exactly plan.Targets().
type ListenerShim struct {
	plan    *rules.Plan
	resolve BrokerResolver
	tport   Transport

	mu    sync.Mutex
	recvs []Receiver
}

// NewListener builds a listener over a plan, a broker->uri resolver, and a transport.
func NewListener(plan *rules.Plan, resolve BrokerResolver, tport Transport) *ListenerShim {
	return &ListenerShim{plan: plan, resolve: resolve, tport: tport}
}

// Subscribe opens a receiver on the given source (a topic pattern or queue) for every broker the plan
// can route to, and returns a single channel fanning in all their deliveries. The channel closes when
// ctx is cancelled and every receiver has stopped. Call Close to release the receivers.
func (l *ListenerShim) Subscribe(ctx context.Context, source string) (<-chan Delivery, error) {
	brokers := l.plan.Targets()
	if len(brokers) == 0 {
		return nil, fmt.Errorf("plan has no target brokers to subscribe to")
	}

	out := make(chan Delivery)
	var wg sync.WaitGroup

	for _, broker := range brokers {
		uri, err := l.resolve(ctx, broker)
		if err != nil {
			l.Close()
			return nil, fmt.Errorf("resolve broker %q: %w", broker, err)
		}
		r, err := l.tport.Receiver(ctx, uri, source)
		if err != nil {
			l.Close()
			return nil, fmt.Errorf("subscribe broker %q at %s: %w", broker, uri, err)
		}
		l.mu.Lock()
		l.recvs = append(l.recvs, r)
		l.mu.Unlock()

		wg.Add(1)
		go l.pump(ctx, broker, source, r, out, &wg)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out, nil
}

func (l *ListenerShim) pump(ctx context.Context, broker, source string, r Receiver, out chan<- Delivery, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		msg, err := r.Receive(ctx)
		if err != nil {
			return // context cancelled or link closed
		}
		d := Delivery{
			Broker:  broker,
			Source:  source,
			Message: msg,
			GroupID: recoverKey(msg),
		}
		select {
		case out <- d:
		case <-ctx.Done():
			return
		}
	}
}

// recoverKey reads the partition key back from the wire, preferring the explicit application property
// and falling back to the AMQP group-id.
func recoverKey(msg Message) string {
	if msg.Properties != nil {
		if k, ok := msg.Properties[PartitionKeyProperty]; ok && k != "" {
			return k
		}
	}
	return msg.GroupID
}

// Close releases every open receiver, returning the first error encountered.
func (l *ListenerShim) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for _, r := range l.recvs {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	l.recvs = nil
	return firstErr
}
