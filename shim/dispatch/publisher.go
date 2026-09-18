package dispatch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/rules"
)

// BrokerResolver maps a broker name (as it appears in a rule's route) to the connection URI the
// Transport should dial. It is typically backed by the Tier-1 resolver, but any function works, so
// the publisher can be tested with a static map. It is called at most once per (broker, uri) pair
// thanks to the shim's connection cache.
type BrokerResolver func(ctx context.Context, broker string) (uri string, err error)

// RetryPolicy bounds how hard the shim retries a transient send failure before giving up. A send is
// attempted up to MaxAttempts times with a fixed Backoff between tries. Zero values mean a single
// attempt with no delay.
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

func (p RetryPolicy) attempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

// PublisherShim routes and publishes messages. It owns a connection cache keyed by (brokerID, uri):
// a rule names a broker, the BrokerResolver turns that into a URI, and the shim opens at most one
// Sender per distinct (broker, uri) and reuses it. Close shuts every cached sender.
type PublisherShim struct {
	plan    *rules.Plan
	resolve BrokerResolver
	tport   Transport
	retry   RetryPolicy
	sleep   func(time.Duration) // seam for tests; nil -> time.Sleep

	mu    sync.Mutex
	conns map[connKey]Sender
}

type connKey struct{ broker, uri string }

// NewPublisher builds a publisher over a plan, a broker->uri resolver, and a transport.
func NewPublisher(plan *rules.Plan, resolve BrokerResolver, tport Transport, retry RetryPolicy) *PublisherShim {
	return &PublisherShim{
		plan:    plan,
		resolve: resolve,
		tport:   tport,
		retry:   retry,
		conns:   map[connKey]Sender{},
	}
}

// Result reports what the shim did with a message: the routing decision and the broker URI used.
type Result struct {
	Decision rules.Decision
	URI      string
}

// Publish routes one message by (topic, payload), then sends it to the chosen broker with the
// partition key stamped as both the AMQP group-id and the PartitionKeyProperty. Extra application
// properties are passed through. It returns the routing decision so callers can log or assert on it.
func (s *PublisherShim) Publish(ctx context.Context, topic string, payload []byte, props map[string]string) (Result, error) {
	dec, err := s.plan.Decide(topic, payload)
	if err != nil {
		return Result{}, fmt.Errorf("route %q: %w", topic, err)
	}
	uri, err := s.resolve(ctx, dec.Broker)
	if err != nil {
		return Result{Decision: dec}, fmt.Errorf("resolve broker %q: %w", dec.Broker, err)
	}
	sender, err := s.sender(ctx, dec.Broker, uri)
	if err != nil {
		return Result{Decision: dec, URI: uri}, fmt.Errorf("connect broker %q: %w", dec.Broker, err)
	}

	merged := make(map[string]string, len(props)+1)
	for k, v := range props {
		merged[k] = v
	}
	if dec.Key != "" {
		merged[PartitionKeyProperty] = dec.Key
	}
	msg := Message{Address: dec.Address, Body: payload, GroupID: dec.Key, Properties: merged}

	if err := s.sendWithRetry(ctx, sender, msg); err != nil {
		return Result{Decision: dec, URI: uri}, fmt.Errorf("send to broker %q: %w", dec.Broker, err)
	}
	return Result{Decision: dec, URI: uri}, nil
}

func (s *PublisherShim) sendWithRetry(ctx context.Context, sender Sender, msg Message) error {
	var err error
	attempts := s.retry.attempts()
	for i := 0; i < attempts; i++ {
		if i > 0 && s.retry.Backoff > 0 {
			if !s.wait(ctx, s.retry.Backoff) {
				return ctx.Err()
			}
		}
		if err = sender.Send(ctx, msg); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return err
}

func (s *PublisherShim) wait(ctx context.Context, d time.Duration) bool {
	if s.sleep != nil {
		s.sleep(d)
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (s *PublisherShim) sender(ctx context.Context, broker, uri string) (Sender, error) {
	key := connKey{broker, uri}
	s.mu.Lock()
	if c, ok := s.conns[key]; ok {
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()

	// Open outside the lock so a slow dial does not block other brokers, then dedupe on insert.
	c, err := s.tport.Sender(ctx, uri)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.conns[key]; ok {
		_ = c.Close() // lost the race; keep the first
		return existing, nil
	}
	s.conns[key] = c
	return c, nil
}

// Targets returns the broker names this shim can route to (what a matching listener must cover).
func (s *PublisherShim) Targets() []string { return s.plan.Targets() }

// Close shuts every cached sender, returning the first error encountered.
func (s *PublisherShim) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for key, c := range s.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.conns, key)
	}
	return firstErr
}
