// Package memory is an in-memory Transport for the smart shim, used to run and test the whole shim
// offline with no broker. One Transport holds a set of named brokers addressed by URI; a send to a
// broker is delivered to every receiver on that broker whose subscription source matches the
// message address (using the same Solace-wildcard matching as the rule engine). It records every
// message so tests can assert on what was routed where.
package memory

import (
	"context"
	"sync"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
	"github.com/solacese/solace-broker-autoscale/shim/rules"
)

// Transport is an in-memory implementation of dispatch.Transport. The zero value is not ready; use
// New. Safe for concurrent use.
type Transport struct {
	mu      sync.Mutex
	brokers map[string]*broker // keyed by URI
}

// New returns an empty in-memory transport.
func New() *Transport {
	return &Transport{brokers: map[string]*broker{}}
}

type broker struct {
	mu        sync.Mutex
	delivered []dispatch.Message
	receivers []*receiver
}

func (t *Transport) broker(uri string) *broker {
	t.mu.Lock()
	defer t.mu.Unlock()
	b, ok := t.brokers[uri]
	if !ok {
		b = &broker{}
		t.brokers[uri] = b
	}
	return b
}

// Sender implements dispatch.Transport.
func (t *Transport) Sender(_ context.Context, uri string) (dispatch.Sender, error) {
	return &sender{b: t.broker(uri)}, nil
}

// Receiver implements dispatch.Transport.
func (t *Transport) Receiver(_ context.Context, uri, source string) (dispatch.Receiver, error) {
	b := t.broker(uri)
	r := &receiver{source: source, ch: make(chan dispatch.Message, 64)}
	b.mu.Lock()
	b.receivers = append(b.receivers, r)
	b.mu.Unlock()
	return r, nil
}

// Delivered returns a copy of every message a broker (by URI) has accepted, in order. Test helper.
func (t *Transport) Delivered(uri string) []dispatch.Message {
	b := t.broker(uri)
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]dispatch.Message, len(b.delivered))
	copy(out, b.delivered)
	return out
}

type sender struct{ b *broker }

func (s *sender) Send(_ context.Context, msg dispatch.Message) error {
	s.b.mu.Lock()
	s.b.delivered = append(s.b.delivered, msg)
	recvs := make([]*receiver, len(s.b.receivers))
	copy(recvs, s.b.receivers)
	s.b.mu.Unlock()

	for _, r := range recvs {
		if rules.TopicMatches(r.source, msg.Address) {
			r.deliver(msg)
		}
	}
	return nil
}

func (s *sender) Close() error { return nil }

type receiver struct {
	source string
	ch     chan dispatch.Message
	once   sync.Once
}

func (r *receiver) deliver(msg dispatch.Message) {
	defer func() { _ = recover() }() // ignore send-on-closed during shutdown races
	r.ch <- msg
}

func (r *receiver) Receive(ctx context.Context) (dispatch.Message, error) {
	select {
	case <-ctx.Done():
		return dispatch.Message{}, ctx.Err()
	case msg, ok := <-r.ch:
		if !ok {
			return dispatch.Message{}, context.Canceled
		}
		return msg, nil
	}
}

func (r *receiver) Close() error {
	r.once.Do(func() { close(r.ch) })
	return nil
}
