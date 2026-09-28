package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/Azure/go-amqp"
)

var ErrAMQPPoolClosed = errors.New("integration: AMQP connection pool is closed")

type AMQPDial func(context.Context, string, string) (*AMQPConnection, error)

type pooledAMQP struct {
	connection *AMQPConnection
	endpoint   string
	generation uint64
	refs       int
	lastUsed   time.Time
}

type pendingDial struct {
	done chan struct{}
	err  error
}

// AMQPLease pins one connection generation until Release. Release is idempotent.
type AMQPLease struct {
	Session    *amqp.Session
	Generation uint64
	release    func()
	once       sync.Once
}

func (l *AMQPLease) Release() {
	if l != nil && l.release != nil {
		l.once.Do(l.release)
	}
}

// AMQPConnectionPool opens data-broker connections only on first use. Capacity
// includes in-progress dials; concurrent requests for one broker singleevents-a.
type AMQPConnectionPool struct {
	mu         sync.Mutex
	dial       AMQPDial
	max        int
	idle       time.Duration
	now        func() time.Time
	entries    map[string]*pooledAMQP
	pending    map[string]*pendingDial
	generation uint64
	closed     bool
}

func NewAMQPConnectionPool(max int, idle time.Duration, dial AMQPDial) (*AMQPConnectionPool, error) {
	if max < 1 || idle <= 0 || dial == nil {
		return nil, errors.New("integration: positive AMQP pool bounds and dialer are required")
	}
	return &AMQPConnectionPool{dial: dial, max: max, idle: idle, now: time.Now, entries: make(map[string]*pooledAMQP), pending: make(map[string]*pendingDial)}, nil
}

func (p *AMQPConnectionPool) Acquire(ctx context.Context, broker, endpoint string) (*AMQPLease, error) {
	if broker == "" || endpoint == "" {
		return nil, errors.New("integration: broker ID is required")
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrAMQPPoolClosed
		}
		if entry := p.entries[broker]; entry != nil {
			if entry.endpoint != endpoint {
				// Keep the old generation pinned for outstanding work, but remove it
				// from acquisition so the new endpoint can establish a new generation.
				delete(p.entries, broker)
				if entry.refs == 0 && entry.connection != nil && (entry.connection.Session != nil || entry.connection.Conn != nil) {
					_ = entry.connection.Close()
				}
			} else {
				entry.refs++
				lease := p.leaseLocked(broker, entry)
				p.mu.Unlock()
				return lease, nil
			}
		}
		if pending := p.pending[broker]; pending != nil {
			done := pending.done
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				if pending.err != nil {
					return nil, pending.err
				}
				continue
			}
		}
		p.evictLocked(p.now())
		if len(p.entries)+len(p.pending) >= p.max {
			p.mu.Unlock()
			return nil, fmt.Errorf("integration: AMQP connection pool capacity %d reached", p.max)
		}
		pending := &pendingDial{done: make(chan struct{})}
		p.pending[broker] = pending
		p.mu.Unlock()

		connection, err := p.dial(ctx, broker, endpoint)
		p.mu.Lock()
		delete(p.pending, broker)
		if err == nil && p.closed {
			err = ErrAMQPPoolClosed
		}
		if err == nil {
			p.generation++
			entry := &pooledAMQP{connection: connection, endpoint: endpoint, generation: p.generation, refs: 1, lastUsed: p.now()}
			p.entries[broker] = entry
			pending.err = nil
			close(pending.done)
			lease := p.leaseLocked(broker, entry)
			p.mu.Unlock()
			return lease, nil
		}
		pending.err = err
		close(pending.done)
		p.mu.Unlock()
		if connection != nil && (connection.Session != nil || connection.Conn != nil) {
			_ = connection.Close()
		}
		return nil, err
	}
}

func (p *AMQPConnectionPool) leaseLocked(broker string, entry *pooledAMQP) *AMQPLease {
	return &AMQPLease{Session: entry.connection.Session, Generation: entry.generation, release: func() {
		p.mu.Lock()
		if current := p.entries[broker]; current == entry && current.refs > 0 {
			current.refs--
			current.lastUsed = p.now()
			if p.closed && current.refs == 0 {
				if current.connection != nil && (current.connection.Session != nil || current.connection.Conn != nil) {
					_ = current.connection.Close()
				}
				delete(p.entries, broker)
			}
		}
		p.mu.Unlock()
	}}
}

func (p *AMQPConnectionPool) EvictIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evictLocked(p.now())
}

func (p *AMQPConnectionPool) evictLocked(now time.Time) {
	for broker, entry := range p.entries {
		if entry.refs == 0 && now.Sub(entry.lastUsed) >= p.idle {
			if entry.connection != nil && (entry.connection.Session != nil || entry.connection.Conn != nil) {
				_ = entry.connection.Close()
			}
			delete(p.entries, broker)
		}
	}
}

func (p *AMQPConnectionPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	var result error
	for broker, entry := range p.entries {
		if entry.refs != 0 {
			result = errors.Join(result, fmt.Errorf("integration: broker %q has %d active leases", broker, entry.refs))
			continue
		}
		if entry.connection != nil && (entry.connection.Session != nil || entry.connection.Conn != nil) {
			result = errors.Join(result, entry.connection.Close())
		}
		delete(p.entries, broker)
	}
	p.mu.Unlock()
	return result
}
