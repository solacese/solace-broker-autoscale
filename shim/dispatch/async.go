package dispatch

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
)

var ErrBackpressure = errors.New("asynchronous publish queue full")
var ErrPublisherStopped = errors.New("asynchronous publisher stopped; retain and retry unconfirmed events")

type Outcome struct {
	Result Result
	Err    error
}
type publication struct {
	ctx   context.Context
	topic string
	body  []byte
	props map[string]string
	done  chan Outcome
}

// AsyncPublisher queues bounded, copied messages and returns broker outcomes asynchronously.
// This queue is in memory, NOT a durable outbox. Retain an event until Outcome.Err == nil.
// Related keys share a sequential worker. An exhausted send failure stops all lanes so subsequent
// messages cannot silently overtake failed work. Close cancels and completes every accepted outcome.
type AsyncPublisher struct {
	publisher *PublisherShim
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	lanes     []chan publication
	stopped   bool
	workers   sync.WaitGroup
}

func NewAsyncPublisher(p *PublisherShim, lanes, capacityPerLane int) (*AsyncPublisher, error) {
	if lanes < 1 || lanes > 128 || capacityPerLane < 1 || capacityPerLane > 4096 {
		return nil, errors.New("use 1..128 workers and 1..4096 pending messages per worker")
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &AsyncPublisher{publisher: p, ctx: ctx, cancel: cancel}
	for i := 0; i < lanes; i++ {
		lane := make(chan publication, capacityPerLane)
		a.lanes = append(a.lanes, lane)
		a.workers.Add(1)
		go a.run(lane)
	}
	return a, nil
}

// Publish accepts into memory without waiting for a broker or subscriber.
// The returned channel always receives one outcome. Cancellation after acceptance is an ambiguous
// delivery outcome: callers retain event IDs and subscribers deduplicate business effects.
func (a *AsyncPublisher) Publish(ctx context.Context, topic string, body []byte, props map[string]string) (<-chan Outcome, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(body) > 1024*1024 {
		return nil, errors.New("async payload exceeds 1 MiB; use synchronous API or split it")
	}
	dec, err := a.publisher.plan.Decide(topic, body)
	if err != nil {
		return nil, err
	}
	hash := fnv.New32a()
	hash.Write([]byte(dec.Broker + "\x00" + dec.Key))
	lane := a.lanes[int(hash.Sum32())%len(a.lanes)]
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stopped {
		return nil, ErrPublisherStopped
	}
	copied := make(map[string]string, len(props))
	for k, v := range props {
		copied[k] = v
	}
	task := publication{ctx: ctx, topic: topic, body: append([]byte(nil), body...), props: copied, done: make(chan Outcome, 1)}
	select {
	case lane <- task:
		return task.done, nil
	default:
		return nil, ErrBackpressure
	}
}

func (a *AsyncPublisher) stop() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.stopped {
		a.stopped = true
		a.cancel()
		for _, lane := range a.lanes {
			close(lane)
		}
	}
}

func (a *AsyncPublisher) run(lane <-chan publication) {
	defer a.workers.Done()
	for task := range lane {
		outcome := Outcome{Err: ErrPublisherStopped}
		if a.ctx.Err() == nil {
			ctx, cancel := context.WithCancel(task.ctx)
			unregister := context.AfterFunc(a.ctx, cancel)
			outcome.Result, outcome.Err = a.publisher.Publish(ctx, task.topic, task.body, task.props)
			unregister()
			cancel()
			if outcome.Err != nil {
				a.stop()
			}
		}
		task.done <- outcome
		close(task.done)
	}
}

func (a *AsyncPublisher) Close() error {
	a.stop()
	a.workers.Wait()
	return a.publisher.Close()
}
