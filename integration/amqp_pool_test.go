package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAMQPPoolConcurrentCapacityAndSingleeventsA(t *testing.T) {
	var dials atomic.Int32
	gate := make(chan struct{})
	pool, _ := NewAMQPConnectionPool(1, time.Minute, func(context.Context, string, string) (*AMQPConnection, error) {
		dials.Add(1)
		<-gate
		return &AMQPConnection{}, nil
	})
	var wg sync.WaitGroup
	leases := make(chan *AMQPLease, 2)
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			lease, err := pool.Acquire(context.Background(), "a", "amqps://a:5671")
			if err != nil {
				t.Errorf("Acquire=%v", err)
				return
			}
			leases <- lease
		}()
	}
	for dials.Load() != 1 {
		time.Sleep(time.Millisecond)
	}
	if _, err := pool.Acquire(context.Background(), "b", "amqps://b:5671"); err == nil {
		t.Fatal("distinct dial exceeded capacity reservation")
	}
	close(gate)
	wg.Wait()
	close(leases)
	for lease := range leases {
		lease.Release()
	}
	if dials.Load() != 1 {
		t.Fatalf("same broker dials=%d", dials.Load())
	}
}

func TestAMQPPoolLeaseProtectsIdleAndReacquireUsesNewGeneration(t *testing.T) {
	now := time.Unix(100, 0)
	dials := 0
	pool, _ := NewAMQPConnectionPool(1, time.Minute, func(context.Context, string, string) (*AMQPConnection, error) { dials++; return &AMQPConnection{}, nil })
	pool.now = func() time.Time { return now }
	first, _ := pool.Acquire(context.Background(), "a", "amqps://a:5671")
	now = now.Add(2 * time.Minute)
	pool.EvictIdle()
	if _, err := pool.Acquire(context.Background(), "b", "amqps://b:5671"); err == nil {
		t.Fatal("evicted active lease")
	}
	first.Release()
	now = now.Add(2 * time.Minute)
	pool.EvictIdle()
	second, err := pool.Acquire(context.Background(), "a", "amqps://a:5671")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if second.Generation == first.Generation || dials != 2 {
		t.Fatalf("generations %d/%d dials=%d", first.Generation, second.Generation, dials)
	}
}

func TestAMQPPoolFailureReleasesReservationAndClosedState(t *testing.T) {
	calls := 0
	pool, _ := NewAMQPConnectionPool(1, time.Minute, func(context.Context, string, string) (*AMQPConnection, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("dial")
		}
		return &AMQPConnection{}, nil
	})
	if _, err := pool.Acquire(context.Background(), "a", "amqps://a:5671"); err == nil {
		t.Fatal("dial failure hidden")
	}
	lease, err := pool.Acquire(context.Background(), "b", "amqps://b:5671")
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Acquire(context.Background(), "c", "amqps://c:5671"); !errors.Is(err, ErrAMQPPoolClosed) {
		t.Fatalf("closed Acquire=%v", err)
	}
}

func TestAMQPPoolCloseWinsDialRace(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pool, _ := NewAMQPConnectionPool(1, time.Minute, func(context.Context, string, string) (*AMQPConnection, error) {
		close(started)
		<-release
		return &AMQPConnection{}, nil
	})
	result := make(chan error, 1)
	go func() { _, err := pool.Acquire(context.Background(), "a", "amqps://a:5671"); result <- err }()
	<-started
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrAMQPPoolClosed) {
		t.Fatalf("dial race=%v", err)
	}
}
