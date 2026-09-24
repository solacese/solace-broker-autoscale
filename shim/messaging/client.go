// Package messaging provides durable, controller-managed Solace pub/sub over AMQP 1.0.
// Publish returns after local disk acceptance. Broker receipts run in the background.
// Subscriber handlers must commit event-ID deduplication with their business transaction.
package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
	amqptransport "github.com/solacese/solace-broker-autoscale/shim/transport/amqp"
)

// Options keeps deployment details outside business policy. Credentials are looked
// up locally by broker ID and never returned by the controller.
type Options struct {
	ControllerURL, APIKey, OutboxPath string
	Credentials                       func(broker string) (username, password string, err error)
	Transport                         dispatch.Transport
	PollInterval                      time.Duration
	// MaxOutboxBytes bounds serialized logical pending records, not the bbolt file size.
	// Database pages, indexes, and high-water allocation require additional disk space.
	MaxOutboxBytes, MaxOutboxMessages uint64
	MaxInflight                       int
	RoutingEvaluators                 *EvaluatorRegistry
}
type Status struct {
	Pending           uint64 `json:"pending"`
	Paused            bool   `json:"paused"`
	PolicyError       string `json:"policy_error,omitempty"`
	DeliveryError     string `json:"delivery_error,omitempty"`
	SubscriptionError string `json:"subscription_error,omitempty"`
	Receivers         int    `json:"receivers"`
}
type assignment struct {
	Broker    string            `json:"broker_id"`
	Endpoints map[string]string `json:"endpoints"`
	Prefix    string            `json:"topic_prefix"`
	Partition int               `json:"partition_id"`
	Count     int               `json:"partition_count"`
	Lease     int               `json:"lease_seconds"`
	Revision  uint64            `json:"revision"`
	fetched   time.Time
}
type cachedSender struct {
	uri    string
	sender dispatch.Sender
}
type Client struct {
	opts          Options
	http          *http.Client
	box           *outbox
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	closeOnce     sync.Once
	closed        bool
	mu            sync.Mutex
	cfg           config
	policyAt      time.Time
	status        Status
	assignments   map[string]assignment
	senders       map[string]*cachedSender
	senderMu      sync.Mutex
	subscriptions map[string]subscription
	flows         map[string]*flow
	wake          chan struct{}
	controlWake   chan struct{}
}

// Open locks the durable outbox to one process and validates its routing contract.
// Restart with the same file on persistent local storage to resume accepted work.
func Open(ctx context.Context, opts Options) (*Client, error) {
	u, err := url.Parse(opts.ControllerURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return nil, errors.New("invalid controller URL")
	}
	if opts.OutboxPath == "" || opts.Credentials == nil {
		return nil, errors.New("outbox path and credentials callback are required")
	}
	if opts.Transport == nil {
		opts.Transport = amqptransport.New()
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = time.Second
	}
	if opts.PollInterval < 10*time.Millisecond {
		return nil, errors.New("poll interval must be at least 10ms")
	}
	if opts.MaxInflight == 0 {
		opts.MaxInflight = 128
	}
	if opts.MaxInflight < 1 || opts.MaxInflight > 128 {
		return nil, errors.New("max inflight must be 1-128")
	}
	if opts.MaxOutboxBytes == 0 {
		opts.MaxOutboxBytes = 100_000_000
	}
	if opts.MaxOutboxMessages == 0 {
		opts.MaxOutboxMessages = 100_000
	}
	workerCtx, cancel := context.WithCancel(ctx)
	c := &Client{opts: opts, ctx: workerCtx, cancel: cancel, http: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, assignments: map[string]assignment{}, senders: map[string]*cachedSender{}, subscriptions: map[string]subscription{}, flows: map[string]*flow{}, wake: make(chan struct{}, 1), controlWake: make(chan struct{}, 1)}
	if err = c.request(ctx, "/messaging/config", nil, &c.cfg); err != nil {
		cancel()
		return nil, err
	}
	if err = c.cfg.validate(); err != nil {
		cancel()
		return nil, err
	}
	if err = c.requireEvaluators(c.cfg); err != nil {
		cancel()
		return nil, err
	}
	c.box, err = openOutbox(opts.OutboxPath, c.cfg, opts.MaxOutboxBytes, opts.MaxOutboxMessages)
	if err != nil {
		cancel()
		return nil, err
	}
	c.policyAt = time.Now()
	events := c.cfg.Events // Immutable startup contract; control() may replace c.cfg immediately.
	workers := 2
	if events.Enabled {
		workers++
	}
	c.wg.Add(workers)
	go c.control()
	go c.deliver()
	if events.Enabled {
		go c.controlEvents(events)
	}
	return c, nil
}

type httpError int

func (e httpError) Error() string { return fmt.Sprintf("controller HTTP %d", e) }
func (c *Client) request(ctx context.Context, path string, body, out any) error {
	var reader io.Reader
	method := "GET"
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
		method = "POST"
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.opts.ControllerURL, "/")+path, reader)
	if err != nil {
		return errors.New("invalid controller request")
	}
	if c.opts.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.opts.APIKey)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.New("controller unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httpError(resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out)
}

func (c *Client) requireEvaluators(cfg config) error {
	for _, route := range cfg.Routes {
		if route.KeyEvaluator != nil {
			if _, err := c.opts.RoutingEvaluators.require(route.KeyEvaluator.Name, route.KeyEvaluator.Version); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) publicationPolicy(topic string) (config, route, error) {
	if c.ctx.Err() != nil {
		return config{}, route{}, ErrClosed
	}
	if c.status.Paused || time.Since(c.policyAt) > 5*time.Minute {
		return config{}, route{}, errors.New("publishing paused; controller policy unavailable or rejected")
	}
	var selected *route
	for _, candidate := range c.cfg.Routes {
		if matches(candidate.Pattern, topic) {
			if selected != nil {
				return config{}, route{}, errors.New("ambiguous route")
			}
			copy := candidate
			selected = &copy
		}
	}
	if selected == nil {
		return config{}, route{}, errors.New("no configured route")
	}
	ready := map[string]bool{}
	for _, group := range c.cfg.Ready {
		ready[group] = true
	}
	matched := false
	for group, patterns := range c.cfg.Groups {
		for _, pattern := range patterns {
			if matches(pattern, topic) {
				matched = true
				if !ready[group] {
					return config{}, route{}, errors.New("matching subscriber group is not ready")
				}
			}
		}
	}
	if !matched {
		return config{}, route{}, errors.New("no matching subscriber group")
	}
	return c.cfg.contract(), *selected, nil
}

// Publish preserves the original API and borrows payload until local acceptance returns.
func (c *Client) Publish(topic string, payload any, eventID string) error {
	return c.PublishMessage(&Publication{Topic: topic, Payload: payload, EventID: eventID})
}

// PublishMessage passes this exact Publication pointer to trusted customer code. Evaluation and
// serialization run without the client lock; current policy is rechecked before durable acceptance.
func (c *Client) PublishMessage(message *Publication) error {
	if message == nil {
		return errors.New("publication is required")
	}
	c.mu.Lock()
	contract, selected, err := c.publicationPolicy(message.Topic)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	snapshot := contract
	snapshot.Routes = []route{selected}
	snapshot.Groups = map[string][]string{"selected": {selected.Pattern}}
	snapshot.Ready = []string{"selected"}
	r, err := snapshot.publicationMessage(message, c.opts.RoutingEvaluators)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current, currentRoute, err := c.publicationPolicy(message.Topic)
	if err != nil {
		return err
	}
	oldRoute, _ := json.Marshal(selected)
	newRoute, _ := json.Marshal(currentRoute)
	if !compatible(contract, current) || !bytes.Equal(oldRoute, newRoute) {
		return errors.New("routing policy changed during evaluation; publication was not accepted")
	}
	if err = c.box.enqueue(r); err != nil {
		return err
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return nil
}
func (c *Client) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.status
	s.Receivers = 0
	for _, f := range c.flows {
		if f.bound {
			s.Receivers++
		}
	}
	if !c.closed {
		n, err := c.box.pending()
		if err == nil {
			s.Pending = n
		} else {
			s.DeliveryError = "outbox read failed"
		}
	}
	return s
}

// Flush waits for broker acceptance only; it does not imply business processing.
func (c *Client) Flush(ctx context.Context) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.mu.Lock()
		if c.ctx.Err() != nil {
			c.mu.Unlock()
			return ErrClosed
		}
		n, err := c.box.pending()
		c.mu.Unlock()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return ErrClosed
		case <-ticker.C:
		}
	}
}
func (c *Client) control() {
	defer c.wg.Done()
	nextPeriodic := time.Now().Add(c.opts.PollInterval)
	lastRefresh := time.Now()
	for {
		wait := time.Until(nextPeriodic)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		event := false
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-c.controlWake:
			timer.Stop()
			event = true
		}
		now := time.Now()
		minGap := min(c.opts.PollInterval/2, 250*time.Millisecond)
		if event && now.Sub(lastRefresh) < minGap {
			timer := time.NewTimer(minGap - now.Sub(lastRefresh))
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			now = time.Now()
		}
		lastRefresh = now
		if !now.Before(nextPeriodic) {
			for !nextPeriodic.After(now) {
				nextPeriodic = nextPeriodic.Add(c.opts.PollInterval)
			}
		}
		var cfg config
		err := c.request(c.ctx, "/messaging/config", nil, &cfg)
		c.mu.Lock()
		if err == nil {
			if cfg.validate() != nil || c.requireEvaluators(cfg) != nil || !compatible(c.cfg, cfg) {
				err = errors.New("routing contract changed or evaluator unavailable")
				c.status.Paused = true
			} else if cfg.Revision < c.cfg.Revision {
				err = errors.New("stale controller revision")
			} else {
				cfg.Routes = c.cfg.Routes
				c.cfg = cfg
				c.policyAt = time.Now()
				c.status.Paused = false
				c.status.PolicyError = ""
			}
		}
		if err != nil {
			c.status.PolicyError = err.Error()
			var h httpError
			if errors.As(err, &h) && h < 500 {
				c.status.Paused = true
			}
		}
		if time.Since(c.policyAt) > 5*time.Minute {
			c.status.Paused = true
		}
		paused := c.status.Paused
		c.mu.Unlock()
		if paused {
			c.stopFlows()
		} else {
			c.syncSubscriptions()
		}
	}
}
func (c *Client) resolve(r record) (assignment, error) {
	c.mu.Lock()
	a, ok := c.assignments[r.lane()]
	requiredRevision := c.cfg.Revision
	c.mu.Unlock()
	leaseValid := ok && a.Lease > 0 && time.Since(a.fetched) < time.Duration(a.Lease)*time.Second
	if leaseValid && a.Revision >= requiredRevision {
		return a, nil
	}
	params := url.Values{"shard": {r.Shard}, "client_id": {"go-messaging"}, "mode": {"guaranteed"}, "protocol": {"amqp"}, "partition": {fmt.Sprint(r.Partition)}}
	var fresh assignment
	if err := c.request(c.ctx, "/assignment?"+params.Encode(), nil, &fresh); err != nil {
		var h httpError
		if errors.As(err, &h) && h < 500 {
			return assignment{}, err
		}
		// A hint never invalidates a still-valid lease. Broker ingress fencing remains authoritative.
		if leaseValid {
			return a, nil
		}
		return assignment{}, err
	}
	c.mu.Lock()
	partitions := c.cfg.Partitions
	c.mu.Unlock()
	if fresh.Partition != r.Partition || fresh.Count != partitions || fresh.Prefix == "" || fresh.Broker == "" || fresh.Revision < requiredRevision {
		return assignment{}, errors.New("assignment contract mismatch or stale revision")
	}
	fresh.fetched = time.Now()
	c.mu.Lock()
	c.assignments[r.lane()] = fresh
	c.mu.Unlock()
	return fresh, nil
}
func (c *Client) uri(broker string, endpoints map[string]string) (string, error) {
	endpoint := endpoints["amqp"]
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "amqp" && u.Scheme != "amqps") || u.User != nil {
		return "", errors.New("missing or invalid AMQP endpoint")
	}
	user, password, err := c.opts.Credentials(broker)
	if err != nil {
		return "", errors.New("broker credential lookup failed")
	}
	u.User = url.UserPassword(user, password)
	return u.String(), nil
}
func (c *Client) send(r record) error {
	a, err := c.resolve(r)
	if err != nil {
		return err
	}
	uri, err := c.uri(a.Broker, a.Endpoints)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	// Only connection creation is serialized. Concurrent partition sends share a link.
	c.senderMu.Lock()
	s := c.senders[a.Broker]
	if s != nil && s.uri != uri {
		_ = s.sender.Close()
		delete(c.senders, a.Broker)
		s = nil
	}
	if s == nil {
		sender, e := c.opts.Transport.Sender(ctx, uri)
		if e != nil {
			c.senderMu.Unlock()
			return errors.New("broker connection failed")
		}
		s = &cachedSender{uri, sender}
		c.senders[a.Broker] = s
	}
	c.senderMu.Unlock()
	payload := map[string]any{"topic": r.Topic, "data": r.Data}
	if len(r.Headers) > 0 {
		payload["headers"] = r.Headers
	}
	if r.EvaluatorName != "" {
		payload["routing"] = map[string]any{"kind": r.RoutingKind, "value": r.RoutingValue, "partition": r.Partition, "evaluator": r.EvaluatorName, "version": r.EvaluatorVersion}
	}
	body, err := json.Marshal(map[string]any{"event_id": r.ID, "payload": payload})
	if err != nil {
		return err
	}
	err = s.sender.Send(ctx, dispatch.Message{Address: "topic://" + a.Prefix + r.Topic, Body: body, GroupID: fmt.Sprint(r.Partition), Properties: map[string]string{"event_id": r.ID}})
	if err != nil {
		c.mu.Lock()
		delete(c.assignments, r.lane())
		c.mu.Unlock()
		c.senderMu.Lock()
		if c.senders[a.Broker] == s {
			delete(c.senders, a.Broker)
			_ = s.sender.Close()
		}
		c.senderMu.Unlock()
		return errors.New("broker did not confirm persistence; publication retained")
	}
	return c.box.accepted(r)
}
func (c *Client) deliver() {
	defer c.wg.Done()
	type result struct {
		lane string
		err  error
	}
	done := make(chan result, c.opts.MaxInflight)
	busy := map[string]bool{}
	retry := map[string]time.Time{}
	var workers sync.WaitGroup
	cursor := 0
	defer workers.Wait()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case r := <-done:
			delete(busy, r.lane)
			c.mu.Lock()
			if r.err != nil {
				retry[r.lane] = time.Now().Add(500 * time.Millisecond)
				c.status.DeliveryError = r.err.Error()
			} else {
				delete(retry, r.lane)
				if len(retry) == 0 {
					c.status.DeliveryError = ""
				}
			}
			c.mu.Unlock()
		case <-ticker.C:
		case <-c.wake:
		}
		c.mu.Lock()
		paused := c.status.Paused || time.Since(c.policyAt) > 5*time.Minute
		c.mu.Unlock()
		if paused {
			continue
		}
		heads, err := c.box.heads()
		if err != nil {
			c.mu.Lock()
			c.status.DeliveryError = "outbox read failed"
			c.mu.Unlock()
			continue
		}
		if len(heads) > 0 {
			cursor = (cursor + 1) % len(heads)
		}
		for offset := 0; offset < len(heads); offset++ {
			r := heads[(cursor+offset)%len(heads)]
			lane := r.lane()
			if len(busy) >= c.opts.MaxInflight {
				break
			}
			if busy[lane] || time.Now().Before(retry[lane]) {
				continue
			}
			busy[lane] = true
			workers.Add(1)
			go func(r record) { defer workers.Done(); err := c.send(r); done <- result{r.lane(), err} }(r)
		}
	}
}

// Close preserves unsent data and waits for workers. Handlers must honor their context.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.cancel()
		select {
		case c.controlWake <- struct{}{}:
		default:
		}
		c.wg.Wait()
		c.stopFlows()
		c.senderMu.Lock()
		for _, s := range c.senders {
			_ = s.sender.Close()
		}
		c.senderMu.Unlock()
		c.mu.Lock()
		if n, e := c.box.pending(); e == nil {
			c.status.Pending = n
		}
		err = c.box.db.Close()
		c.closed = true
		c.mu.Unlock()
	})
	return err
}
