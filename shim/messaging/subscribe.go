package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"
)

// Message contains the business topic, JSON payload and stable deduplication ID.
type Message struct {
	Topic                   string
	Payload                 json.RawMessage
	EventID                 string
	Headers                 map[string]string
	RoutingKind             string
	RoutingValue            string
	RoutingPartition        int
	RoutingEvaluator        string
	RoutingEvaluatorVersion string
}
type Handler func(context.Context, Message) error
type subscription struct {
	shards          []string
	handler         Handler
	validateRouting bool
}
type location struct {
	Broker    string            `json:"broker_id"`
	Endpoints map[string]string `json:"endpoints"`
}
type discovery struct {
	Revision   uint64 `json:"revision"`
	Partitions []struct {
		ID        int        `json:"partition_id"`
		Queue     string     `json:"queue_name"`
		Locations []location `json:"locations"`
	} `json:"partitions"`
}
type flow struct {
	cancel context.CancelFunc
	done   chan struct{}
	bound  bool
}

// Subscribe registers a YAML-declared durable group. Queue binding happens in the
// background and includes preparing destinations. Replicas use the same group.
// A handler retries the same head until success; only then is the message ACKed.
func (c *Client) Subscribe(ctx context.Context, group string, handler Handler) error {
	return c.SubscribeValidated(ctx, group, handler, false)
}

// SubscribeValidated optionally recomputes the publisher classification. A mismatch retains the
// message unacknowledged; validation never chooses or changes a broker owner.
func (c *Client) SubscribeValidated(ctx context.Context, group string, handler Handler, validateRouting bool) error {
	if handler == nil {
		return errors.New("handler is required")
	}
	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		return ErrClosed
	}
	topics, ok := c.cfg.Groups[group]
	if !ok {
		c.mu.Unlock()
		return errors.New("subscriber group must be declared in YAML")
	}
	existingSub, existing := c.subscriptions[group]
	if existing && existingSub.validateRouting != validateRouting {
		c.mu.Unlock()
		return errors.New("subscriber group routing-validation mode cannot change in one client")
	}
	if validateRouting {
		applicableCount := 0
		for _, candidate := range c.cfg.Routes {
			applicable := false
			for _, topic := range topics {
				if overlaps(candidate.Pattern, topic) {
					applicable = true
					break
				}
			}
			if applicable {
				applicableCount++
			}
			if applicable && candidate.KeyEvaluator == nil {
				c.mu.Unlock()
				return errors.New("routing validation requires evaluator routes for all group topics")
			}
		}
		if applicableCount == 0 {
			c.mu.Unlock()
			return errors.New("routing validation requires an applicable evaluator route")
		}
	}
	c.mu.Unlock()
	if existing {
		return errors.New("group already subscribed in this client")
	}
	var response struct {
		Shards []string `json:"shards"`
	}
	if err := c.request(ctx, "/messaging/subscriptions", map[string]any{"group": group, "topics": topics}, &response); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx.Err() != nil {
		return ErrClosed
	}
	if _, ok := c.subscriptions[group]; ok {
		return errors.New("group already subscribed in this client")
	}
	c.subscriptions[group] = subscription{response.Shards, handler, validateRouting}
	return nil
}
func (c *Client) syncSubscriptions() {
	c.mu.Lock()
	subs := map[string]subscription{}
	for k, v := range c.subscriptions {
		subs[k] = v
	}
	c.mu.Unlock()
	desired := map[string]bool{}
	allOK := true
	lastError := ""
	for group, sub := range subs {
		for _, shard := range sub.shards {
			var d discovery
			err := c.request(c.ctx, "/partitions?"+url.Values{"shard": {shard}, "group": {group}}.Encode(), nil, &d)
			if err != nil {
				allOK = false
				lastError = err.Error()
				continue
			}
			c.mu.Lock()
			stale := d.Revision < c.cfg.Revision
			c.mu.Unlock()
			if stale {
				allOK = false
				lastError = "stale partition discovery revision"
				continue
			}
			for _, p := range d.Partitions {
				for _, l := range p.Locations {
					uri, err := c.uri(l.Broker, l.Endpoints)
					if err != nil {
						allOK = false
						lastError = err.Error()
						continue
					}
					// Include queue and endpoint so a replacement location causes a rebind.
					key := fmt.Sprintf("%s/%s/%d/%s/%s/%s", group, shard, p.ID, l.Broker, p.Queue, uri)
					desired[key] = true
					c.mu.Lock()
					_, exists := c.flows[key]
					if !exists && len(c.flows) < 1024 && c.ctx.Err() == nil {
						ctx, cancel := context.WithCancel(c.ctx)
						f := &flow{cancel: cancel, done: make(chan struct{})}
						c.flows[key] = f
						go c.consume(ctx, f, uri, p.Queue, sub.handler, sub.validateRouting)
					} else if !exists {
						allOK = false
						lastError = "receiver limit reached (1024)"
					}
					c.mu.Unlock()
				}
			}
		}
	}
	c.mu.Lock()
	var stopped []*flow
	if allOK {
		for key, f := range c.flows {
			if !desired[key] {
				f.cancel()
				stopped = append(stopped, f)
				delete(c.flows, key)
			}
		}
	}
	if lastError != "" {
		c.status.SubscriptionError = lastError
	}
	c.mu.Unlock()
	for _, f := range stopped {
		<-f.done
	}
}
func pause(ctx context.Context) bool {
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (c *Client) consumerError(message string) {
	c.mu.Lock()
	c.status.SubscriptionError = message
	c.mu.Unlock()
}
func (c *Client) consume(ctx context.Context, f *flow, uri, queue string, handler Handler, validateRouting bool) {
	defer close(f.done)
	for ctx.Err() == nil {
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		receiver, err := c.opts.Transport.Receiver(dialCtx, uri, "queue://"+queue)
		cancel()
		if err != nil {
			c.consumerError("queue binding pending or connection failed")
			if !pause(ctx) {
				return
			}
			continue
		}
		c.mu.Lock()
		f.bound = true
		c.mu.Unlock()
		for ctx.Err() == nil {
			msg, err := receiver.Receive(ctx)
			if err != nil {
				if ctx.Err() == nil {
					c.consumerError("receiver disconnected; reconnecting")
				}
				break
			}
			var envelope struct {
				ID      string `json:"event_id"`
				Payload struct {
					Topic   string            `json:"topic"`
					Data    json.RawMessage   `json:"data"`
					Headers map[string]string `json:"headers"`
					Routing struct {
						Kind      string `json:"kind"`
						Value     string `json:"value"`
						Partition int    `json:"partition"`
						Evaluator string `json:"evaluator"`
						Version   string `json:"version"`
					} `json:"routing"`
				} `json:"payload"`
			}
			err = json.Unmarshal(msg.Body, &envelope)
			if err != nil || envelope.ID == "" || envelope.Payload.Topic == "" {
				c.consumerError("invalid message blocks queue; operator action required")
				<-ctx.Done()
				break
			}
			message := Message{Topic: envelope.Payload.Topic, Payload: envelope.Payload.Data, EventID: envelope.ID, Headers: envelope.Payload.Headers, RoutingKind: envelope.Payload.Routing.Kind, RoutingValue: envelope.Payload.Routing.Value, RoutingPartition: envelope.Payload.Routing.Partition, RoutingEvaluator: envelope.Payload.Routing.Evaluator, RoutingEvaluatorVersion: envelope.Payload.Routing.Version}
			// Hold the failed head locally, including ACK uncertainty, to avoid overtaking.
			processed := false
			for ctx.Err() == nil {
				if !processed {
					if validateRouting {
						err = c.validateMessageRouting(message)
					} else {
						err = nil
					}
					if err == nil {
						err = invokeHandler(ctx, handler, message)
					}
					processed = err == nil
				}
				if processed {
					if msg.Ack == nil {
						err = errors.New("transport has no settlement")
					} else {
						ackCtx, ackCancel := context.WithTimeout(ctx, 5*time.Second)
						err = msg.Ack(ackCtx)
						ackCancel()
					}
				}
				if err == nil {
					break
				}
				c.consumerError("handler or settlement failed; head retained")
				if processed {
					break
				} // Failed link settlement requires reconnect/redelivery.
				if !pause(ctx) {
					break
				}
			}
			if err != nil {
				break
			}
		}
		c.mu.Lock()
		f.bound = false
		c.mu.Unlock()
		_ = receiver.Close()
		if !pause(ctx) {
			return
		}
	}
}
func (c *Client) validateMessageRouting(message Message) error {
	c.mu.Lock()
	var selected *route
	partitions := c.cfg.Partitions
	for _, candidate := range c.cfg.Routes {
		if matches(candidate.Pattern, message.Topic) {
			copy := candidate
			selected = &copy
			break
		}
	}
	c.mu.Unlock()
	if selected == nil || selected.KeyEvaluator == nil {
		return errors.New("routing validation requires an evaluator route")
	}
	expected := selected.KeyEvaluator
	if message.RoutingEvaluator != expected.Name || message.RoutingEvaluatorVersion != expected.Version {
		return errors.New("routing evaluator identity mismatch")
	}
	evaluator, err := c.opts.RoutingEvaluators.require(expected.Name, expected.Version)
	if err != nil {
		return err
	}
	var payload any
	if err = json.Unmarshal(message.Payload, &payload); err != nil {
		return errors.New("invalid JSON payload for routing validation")
	}
	publication := &Publication{Topic: message.Topic, Payload: payload, EventID: message.EventID, Headers: message.Headers}
	kind, value, partition, err := resolveEvaluator(evaluator, expected.Result, publication, selected.Shard, partitions)
	if err != nil {
		return err
	}
	if kind != message.RoutingKind || value != message.RoutingValue || partition != message.RoutingPartition {
		return errors.New("routing validation mismatch")
	}
	return nil
}

func invokeHandler(ctx context.Context, h Handler, m Message) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("handler panicked")
		}
	}()
	return h(ctx, m)
}
func (c *Client) stopFlows() {
	c.mu.Lock()
	var stopped []*flow
	for k, f := range c.flows {
		f.cancel()
		stopped = append(stopped, f)
		delete(c.flows, k)
	}
	c.mu.Unlock()
	for _, f := range stopped {
		<-f.done
	}
}
