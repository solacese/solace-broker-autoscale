package messaging

import (
	"context"
	"errors"
	"time"
)

// controlEvents subscribes to best-effort managed hints. Message bodies are deliberately ignored:
// only a successful authenticated HTTP refresh can advance cfg.Revision or affect cached assignments.
func (c *Client) controlEvents(events eventConfig) {
	defer c.wg.Done()
	uri, err := c.uri(events.Broker, events.Endpoints)
	if err != nil {
		c.consumerError("managed event credential or endpoint unavailable")
		return
	}
	for c.ctx.Err() == nil {
		dialCtx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		receiver, err := c.opts.Transport.Receiver(dialCtx, uri, "topic://"+events.Topic)
		cancel()
		if err != nil {
			c.consumerError("managed event receiver disconnected; reconnecting")
			if !pause(c.ctx) {
				return
			}
			continue
		}
		for c.ctx.Err() == nil {
			msg, err := receiver.Receive(c.ctx)
			if err != nil {
				if c.ctx.Err() == nil {
					c.consumerError("managed event receiver disconnected; reconnecting")
				}
				break
			}
			select {
			case c.controlWake <- struct{}{}:
			default:
			}
			if msg.Ack != nil {
				ackCtx, ackCancel := context.WithTimeout(c.ctx, 5*time.Second)
				err = msg.Ack(ackCtx)
				ackCancel()
				if err != nil {
					break
				}
			}
		}
		if err := receiver.Close(); err != nil && !errors.Is(err, context.Canceled) {
			c.consumerError("managed event receiver close failed")
		}
		if !pause(c.ctx) {
			return
		}
	}
}
