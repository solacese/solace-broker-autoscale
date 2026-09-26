package semp

import (
	"context"
	"errors"
	"fmt"
)

type queueSubscription struct {
	MessageVPN string `json:"msgVpnName"`
	QueueName  string `json:"queueName"`
	Topic      string `json:"subscriptionTopic"`
}

// ListExactQueueSubscriptions returns every topic subscription attached to one
// exact queue. Every page and item must retain the requested queue identity;
// malformed or mismatched responses fail closed without returning partial data.
func (c *Client) ListExactQueueSubscriptions(ctx context.Context, messageVPN, queueName string) ([]string, error) {
	if messageVPN == "" || queueName == "" {
		return nil, errors.New("semp: message VPN and exact queue name are required")
	}
	path := queuePath("config", messageVPN, queueName) + pathSegments("subscriptions")
	subscriptions, err := list[queueSubscription](ctx, c, path)
	if err != nil {
		return nil, fmt.Errorf("semp: list subscriptions for exact queue %q: %w", queueName, err)
	}
	topics := make([]string, len(subscriptions))
	for index, subscription := range subscriptions {
		if subscription.MessageVPN != messageVPN || subscription.QueueName != queueName || subscription.Topic == "" {
			return nil, fmt.Errorf("semp: queue %q subscription %d has missing or mismatched identity", queueName, index)
		}
		topics[index] = subscription.Topic
	}
	return topics, nil
}
