package runtime

import (
	"context"
	"errors"
	"fmt"
)

type exactQueueSubscriptionLister interface {
	ListExactQueueSubscriptions(context.Context, string, string) ([]string, error)
}

// ensureExactQueueSubscription permits adoption only when the queue has the one
// epoch ingress subscription managed by runtime. An empty set is completed and
// re-read; any pre-existing or concurrently added extra fails closed.
func ensureExactQueueSubscription(ctx context.Context, client SEMPQueueClient, messageVPN, queueName, desiredTopic string) error {
	lister, ok := client.(exactQueueSubscriptionLister)
	if !ok {
		return errors.New("runtime: exact queue subscription listing is required")
	}
	topics, err := lister.ListExactQueueSubscriptions(ctx, messageVPN, queueName)
	if err != nil {
		return err
	}
	if len(topics) == 1 && topics[0] == desiredTopic {
		return nil
	}
	if len(topics) != 0 {
		return fmt.Errorf("runtime: managed queue %q has %d subscriptions; expected only %q", queueName, len(topics), desiredTopic)
	}
	if err := client.CreateSubscription(ctx, messageVPN, queueName, desiredTopic); err != nil {
		return err
	}
	topics, err = lister.ListExactQueueSubscriptions(ctx, messageVPN, queueName)
	if err != nil {
		return err
	}
	if len(topics) != 1 || topics[0] != desiredTopic {
		return fmt.Errorf("runtime: managed queue %q did not verify exact subscription %q after creation", queueName, desiredTopic)
	}
	return nil
}
