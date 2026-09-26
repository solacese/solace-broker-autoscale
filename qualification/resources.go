package qualification

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/solacese/solace-workload-balancer/cloud"
	"github.com/solacese/solace-workload-balancer/semp"
)

func newSEMPClient(bundle cloud.ConnectionBundle) (*semp.Client, error) {
	if len(bundle.ManagementURLs) == 0 {
		return nil, errors.New("qualification: bundle has no management URL")
	}
	return semp.NewClient(semp.ClientOptions{
		BaseURL: bundle.ManagementURLs[0], Username: bundle.ManagementCredential.Username,
		Password: bundle.ManagementCredential.Password,
	})
}

const cleanupPollInterval = 25 * time.Millisecond

type managedResources struct {
	clients map[string]QueueAPI
	created []queueResource
}

func provision(ctx context.Context, plan resourcePlan, factory QueueAPIFactory) (*managedResources, error) {
	result := &managedResources{clients: make(map[string]QueueAPI, 4)}
	bundles := map[string]cloud.ConnectionBundle{"broker-0": plan.public.Bundles.Control}
	for index, role := range []string{"broker-a", "broker-b", "broker-c"} {
		bundles[role] = plan.public.Bundles.Data[index]
	}
	for role, bundle := range bundles {
		client, err := factory.New(bundle)
		if err != nil {
			return result, fmt.Errorf("qualification: create SEMP client for %s: %w", role, err)
		}
		result.clients[role] = client
	}
	for _, resource := range plan.queues {
		client := result.clients[resource.ref.Role]
		created, err := client.CreateQueue(ctx, resource.spec)
		if created {
			// The POST succeeded even if its verification failed, so cleanup owns this
			// exact queue and must attempt deletion before returning the error.
			result.created = append(result.created, resource)
		}
		if err != nil {
			return result, fmt.Errorf("qualification: create %s queue on %s: %w", resource.ref.Kind, resource.ref.Role, err)
		}
		if err := client.CreateSubscription(ctx, resource.ref.MessageVPN, resource.ref.Queue, resource.subscription); err != nil {
			return result, fmt.Errorf("qualification: subscribe %s queue on %s: %w", resource.ref.Kind, resource.ref.Role, err)
		}
	}
	return result, nil
}

func (m *managedResources) cleanup(ctx context.Context) error {
	if m == nil {
		return nil
	}
	var errs []error
	for index := len(m.created) - 1; index >= 0; index-- {
		resource := m.created[index]
		client := m.clients[resource.ref.Role]
		if client == nil {
			errs = append(errs, fmt.Errorf("qualification: cleanup client unavailable for %s", resource.ref.Role))
			continue
		}
		if err := client.DeleteQueue(ctx, resource.ref.MessageVPN, resource.ref.Queue); err != nil {
			errs = append(errs, fmt.Errorf("qualification: delete exact queue on %s: %w", resource.ref.Role, err))
			continue
		}
		if err := waitForQueueAbsent(ctx, client, resource.ref.MessageVPN, resource.ref.Queue); err != nil {
			errs = append(errs, fmt.Errorf("qualification: verify exact queue absent on %s: %w", resource.ref.Role, err))
		}
	}
	return errors.Join(errs...)
}

func waitForQueueAbsent(ctx context.Context, client QueueAPI, messageVPN, queueName string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		exists, err := client.QueueExists(ctx, messageVPN, queueName)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}

		timer := time.NewTimer(cleanupPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
