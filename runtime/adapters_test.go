package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/semp"
)

type fakeQueueClient struct {
	status        semp.QueueStatus
	fenced        int
	subscriptions map[string][]string
}

func (c *fakeQueueClient) EnsureQueue(context.Context, semp.QueueSpec) error { return nil }
func (c *fakeQueueClient) ListExactQueueSubscriptions(_ context.Context, _, queue string) ([]string, error) {
	return append([]string(nil), c.subscriptions[queue]...), nil
}
func (c *fakeQueueClient) CreateSubscription(_ context.Context, _, queue, topic string) error {
	if c.subscriptions == nil {
		c.subscriptions = make(map[string][]string)
	}
	c.subscriptions[queue] = []string{topic}
	return nil
}
func (c *fakeQueueClient) FenceQueue(context.Context, string, string) error {
	c.fenced++
	return nil
}
func (c *fakeQueueClient) UnfenceQueue(context.Context, string, string) error { return nil }
func (c *fakeQueueClient) MonitorQueue(context.Context, string, string) (semp.QueueStatus, error) {
	return c.status, nil
}
func (c *fakeQueueClient) MonitorDrain(context.Context, string, string) (semp.QueueStatus, error) {
	return c.status, nil
}
func (c *fakeQueueClient) DeleteQueue(context.Context, string, string) error { return nil }
func (c *fakeQueueClient) QueueExists(context.Context, string, string) (bool, error) {
	return true, nil
}

func TestSEMPBrokerFenceVerifiesIngressState(t *testing.T) {
	client := &fakeQueueClient{status: semp.QueueStatus{IngressEnabled: false}}
	observedAt := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	fence := SEMPBrokerFence{
		Brokers: map[string]BrokerTarget{"a": {Client: client}},
		Queues: FenceQueueResolverFunc(func(context.Context, controller.FenceRequest, controller.Broker) ([]ManagedQueue, error) {
			return []ManagedQueue{{MessageVPN: "vpn", Name: "managed-fence-queue"}}, nil
		}),
		Now: func() time.Time { return observedAt },
	}
	request := controller.FenceRequest{Brokers: []controller.Broker{{ID: "a", Destination: "queue"}}}
	if err := fence.Fence(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	status, err := fence.VerifyFence(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if client.fenced != 1 || !status.Fenced || !status.ObservedAt.Equal(observedAt) {
		t.Fatalf("unexpected fence result: count=%d status=%#v", client.fenced, status)
	}
	client.status.IngressEnabled = true
	status, err = fence.VerifyFence(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if status.Fenced {
		t.Fatal("enabled ingress reported as fenced")
	}
}

func TestComposeControllerRejectsMissingNativeAdapters(t *testing.T) {
	_, _, err := ComposeController(config.Config{}, ControllerDependencies{}, ControllerOptions{})
	if !errors.Is(err, ErrNativeAdapterUnavailable) {
		t.Fatalf("error = %v", err)
	}
}

func TestActionTimeoutUsesEarlierPhaseDeadline(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	reconciler := &fakeReconciler{state: controller.PersistentState{Groups: map[string]*controller.GroupState{
		"a": {PhaseDeadline: now.Add(250 * time.Millisecond)},
	}}}
	process := &ControllerProcess{reconciler: reconciler, options: ControllerOptions{ActionTimeout: time.Second, Now: func() time.Time { return now }}}
	if got := process.actionTimeout("a"); got != 250*time.Millisecond {
		t.Fatalf("action timeout = %v", got)
	}
}
