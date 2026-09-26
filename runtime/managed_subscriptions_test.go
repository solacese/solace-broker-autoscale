package runtime

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/solacese/solace-workload-balancer/semp"
)

func managedSubscriptionFixture(t *testing.T, subscriptions []string) (*ManagedEpochResources, *lifecycleQueueClient, string, string) {
	t.Helper()
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group)
	snapshot, err := GenesisSnapshot(cfg.Namespace, group)
	if err != nil {
		t.Fatal(err)
	}
	resource := snapshot.CurrentResources[0]
	spec, err := managedEpochQueueSpec(group, "data-a", resource.QueueName, true)
	if err != nil {
		t.Fatal(err)
	}
	client := &lifecycleQueueClient{
		queues: map[string]semp.QueueSpec{resource.QueueName: spec},
	}
	if subscriptions != nil {
		client.subscriptions = map[string][]string{resource.QueueName: append([]string(nil), subscriptions...)}
	}
	manager := &ManagedEpochResources{Config: cfg, Clients: map[string]SEMPQueueClient{"broker-a": client}}
	return manager, client, resource.QueueName, resource.IngressTopic
}

func TestManagedEpochResourcesAdoptsExactDesiredSubscriptionSet(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group)
	snapshot, err := GenesisSnapshot(cfg.Namespace, group)
	if err != nil {
		t.Fatal(err)
	}
	resource := snapshot.CurrentResources[0]
	manager, client, _, _ := managedSubscriptionFixture(t, []string{resource.IngressTopic})

	if err := manager.EnsureCurrent(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if client.subscriptionCreates != 0 || client.subscriptionLists != 1 {
		t.Fatalf("creates=%d lists=%d, want 0 and 1", client.subscriptionCreates, client.subscriptionLists)
	}
}

func TestManagedEpochResourcesCreatesAndVerifiesMissingDesiredSubscription(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group)
	snapshot, err := GenesisSnapshot(cfg.Namespace, group)
	if err != nil {
		t.Fatal(err)
	}
	manager, client, queue, topic := managedSubscriptionFixture(t, nil)

	if err := manager.EnsureCurrent(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if got := client.subscriptions[queue]; !reflect.DeepEqual(got, []string{topic}) {
		t.Fatalf("subscriptions = %v, want [%q]", got, topic)
	}
	if client.subscriptionCreates != 1 || client.subscriptionLists != 2 {
		t.Fatalf("creates=%d lists=%d, want 1 and 2", client.subscriptionCreates, client.subscriptionLists)
	}
}

func TestManagedEpochResourcesRejectsExtraSubscription(t *testing.T) {
	group := productionGroup()
	group.OrderedBrokerIDs = []string{"broker-a"}
	cfg := resourceTestConfig(group)
	snapshot, err := GenesisSnapshot(cfg.Namespace, group)
	if err != nil {
		t.Fatal(err)
	}
	resource := snapshot.CurrentResources[0]
	manager, client, _, _ := managedSubscriptionFixture(t, []string{resource.IngressTopic, "swlb/data/orders/epoch/0/>"})

	err = manager.EnsureCurrent(context.Background(), snapshot)
	if err == nil || !strings.Contains(err.Error(), "expected only") {
		t.Fatalf("expected extra-subscription rejection, got %v", err)
	}
	if client.subscriptionCreates != 0 {
		t.Fatalf("created %d subscriptions while rejecting extras", client.subscriptionCreates)
	}
}
