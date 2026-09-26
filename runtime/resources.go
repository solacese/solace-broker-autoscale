package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
	"github.com/solacese/solace-workload-balancer/semp"
)

const (
	defaultManagedQueueSpoolMB      = 256
	defaultManagedUnackedPerFlow    = 1000
	managedQueuePermission          = "delete"
	managedQueueRejectOnDiscardMode = "always"
	managedCleanupPollInterval      = 25 * time.Millisecond
)

// ManagedEpochResources owns only deterministic, namespace-scoped data-plane
// queues named by ManagedNames. Existing queues are accepted only through
// EnsureQueue's exact-config comparison.
type ManagedEpochResources struct {
	Config  config.Config
	Clients map[string]SEMPQueueClient
	Now     func() time.Time
}

func (m *ManagedEpochResources) EnsureCurrent(ctx context.Context, snapshot control.MembershipSnapshot) error {
	return m.EnsureBootstrap(ctx, snapshot, nil)
}

// EnsureBootstrap verifies or creates all resources required by the retained
// state. Durable transition flags determine ingress exactly at restart. Once a
// multi-broker ingress change has durable intent, bootstrap reapplies it to the
// full source or target set before exact configuration verification.
func (m *ManagedEpochResources) EnsureBootstrap(ctx context.Context, snapshot control.MembershipSnapshot, transition *controller.GroupState) error {
	group, ok := configuredGroup(m.Config.Groups, snapshot.ScalingGroup)
	if !ok {
		return fmt.Errorf("runtime: current resources reference unknown group %q", snapshot.ScalingGroup)
	}
	if snapshot.Namespace != m.Config.Namespace {
		return fmt.Errorf("runtime: membership namespace %q does not match managed namespace %q", snapshot.Namespace, m.Config.Namespace)
	}
	if transition == nil || transition.Completed {
		currentIngress := snapshot.Phase == control.PhaseActive || snapshot.Phase == control.PhasePrepare
		return m.ensure(ctx, group, snapshot.CurrentMembership, snapshot.Epoch, snapshot.CurrentResources, currentIngress)
	}

	sourceIngress := true
	reapplySourceIngress := false
	if transition.Unfenced {
		reapplySourceIngress = true
	} else if transition.FenceAttempted {
		sourceIngress = false
		reapplySourceIngress = true
	}
	if err := m.ensureBootstrapEpoch(ctx, group, brokerIDs(transition.Spec.Current), transition.Spec.FromEpoch, transition.Spec.CurrentResources, sourceIngress, reapplySourceIngress); err != nil {
		return err
	}
	return m.ensureBootstrapEpoch(ctx, group, brokerIDs(transition.Spec.Proposed), transition.Spec.ToEpoch, transition.Spec.ProposedResources, transition.TargetIngressEnabled, transition.TargetIngressEnabled)
}

func (m *ManagedEpochResources) ensureBootstrapEpoch(ctx context.Context, group config.ScalingGroup, membership []string, epoch uint64, resources []control.EpochResourceIdentity, ingress, reapplyIngress bool) error {
	if !reapplyIngress {
		return m.ensure(ctx, group, membership, epoch, resources, ingress)
	}
	if m == nil || len(m.Clients) == 0 {
		return errors.New("runtime: managed epoch resource clients are required")
	}
	if err := validateManagedNamespace(m.Config.Namespace); err != nil {
		return err
	}
	expected, err := expectedEpochResources(m.Config.Namespace, group, membership, epoch)
	if err != nil {
		return err
	}
	if !slices.Equal(resources, expected) {
		return fmt.Errorf("runtime: epoch %d resources for group %q are not the exact managed identities", epoch, group.ID)
	}
	for _, resource := range expected {
		broker, client, err := m.broker(resource.BrokerID)
		if err != nil {
			return err
		}
		spec, err := managedEpochQueueSpec(group, broker.MessageVPN, resource.QueueName, ingress)
		if err != nil {
			return err
		}
		if err := client.EnsureQueue(ctx, spec); err != nil {
			spec.IngressEnabled = !ingress
			if alternateErr := client.EnsureQueue(ctx, spec); alternateErr != nil {
				return fmt.Errorf("runtime: verify managed epoch queue for group %q broker %q epoch %d before ingress replay: %w", group.ID, broker.ID, epoch, errors.Join(err, alternateErr))
			}
		}
		if err := ensureExactQueueSubscription(ctx, client, broker.MessageVPN, resource.QueueName, resource.IngressTopic); err != nil {
			return fmt.Errorf("runtime: ensure exact managed epoch subscription for group %q broker %q epoch %d: %w", group.ID, broker.ID, epoch, err)
		}
	}
	for _, resource := range expected {
		broker, client, err := m.broker(resource.BrokerID)
		if err != nil {
			return err
		}
		if ingress {
			err = client.UnfenceQueue(ctx, broker.MessageVPN, resource.QueueName)
		} else {
			err = client.FenceQueue(ctx, broker.MessageVPN, resource.QueueName)
		}
		if err != nil {
			return fmt.Errorf("runtime: replay managed epoch ingress for group %q broker %q epoch %d: %w", group.ID, broker.ID, epoch, err)
		}
	}
	return m.ensure(ctx, group, membership, epoch, resources, ingress)
}

func (m *ManagedEpochResources) Prepare(ctx context.Context, spec controller.TransitionSpec) error {
	group, ok := configuredGroup(m.Config.Groups, spec.Group)
	if !ok {
		return fmt.Errorf("runtime: proposed resources reference unknown group %q", spec.Group)
	}
	return m.ensure(ctx, group, brokerIDs(spec.Proposed), spec.ToEpoch, spec.ProposedResources, false)
}

func (m *ManagedEpochResources) ensure(ctx context.Context, group config.ScalingGroup, membership []string, epoch uint64, resources []control.EpochResourceIdentity, ingress bool) error {
	if m == nil || len(m.Clients) == 0 {
		return errors.New("runtime: managed epoch resource clients are required")
	}
	if err := validateManagedNamespace(m.Config.Namespace); err != nil {
		return err
	}
	expected, err := expectedEpochResources(m.Config.Namespace, group, membership, epoch)
	if err != nil {
		return err
	}
	if !slices.Equal(resources, expected) {
		return fmt.Errorf("runtime: epoch %d resources for group %q are not the exact managed identities", epoch, group.ID)
	}
	for _, resource := range expected {
		broker, client, err := m.broker(resource.BrokerID)
		if err != nil {
			return err
		}
		spec, err := managedEpochQueueSpec(group, broker.MessageVPN, resource.QueueName, ingress)
		if err != nil {
			return err
		}
		if err := client.EnsureQueue(ctx, spec); err != nil {
			return fmt.Errorf("runtime: ensure managed epoch queue for group %q broker %q epoch %d: %w", group.ID, broker.ID, epoch, err)
		}
		if err := ensureExactQueueSubscription(ctx, client, broker.MessageVPN, resource.QueueName, resource.IngressTopic); err != nil {
			return fmt.Errorf("runtime: ensure exact managed epoch subscription for group %q broker %q epoch %d: %w", group.ID, broker.ID, epoch, err)
		}
	}
	return nil
}

// CleanupCompleted consumes immutable controller evidence but deletes only the
// exact old queue named by that evidence and transition specification. Evidence
// is revalidated on every call, making restart retries idempotent.
type managedEpochCleaner struct {
	Resources *ManagedEpochResources
	Catalog   *MembershipCatalog
}

func (c managedEpochCleaner) CleanupCompleted(ctx context.Context, state controller.PersistentState, group string) error {
	if c.Resources == nil {
		return errors.New("runtime: managed epoch resources are required")
	}
	return c.Resources.cleanupCompletedGroup(ctx, state, c.Catalog, group)
}

func (m *ManagedEpochResources) CleanupCompleted(ctx context.Context, state controller.PersistentState, catalog *MembershipCatalog) error {
	if m == nil || catalog == nil {
		return errors.New("runtime: managed epoch resources and membership catalog are required")
	}
	if err := validateManagedNamespace(m.Config.Namespace); err != nil {
		return err
	}
	var errs []error
	for group := range state.CleanupEvidence {
		if err := m.cleanupCompletedGroup(ctx, state, catalog, group); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *ManagedEpochResources) cleanupCompletedGroup(ctx context.Context, state controller.PersistentState, catalog *MembershipCatalog, group string) error {
	if m == nil || catalog == nil {
		return errors.New("runtime: managed epoch resources and membership catalog are required")
	}
	if err := validateManagedNamespace(m.Config.Namespace); err != nil {
		return err
	}
	var errs []error
	for _, record := range state.CleanupEvidence[group] {
		transition := completedTransitionForEvidence(state, group, record)
		if transition == nil {
			continue
		}
		if err := m.cleanupTransition(ctx, state, catalog, transition, record); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func completedTransitionForEvidence(state controller.PersistentState, group string, evidence controller.CleanupEvidence) *controller.GroupState {
	transition := state.Groups[group]
	if transition == nil || !transition.Completed || transition.RollingBack || transition.CompletedAt == nil {
		return nil
	}
	if transition.Spec.ID != evidence.TransitionID || transition.Spec.FromEpoch != evidence.SourceEpoch || transition.Spec.ToEpoch != evidence.TargetEpoch {
		return nil
	}
	return transition
}

func (m *ManagedEpochResources) cleanupTransition(ctx context.Context, state controller.PersistentState, catalog *MembershipCatalog, transition *controller.GroupState, evidence controller.CleanupEvidence) error {
	if evidence.TransitionID != transition.Spec.ID || evidence.SourceEpoch != transition.Spec.FromEpoch || evidence.TargetEpoch != transition.Spec.ToEpoch || !evidence.OldEpochFenced || evidence.ZeroSince.IsZero() || evidence.DrainObservedAt.IsZero() || evidence.FenceVerifiedAt.IsZero() || evidence.CommittedAt.IsZero() {
		return fmt.Errorf("runtime: cleanup evidence for group %q transition %q is incomplete or mismatched", transition.Spec.Group, evidence.TransitionID)
	}
	if evidence.DrainObservedAt.Before(evidence.ZeroSince) || evidence.FenceVerifiedAt.After(evidence.CommittedAt) || evidence.DrainObservedAt.After(evidence.CommittedAt) || transition.CompletedAt.Before(evidence.CommittedAt) {
		return fmt.Errorf("runtime: cleanup evidence for group %q transition %q has invalid timing", transition.Spec.Group, evidence.TransitionID)
	}
	if transition.Spec.DrainGrace <= 0 || evidence.DrainObservedAt.Sub(evidence.ZeroSince) < transition.Spec.DrainGrace {
		return fmt.Errorf("runtime: cleanup evidence for group %q transition %q does not prove the drain grace", transition.Spec.Group, evidence.TransitionID)
	}

	expectedSources := make(map[string]controller.DrainSourceEvidence, len(transition.Spec.CurrentResources))
	for _, source := range evidence.Sources {
		key := source.Broker + "\x00" + source.Queue
		if source.Broker == "" || source.Queue == "" || source.ZeroSince.IsZero() || source.ObservedAt.IsZero() || source.ObservedAt.Before(source.ZeroSince) || source.ObservedAt.After(evidence.CommittedAt) {
			return fmt.Errorf("runtime: cleanup source evidence for group %q is invalid", transition.Spec.Group)
		}
		if _, duplicate := expectedSources[key]; duplicate {
			return fmt.Errorf("runtime: duplicate cleanup source evidence for group %q", transition.Spec.Group)
		}
		expectedSources[key] = source
	}
	if len(expectedSources) != len(transition.Spec.CurrentResources) {
		return fmt.Errorf("runtime: cleanup evidence for group %q is missing exact source queues", transition.Spec.Group)
	}

	for _, resource := range transition.Spec.CurrentResources {
		if resource.Epoch != evidence.SourceEpoch {
			return fmt.Errorf("runtime: cleanup resource for group %q has mismatched epoch", transition.Spec.Group)
		}
		source, ok := expectedSources[resource.BrokerID+"\x00"+resource.QueueName]
		if !ok || source.ObservedAt.Sub(source.ZeroSince) < transition.Spec.DrainGrace {
			return fmt.Errorf("runtime: cleanup resource %q for group %q lacks drain evidence", resource.QueueName, transition.Spec.Group)
		}
		if m.resourceReferenced(state, catalog, transition.Spec.Group, resource) {
			continue
		}
		broker, client, err := m.broker(resource.BrokerID)
		if err != nil {
			return err
		}
		if err := client.DeleteQueue(ctx, broker.MessageVPN, resource.QueueName); err != nil {
			return fmt.Errorf("runtime: delete exact old epoch queue for group %q broker %q: %w", transition.Spec.Group, broker.ID, err)
		}
		if err := waitForManagedQueueAbsent(ctx, client, broker.MessageVPN, resource.QueueName); err != nil {
			return fmt.Errorf("runtime: verify exact old epoch queue absent for group %q broker %q: %w", transition.Spec.Group, broker.ID, err)
		}
	}
	return nil
}

func (m *ManagedEpochResources) resourceReferenced(state controller.PersistentState, catalog *MembershipCatalog, cleanupGroup string, candidate control.EpochResourceIdentity) bool {
	for _, configured := range m.Config.Groups {
		snapshot, ok := catalog.Get(configured.ID)
		if !ok {
			return true
		}
		for _, resource := range append(slices.Clone(snapshot.CurrentResources), snapshot.ProposedResources...) {
			if samePhysicalQueue(resource, candidate) {
				return true
			}
		}
	}
	for group, transition := range state.Groups {
		if transition == nil {
			continue
		}
		if group == cleanupGroup && transition.Completed && !transition.RollingBack && transition.Spec.FromEpoch == candidate.Epoch {
			continue
		}
		for _, resource := range append(slices.Clone(transition.Spec.CurrentResources), transition.Spec.ProposedResources...) {
			if samePhysicalQueue(resource, candidate) {
				return true
			}
		}
	}
	return false
}

func samePhysicalQueue(left, right control.EpochResourceIdentity) bool {
	return left.BrokerID == right.BrokerID && left.QueueName == right.QueueName
}

func (m *ManagedEpochResources) broker(id string) (config.DataBroker, SEMPQueueClient, error) {
	broker, ok := configuredBroker(m.Config.DataBrokers, id)
	if !ok {
		return config.DataBroker{}, nil, fmt.Errorf("runtime: managed epoch resource references unknown broker %q", id)
	}
	client := m.Clients[id]
	if client == nil {
		return config.DataBroker{}, nil, fmt.Errorf("runtime: no SEMP resource client for broker %q", id)
	}
	return broker, client, nil
}

func expectedEpochResources(namespace string, group config.ScalingGroup, membership []string, epoch uint64) ([]control.EpochResourceIdentity, error) {
	names, err := control.NewManagedNames(namespace)
	if err != nil {
		return nil, err
	}
	return epochResources(names, group.ID, membership, group.EffectiveConsumerSets(), epoch)
}

func managedEpochQueueSpec(group config.ScalingGroup, vpn, name string, ingress bool) (semp.QueueSpec, error) {
	partitions := uint32(0)
	if group.Queue.Partitions < 0 || uint64(group.Queue.Partitions) > uint64(^uint32(0)) {
		return semp.QueueSpec{}, fmt.Errorf("runtime: invalid partition count for group %q", group.ID)
	}
	if group.Queue.Type == config.QueueTypePartitioned {
		partitions = uint32(group.Queue.Partitions)
	}
	return semp.QueueSpec{
		MessageVPN: vpn, Name: name, AccessType: group.Queue.Access, Permission: managedQueuePermission,
		IngressEnabled: ingress, EgressEnabled: true, MaxMsgSpoolUsage: defaultManagedQueueSpoolMB,
		MaxRedeliveryCount: uint64(group.Queue.MaxRedeliveries), DeadMsgQueue: group.Queue.DeadMessageQueue,
		PartitionCount: partitions, ConsumerAckPropagationEnabled: true,
		MaxDeliveredUnackedMsgsPerFlow:     defaultManagedUnackedPerFlow,
		RejectMsgToSenderOnDiscardBehavior: managedQueueRejectOnDiscardMode,
	}, nil
}

func brokerIDs(brokers []controller.Broker) []string {
	result := make([]string, len(brokers))
	for index, broker := range brokers {
		result[index] = broker.ID
	}
	return result
}

func waitForManagedQueueAbsent(ctx context.Context, client SEMPQueueClient, vpn, queue string) error {
	for {
		exists, err := client.QueueExists(ctx, vpn, queue)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		timer := time.NewTimer(managedCleanupPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func validateManagedNamespace(namespace string) error {
	if namespace == "" || strings.TrimSpace(namespace) != namespace {
		return errors.New("runtime: explicit managed namespace is required")
	}
	_, err := control.NewManagedNames(namespace)
	return err
}
