package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
)

// DrainObserver is the controller surface used by the production SEMP drain
// monitor. Snapshot supplies immutable transition identities; Observe persists
// each exact source observation before reconciliation is triggered.
type DrainObserver interface {
	Snapshot() controller.PersistentState
	Observe(controller.Telemetry) error
}

// SEMPDrainMonitor periodically polls every exact source-epoch queue in active
// DRAIN transitions. A failed or identity-mismatched read is submitted as
// unknown so previously established continuous-zero evidence is invalidated.
type SEMPDrainMonitor struct {
	Controller  DrainObserver
	Brokers     map[string]BrokerTarget
	MessageVPNs map[string]string
	Triggers    chan<- Trigger
	Interval    time.Duration
	Participant string
	Logger      *slog.Logger
	Now         func() time.Time

	mu       sync.Mutex
	sequence uint64
}

func (m *SEMPDrainMonitor) Run(ctx context.Context) error {
	if err := m.validate(); err != nil {
		return err
	}
	m.runCycle(ctx)

	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.runCycle(ctx)
		}
	}
}

func (m *SEMPDrainMonitor) runCycle(ctx context.Context) {
	if err := m.observe(ctx); err != nil && ctx.Err() == nil {
		m.Logger.Error("SEMP drain monitoring cycle failed", "error", err)
	}
}

func (*SEMPDrainMonitor) Close() error { return nil }

func (m *SEMPDrainMonitor) validate() error {
	if m == nil || m.Controller == nil {
		return errors.New("runtime: drain monitor controller is required")
	}
	if m.Interval <= 0 {
		return errors.New("runtime: drain monitor interval must be positive")
	}
	if m.Participant == "" {
		return errors.New("runtime: drain monitor participant identity is required")
	}
	if m.Logger == nil {
		m.Logger = slog.Default()
	}
	if m.Now == nil {
		m.Now = time.Now
	}
	return nil
}

func (m *SEMPDrainMonitor) observe(ctx context.Context) error {
	snapshot := m.Controller.Snapshot()
	var wg sync.WaitGroup
	errorsByGroup := make(chan error, len(snapshot.Groups))
	for group, state := range snapshot.Groups {
		if state == nil || state.Completed || state.RollingBack || state.Phase != controller.PhaseDrain || !state.DrainPublished {
			continue
		}
		group, stateCopy := group, *state
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.observeGroup(ctx, group, stateCopy); err != nil {
				errorsByGroup <- err
			}
		}()
	}
	wg.Wait()
	close(errorsByGroup)

	var result error
	for err := range errorsByGroup {
		result = errors.Join(result, err)
	}
	return result
}

func (m *SEMPDrainMonitor) observeGroup(ctx context.Context, group string, state controller.GroupState) error {
	resources, err := exactDrainResources(state.Spec)
	if err != nil {
		return fmt.Errorf("runtime: resolve drain resources for group %q: %w", group, err)
	}

	observed := false
	var result error
	for _, resource := range resources {
		if ctx.Err() != nil {
			return errors.Join(result, ctx.Err())
		}
		sample, readErr := m.read(ctx, state.Spec, resource)
		if readErr != nil {
			m.Logger.Warn("SEMP drain observation is unknown", "group", group, "broker", resource.broker, "queue", resource.queue, "error", readErr)
		}
		if err := m.Controller.Observe(sample); err != nil {
			if m.observationBecameStale(group, state.Spec.ID) {
				return result
			}
			result = errors.Join(result, fmt.Errorf("runtime: observe drain source %q/%q for group %q: %w", resource.broker, resource.queue, group, err))
			continue
		}
		observed = true
	}
	if observed {
		m.trigger(group)
	}
	return result
}

type drainResource struct {
	broker string
	queue  string
}

func exactDrainResources(spec controller.TransitionSpec) ([]drainResource, error) {
	if len(spec.CurrentResources) == 0 {
		return nil, errors.New("current epoch resources are required")
	}
	current := make(map[string]struct{}, len(spec.Current))
	for _, broker := range spec.Current {
		current[broker.ID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(spec.CurrentResources))
	resources := make([]drainResource, 0, len(spec.CurrentResources))
	for _, resource := range spec.CurrentResources {
		if resource.Epoch != spec.FromEpoch {
			return nil, fmt.Errorf("resource %q has epoch %d, want %d", resource.QueueName, resource.Epoch, spec.FromEpoch)
		}
		if _, ok := current[resource.BrokerID]; !ok {
			return nil, fmt.Errorf("resource %q references non-current broker %q", resource.QueueName, resource.BrokerID)
		}
		key := resource.BrokerID + "\x00" + resource.QueueName
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("duplicate drain source %q/%q", resource.BrokerID, resource.QueueName)
		}
		seen[key] = struct{}{}
		resources = append(resources, drainResource{broker: resource.BrokerID, queue: resource.QueueName})
	}
	return resources, nil
}

func (m *SEMPDrainMonitor) read(ctx context.Context, spec controller.TransitionSpec, resource drainResource) (controller.Telemetry, error) {
	sample := controller.Telemetry{
		Namespace: spec.Namespace, Group: spec.Group, TransitionID: spec.ID, Epoch: spec.FromEpoch,
		Participant: m.Participant, Role: control.RoleObserver,
		SourceBroker: resource.broker, SourceQueue: resource.queue,
	}

	target, ok := m.Brokers[resource.broker]
	messageVPN, vpnOK := m.MessageVPNs[resource.broker]
	if !ok || target.Client == nil || !vpnOK || messageVPN == "" {
		sample.ObservedAt, sample.MessageID = m.observationIdentity(sample)
		return sample, fmt.Errorf("no SEMP drain target for broker %q", resource.broker)
	}
	status, err := target.Client.MonitorDrain(ctx, messageVPN, resource.queue)
	sample.ObservedAt, sample.MessageID = m.observationIdentity(sample)
	if err != nil {
		return sample, err
	}
	if status.MessageVPN != messageVPN || status.Name != resource.queue {
		return sample, fmt.Errorf("SEMP monitor returned mismatched identity %q/%q", status.MessageVPN, status.Name)
	}
	if status.UnackedMessages > math.MaxUint64-status.InProgressAckMessages {
		return sample, errors.New("SEMP unacknowledged message count overflow")
	}
	sample.Queued = controller.Count{Known: true, Value: status.SpooledMessages}
	// SEMP's msgSpoolUsage is a byte allocation diagnostic and may remain nonzero
	// after the last message is gone. Use the current message count as the blocking
	// stored count so residual allocation cannot prevent a proven drain.
	sample.Stored = controller.Count{Known: true, Value: status.SpooledMessages}
	sample.Unacked = controller.Count{Known: true, Value: status.UnackedMessages + status.InProgressAckMessages}
	return sample, nil
}

func (m *SEMPDrainMonitor) observationIdentity(sample controller.Telemetry) (time.Time, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	observedAt := m.Now().UTC()
	m.sequence++
	source := sha256.Sum256([]byte(sample.SourceBroker + "\x00" + sample.SourceQueue))
	messageID := sample.Group + "/" + sample.TransitionID + "/drain/" + hex.EncodeToString(source[:16]) + "/" + strconv.FormatInt(observedAt.UnixNano(), 10) + "/" + strconv.FormatUint(m.sequence, 10)
	return observedAt, messageID
}

func (m *SEMPDrainMonitor) observationBecameStale(group, transitionID string) bool {
	state := m.Controller.Snapshot().Groups[group]
	return state == nil || state.Completed || state.RollingBack || state.Phase != controller.PhaseDrain || !state.DrainPublished || state.Spec.ID != transitionID
}

func (m *SEMPDrainMonitor) trigger(group string) {
	if m.Triggers == nil {
		return
	}
	select {
	case m.Triggers <- Trigger{Group: group, Reason: "SEMP drain observation"}:
	default:
	}
}
