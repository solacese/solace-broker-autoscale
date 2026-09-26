package semp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/solacese/solace-workload-balancer/policy"
)

// VPNStatus is the read-only broker/VPN telemetry needed by capacity policy.
// All resource fields are exact SEMP Monitor counters. Connections is the
// number of paginated MsgVpnClientConnection objects, not the client count.
type VPNStatus struct {
	MessageVPN            string
	IngressBytesPerSecond uint64
	EgressBytesPerSecond  uint64
	SpoolBytes            uint64
	Connections           uint64
	ObservedClients       uint64
}

// MonitorVPN samples one Message VPN and enumerates every client connection.
// Missing counters, malformed identities, and any partial pagination failure
// fail the sample rather than converting unavailable telemetry to zero.
func (c *Client) MonitorVPN(ctx context.Context, messageVPN string) (VPNStatus, error) {
	if messageVPN == "" {
		return VPNStatus{}, errors.New("semp: message VPN is required")
	}
	var envelope struct {
		Data struct {
			MessageVPN string         `json:"msgVpnName"`
			Ingress    requiredUint64 `json:"rxByteRate"`
			Egress     requiredUint64 `json:"txByteRate"`
			Spool      requiredUint64 `json:"msgSpoolUsage"`
		} `json:"data"`
	}
	path := pathSegments("SEMP", "v2", "monitor", "msgVpns", messageVPN)
	if err := c.request(ctx, http.MethodGet, path, nil, &envelope); err != nil {
		return VPNStatus{}, err
	}
	if envelope.Data.MessageVPN != messageVPN {
		return VPNStatus{}, fmt.Errorf("semp: monitor response identity does not match message VPN %q", messageVPN)
	}
	ingress, err := requiredCounter(messageVPN, "rxByteRate", envelope.Data.Ingress)
	if err != nil {
		return VPNStatus{}, err
	}
	egress, err := requiredCounter(messageVPN, "txByteRate", envelope.Data.Egress)
	if err != nil {
		return VPNStatus{}, err
	}
	spool, err := requiredCounter(messageVPN, "msgSpoolUsage", envelope.Data.Spool)
	if err != nil {
		return VPNStatus{}, err
	}

	type client struct {
		Name string `json:"clientName"`
		VPN  string `json:"msgVpnName"`
	}
	clients, err := list[client](ctx, c, path+pathSegments("clients"))
	if err != nil {
		return VPNStatus{}, fmt.Errorf("semp: list clients for message VPN %q: %w", messageVPN, err)
	}
	var connections uint64
	for index, monitoredClient := range clients {
		if monitoredClient.Name == "" || monitoredClient.VPN != messageVPN {
			return VPNStatus{}, fmt.Errorf("semp: message VPN %q client %d has invalid identity", messageVPN, index)
		}
		type connection struct {
			VPN           string `json:"msgVpnName"`
			ClientName    string `json:"clientName"`
			ClientAddress string `json:"clientAddress"`
		}
		items, listErr := list[connection](ctx, c, path+pathSegments("clients", monitoredClient.Name, "connections"))
		if listErr != nil {
			return VPNStatus{}, fmt.Errorf("semp: list connections for message VPN %q client %q: %w", messageVPN, monitoredClient.Name, listErr)
		}
		for connectionIndex, item := range items {
			if item.VPN != messageVPN || item.ClientName != monitoredClient.Name || item.ClientAddress == "" {
				return VPNStatus{}, fmt.Errorf("semp: message VPN %q client %q connection %d has invalid identity", messageVPN, monitoredClient.Name, connectionIndex)
			}
		}
		if uint64(len(items)) > ^uint64(0)-connections {
			return VPNStatus{}, fmt.Errorf("semp: message VPN %q connection count overflow", messageVPN)
		}
		connections += uint64(len(items))
	}
	return VPNStatus{
		MessageVPN: messageVPN, IngressBytesPerSecond: ingress, EgressBytesPerSecond: egress,
		SpoolBytes: spool, Connections: connections, ObservedClients: uint64(len(clients)),
	}, nil
}

// MonitorClient is the authenticated, read-only SEMP surface needed by the
// production telemetry collector.
type MonitorClient interface {
	MonitorVPN(context.Context, string) (VPNStatus, error)
	MonitorQueue(context.Context, string, string) (QueueStatus, error)
}

// BrokerTarget identifies one logical data service and its authenticated SEMP
// client. Service class/version come from operator configuration, not SEMP
// inference, and must exactly match a measured capacity profile.
type BrokerTarget struct {
	ID            string
	MessageVPN    string
	ServiceClass  string
	BrokerVersion string
	Client        MonitorClient
}

// GroupTarget identifies one exact current managed queue for a group on one
// broker. Multiple targets for the same group/broker are summed, covering all
// independent consumer-set queues. A provider updates the set after transitions.
type GroupTarget struct {
	GroupID  string
	BrokerID string
	Queue    string
}

// PolicyTargetProvider supplies current broker inventory and exact current
// group queues at the start of every collector cycle.
type PolicyTargetProvider interface {
	PolicyTargets() ([]BrokerTarget, []GroupTarget, error)
}

// PolicyTargetProviderFunc adapts a function to PolicyTargetProvider.
type PolicyTargetProviderFunc func() ([]BrokerTarget, []GroupTarget, error)

func (provider PolicyTargetProviderFunc) PolicyTargets() ([]BrokerTarget, []GroupTarget, error) {
	return provider()
}

// PolicyCollector performs a coherent read-only SEMP polling cycle. Broker and
// group observations retain separate successful-response times so freshness is
// never inferred from when the cycle began. SEMP is polled; native events may
// call Collect to trigger a refresh but are not treated as SEMP subscriptions.
type PolicyCollector struct {
	Targets PolicyTargetProvider
	Now     func() time.Time
}

// Validate resolves and validates the current target inventory without issuing
// monitor requests. Runtime calls it once at startup so permanent target/config
// defects remain fatal rather than entering the transient telemetry retry loop.
func (c PolicyCollector) Validate() error {
	if c.Targets == nil {
		return errors.New("semp: policy collector target provider is required")
	}
	brokers, groups, err := c.Targets.PolicyTargets()
	if err != nil {
		return fmt.Errorf("semp: resolve policy targets: %w", err)
	}
	_, err = validatePolicyTargets(brokers, groups)
	return err
}

func (c PolicyCollector) Collect(ctx context.Context) (policy.TelemetrySnapshot, error) {
	if c.Targets == nil {
		return policy.TelemetrySnapshot{}, errors.New("semp: policy collector target provider is required")
	}
	brokers, groups, err := c.Targets.PolicyTargets()
	if err != nil {
		return policy.TelemetrySnapshot{}, fmt.Errorf("semp: resolve policy targets: %w", err)
	}
	clients, err := validatePolicyTargets(brokers, groups)
	if err != nil {
		return policy.TelemetrySnapshot{}, err
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	snapshot := policy.TelemetrySnapshot{}
	for _, broker := range brokers {
		status, err := broker.Client.MonitorVPN(ctx, broker.MessageVPN)
		if err != nil {
			return policy.TelemetrySnapshot{}, fmt.Errorf("semp: monitor broker %q: %w", broker.ID, err)
		}
		if status.MessageVPN != broker.MessageVPN {
			return policy.TelemetrySnapshot{}, fmt.Errorf("semp: monitor broker %q returned mismatched message VPN identity", broker.ID)
		}
		observedAt := now().UTC()
		snapshot.Brokers = append(snapshot.Brokers, policy.BrokerSample{
			BrokerID: broker.ID, ServiceClass: broker.ServiceClass, BrokerVersion: broker.BrokerVersion,
			ObservedAt: observedAt,
			Resources: policy.Resources{
				IngressBytesPerSecond: policy.KnownMetric(float64(status.IngressBytesPerSecond)),
				EgressBytesPerSecond:  policy.KnownMetric(float64(status.EgressBytesPerSecond)),
				SpoolBytes:            policy.KnownMetric(float64(status.SpoolBytes)),
				Connections:           policy.KnownMetric(float64(status.Connections)),
			},
		})
	}

	type aggregateKey struct{ group, broker string }
	aggregates := make(map[aggregateKey]policy.GroupSample)
	throughputKnown := make(map[aggregateKey]bool)
	order := make([]aggregateKey, 0, len(groups))
	for _, group := range groups {
		broker, exists := clients[group.BrokerID]
		if !exists {
			return policy.TelemetrySnapshot{}, fmt.Errorf("semp: group %q references unknown broker %q", group.GroupID, group.BrokerID)
		}
		status, err := broker.Client.MonitorQueue(ctx, broker.MessageVPN, group.Queue)
		if err != nil {
			return policy.TelemetrySnapshot{}, fmt.Errorf("semp: monitor group %q broker %q queue %q: %w", group.GroupID, group.BrokerID, group.Queue, err)
		}
		if status.MessageVPN != broker.MessageVPN || status.Name != group.Queue {
			return policy.TelemetrySnapshot{}, fmt.Errorf("semp: monitor group %q broker %q returned mismatched queue identity", group.GroupID, group.BrokerID)
		}
		key := aggregateKey{group: group.GroupID, broker: group.BrokerID}
		sample, exists := aggregates[key]
		if !exists {
			sample = policy.GroupSample{GroupID: group.GroupID, BrokerID: group.BrokerID, ObservedAt: now().UTC()}
			throughputKnown[key] = true
			order = append(order, key)
		}
		throughputKnown[key] = throughputKnown[key] && status.ThroughputKnown
		sample.Resources.IngressBytesPerSecond = policy.KnownMetric(sample.Resources.IngressBytesPerSecond.Value + float64(status.IngressBytesPerSecond))
		sample.Resources.EgressBytesPerSecond = policy.KnownMetric(sample.Resources.EgressBytesPerSecond.Value + float64(status.EgressBytesPerSecond))
		sample.Resources.SpoolBytes = sumKnownMetric(sample.Resources.SpoolBytes, status.SpoolUsageBytes)
		sample.Resources.Connections = sumKnownMetric(sample.Resources.Connections, status.Consumers)
		sample.Backlog.QueuedMessages = sumKnownMetric(sample.Backlog.QueuedMessages, status.SpooledMessages)
		sample.Backlog.UnackedMessages = sumKnownMetric(sample.Backlog.UnackedMessages, status.UnackedMessages+status.InProgressAckMessages)
		aggregates[key] = sample
	}
	for _, key := range order {
		sample := aggregates[key]
		if !throughputKnown[key] {
			sample.Resources.IngressBytesPerSecond = policy.UnknownMetric()
			sample.Resources.EgressBytesPerSecond = policy.UnknownMetric()
		}
		snapshot.Groups = append(snapshot.Groups, sample)
	}
	snapshot.CapturedAt = now().UTC()
	if err := snapshot.Validate(); err != nil {
		return policy.TelemetrySnapshot{}, err
	}
	return snapshot, nil
}

func validatePolicyTargets(brokers []BrokerTarget, groups []GroupTarget) (map[string]BrokerTarget, error) {
	if len(brokers) == 0 {
		return nil, errors.New("semp: policy collector requires at least one broker")
	}
	clients := make(map[string]BrokerTarget, len(brokers))
	for index, broker := range brokers {
		if broker.ID == "" || broker.MessageVPN == "" || broker.ServiceClass == "" || broker.BrokerVersion == "" || broker.Client == nil {
			return nil, fmt.Errorf("semp: policy collector broker %d is incomplete", index)
		}
		if _, duplicate := clients[broker.ID]; duplicate {
			return nil, fmt.Errorf("semp: policy collector repeats broker %q", broker.ID)
		}
		clients[broker.ID] = broker
	}
	seenGroups := make(map[string]struct{}, len(groups))
	for index, group := range groups {
		if group.GroupID == "" || group.BrokerID == "" || group.Queue == "" {
			return nil, fmt.Errorf("semp: policy collector group target %d is incomplete", index)
		}
		key := group.GroupID + "\x00" + group.BrokerID + "\x00" + group.Queue
		if _, duplicate := seenGroups[key]; duplicate {
			return nil, fmt.Errorf("semp: policy collector repeats group target %q/%q queue %q", group.GroupID, group.BrokerID, group.Queue)
		}
		seenGroups[key] = struct{}{}
		if _, exists := clients[group.BrokerID]; !exists {
			return nil, fmt.Errorf("semp: group %q references unknown broker %q", group.GroupID, group.BrokerID)
		}
	}
	return clients, nil
}

func sumKnownMetric(current policy.Metric, value uint64) policy.Metric {
	return policy.KnownMetric(current.Value + float64(value))
}
