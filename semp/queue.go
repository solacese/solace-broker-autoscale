package semp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
)

const rejectAlways = "always"

// QueueSpec is the complete managed queue contract. EnsureQueue adopts an
// existing queue only when every field in this contract matches exactly.
type QueueSpec struct {
	MessageVPN                         string
	Name                               string
	AccessType                         string
	Owner                              string
	Permission                         string
	IngressEnabled                     bool
	EgressEnabled                      bool
	MaxMsgSpoolUsage                   uint64
	MaxRedeliveryCount                 uint64
	DeadMsgQueue                       string
	PartitionCount                     uint32
	ConsumerAckPropagationEnabled      bool
	MaxDeliveredUnackedMsgsPerFlow     uint64
	RejectMsgToSenderOnDiscardBehavior string
}

func (spec QueueSpec) validate() error {
	if spec.MessageVPN == "" || spec.Name == "" {
		return errors.New("semp: queue message VPN and name are required")
	}
	if spec.AccessType != "exclusive" && spec.AccessType != "non-exclusive" {
		return errors.New("semp: queue access type must be exclusive or non-exclusive")
	}
	switch spec.Permission {
	case "no-access", "read-only", "consume", "modify-topic", "delete":
	default:
		return errors.New("semp: invalid queue permission")
	}
	if spec.RejectMsgToSenderOnDiscardBehavior != rejectAlways {
		return errors.New("semp: managed queues must always reject discarded messages to the sender")
	}
	return nil
}

// QueueStatus is a point-in-time view of all drain and readiness dimensions.
// Missing or invalid broker counters cause MonitorQueue to fail rather than
// being silently treated as zero.
type QueueStatus struct {
	MessageVPN            string
	Name                  string
	IngressBytesPerSecond uint64
	EgressBytesPerSecond  uint64
	ThroughputKnown       bool
	SpooledMessages       uint64
	SpoolUsageBytes       uint64
	UnackedMessages       uint64
	InProgressAckMessages uint64
	Flows                 uint64
	Consumers             uint64
	IngressEnabled        bool
}

// Drained reports whether no message can remain queued, delivered but unacked,
// or in acknowledgement processing. SpoolUsageBytes is diagnostic only: the
// broker may retain spool allocation after the last message is removed.
func (status QueueStatus) Drained() bool {
	return status.SpooledMessages == 0 && status.UnackedMessages == 0 &&
		status.InProgressAckMessages == 0
}

type queueConfig struct {
	MessageVPN                         string `json:"msgVpnName,omitempty"`
	Name                               string `json:"queueName"`
	AccessType                         string `json:"accessType"`
	Owner                              string `json:"owner"`
	Permission                         string `json:"permission"`
	IngressEnabled                     bool   `json:"ingressEnabled"`
	EgressEnabled                      bool   `json:"egressEnabled"`
	MaxMsgSpoolUsage                   uint64 `json:"maxMsgSpoolUsage"`
	MaxRedeliveryCount                 uint64 `json:"maxRedeliveryCount"`
	DeadMsgQueue                       string `json:"deadMsgQueue"`
	PartitionCount                     uint32 `json:"partitionCount"`
	ConsumerAckPropagationEnabled      bool   `json:"consumerAckPropagationEnabled"`
	MaxDeliveredUnackedMsgsPerFlow     uint64 `json:"maxDeliveredUnackedMsgsPerFlow"`
	RejectMsgToSenderOnDiscardBehavior string `json:"rejectMsgToSenderOnDiscardBehavior"`
}

func configFromSpec(spec QueueSpec) queueConfig {
	return queueConfig{
		MessageVPN:                         spec.MessageVPN,
		Name:                               spec.Name,
		AccessType:                         spec.AccessType,
		Owner:                              spec.Owner,
		Permission:                         spec.Permission,
		IngressEnabled:                     spec.IngressEnabled,
		EgressEnabled:                      spec.EgressEnabled,
		MaxMsgSpoolUsage:                   spec.MaxMsgSpoolUsage,
		MaxRedeliveryCount:                 spec.MaxRedeliveryCount,
		DeadMsgQueue:                       spec.DeadMsgQueue,
		PartitionCount:                     spec.PartitionCount,
		ConsumerAckPropagationEnabled:      spec.ConsumerAckPropagationEnabled,
		MaxDeliveredUnackedMsgsPerFlow:     spec.MaxDeliveredUnackedMsgsPerFlow,
		RejectMsgToSenderOnDiscardBehavior: spec.RejectMsgToSenderOnDiscardBehavior,
	}
}

func queuePath(api, vpn, queue string) string {
	return pathSegments("SEMP", "v2", api, "msgVpns", vpn, "queues", queue)
}

func queueCollectionPath(vpn string) string {
	return pathSegments("SEMP", "v2", "config", "msgVpns", vpn, "queues")
}

func (c *Client) getQueue(ctx context.Context, vpn, name string) (json.RawMessage, error) {
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	err := c.request(ctx, http.MethodGet, queuePath("config", vpn, name), nil, &envelope)
	return envelope.Data, err
}

// QueueExists reports whether exactly one named queue exists. It performs only
// an exact-resource GET; HTTP 404 and Solace's exact NOT_FOUND status mean false.
func (c *Client) QueueExists(ctx context.Context, messageVPN, queueName string) (bool, error) {
	if messageVPN == "" || queueName == "" {
		return false, errors.New("semp: message VPN and exact queue name are required")
	}
	if _, err := c.getQueue(ctx, messageVPN, queueName); err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("semp: get exact queue %q: %w", queueName, err)
	}
	return true, nil
}

func (c *Client) queueMatches(ctx context.Context, path string, want queueConfig) (bool, error) {
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := c.request(ctx, http.MethodGet, path, nil, &envelope); err != nil {
		return false, err
	}
	return exactManagedConfig(envelope.Data, want)
}

// CreateQueue creates a new managed queue and rejects any pre-existing name,
// even when its configuration matches. Qualification uses this path so cleanup
// can never delete a resource it merely adopted.
func (c *Client) CreateQueue(ctx context.Context, spec QueueSpec) (bool, error) {
	if err := spec.validate(); err != nil {
		return false, err
	}
	want := configFromSpec(spec)
	if err := c.request(ctx, http.MethodPost, queueCollectionPath(spec.MessageVPN), want, nil); err != nil {
		// Apart from an authoritative ALREADY_EXISTS response, a write error may
		// occur after the broker committed the POST. Mark it cleanup-owned so the
		// caller attempts exact-name deletion rather than orphaning a queue.
		return !isAlreadyExists(err), fmt.Errorf("semp: create exclusive managed queue %q: %w", spec.Name, err)
	}
	match, err := c.queueMatches(ctx, queuePath("config", spec.MessageVPN, spec.Name), want)
	if err != nil {
		return true, fmt.Errorf("semp: verify exclusively created queue %q: %w", spec.Name, err)
	}
	return true, compatibleQueue(spec.Name, match)
}

// EnsureQueue creates a managed queue if absent. An existing queue is adopted
// only when its entire managed configuration equals spec. Races with another
// creator are resolved by fetching and comparing the resulting queue.
func (c *Client) EnsureQueue(ctx context.Context, spec QueueSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	want := configFromSpec(spec)
	path := queuePath("config", spec.MessageVPN, spec.Name)
	match, err := c.queueMatches(ctx, path, want)
	if err == nil {
		return compatibleQueue(spec.Name, match)
	}
	if !isNotFound(err) {
		return err
	}

	err = c.request(ctx, http.MethodPost, queueCollectionPath(spec.MessageVPN), want, nil)
	if err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("semp: create managed queue %q: %w", spec.Name, err)
	}
	match, err = c.queueMatches(ctx, path, want)
	if err != nil {
		return fmt.Errorf("semp: verify managed queue %q: %w", spec.Name, err)
	}
	return compatibleQueue(spec.Name, match)
}

func compatibleQueue(name string, match bool) error {
	if match {
		return nil
	}
	return fmt.Errorf("semp: managed queue %q exists with incompatible configuration", name)
}

func exactManagedConfig[T any](data json.RawMessage, want T) (bool, error) {
	var got T
	if err := json.Unmarshal(data, &got); err != nil {
		return false, err
	}
	wantBytes, err := json.Marshal(want)
	if err != nil {
		return false, err
	}
	var expected map[string]json.RawMessage
	var actual map[string]json.RawMessage
	if err := json.Unmarshal(wantBytes, &expected); err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, &actual); err != nil {
		return false, err
	}
	for field, expectedValue := range expected {
		actualValue, exists := actual[field]
		if !exists || !reflect.DeepEqual(expectedValue, actualValue) {
			return false, nil
		}
	}
	return reflect.DeepEqual(want, got), nil
}

// CreateSubscription creates an exact topic subscription on a managed queue.
// Creating an existing subscription is idempotent.
func (c *Client) CreateSubscription(ctx context.Context, messageVPN, queueName, topic string) error {
	if messageVPN == "" || queueName == "" || topic == "" {
		return errors.New("semp: message VPN, queue name, and subscription topic are required")
	}
	body := struct {
		Topic string `json:"subscriptionTopic"`
	}{Topic: topic}
	path := queuePath("config", messageVPN, queueName) + pathSegments("subscriptions")
	if err := c.request(ctx, http.MethodPost, path, body, nil); err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("semp: create subscription on queue %q: %w", queueName, err)
	}
	return nil
}

// FenceQueue disables ingress and verifies that the broker committed both the
// disabled state and mandatory reject-on-discard behavior.
func (c *Client) FenceQueue(ctx context.Context, messageVPN, queueName string) error {
	return c.setQueueIngress(ctx, messageVPN, queueName, false)
}

// UnfenceQueue enables ingress and verifies that the broker committed both the
// enabled state and mandatory reject-on-discard behavior.
func (c *Client) UnfenceQueue(ctx context.Context, messageVPN, queueName string) error {
	return c.setQueueIngress(ctx, messageVPN, queueName, true)
}

func (c *Client) setQueueIngress(ctx context.Context, messageVPN, queueName string, enabled bool) error {
	if messageVPN == "" || queueName == "" {
		return errors.New("semp: message VPN and queue name are required")
	}
	body := struct {
		IngressEnabled bool   `json:"ingressEnabled"`
		RejectBehavior string `json:"rejectMsgToSenderOnDiscardBehavior"`
	}{IngressEnabled: enabled, RejectBehavior: rejectAlways}
	path := queuePath("config", messageVPN, queueName)
	if err := c.request(ctx, http.MethodPatch, path, body, nil); err != nil {
		return fmt.Errorf("semp: update ingress for queue %q: %w", queueName, err)
	}
	data, err := c.getQueue(ctx, messageVPN, queueName)
	if err != nil {
		return fmt.Errorf("semp: verify ingress for queue %q: %w", queueName, err)
	}
	var got struct {
		IngressEnabled *bool  `json:"ingressEnabled"`
		RejectBehavior string `json:"rejectMsgToSenderOnDiscardBehavior"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		return fmt.Errorf("semp: verify ingress for queue %q: %w", queueName, err)
	}
	if got.IngressEnabled == nil || *got.IngressEnabled != enabled || got.RejectBehavior != rejectAlways {
		return fmt.Errorf("semp: broker did not verify ingress state for queue %q", queueName)
	}
	return nil
}

type requiredUint64 struct {
	value uint64
	set   bool
	valid bool
}

func (counter *requiredUint64) UnmarshalJSON(data []byte) error {
	counter.set = true
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return nil
	}
	value, err := parseCounter(number)
	if err == nil {
		counter.value = value
		counter.valid = true
	}
	return nil
}

func parseCounter(number json.Number) (uint64, error) {
	text := number.String()
	if text == "" {
		return 0, errors.New("empty counter")
	}
	for _, character := range text {
		if character < '0' || character > '9' {
			return 0, errors.New("not an unsigned integer")
		}
	}
	var value uint64
	for _, character := range text {
		digit := uint64(character - '0')
		if value > (^uint64(0)-digit)/10 {
			return 0, errors.New("counter overflow")
		}
		value = value*10 + digit
	}
	return value, nil
}

func requiredCounter(resourceName, field string, counter requiredUint64) (uint64, error) {
	if !counter.set || !counter.valid {
		return 0, fmt.Errorf("semp: resource %q monitor response has missing or invalid %s", resourceName, field)
	}
	return counter.value, nil
}

type drainMonitorEnvelope struct {
	Collections struct {
		Messages struct {
			Count requiredUint64 `json:"count"`
		} `json:"msgs"`
	} `json:"collections"`
	Data struct {
		Name                  string         `json:"queueName"`
		MessageVPN            string         `json:"msgVpnName"`
		SpoolUsageBytes       requiredUint64 `json:"msgSpoolUsage"`
		UnackedMessages       requiredUint64 `json:"txUnackedMsgCount"`
		InProgressAckMessages requiredUint64 `json:"inProgressAckMsgCount"`
	} `json:"data"`
}

const drainMonitorSelect = "queueName,msgVpnName,msgSpoolUsage,txUnackedMsgCount,inProgressAckMsgCount"

// MonitorDrain requests and parses only the Cloud-available point-in-time queue
// fields used to prove drain. Message spool usage is retained for diagnostics;
// message count and acknowledgement state are the authoritative zero predicate.
func (c *Client) MonitorDrain(ctx context.Context, messageVPN, queueName string) (QueueStatus, error) {
	if messageVPN == "" || queueName == "" {
		return QueueStatus{}, errors.New("semp: message VPN and queue name are required")
	}
	path := queuePath("monitor", messageVPN, queueName) + "?" + url.Values{"select": {drainMonitorSelect}}.Encode()
	var envelope drainMonitorEnvelope
	if err := c.request(ctx, http.MethodGet, path, nil, &envelope); err != nil {
		return QueueStatus{}, err
	}
	return parseDrainStatus(messageVPN, queueName, envelope)
}

func parseDrainStatus(messageVPN, queueName string, envelope drainMonitorEnvelope) (QueueStatus, error) {
	if envelope.Data.Name != queueName || envelope.Data.MessageVPN != messageVPN {
		return QueueStatus{}, fmt.Errorf("semp: monitor response identity does not match queue %q", queueName)
	}
	queued, err := requiredCounter(queueName, "collections.msgs.count", envelope.Collections.Messages.Count)
	if err != nil {
		return QueueStatus{}, err
	}
	spoolUsage, err := requiredCounter(queueName, "msgSpoolUsage", envelope.Data.SpoolUsageBytes)
	if err != nil {
		return QueueStatus{}, err
	}
	unacked, err := requiredCounter(queueName, "txUnackedMsgCount", envelope.Data.UnackedMessages)
	if err != nil {
		return QueueStatus{}, err
	}
	inProgress, err := requiredCounter(queueName, "inProgressAckMsgCount", envelope.Data.InProgressAckMessages)
	if err != nil {
		return QueueStatus{}, err
	}
	return QueueStatus{
		MessageVPN: messageVPN, Name: queueName, SpooledMessages: queued,
		SpoolUsageBytes: spoolUsage, UnackedMessages: unacked, InProgressAckMessages: inProgress,
	}, nil
}

// MonitorQueue reads queue counters and paginates through every transmit flow.
// Consumers are distinct client/session pairs, while Flows is the complete flow
// count. Unknown counters are errors, never inferred zeros.
func (c *Client) MonitorQueue(ctx context.Context, messageVPN, queueName string) (QueueStatus, error) {
	if messageVPN == "" || queueName == "" {
		return QueueStatus{}, errors.New("semp: message VPN and queue name are required")
	}
	var envelope struct {
		Data struct {
			Name                  string         `json:"queueName"`
			MessageVPN            string         `json:"msgVpnName"`
			IngressBytesPerSecond requiredUint64 `json:"rxByteRate"`
			EgressBytesPerSecond  requiredUint64 `json:"txByteRate"`
			SpooledMessages       requiredUint64 `json:"spooledMsgCount"`
			SpoolUsageBytes       requiredUint64 `json:"msgSpoolUsage"`
			UnackedMessages       requiredUint64 `json:"txUnackedMsgCount"`
			InProgressAckMessages requiredUint64 `json:"inProgressAckMsgCount"`
			IngressEnabled        *bool          `json:"ingressEnabled"`
		} `json:"data"`
	}
	path := queuePath("monitor", messageVPN, queueName)
	if err := c.request(ctx, http.MethodGet, path, nil, &envelope); err != nil {
		return QueueStatus{}, err
	}
	if envelope.Data.Name != queueName || envelope.Data.MessageVPN != messageVPN {
		return QueueStatus{}, fmt.Errorf("semp: monitor response identity does not match queue %q", queueName)
	}
	ingress, err := requiredCounter(queueName, "rxByteRate", envelope.Data.IngressBytesPerSecond)
	if err != nil {
		return QueueStatus{}, err
	}
	egress, err := requiredCounter(queueName, "txByteRate", envelope.Data.EgressBytesPerSecond)
	if err != nil {
		return QueueStatus{}, err
	}
	spooled, err := requiredCounter(queueName, "spooledMsgCount", envelope.Data.SpooledMessages)
	if err != nil {
		return QueueStatus{}, err
	}
	spoolUsage, err := requiredCounter(queueName, "msgSpoolUsage", envelope.Data.SpoolUsageBytes)
	if err != nil {
		return QueueStatus{}, err
	}
	unacked, err := requiredCounter(queueName, "txUnackedMsgCount", envelope.Data.UnackedMessages)
	if err != nil {
		return QueueStatus{}, err
	}
	inProgress, err := requiredCounter(queueName, "inProgressAckMsgCount", envelope.Data.InProgressAckMessages)
	if err != nil {
		return QueueStatus{}, err
	}
	if envelope.Data.IngressEnabled == nil {
		return QueueStatus{}, fmt.Errorf("semp: queue %q monitor response has missing ingressEnabled", queueName)
	}

	status := QueueStatus{
		MessageVPN: messageVPN, Name: queueName,
		IngressBytesPerSecond: ingress, EgressBytesPerSecond: egress, ThroughputKnown: true,
		SpooledMessages: spooled, SpoolUsageBytes: spoolUsage,
		UnackedMessages: unacked, InProgressAckMessages: inProgress, IngressEnabled: *envelope.Data.IngressEnabled,
	}
	type flow struct {
		MessageVPN  string      `json:"msgVpnName"`
		QueueName   string      `json:"queueName"`
		ClientName  string      `json:"clientName"`
		SessionName string      `json:"sessionName"`
		FlowID      json.Number `json:"flowId"`
	}
	flows, err := list[flow](ctx, c, path+pathSegments("txFlows"))
	if err != nil {
		return QueueStatus{}, fmt.Errorf("semp: list flows for queue %q: %w", queueName, err)
	}
	consumers := make(map[string]struct{}, len(flows))
	for index, flow := range flows {
		if flow.MessageVPN != messageVPN || flow.QueueName != queueName || flow.ClientName == "" || flow.FlowID == "" {
			return QueueStatus{}, fmt.Errorf("semp: queue %q flow %d has missing or mismatched identity", queueName, index)
		}
		if _, err := parseCounter(flow.FlowID); err != nil {
			return QueueStatus{}, fmt.Errorf("semp: queue %q flow %d has invalid identity", queueName, index)
		}
		// Some broker versions omit sessionName. The flow ID remains mandatory and
		// disambiguates multiple flows owned by one client when session identity is absent.
		identity := flow.ClientName + "\x00" + flow.SessionName
		if flow.SessionName == "" {
			identity += "\x00" + flow.FlowID.String()
		}
		consumers[identity] = struct{}{}
	}
	status.Flows = uint64(len(flows))
	status.Consumers = uint64(len(consumers))
	return status, nil
}

// DeleteQueue deletes exactly one named queue; no prefix or wildcard deletion
// API is provided. Deleting an already absent queue is idempotent.
func (c *Client) DeleteQueue(ctx context.Context, messageVPN, queueName string) error {
	if messageVPN == "" || queueName == "" {
		return errors.New("semp: message VPN and exact queue name are required")
	}
	if err := c.request(ctx, http.MethodDelete, queuePath("config", messageVPN, queueName), nil, nil); err != nil && !isNotFound(err) {
		return fmt.Errorf("semp: delete queue %q: %w", queueName, err)
	}
	return nil
}
