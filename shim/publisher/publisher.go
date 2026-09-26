// Package publisher implements the durable publisher-side shim.
//
// Accept only commits to the local outbox. Broker publication is a separate
// Dispatch operation, so an application never mistakes durable local acceptance
// for a broker ACK.
package publisher

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/outbox"
	"github.com/solacese/solace-workload-balancer/routing"
)

const (
	PropertyEventID        = "swlb.event_id"
	PropertyScalingGroup   = "swlb.group"
	PropertyBusinessHash   = "swlb.business_hash"
	PropertyHashContract   = "swlb.hash_contract"
	PropertyLibraryVersion = "swlb.library_version"
	PropertyEpoch          = "swlb.routing_epoch"
	PropertyQueuePartition = "JMSXGroupID"
)

var (
	ErrNoValidMembership = errors.New("publisher has no valid membership for scaling group")
	ErrStaleMembership   = errors.New("publisher membership revision is not newer")
	ErrInvalidConfig     = errors.New("invalid publisher configuration")
	ErrPublishRejected   = errors.New("broker definitively rejected publish")
	ErrPublishRetryable  = errors.New("retryable immediate publish failure")
	ErrAckUncertain      = errors.New("broker ACK is uncertain; record retained and retry may duplicate")
	ErrReservedProperty  = errors.New("publisher: application header uses reserved routing name")
)

// PublishOutcome distinguishes a positive ACK, a definitive pre-acceptance
// rejection, and an ambiguous result where the broker may have accepted the
// message. Errors and unknown outcomes are treated as ambiguous unless the
// implementation explicitly returns OutcomeRejected.
type PublishOutcome uint8

const (
	OutcomeUnknown PublishOutcome = iota
	OutcomeAcknowledged
	OutcomeRejected
)

// BrokerMessage is the immutable message passed directly to the selected
// broker. Properties includes the shim routing metadata constants above.
type BrokerMessage struct {
	EventID     string
	Payload     []byte
	Topic       string
	Properties  map[string]string
	Destination string
	Epoch       uint64
}

// BrokerPublisher publishes directly to the broker named by brokerID. It must
// return OutcomeAcknowledged only after a positive broker ACK, and
// OutcomeRejected only when it knows the broker did not accept the message.
// Everything else must be OutcomeUnknown.
type BrokerPublisher interface {
	Publish(ctx context.Context, brokerID string, message BrokerMessage) (PublishOutcome, error)
}

// PayloadEncoder serializes the application-owned payload after the customer
// library has inspected its borrowed MessageView.
type PayloadEncoder func(payload any) ([]byte, error)

// Contract identifies the routing metadata emitted for one scaling group.
type Contract struct {
	HashContract   string
	LibraryVersion string
}

// Config defines immutable publisher dependencies and routing metadata.
type Config struct {
	Outbox          *outbox.Store
	CustomerLibrary customer.CustomerLibrary
	Broker          BrokerPublisher
	// Contracts maps each scaling group to its independently versioned routing
	// contract. ScalingGroup, HashContract, and LibraryVersion are an explicit
	// single-group shorthand and cannot be combined with Contracts.
	Contracts      map[string]Contract
	ScalingGroup   string
	HashContract   string
	LibraryVersion string
	EncodePayload  PayloadEncoder
}

// Receipt confirms durable local acceptance. It does not represent a broker
// ACK. State is Ready for active/preparing membership and Unassigned while
// paused.
type Receipt struct {
	EventID string
	State   outbox.State
	Group   string
	Hash    string
	Epoch   uint64
	Broker  string
}

// DispatchReport summarizes one pass over the per-key eligible records.
type DispatchReport struct {
	Attempted    int
	Acknowledged int
	Rejected     int
	Uncertain    int
}

// Publisher is safe for concurrent use. Membership is intentionally in memory:
// after restart the shim fails closed until the control plane supplies and
// validates a fresh snapshot. Accepted records remain durable in the outbox.
type Publisher struct {
	mu         sync.RWMutex
	outbox     *outbox.Store
	customer   customer.CustomerLibrary
	broker     BrokerPublisher
	contracts  map[string]Contract
	encode     PayloadEncoder
	membership map[string]control.MembershipSnapshot
}

// New constructs a publisher shim.
func New(config Config) (*Publisher, error) {
	if config.Outbox == nil || config.CustomerLibrary == nil || config.Broker == nil {
		return nil, fmt.Errorf("%w: outbox, customer library, and broker are required", ErrInvalidConfig)
	}
	contracts := make(map[string]Contract, len(config.Contracts)+1)
	for group, contract := range config.Contracts {
		contracts[group] = contract
	}
	shorthandConfigured := config.ScalingGroup != "" || config.HashContract != "" || config.LibraryVersion != ""
	if shorthandConfigured {
		if len(config.Contracts) != 0 || config.ScalingGroup == "" || config.HashContract == "" || config.LibraryVersion == "" {
			return nil, fmt.Errorf("%w: single-group shorthand requires scaling group, hash contract, and library version and cannot be combined with contracts", ErrInvalidConfig)
		}
		contracts[config.ScalingGroup] = Contract{HashContract: config.HashContract, LibraryVersion: config.LibraryVersion}
	}
	if len(contracts) == 0 {
		return nil, fmt.Errorf("%w: at least one group contract is required", ErrInvalidConfig)
	}
	for group, contract := range contracts {
		if group == "" || contract.HashContract == "" || contract.LibraryVersion == "" {
			return nil, fmt.Errorf("%w: contract map cannot contain an empty group, hash contract, or library version", ErrInvalidConfig)
		}
	}
	if config.EncodePayload == nil {
		config.EncodePayload = defaultEncodePayload
	}
	return &Publisher{
		outbox:     config.Outbox,
		customer:   config.CustomerLibrary,
		broker:     config.Broker,
		contracts:  contracts,
		encode:     config.EncodePayload,
		membership: make(map[string]control.MembershipSnapshot),
	}, nil
}

// ApplyMembership validates and installs a newer control snapshot. ACTIVE
// activation assigns or reassigns all buffered retryable records for the
// scaling group in acceptance order. Outbox FIFO eligibility ensures a new
// record cannot overtake an earlier paused record with the same key, including
// if assignment is interrupted partway through.
func (p *Publisher) ApplyMembership(snapshot control.MembershipSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("apply membership: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	contract, configured := p.contracts[snapshot.ScalingGroup]
	if !configured || snapshot.HashContract != contract.HashContract {
		return fmt.Errorf("apply membership: hash contract %q for group %q does not match configured contract %q", snapshot.HashContract, snapshot.ScalingGroup, contract.HashContract)
	}
	if snapshot.LibraryVersion != contract.LibraryVersion {
		return fmt.Errorf("apply membership: library version %q for group %q does not match configured version %q", snapshot.LibraryVersion, snapshot.ScalingGroup, contract.LibraryVersion)
	}
	if current, ok := p.membership[snapshot.ScalingGroup]; ok && snapshot.Revision <= current.Revision {
		if snapshot.Revision == current.Revision && reflect.DeepEqual(snapshot, current) {
			return nil
		}
		return fmt.Errorf("apply membership for %q revision %d: %w (current %d)", snapshot.ScalingGroup, snapshot.Revision, ErrStaleMembership, current.Revision)
	}

	snapshot = snapshot.Clone()
	if snapshot.Phase != control.PhaseActive {
		p.membership[snapshot.ScalingGroup] = snapshot
		return nil
	}

	assignments := make([]outbox.AssignmentUpdate, 0)
	var assignmentErrors []error
	for _, record := range p.outbox.Records() {
		if record.Group != snapshot.ScalingGroup || (record.State != outbox.StateUnassigned && record.State != outbox.StateReady) {
			continue
		}
		if record.HashContract != contract.HashContract || record.LibraryVersion != contract.LibraryVersion {
			assignmentErrors = append(assignmentErrors, fmt.Errorf("assign buffered event %q: routing metadata mismatch", record.EventID))
			continue
		}
		assignment, err := assignmentFor(record.Hash, snapshot)
		if err != nil {
			assignmentErrors = append(assignmentErrors, fmt.Errorf("assign buffered event %q: %w", record.EventID, err))
			continue
		}
		assignments = append(assignments, outbox.AssignmentUpdate{EventID: record.EventID, Assignment: assignment})
	}
	if len(assignmentErrors) == 0 && len(assignments) != 0 {
		assignmentErrors = append(assignmentErrors, p.outbox.AssignBatch(assignments))
	}
	if err := errors.Join(assignmentErrors...); err != nil {
		// Do not expose the new ACTIVE membership to Accept or Dispatch until all
		// buffered retryable records have durable routes for that epoch.
		return err
	}
	p.membership[snapshot.ScalingGroup] = snapshot
	return nil
}

// Accept validates routing data and durably records one application message.
// It never invokes the broker. Before a valid snapshot exists for the computed
// scaling group it fails closed and writes nothing. During PAUSED it accepts an
// unassigned record, which activation will route using the new membership.
func (p *Publisher) Accept(message customer.MessageView) (Receipt, error) {
	receipts, err := p.AcceptBatch([]customer.MessageView{message})
	if err != nil {
		return Receipt{}, err
	}
	return receipts[0], nil
}

// AcceptBatch validates and atomically commits a batch in input order. No
// record is written unless every message can be encoded and routed under the
// same stable view of membership. Like Accept, success confirms local durable
// acceptance only and never a broker acknowledgement.
func (p *Publisher) AcceptBatch(messages []customer.MessageView) ([]Receipt, error) {
	if len(messages) == 0 {
		return []Receipt{}, nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	records := make([]outbox.Record, len(messages))
	for i, message := range messages {
		record, err := p.recordFor(message)
		if err != nil {
			return nil, fmt.Errorf("accept batch record %d: %w", i, err)
		}
		records[i] = record
	}
	accepted, err := p.outbox.AcceptBatch(records)
	if err != nil {
		return nil, err
	}
	receipts := make([]Receipt, len(accepted))
	for i, record := range accepted {
		receipts[i] = receiptFor(record)
	}
	return receipts, nil
}

// recordFor builds one outbox record while the caller holds p.mu for reading.
func (p *Publisher) recordFor(message customer.MessageView) (outbox.Record, error) {
	if message.EventID == "" || message.Topic == "" {
		return outbox.Record{}, fmt.Errorf("accept message: event ID and topic are required")
	}
	for key := range message.Headers {
		if strings.HasPrefix(key, "swlb.") || key == PropertyQueuePartition {
			return outbox.Record{}, fmt.Errorf("accept %q: %w %q", message.EventID, ErrReservedProperty, key)
		}
	}
	group, err := p.customer.GetScalingGroup(message)
	if err != nil {
		return outbox.Record{}, fmt.Errorf("accept %q: get scaling group: %w", message.EventID, err)
	}
	if group == "" {
		return outbox.Record{}, fmt.Errorf("accept %q: customer library returned empty scaling group", message.EventID)
	}
	snapshot, ok := p.membership[group]
	if !ok {
		return outbox.Record{}, fmt.Errorf("accept %q: %w %q", message.EventID, ErrNoValidMembership, group)
	}
	contract, configured := p.contracts[group]
	if !configured || snapshot.HashContract != contract.HashContract || snapshot.LibraryVersion != contract.LibraryVersion {
		return outbox.Record{}, fmt.Errorf("accept %q: incompatible routing contract for group %q", message.EventID, group)
	}
	hash, err := p.customer.GetBusinessHash(message)
	if err != nil {
		return outbox.Record{}, fmt.Errorf("accept %q: get business hash: %w", message.EventID, err)
	}
	payload, err := p.encode(message.Payload)
	if err != nil {
		return outbox.Record{}, fmt.Errorf("accept %q: encode payload: %w", message.EventID, err)
	}
	hashString := hex.EncodeToString(hash[:])
	record := outbox.Record{
		EventID:        message.EventID,
		Payload:        payload,
		Topic:          message.Topic,
		Properties:     cloneProperties(message.Headers),
		OrderingKey:    group + "\x00" + hashString,
		Group:          group,
		Hash:           hashString,
		HashContract:   contract.HashContract,
		LibraryVersion: contract.LibraryVersion,
		State:          outbox.StateUnassigned,
	}
	if snapshot.Phase == control.PhaseActive || snapshot.Phase == control.PhasePrepare {
		assignment, err := assignmentFor(hashString, snapshot)
		if err != nil {
			return outbox.Record{}, fmt.Errorf("accept %q: assign route: %w", message.EventID, err)
		}
		record.State = outbox.StateReady
		record.Epoch = assignment.Epoch
		record.Broker = assignment.Broker
		record.Destination = assignment.Destination
	}
	return record, nil
}

func receiptFor(record outbox.Record) Receipt {
	return Receipt{EventID: record.EventID, State: record.State, Group: record.Group, Hash: record.Hash, Epoch: record.Epoch, Broker: record.Broker}
}

// Dispatch attempts at most one eligible record per ordering key, up to limit.
// A failed or uncertain key does not prevent independent keys selected in the
// same pass from progressing. Definitive rejection stays ready for a later
// retry; ambiguous outcomes remain AckUncertain until an operator explicitly
// calls RetryAckUncertain or supplies an authoritative late ACK via Ack.
func (p *Publisher) Dispatch(ctx context.Context, limit int) (DispatchReport, error) {
	var report DispatchReport
	if limit <= 0 {
		return report, nil
	}
	p.mu.RLock()
	activeEpochs := make(map[string]uint64, len(p.membership))
	for group, snapshot := range p.membership {
		if snapshot.Phase == control.PhaseActive || snapshot.Phase == control.PhasePrepare {
			activeEpochs[group] = snapshot.Epoch
		}
	}
	records := p.outbox.EligibleFor(limit, activeEpochs, nil)
	p.mu.RUnlock()

	var dispatchErrors []error
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			dispatchErrors = append(dispatchErrors, err)
			break
		}

		// Keep membership stable from the final admission check through the
		// broker call. ApplyMembership therefore cannot return a PAUSED snapshot
		// while a newly admitted publish is still entering the old membership.
		p.mu.RLock()
		snapshot, ok := p.membership[record.Group]
		if !ok || (snapshot.Phase != control.PhaseActive && snapshot.Phase != control.PhasePrepare) || snapshot.Epoch != record.Epoch {
			p.mu.RUnlock()
			continue
		}
		if err := p.outbox.MarkInFlight(record.EventID); err != nil {
			p.mu.RUnlock()
			dispatchErrors = append(dispatchErrors, err)
			continue
		}
		report.Attempted++
		message := brokerMessage(record)
		outcome, publishErr := p.broker.Publish(ctx, record.Broker, message)
		p.mu.RUnlock()
		switch {
		case outcome == OutcomeAcknowledged && publishErr == nil:
			if err := p.outbox.Ack(record.EventID, record.Attempts+1, record.Epoch); err != nil {
				// The broker ACK happened but durable deletion failed. The record
				// remains in-flight and recovery will conservatively make it
				// uncertain rather than risking a silent drop.
				dispatchErrors = append(dispatchErrors, fmt.Errorf("persist ACK for %q: %w", record.EventID, err))
				continue
			}
			report.Acknowledged++
		case outcome == OutcomeRejected:
			reason := errorText(publishErr, ErrPublishRejected)
			if err := p.outbox.MarkRejected(record.EventID, reason); err != nil {
				dispatchErrors = append(dispatchErrors, errors.Join(publishErr, err))
				continue
			}
			report.Rejected++
			dispatchErrors = append(dispatchErrors, fmt.Errorf("publish %q: %w: %s", record.EventID, ErrPublishRejected, reason))
		default:
			reason := errorText(publishErr, ErrAckUncertain)
			if err := p.outbox.MarkAckUncertain(record.EventID, reason); err != nil {
				dispatchErrors = append(dispatchErrors, errors.Join(publishErr, err))
				continue
			}
			report.Uncertain++
			dispatchErrors = append(dispatchErrors, fmt.Errorf("publish %q: %w: %s", record.EventID, ErrAckUncertain, reason))
		}
	}
	return report, errors.Join(dispatchErrors...)
}

// RetryAckUncertain explicitly permits retry of an ambiguously ACKed record.
// Duplicate delivery is possible; EventID is attached for deduplication.
func (p *Publisher) RetryAckUncertain(eventID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	record, err := p.outbox.Get(eventID)
	if err != nil {
		return err
	}
	snapshot, ok := p.membership[record.Group]
	if !ok || snapshot.Phase != control.PhaseActive {
		return fmt.Errorf("retry %q: %w %q", eventID, ErrNoValidMembership, record.Group)
	}
	if err := p.outbox.RetryAckUncertain(eventID); err != nil {
		return err
	}
	assignment, err := assignmentFor(record.Hash, snapshot)
	if err != nil {
		return err
	}
	return p.outbox.Assign(eventID, assignment)
}

// Ack resolves an exact retained uncertain attempt after an authoritative late
// broker ACK. Attempt and epoch scope prevent an old ACK deleting a newer retry.
func (p *Publisher) Ack(eventID string, attempts, epoch uint64) error {
	return p.outbox.Ack(eventID, attempts, epoch)
}

// Lookup returns the current durable routing state without exposing payload data.
func (p *Publisher) Lookup(eventID string) (Receipt, error) {
	record, err := p.outbox.Get(eventID)
	if err != nil {
		return Receipt{}, err
	}
	return receiptFor(record), nil
}

func assignmentFor(hashString string, snapshot control.MembershipSnapshot) (outbox.Assignment, error) {
	digest, err := routing.ParseSHA256(hashString)
	if err != nil {
		return outbox.Assignment{}, err
	}
	// Broker selection is deliberately delegated only to routing. Membership
	// order is passed through exactly as supplied by the control snapshot.
	broker, err := routing.BrokerForHash(digest, snapshot.CurrentMembership)
	if err != nil {
		return outbox.Assignment{}, err
	}
	destination := snapshot.Destination.Name
	if len(snapshot.CurrentResources) != 0 {
		matched := false
		for _, identity := range snapshot.CurrentResources {
			if identity.ConsumerSet != broker {
				continue
			}
			if matched {
				return outbox.Assignment{}, fmt.Errorf("routing resources contain duplicate broker %q", broker)
			}
			matched = true
			destination = strings.TrimSuffix(identity.IngressTopic, ">") + "events"
		}
		if !matched {
			return outbox.Assignment{}, fmt.Errorf("routing resources do not contain selected broker %q", broker)
		}
	}
	return outbox.Assignment{Epoch: snapshot.Epoch, Broker: broker, Destination: destination}, nil
}

func brokerMessage(record outbox.Record) BrokerMessage {
	properties := cloneProperties(record.Properties)
	if properties == nil {
		properties = make(map[string]string, 6)
	}
	properties[PropertyEventID] = record.EventID
	properties[PropertyScalingGroup] = record.Group
	properties[PropertyBusinessHash] = record.Hash
	properties[PropertyHashContract] = record.HashContract
	properties[PropertyLibraryVersion] = record.LibraryVersion
	properties[PropertyEpoch] = strconv.FormatUint(record.Epoch, 10)
	return BrokerMessage{
		EventID: record.EventID, Payload: append([]byte(nil), record.Payload...), Topic: record.Topic,
		Properties: properties, Destination: record.Destination, Epoch: record.Epoch,
	}
}

func defaultEncodePayload(payload any) ([]byte, error) {
	switch value := payload.(type) {
	case []byte:
		return append([]byte(nil), value...), nil
	case string:
		return []byte(value), nil
	default:
		return json.Marshal(value)
	}
}

func errorText(err error, fallback error) string {
	if err != nil {
		return err.Error()
	}
	return fallback.Error()
}

func cloneProperties(properties map[string]string) map[string]string {
	if properties == nil {
		return nil
	}
	cloned := make(map[string]string, len(properties))
	for key, value := range properties {
		cloned[key] = value
	}
	return cloned
}
