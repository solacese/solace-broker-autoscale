package integration

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

const (
	PropertyScalingGroup   = "swlb.group"
	PropertyBusinessHash   = "swlb.business_hash"
	PropertyRoutingEpoch   = "swlb.routing_epoch"
	PropertyEventID        = "swlb.event_id"
	PropertyOriginalTopic  = "swlb.original_topic"
	PropertyHashContract   = "swlb.hash_contract"
	PropertyLibraryVersion = "swlb.library_version"
)

func TopicAddress(name string) string {
	if strings.HasPrefix(name, "topic://") {
		return name
	}
	return "topic://" + name
}
func QueueAddress(name string) string {
	if strings.HasPrefix(name, "queue://") {
		return name
	}
	return "queue://" + name
}
func RawQueueAddress(address string) string { return strings.TrimPrefix(address, "queue://") }

// AMQPConnection is the transport owned by one logical broker. The Azure client
// does not reconnect links automatically; callers recreate the connection and
// links after an error, which also triggers a fresh control snapshot request.
type AMQPConnection struct {
	Conn    *amqp.Conn
	Session *amqp.Session
}

func ConnectAMQP(ctx context.Context, endpoint, username, password, applicationID string, tlsConfig *tls.Config) (*AMQPConnection, error) {
	if endpoint == "" || username == "" || applicationID == "" {
		return nil, errors.New("integration: AMQP endpoint, username, and application ID are required")
	}
	conn, err := amqp.Dial(ctx, endpoint, &amqp.ConnOptions{
		ContainerID: applicationID, SASLType: amqp.SASLTypePlain(username, password), TLSConfig: tlsConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("integration: connect AMQP: %w", err)
	}
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("integration: open AMQP session: %w", err)
	}
	return &AMQPConnection{Conn: conn, Session: session}, nil
}

func (c *AMQPConnection) Close() error {
	if c == nil {
		return nil
	}
	var err error
	if c.Session != nil {
		ctx, cancel := context.WithTimeout(context.Background(), amqpCloseTimeout)
		err = c.Session.Close(ctx)
		cancel()
	}
	if c.Conn != nil {
		err = errors.Join(err, c.Conn.Close())
	}
	return err
}

// AMQPRequestClient uses a pre-provisioned participant-exclusive reply queue.
// The receiver is attached before the request is sent, closing the response race.
type AMQPRequestClient struct {
	Session        *amqp.Session
	RequestAddress string
	ReplyAddress   string
}

type snapshotReplyAction uint8

const (
	snapshotReplyAccept snapshotReplyAction = iota + 1
	snapshotReplyDiscard
	snapshotReplyReject
)

func inspectSnapshotReply(request control.SnapshotRequest, message *amqp.Message) (control.SnapshotResponse, snapshotReplyAction, error) {
	correlation, ok := amqpStringID(message.Properties, false)
	if !ok || correlation != request.CorrelationID {
		return control.SnapshotResponse{}, snapshotReplyDiscard, nil
	}
	response, err := control.ParseSnapshotResponse(message.GetData())
	if err != nil {
		return control.SnapshotResponse{}, snapshotReplyReject, err
	}
	if err := response.ValidateFor(request); err != nil {
		return control.SnapshotResponse{}, snapshotReplyReject, err
	}
	return response, snapshotReplyAccept, nil
}

func (c AMQPRequestClient) RequestSnapshot(ctx context.Context, request control.SnapshotRequest) (control.SnapshotResponse, error) {
	if c.Session == nil || c.RequestAddress == "" || c.ReplyAddress == "" {
		return control.SnapshotResponse{}, errors.New("integration: AMQP snapshot client is incomplete")
	}
	if err := request.Validate(); err != nil {
		return control.SnapshotResponse{}, err
	}
	receiver, err := c.Session.NewReceiver(ctx, QueueAddress(c.ReplyAddress), &amqp.ReceiverOptions{Credit: 1})
	if err != nil {
		return control.SnapshotResponse{}, fmt.Errorf("integration: attach AMQP snapshot reply receiver: %w", err)
	}
	defer closeAMQPReceiver(receiver)
	sender, err := c.Session.NewSender(ctx, TopicAddress(c.RequestAddress), nil)
	if err != nil {
		return control.SnapshotResponse{}, fmt.Errorf("integration: attach AMQP snapshot request sender: %w", err)
	}
	defer closeAMQPSender(sender)
	payload, err := marshalAMQPJSON(request)
	if err != nil {
		return control.SnapshotResponse{}, err
	}
	message := amqp.NewMessage(payload)
	message.Header = &amqp.MessageHeader{Durable: true}
	message.Properties = &amqp.MessageProperties{MessageID: request.CorrelationID, CorrelationID: request.CorrelationID, ReplyTo: pointer(QueueAddress(c.ReplyAddress)), ContentType: pointer("application/json")}
	if err := sender.Send(ctx, message, nil); err != nil {
		return control.SnapshotResponse{}, fmt.Errorf("integration: send AMQP snapshot request: %w", err)
	}
	for {
		reply, err := receiver.Receive(ctx, nil)
		if err != nil {
			return control.SnapshotResponse{}, fmt.Errorf("integration: receive AMQP snapshot response: %w", err)
		}
		correlation, ok := amqpStringID(reply.Properties, false)
		if !ok || correlation != request.CorrelationID {
			// This participant-exclusive queue can contain a late response from a
			// timed-out attempt. Accepting that stale response removes it permanently;
			// releasing it would redeliver forever and starve the current request.
			if discardErr := receiver.AcceptMessage(ctx, reply); discardErr != nil {
				return control.SnapshotResponse{}, fmt.Errorf("integration: discard stale AMQP snapshot response: %w", discardErr)
			}
			continue
		}
		response, parseErr := control.ParseSnapshotResponse(reply.GetData())
		if parseErr != nil || response.ValidateFor(request) != nil {
			_ = receiver.RejectMessage(ctx, reply, nil)
			return control.SnapshotResponse{}, errors.Join(parseErr, response.ValidateFor(request))
		}
		if err := receiver.AcceptMessage(ctx, reply); err != nil {
			return control.SnapshotResponse{}, fmt.Errorf("integration: accept AMQP snapshot response: %w", err)
		}
		return response, nil
	}
}

// AMQPSnapshotResponsePublisher sends a correlated guaranteed response. Send
// returns only after the peer's disposition; timeout/disconnect remains unknown.
type AMQPSnapshotResponsePublisher struct{ Session *amqp.Session }

func (p AMQPSnapshotResponsePublisher) PublishSnapshotResponse(ctx context.Context, replyTo, correlationID string, response control.SnapshotResponse) error {
	if p.Session == nil || replyTo == "" || correlationID == "" {
		return errors.New("integration: AMQP snapshot responder is incomplete")
	}
	sender, err := p.Session.NewSender(ctx, QueueAddress(replyTo), nil)
	if err != nil {
		return err
	}
	defer closeAMQPSender(sender)
	payload, err := marshalAMQPJSON(response)
	if err != nil {
		return err
	}
	message := amqp.NewMessage(payload)
	message.Header = &amqp.MessageHeader{Durable: true}
	message.Properties = &amqp.MessageProperties{MessageID: correlationID + "/response", CorrelationID: correlationID, ContentType: pointer("application/json")}
	return sender.Send(ctx, message, nil)
}

// AMQPAsyncPublisher implements durable outbox publication. Only StateAccepted
// deletes a record; StateRejected or StateReleased is retryable, while a missing
// disposition, timeout, or link failure remains ACK-uncertain. Senders are opened
// lazily per broker and epoch destination and reused safely.
type AMQPAsyncPublisher struct {
	Sender   *amqp.Sender
	Sessions map[string]*amqp.Session
	Pool     *AMQPConnectionPool
	MaxLinks int
	Idle     time.Duration
	mu       sync.Mutex
	senders  map[string]*pooledSender
}
type pooledSender struct {
	sender     *amqp.Sender
	generation uint64
	refs       int
	lastUsed   time.Time
}

func (p *AMQPAsyncPublisher) PublishAsync(ctx context.Context, brokerID string, message shimPublisher.BrokerMessage, attempt shimPublisher.PublishAttempt) (shimPublisher.AsyncPublishFuture, error) {
	sender := p.Sender
	var release func()
	if p.Pool != nil {
		lease, err := p.Pool.Acquire(ctx, brokerID, message.BrokerEndpoint)
		if err != nil {
			return nil, err
		}
		release = lease.Release
		outbound, buildErr := amqpBusinessMessage(message)
		if buildErr != nil {
			release()
			return nil, buildErr
		}
		entry, entryErr := p.acquireSender(ctx, lease.Session, lease.Generation, brokerID, message.Destination)
		if entryErr != nil {
			release()
			return nil, entryErr
		}
		receipt, sendErr := entry.sender.SendWithReceipt(ctx, outbound, nil)
		if sendErr != nil {
			p.releaseSender(entry)
			release()
			return nil, sendErr
		}
		return amqpPublishFuture{receipt: &receipt, attempt: attempt, release: func() { p.releaseSender(entry); release() }}, nil
	} else if len(p.Sessions) != 0 {
		session := p.Sessions[brokerID]
		if session == nil {
			return nil, fmt.Errorf("integration: AMQP session for broker %q is required", brokerID)
		}
		entry, err := p.acquireSender(ctx, session, 0, brokerID, message.Destination)
		if err != nil {
			return nil, err
		}
		sender = entry.sender
		release = func() { p.releaseSender(entry) }
	}
	if sender == nil {
		return nil, fmt.Errorf("integration: AMQP sender for broker %q is required", brokerID)
	}
	outbound, err := amqpBusinessMessage(message)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, err
	}
	receipt, err := sender.SendWithReceipt(ctx, outbound, nil)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, err
	}
	return amqpPublishFuture{receipt: &receipt, attempt: attempt, release: release}, nil
}

func (p *AMQPAsyncPublisher) acquireSender(ctx context.Context, session *amqp.Session, generation uint64, broker, destination string) (*pooledSender, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.MaxLinks <= 0 {
		p.MaxLinks = 128
	}
	if p.Idle <= 0 {
		p.Idle = 5 * time.Minute
	}
	if p.senders == nil {
		p.senders = make(map[string]*pooledSender)
	}
	now := time.Now()
	for key, entry := range p.senders {
		if entry.refs == 0 && (entry.generation != generation || now.Sub(entry.lastUsed) >= p.Idle) {
			_ = closeAMQPSender(entry.sender)
			delete(p.senders, key)
		}
	}
	key := fmt.Sprintf("%s\x00%d\x00%s", broker, generation, destination)
	if entry := p.senders[key]; entry != nil {
		entry.refs++
		return entry, nil
	}
	if len(p.senders) >= p.MaxLinks {
		return nil, fmt.Errorf("integration: AMQP sender link capacity %d reached", p.MaxLinks)
	}
	sender, err := session.NewSender(ctx, TopicAddress(destination), nil)
	if err != nil {
		return nil, err
	}
	entry := &pooledSender{sender: sender, generation: generation, refs: 1, lastUsed: now}
	p.senders[key] = entry
	return entry, nil
}
func (p *AMQPAsyncPublisher) releaseSender(entry *pooledSender) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry.refs > 0 {
		entry.refs--
		entry.lastUsed = time.Now()
	}
}

type amqpPublishFuture struct {
	receipt *amqp.SendReceipt
	attempt shimPublisher.PublishAttempt
	release func()
}

func (f amqpPublishFuture) Await(ctx context.Context) (shimPublisher.AsyncPublishResult, error) {
	if f.release != nil {
		defer f.release()
	}
	state, err := f.receipt.Wait(ctx)
	outcome, mappedErr := classifyAMQPSettlement(state, err)
	return shimPublisher.AsyncPublishResult{Attempt: f.attempt, Outcome: outcome, Err: mappedErr}, nil
}

func classifyAMQPSettlement(state amqp.DeliveryState, err error) (shimPublisher.PublishOutcome, error) {
	if err != nil {
		return shimPublisher.OutcomeUnknown, err
	}
	switch state.(type) {
	case *amqp.StateAccepted:
		return shimPublisher.OutcomeAcknowledged, nil
	case *amqp.StateRejected, *amqp.StateReleased:
		return shimPublisher.OutcomeRejected, nil
	default:
		return shimPublisher.OutcomeUnknown, fmt.Errorf("integration: AMQP settlement %T is not terminally accepted", state)
	}
}

func amqpBusinessMessage(message shimPublisher.BrokerMessage) (*amqp.Message, error) {
	if message.EventID == "" || message.Destination == "" || message.Topic == "" || message.Epoch == 0 {
		return nil, errors.New("integration: incomplete AMQP business message")
	}
	properties := make(map[string]any, len(message.Properties)+2)
	for key, value := range message.Properties {
		properties[key] = value
	}
	properties[PropertyOriginalTopic] = message.Topic
	properties[PropertyRoutingEpoch] = strconv.FormatUint(message.Epoch, 10)
	outbound := amqp.NewMessage(message.Payload)
	outbound.Header = &amqp.MessageHeader{Durable: true}
	outbound.Properties = &amqp.MessageProperties{MessageID: message.EventID, To: pointer(TopicAddress(message.Destination)), ContentType: pointer("application/octet-stream")}
	outbound.ApplicationProperties = properties
	return outbound, nil
}

// AMQPConsumerFactory creates one paused serialized receiver per exclusive queue.
type AMQPConsumerFactory struct {
	Sessions map[string]*amqp.Session
	Pool     *AMQPConnectionPool
}

func (f AMQPConsumerFactory) Prepare(ctx context.Context, binding shimSubscriber.Binding, deliver func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
	session := f.Sessions[binding.BrokerID]
	var lease *AMQPLease
	var err error
	if f.Pool != nil {
		lease, err = f.Pool.Acquire(ctx, binding.BrokerID, binding.Endpoint)
		if err != nil {
			return nil, err
		}
		session = lease.Session
	}
	if session == nil || deliver == nil {
		if lease != nil {
			lease.Release()
		}
		return nil, fmt.Errorf("integration: AMQP session for broker %q and delivery callback are required", binding.BrokerID)
	}
	receiver, err := session.NewReceiver(ctx, QueueAddress(binding.Destination), &amqp.ReceiverOptions{Credit: -1})
	if err != nil {
		if lease != nil {
			lease.Release()
		}
		return nil, fmt.Errorf("integration: attach exclusive AMQP receiver %q: %w", binding.Destination, err)
	}
	consumerCtx, cancel := context.WithCancel(context.Background())
	return &AMQPConsumer{receiver: receiver, binding: binding, deliver: deliver, ctx: consumerCtx, cancel: cancel, lease: lease}, nil
}

type AMQPConsumer struct {
	mu       sync.Mutex
	receiver *amqp.Receiver
	binding  shimSubscriber.Binding
	deliver  func(context.Context, shimSubscriber.Delivery) error
	ctx      context.Context
	cancel   context.CancelFunc
	active   bool
	closed   bool
	lease    *AMQPLease
}

func (c *AMQPConsumer) Activate(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("integration: AMQP consumer is closed")
	}
	if c.active {
		return nil
	}
	if err := c.receiver.IssueCredit(1); err != nil {
		return err
	}
	c.active = true
	go c.run()
	return ctx.Err()
}

func (c *AMQPConsumer) run() {
	for {
		message, err := c.receiver.Receive(c.ctx, nil)
		if err != nil {
			return
		}
		delivery := &AMQPDelivery{receiver: c.receiver, message: message, binding: c.binding}
		err = c.deliver(c.ctx, delivery)
		if err != nil && !shimSubscriber.RetainedDelivery(err) {
			_ = delivery.Reject(context.WithoutCancel(c.ctx))
		}
		c.mu.Lock()
		active := c.active && !c.closed
		c.mu.Unlock()
		if active {
			_ = c.receiver.IssueCredit(1)
		}
	}
}

func (c *AMQPConsumer) Pause(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("integration: AMQP consumer is closed")
	}
	c.active = false
	return c.receiver.DrainCredit(ctx, nil)
}

func (c *AMQPConsumer) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	err := c.receiver.Close(ctx)
	if c.lease != nil {
		c.lease.Release()
		c.lease = nil
	}
	return err
}

// AMQPDelivery maps accepted/rejected application outcomes to AMQP dispositions.
// MQTT adapters cannot reuse this type because MQTT acknowledgements do not expose
// equivalent release/reject settlement guarantees.
type AMQPDelivery struct {
	receiver  *amqp.Receiver
	message   *amqp.Message
	binding   shimSubscriber.Binding
	once      sync.Once
	settleErr error
}

func (d *AMQPDelivery) Message() customer.MessageView {
	return customer.MessageView{Topic: amqpProperty(d.message, PropertyOriginalTopic), Headers: amqpHeaders(d.message.ApplicationProperties), Payload: append([]byte(nil), d.message.GetData()...), EventID: amqpMessageID(d.message)}
}
func (d *AMQPDelivery) RoutingMetadata() (shimSubscriber.RoutingMetadata, error) {
	hash, err := customer.ParseBusinessHash(amqpProperty(d.message, PropertyBusinessHash))
	if err != nil {
		return shimSubscriber.RoutingMetadata{}, err
	}
	epoch, err := strconv.ParseUint(amqpProperty(d.message, PropertyRoutingEpoch), 10, 64)
	if err != nil {
		return shimSubscriber.RoutingMetadata{}, err
	}
	return shimSubscriber.RoutingMetadata{Group: amqpProperty(d.message, PropertyScalingGroup), BusinessHash: hash, HashContract: amqpProperty(d.message, PropertyHashContract), LibraryVersion: amqpProperty(d.message, PropertyLibraryVersion), Epoch: epoch, OriginalTopic: amqpProperty(d.message, PropertyOriginalTopic)}, nil
}
func (d *AMQPDelivery) Ack(ctx context.Context) error {
	d.once.Do(func() { d.settleErr = d.receiver.AcceptMessage(ctx, d.message) })
	return d.settleErr
}
func (d *AMQPDelivery) Reject(ctx context.Context) error {
	d.once.Do(func() { d.settleErr = d.receiver.RejectMessage(ctx, d.message, nil) })
	return d.settleErr
}

const amqpCloseTimeout = 5 * time.Second

func closeAMQPSender(sender *amqp.Sender) error {
	ctx, cancel := context.WithTimeout(context.Background(), amqpCloseTimeout)
	defer cancel()
	return sender.Close(ctx)
}

func closeAMQPReceiver(receiver *amqp.Receiver) error {
	ctx, cancel := context.WithTimeout(context.Background(), amqpCloseTimeout)
	defer cancel()
	return receiver.Close(ctx)
}

func marshalAMQPJSON(value any) ([]byte, error) { return json.Marshal(value) }
func pointer[T any](value T) *T                 { return &value }
func amqpStringID(properties *amqp.MessageProperties, messageID bool) (string, bool) {
	if properties == nil {
		return "", false
	}
	value := properties.CorrelationID
	if messageID {
		value = properties.MessageID
	}
	text, ok := value.(string)
	return text, ok && text != ""
}
func amqpMessageID(message *amqp.Message) string {
	value, _ := amqpStringID(message.Properties, true)
	return value
}
func amqpProperty(message *amqp.Message, name string) string {
	if message == nil {
		return ""
	}
	value, _ := message.ApplicationProperties[name].(string)
	return value
}
func amqpHeaders(properties map[string]any) map[string]string {
	result := map[string]string{}
	for key, value := range properties {
		if strings.HasPrefix(key, "swlb.") {
			continue
		}
		if text, ok := value.(string); ok {
			result[key] = text
		}
	}
	return result
}

var _ broker0.SnapshotRequester = AMQPRequestClient{}
var _ broker0.SnapshotResponsePublisher = AMQPSnapshotResponsePublisher{}
var _ shimPublisher.AsyncBrokerPublisher = (*AMQPAsyncPublisher)(nil)
var _ shimSubscriber.ConsumerFactory = AMQPConsumerFactory{}
var _ shimSubscriber.Consumer = (*AMQPConsumer)(nil)
var _ shimSubscriber.RoutedDelivery = (*AMQPDelivery)(nil)
