package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/integration"
)

// AMQPControlConnection is the active Broker 0 transport. Future native SMF
// and MQTT adapters must implement the same narrow control contracts separately.
type AMQPControlConnection struct{ *integration.AMQPConnection }

func ConnectAMQPBroker0(ctx context.Context, endpoint, username, password, applicationID string) (*AMQPControlConnection, error) {
	connection, err := integration.ConnectAMQP(ctx, endpoint, username, password, applicationID, nil)
	if err != nil {
		return nil, err
	}
	return &AMQPControlConnection{AMQPConnection: connection}, nil
}

type AMQPNativePublisher struct {
	session *amqp.Session
	mu      sync.Mutex
	senders map[string]*amqp.Sender
}

func NewAMQPNativePublisher(connection *AMQPControlConnection) (*AMQPNativePublisher, error) {
	if connection == nil || connection.Session == nil {
		return nil, errors.New("runtime: AMQP control session is required")
	}
	return &AMQPNativePublisher{session: connection.Session, senders: make(map[string]*amqp.Sender)}, nil
}
func (p *AMQPNativePublisher) PublishPersistent(ctx context.Context, publication broker0.Publication) error {
	p.mu.Lock()
	sender := p.senders[publication.Destination]
	p.mu.Unlock()
	if sender == nil {
		var err error
		sender, err = p.session.NewSender(ctx, integration.TopicAddress(publication.Destination), nil)
		if err != nil {
			return err
		}
		p.mu.Lock()
		p.senders[publication.Destination] = sender
		p.mu.Unlock()
	}
	message := amqp.NewMessage(publication.Payload)
	message.Header = &amqp.MessageHeader{Durable: true}
	message.Properties = &amqp.MessageProperties{MessageID: publication.OperationID, Subject: stringPointer(string(publication.Kind))}
	return sender.Send(ctx, message, nil)
}
func (p *AMQPNativePublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var result error
	for _, sender := range p.senders {
		result = errors.Join(result, closeRuntimeSender(sender))
	}
	return result
}

// AMQPDurableReceiverFactory binds existing exclusive queues with client
// settlement. It does not create resources or subscriptions.
type AMQPDurableReceiverFactory struct{ Session *amqp.Session }

func (f AMQPDurableReceiverFactory) BindDurable(ctx context.Context, binding broker0.ParticipantQueue) (broker0.DurableReceiver, error) {
	if f.Session == nil || binding.Queue == "" {
		return nil, errors.New("runtime: AMQP session and queue are required")
	}
	receiver, err := f.Session.NewReceiver(ctx, integration.QueueAddress(binding.Queue), &amqp.ReceiverOptions{Credit: 1})
	if err != nil {
		return nil, err
	}
	return &amqpDurableReceiver{receiver: receiver, binding: binding}, nil
}

type amqpDurableReceiver struct {
	receiver *amqp.Receiver
	binding  broker0.ParticipantQueue
}

func (r *amqpDurableReceiver) Receive(ctx context.Context) (broker0.Delivery, error) {
	message, err := r.receiver.Receive(ctx, nil)
	if err != nil {
		return nil, err
	}
	kind := r.binding.Kind
	if message.Properties != nil && message.Properties.Subject != nil {
		kind = broker0.Kind(*message.Properties.Subject)
	}
	operation, ok := amqpTextID(message.Properties, true)
	if !ok {
		_ = r.receiver.RejectMessage(ctx, message, nil)
		return nil, errors.New("runtime: AMQP control message has no string message-id")
	}
	return &amqpControlDelivery{receiver: r.receiver, message: message, binding: r.binding, kind: kind, operation: operation, payload: append([]byte(nil), message.GetData()...)}, nil
}
func (r *amqpDurableReceiver) Close() error { return closeRuntimeReceiver(r.receiver) }

type amqpControlDelivery struct {
	mu        sync.Mutex
	receiver  *amqp.Receiver
	message   *amqp.Message
	binding   broker0.ParticipantQueue
	kind      broker0.Kind
	operation string
	payload   []byte
	done      bool
}

func (d *amqpControlDelivery) Kind() broker0.Kind  { return d.kind }
func (d *amqpControlDelivery) OperationID() string { return d.operation }
func (d *amqpControlDelivery) Payload() []byte     { return append([]byte(nil), d.payload...) }
func (d *amqpControlDelivery) AuthenticatedParticipant() (string, bool) {
	return d.binding.Participant, d.binding.Participant != ""
}
func (d *amqpControlDelivery) AuthenticatedScope() (broker0.Kind, string, control.ParticipantRole) {
	return d.binding.Kind, d.binding.Group, d.binding.Role
}
func (d *amqpControlDelivery) Ack(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done {
		return nil
	}
	if err := d.receiver.AcceptMessage(ctx, d.message); err != nil {
		return err
	}
	d.done = true
	return nil
}
func (d *amqpControlDelivery) Reject(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done {
		return nil
	}
	if err := d.receiver.RejectMessage(ctx, d.message, nil); err != nil {
		return err
	}
	d.done = true
	return nil
}

// AMQPSnapshotRequestReceiver receives from one participant-scoped request queue;
// the queue binding is the authenticated identity used by the server.
type AMQPSnapshotRequestReceiver struct {
	receiver *amqp.Receiver
	binding  broker0.ParticipantQueue
}

func BindAMQPSnapshotRequests(ctx context.Context, session *amqp.Session, binding broker0.ParticipantQueue) (*AMQPSnapshotRequestReceiver, error) {
	if session == nil || binding.Queue == "" || binding.Participant == "" {
		return nil, errors.New("runtime: exact AMQP snapshot request binding is required")
	}
	receiver, err := session.NewReceiver(ctx, integration.QueueAddress(binding.Queue), &amqp.ReceiverOptions{Credit: 1})
	if err != nil {
		return nil, err
	}
	return &AMQPSnapshotRequestReceiver{receiver: receiver, binding: binding}, nil
}
func (r *AMQPSnapshotRequestReceiver) ReceiveSnapshotRequest(ctx context.Context) (broker0.SnapshotRequestDelivery, error) {
	message, err := r.receiver.Receive(ctx, nil)
	if err != nil {
		return nil, err
	}
	request, err := control.ParseSnapshotRequest(message.GetData())
	if err != nil {
		_ = r.receiver.RejectMessage(ctx, message, nil)
		return nil, err
	}
	correlation, _ := amqpTextID(message.Properties, false)
	reply := ""
	if message.Properties != nil && message.Properties.ReplyTo != nil {
		reply = integration.RawQueueAddress(*message.Properties.ReplyTo)
	}
	return &amqpSnapshotRequestDelivery{amqpControlDelivery: amqpControlDelivery{receiver: r.receiver, message: message, binding: r.binding, kind: broker0.KindSnapshotRequest, operation: request.CorrelationID, payload: append([]byte(nil), message.GetData()...)}, request: request, correlation: correlation, reply: reply}, nil
}
func (r *AMQPSnapshotRequestReceiver) Close() error { return closeRuntimeReceiver(r.receiver) }

type amqpSnapshotRequestDelivery struct {
	amqpControlDelivery
	request            control.SnapshotRequest
	correlation, reply string
}

func (d *amqpSnapshotRequestDelivery) Request() control.SnapshotRequest { return d.request }
func (d *amqpSnapshotRequestDelivery) ReplyTo() string                  { return d.reply }
func (d *amqpSnapshotRequestDelivery) CorrelationID() string            { return d.correlation }

func amqpTextID(properties *amqp.MessageProperties, messageID bool) (string, bool) {
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
func stringPointer(value string) *string    { return &value }
func marshalAMQP(value any) ([]byte, error) { return json.Marshal(value) }

var _ broker0.NativePersistentPublisher = (*AMQPNativePublisher)(nil)
var _ broker0.DurableReceiverFactory = AMQPDurableReceiverFactory{}
var _ broker0.SnapshotRequestReceiver = (*AMQPSnapshotRequestReceiver)(nil)

const runtimeAMQPCloseTimeout = 5 * time.Second

func closeRuntimeSender(sender *amqp.Sender) error {
	ctx, cancel := context.WithTimeout(context.Background(), runtimeAMQPCloseTimeout)
	defer cancel()
	return sender.Close(ctx)
}
func closeRuntimeReceiver(receiver *amqp.Receiver) error {
	ctx, cancel := context.WithTimeout(context.Background(), runtimeAMQPCloseTimeout)
	defer cancel()
	return receiver.Close(ctx)
}
