// Package integration contains the native Solace SMF adapters used by shims.
package integration

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/customer"
	"github.com/solacese/solace-workload-balancer/routing"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
	messaging "solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
)

const (
	PropertyScalingGroup   = "swlb.group"
	PropertyBusinessHash   = "swlb.business_hash"
	PropertyRoutingEpoch   = "swlb.routing_epoch"
	PropertyEventID        = "swlb.event_id"
	PropertyOriginalTopic  = "swlb.original_topic"
	PropertyHashContract   = "swlb.hash_contract"
	PropertyLibraryVersion = "swlb.library_version"

	defaultCloseGracePeriod = 10 * time.Second
)

var (
	// ErrConsumerClosed is returned when a terminated consumer is reused.
	ErrConsumerClosed = errors.New("integration: consumer is closed")
	// ErrReservedProperty is returned before publication when application metadata
	// attempts to set a property owned by the routing envelope.
	ErrReservedProperty = errors.New("integration: application property uses reserved routing name")
	// ErrPublisherBackpressure is returned when the adapter's bounded number of
	// unresolved publishes is exhausted.
	ErrPublisherBackpressure = errors.New("integration: asynchronous publisher backpressure")
	// ErrPublisherClosed is returned after an asynchronous publisher begins closing.
	ErrPublisherClosed = errors.New("integration: asynchronous publisher is closed")
	// ErrInvalidInboundMetadata marks malformed or incomplete routing metadata.
	ErrInvalidInboundMetadata = errors.New("integration: invalid inbound routing metadata")
)

var reservedProperties = map[string]struct{}{
	PropertyScalingGroup:     {},
	PropertyBusinessHash:     {},
	PropertyRoutingEpoch:     {},
	PropertyEventID:          {},
	PropertyOriginalTopic:    {},
	PropertyHashContract:     {},
	PropertyLibraryVersion:   {},
	config.QueuePartitionKey: {},
}

// PartitionPolicy decides independently for each scaling group whether native
// partition routing is enabled. A nil policy means no group is partitioned.
type PartitionPolicy map[string]bool

func (p PartitionPolicy) Partitioned(group string) bool { return p != nil && p[group] }

// Connection identifies one logical Solace service. An HA host list remains one
// Connection and therefore one routing destination.
type Connection struct {
	Host           string
	MessageVPN     string
	Username       string
	Password       string
	ApplicationID  string
	TrustStorePath string
}

func Connect(c Connection) (solace.MessagingService, error) {
	return ConnectContext(context.Background(), c)
}

// ConnectContext opens a native service while honoring cancellation. If the
// caller cancels before the native asynchronous connect resolves, the service is
// disconnected so an eventual late success cannot leak a live connection.
func ConnectContext(ctx context.Context, c Connection) (solace.MessagingService, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.Host == "" || c.MessageVPN == "" || c.Username == "" || c.ApplicationID == "" {
		return nil, errors.New("solace connection requires host, message VPN, username, and application ID")
	}
	builder := messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(config.ServicePropertyMap{
			config.TransportLayerPropertyHost: c.Host,
			config.ServicePropertyVPNName:     c.MessageVPN,
		}).
		WithAuthenticationStrategy(config.BasicUserNamePasswordAuthentication(c.Username, c.Password)).
		WithConnectionRetryStrategy(config.RetryStrategyNeverRetry()).
		WithReconnectionRetryStrategy(config.RetryStrategyForeverRetry())
	if c.TrustStorePath != "" {
		builder = builder.WithTransportSecurityStrategy(config.NewTransportSecurityStrategy().WithCertificateValidation(false, true, c.TrustStorePath, ""))
	}
	service, err := builder.BuildWithApplicationID(c.ApplicationID)
	if err != nil {
		return nil, fmt.Errorf("build Solace messaging service: %w", err)
	}
	result := service.ConnectAsync()
	select {
	case err := <-result:
		if err != nil {
			return nil, fmt.Errorf("connect Solace messaging service: %w", err)
		}
		return service, nil
	case <-ctx.Done():
		go func() {
			<-result
			_ = service.Disconnect()
		}()
		return nil, ctx.Err()
	}
}

// RoutedMessage is an owned message ready for native guaranteed publication.
type RoutedMessage struct {
	Destination    string
	OriginalTopic  string
	Payload        []byte
	Properties     map[string]string
	EventID        string
	ScalingGroup   string
	BusinessHash   [32]byte
	HashContract   string
	LibraryVersion string
	Epoch          uint64
	Partitioned    bool
}

// GuaranteedPublisher uses native persistent messaging and waits for a positive
// broker acknowledgement. A timeout is ambiguous: callers must retain the
// outbox record because the broker may already have accepted it.
type GuaranteedPublisher struct {
	service   solace.MessagingService
	publisher solace.PersistentMessagePublisher
	timeout   time.Duration
}

func NewGuaranteedPublisher(service solace.MessagingService, timeout time.Duration) (*GuaranteedPublisher, error) {
	if service == nil || !service.IsConnected() {
		return nil, errors.New("connected Solace messaging service is required")
	}
	if timeout <= 0 {
		return nil, errors.New("positive acknowledgement timeout is required")
	}
	publisher, err := service.CreatePersistentMessagePublisherBuilder().OnBackPressureReject(0).Build()
	if err != nil {
		return nil, fmt.Errorf("build persistent publisher: %w", err)
	}
	if err := publisher.Start(); err != nil {
		return nil, fmt.Errorf("start persistent publisher: %w", err)
	}
	return &GuaranteedPublisher{service: service, publisher: publisher, timeout: timeout}, nil
}

func (p *GuaranteedPublisher) Publish(ctx context.Context, routed RoutedMessage) error {
	if err := validateRoutedMessage(routed); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	outbound, err := buildRoutedMessage(p.service, routed)
	if err != nil {
		return err
	}
	defer outbound.Dispose()

	// The native call has its own bounded acknowledgement timeout. It is not run
	// in a detached goroutine because disposing the message while that call is
	// still active would violate the API object's lifetime.
	if err := p.publisher.PublishAwaitAcknowledgement(outbound, resource.TopicOf(routed.Destination), p.timeout, nil); err != nil {
		return fmt.Errorf("publish and await broker acknowledgement: %w", err)
	}
	return nil
}

func validateRoutedMessage(routed RoutedMessage) error {
	if routed.Destination == "" || routed.OriginalTopic == "" || routed.EventID == "" || routed.ScalingGroup == "" || routed.HashContract == "" || routed.LibraryVersion == "" || routed.Epoch == 0 {
		return errors.New("routed message is incomplete")
	}
	for key := range routed.Properties {
		if _, reserved := reservedProperties[key]; reserved || strings.HasPrefix(key, "swlb.") {
			return fmt.Errorf("%w %q", ErrReservedProperty, key)
		}
	}
	return nil
}

func buildRoutedMessage(service interface {
	MessageBuilder() solace.OutboundMessageBuilder
}, routed RoutedMessage) (message.OutboundMessage, error) {
	builder := service.MessageBuilder()
	for key, value := range routed.Properties {
		builder = builder.WithProperty(config.MessageProperty(key), value)
	}
	businessHash := hex.EncodeToString(routed.BusinessHash[:])
	builder = builder.
		WithApplicationMessageID(routed.EventID).
		WithProperty(PropertyEventID, routed.EventID).
		WithProperty(PropertyScalingGroup, routed.ScalingGroup).
		WithProperty(PropertyBusinessHash, businessHash).
		WithProperty(PropertyRoutingEpoch, strconv.FormatUint(routed.Epoch, 10)).
		WithProperty(PropertyOriginalTopic, routed.OriginalTopic).
		WithProperty(PropertyHashContract, routed.HashContract).
		WithProperty(PropertyLibraryVersion, routed.LibraryVersion)
	if key, value, ok := partitionKeyProperty(routed); ok {
		builder = builder.WithProperty(key, value)
	}
	outbound, err := builder.BuildWithByteArrayPayload(routed.Payload)
	if err != nil {
		return nil, fmt.Errorf("build outbound message: %w", err)
	}
	return outbound, nil
}

func partitionKeyProperty(routed RoutedMessage) (config.MessageProperty, string, bool) {
	if !routed.Partitioned {
		return "", "", false
	}
	return config.MessageProperty(config.QueuePartitionKey), hex.EncodeToString(routed.BusinessHash[:]), true
}

func (p *GuaranteedPublisher) Close() error {
	return p.publisher.Terminate(defaultCloseGracePeriod)
}

// DefinitiveBrokerRejection reports whether err is an authoritative negative
// acknowledgement from the broker rather than a timeout or transport failure.
func DefinitiveBrokerRejection(err error) bool {
	var native *solace.NativeError
	if !errors.As(err, &native) {
		return false
	}
	switch native.SubCode() {
	case subcode.NoSubscriptionMatch, subcode.SpoolOverQuota, subcode.MaxMessageUsageExceeded,
		subcode.PublishAclDenied, subcode.QueueShutdown, subcode.EndpointShutdown, subcode.ServiceUnavailable:
		return true
	default:
		return false
	}
}

// PublishResult is the terminal broker receipt for one asynchronous publish.
// Correlation is returned exactly as supplied to PublishAsync.
type PublishResult struct {
	Correlation any
	Persisted   bool
	Timestamp   time.Time
	Err         error
}

// PublishFuture resolves exactly once. Await does not cancel the broker publish;
// a context timeout therefore remains an ambiguous result to the caller.
type PublishFuture struct {
	result <-chan PublishResult
}

func (f PublishFuture) Await(ctx context.Context) (PublishResult, error) {
	if f.result == nil {
		return PublishResult{}, errors.New("integration: invalid publish future")
	}
	select {
	case result := <-f.result:
		return result, nil
	case <-ctx.Done():
		return PublishResult{}, ctx.Err()
	}
}

// asyncPersistentPublisher is the small native seam used for broker-free tests.
type asyncPersistentPublisher interface {
	Publish(message.OutboundMessage, *resource.Topic, config.MessagePropertiesConfigurationProvider, interface{}) error
	SetMessagePublishReceiptListener(solace.MessagePublishReceiptListener)
	Start() error
	Terminate(time.Duration) error
}

type publishCorrelation struct {
	user any
	ch   chan PublishResult
	msg  message.OutboundMessage
	once sync.Once
}

// AsyncPersistentPublisher publishes without blocking on individual receipts.
// maxPending bounds unresolved broker acknowledgements in addition to the native
// SDK's own reject-on-full buffer, so backpressure is deterministic to callers.
type AsyncPersistentPublisher struct {
	build     func(RoutedMessage) (message.OutboundMessage, error)
	publisher asyncPersistentPublisher
	slots     chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func NewAsyncPersistentPublisher(service solace.MessagingService, maxPending uint) (*AsyncPersistentPublisher, error) {
	if service == nil || !service.IsConnected() {
		return nil, errors.New("connected Solace messaging service is required")
	}
	if maxPending == 0 {
		return nil, errors.New("maximum pending publishes must be greater than zero")
	}
	publisher, err := service.CreatePersistentMessagePublisherBuilder().OnBackPressureReject(maxPending).Build()
	if err != nil {
		return nil, fmt.Errorf("build asynchronous persistent publisher: %w", err)
	}
	return newAsyncPersistentPublisher(func(routed RoutedMessage) (message.OutboundMessage, error) {
		return buildRoutedMessage(service, routed)
	}, publisher, maxPending)
}

func newAsyncPersistentPublisher(build func(RoutedMessage) (message.OutboundMessage, error), publisher asyncPersistentPublisher, maxPending uint) (*AsyncPersistentPublisher, error) {
	if build == nil || publisher == nil || maxPending == 0 {
		return nil, errors.New("message builder, native publisher, and positive maximum pending count are required")
	}
	p := &AsyncPersistentPublisher{
		build:     build,
		publisher: publisher,
		slots:     make(chan struct{}, maxPending),
		done:      make(chan struct{}),
	}
	publisher.SetMessagePublishReceiptListener(p.onReceipt)
	if err := publisher.Start(); err != nil {
		return nil, fmt.Errorf("start asynchronous persistent publisher: %w", err)
	}
	return p, nil
}

// PublishAsync accepts a message only while pending capacity is available. The
// returned future carries the application correlation and native broker result.
func (p *AsyncPersistentPublisher) PublishAsync(ctx context.Context, routed RoutedMessage, correlation any) (PublishFuture, error) {
	if err := validateRoutedMessage(routed); err != nil {
		return PublishFuture{}, err
	}
	select {
	case <-p.done:
		return PublishFuture{}, ErrPublisherClosed
	default:
	}
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return PublishFuture{}, ctx.Err()
	default:
		return PublishFuture{}, ErrPublisherBackpressure
	}

	outbound, err := p.build(routed)
	if err != nil {
		<-p.slots
		return PublishFuture{}, err
	}
	result := make(chan PublishResult, 1)
	pending := &publishCorrelation{user: correlation, ch: result, msg: outbound}
	if err := p.publisher.Publish(outbound, resource.TopicOf(routed.Destination), nil, pending); err != nil {
		outbound.Dispose()
		<-p.slots
		return PublishFuture{}, fmt.Errorf("publish persistent message: %w", err)
	}
	return PublishFuture{result: result}, nil
}

func (p *AsyncPersistentPublisher) onReceipt(receipt solace.PublishReceipt) {
	if receipt == nil {
		return
	}
	pending, ok := receipt.GetUserContext().(*publishCorrelation)
	if !ok || pending == nil {
		return
	}
	pending.once.Do(func() {
		pending.msg.Dispose()
		pending.ch <- PublishResult{
			Correlation: pending.user,
			Persisted:   receipt.IsPersisted(),
			Timestamp:   receipt.GetTimeStamp(),
			Err:         receipt.GetError(),
		}
		close(pending.ch)
		<-p.slots
	})
}

func (p *AsyncPersistentPublisher) Close() error {
	p.closeOnce.Do(func() {
		close(p.done)
		p.closeErr = p.publisher.Terminate(defaultCloseGracePeriod)
	})
	return p.closeErr
}

// PublisherAdapter lets the durable publisher shim send through a set of native
// publishers keyed by stable broker ID. The snapshot's destination is used for
// transport; the original application topic remains metadata.
type PublisherAdapter struct {
	Publishers map[string]*GuaranteedPublisher
	// Partitioned is retained as a default for callers using one queue policy.
	Partitioned bool
	// PartitionPolicy overrides Partitioned for every configured group, including
	// explicit false entries. This prevents one group's queue type affecting all.
	PartitionPolicy PartitionPolicy
}

func (a PublisherAdapter) partitioned(group string) bool {
	if a.PartitionPolicy != nil {
		return a.PartitionPolicy.Partitioned(group)
	}
	return a.Partitioned
}

func applicationProperties(properties map[string]string) map[string]string {
	if len(properties) == 0 {
		return nil
	}
	result := make(map[string]string, len(properties))
	for key, value := range properties {
		if _, reserved := reservedProperties[key]; reserved || strings.HasPrefix(key, "swlb.") {
			continue
		}
		result[key] = value
	}
	return result
}

func applicationHeaders(properties map[string]any) map[string]string {
	if len(properties) == 0 {
		return nil
	}
	headers := make(map[string]string, len(properties))
	for key, value := range properties {
		if _, reserved := reservedProperties[key]; reserved || strings.HasPrefix(key, "swlb.") {
			continue
		}
		if text, ok := value.(string); ok {
			headers[key] = text
		}
	}
	if len(headers) == 0 {
		return nil
	}
	return headers
}

func (a PublisherAdapter) Publish(ctx context.Context, brokerID string, brokerMessage shimPublisher.BrokerMessage) (shimPublisher.PublishOutcome, error) {
	publisher := a.Publishers[brokerID]
	if publisher == nil {
		return shimPublisher.OutcomeRejected, fmt.Errorf("no native publisher configured for broker %q", brokerID)
	}
	routed, err := a.routedMessage(brokerMessage)
	if err != nil {
		return shimPublisher.OutcomeRejected, err
	}
	if err := publisher.Publish(ctx, routed); err != nil {
		// PublishAwaitAcknowledgement errors are conservatively ambiguous. The
		// durable outbox will retain the record and expose duplicate risk.
		return shimPublisher.OutcomeUnknown, err
	}
	return shimPublisher.OutcomeAcknowledged, nil
}

func (a PublisherAdapter) routedMessage(brokerMessage shimPublisher.BrokerMessage) (RoutedMessage, error) {
	digest, err := routing.ParseSHA256(brokerMessage.Properties[shimPublisher.PropertyBusinessHash])
	if err != nil {
		return RoutedMessage{}, fmt.Errorf("invalid persisted business hash: %w", err)
	}
	return RoutedMessage{
		Destination:    brokerMessage.Destination,
		OriginalTopic:  brokerMessage.Topic,
		Payload:        brokerMessage.Payload,
		Properties:     applicationProperties(brokerMessage.Properties),
		EventID:        brokerMessage.EventID,
		ScalingGroup:   brokerMessage.Properties[shimPublisher.PropertyScalingGroup],
		BusinessHash:   digest,
		HashContract:   brokerMessage.Properties[shimPublisher.PropertyHashContract],
		LibraryVersion: brokerMessage.Properties[shimPublisher.PropertyLibraryVersion],
		Epoch:          brokerMessage.Epoch,
		Partitioned:    a.partitioned(brokerMessage.Properties[shimPublisher.PropertyScalingGroup]),
	}, nil
}

// AsyncPublisherAdapter implements publisher.AsyncBrokerPublisher over one
// receipt-driven native publisher per stable broker identity.
type AsyncPublisherAdapter struct {
	Publishers      map[string]*AsyncPersistentPublisher
	Partitioned     bool
	PartitionPolicy PartitionPolicy
}

func (a AsyncPublisherAdapter) partitioned(group string) bool {
	return PublisherAdapter{Partitioned: a.Partitioned, PartitionPolicy: a.PartitionPolicy}.partitioned(group)
}

func (a AsyncPublisherAdapter) PublishAsync(ctx context.Context, brokerID string, brokerMessage shimPublisher.BrokerMessage, attempt shimPublisher.PublishAttempt) (shimPublisher.AsyncPublishFuture, error) {
	publisher := a.Publishers[brokerID]
	if publisher == nil {
		return nil, fmt.Errorf("no native asynchronous publisher configured for broker %q", brokerID)
	}
	if attempt.EventID == "" || attempt.Number == 0 || attempt.Epoch == 0 {
		return nil, errors.New("integration: asynchronous publish attempt is incomplete")
	}
	if attempt.EventID != brokerMessage.EventID || attempt.Epoch != brokerMessage.Epoch {
		return nil, fmt.Errorf("integration: asynchronous publish attempt does not match message")
	}
	routed, err := (PublisherAdapter{Partitioned: a.Partitioned, PartitionPolicy: a.PartitionPolicy}).routedMessage(brokerMessage)
	if err != nil {
		return nil, err
	}
	future, err := publisher.PublishAsync(ctx, routed, attempt)
	if err != nil {
		return nil, err
	}
	return asyncPublisherFuture{future: future, attempt: attempt}, nil
}

type asyncPublisherFuture struct {
	future  PublishFuture
	attempt shimPublisher.PublishAttempt
}

func (f asyncPublisherFuture) Await(ctx context.Context) (shimPublisher.AsyncPublishResult, error) {
	result, err := f.future.Await(ctx)
	if err != nil {
		return shimPublisher.AsyncPublishResult{}, err
	}
	attempt, ok := result.Correlation.(shimPublisher.PublishAttempt)
	if !ok || attempt != f.attempt {
		return shimPublisher.AsyncPublishResult{}, fmt.Errorf("integration: native publish correlation mismatch")
	}
	outcome := shimPublisher.OutcomeUnknown
	switch {
	case result.Err == nil && result.Persisted:
		outcome = shimPublisher.OutcomeAcknowledged
	case result.Err != nil && !result.Persisted && DefinitiveBrokerRejection(result.Err):
		// Only explicit broker NACK subcodes are safe to retry. Connection failures
		// and timeouts remain unknown because the broker may have accepted the message.
		outcome = shimPublisher.OutcomeRejected
	}
	return shimPublisher.AsyncPublishResult{Attempt: attempt, Outcome: outcome, Err: result.Err}, nil
}

// ReceivedMessage is the application-facing form of a native inbound message.
type ReceivedMessage struct {
	Topic          string
	Payload        []byte
	Properties     map[string]any
	EventID        string
	Group          string
	Hash           string
	HashContract   string
	LibraryVersion string
	Epoch          uint64
}

// RoutingMetadata validates and converts all publisher-owned metadata. It can
// optionally compare recomputed group/hash values before returning the result.
func (m ReceivedMessage) RoutingMetadata(recomputedGroup string, recomputedHash *customer.BusinessHash) (shimSubscriber.RoutingMetadata, error) {
	if m.Topic == "" || m.EventID == "" || m.Group == "" || m.Hash == "" || m.HashContract == "" || m.LibraryVersion == "" || m.Epoch == 0 {
		return shimSubscriber.RoutingMetadata{}, fmt.Errorf("%w: required property is missing", ErrInvalidInboundMetadata)
	}
	digest, err := routing.ParseSHA256(m.Hash)
	if err != nil {
		return shimSubscriber.RoutingMetadata{}, fmt.Errorf("%w: business hash: %v", ErrInvalidInboundMetadata, err)
	}
	if recomputedGroup != "" && recomputedGroup != m.Group {
		return shimSubscriber.RoutingMetadata{}, fmt.Errorf("%w: group %q does not match recomputed group %q", ErrInvalidInboundMetadata, m.Group, recomputedGroup)
	}
	if recomputedHash != nil && digest != *recomputedHash {
		return shimSubscriber.RoutingMetadata{}, fmt.Errorf("%w: business hash does not match recomputed value", ErrInvalidInboundMetadata)
	}
	return shimSubscriber.RoutingMetadata{
		Group:          m.Group,
		BusinessHash:   digest,
		HashContract:   m.HashContract,
		LibraryVersion: m.LibraryVersion,
		Epoch:          m.Epoch,
		OriginalTopic:  m.Topic,
	}, nil
}

// PersistentReceiver binds an existing managed queue and uses client
// acknowledgements. Provisioning is intentionally outside this adapter so it
// cannot adopt or mutate arbitrary customer queues.
type PersistentReceiver struct {
	receiver solace.PersistentMessageReceiver
}

func NewPersistentReceiver(service solace.MessagingService, queueName string, exclusive bool) (*PersistentReceiver, error) {
	if service == nil || !service.IsConnected() || queueName == "" {
		return nil, errors.New("connected service and queue name are required")
	}
	var queue *resource.Queue
	if exclusive {
		queue = resource.QueueDurableExclusive(queueName)
	} else {
		queue = resource.QueueDurableNonExclusive(queueName)
	}
	receiver, err := service.CreatePersistentMessageReceiverBuilder().
		WithMessageClientAcknowledgement().
		WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome).
		Build(queue)
	if err != nil {
		return nil, fmt.Errorf("build persistent receiver: %w", err)
	}
	if err := receiver.Start(); err != nil {
		return nil, fmt.Errorf("start persistent receiver: %w", err)
	}
	return &PersistentReceiver{receiver: receiver}, nil
}

func (r *PersistentReceiver) Receive(timeout time.Duration) (ReceivedMessage, message.InboundMessage, error) {
	inbound, err := r.receiver.ReceiveMessage(timeout)
	if err != nil {
		return ReceivedMessage{}, nil, err
	}
	result, err := parseInbound(inbound)
	if err != nil {
		inbound.Dispose()
		return ReceivedMessage{}, nil, err
	}
	return result, inbound, nil
}

func (r *PersistentReceiver) Ack(inbound message.InboundMessage) error {
	if inbound == nil {
		return errors.New("inbound message is required")
	}
	if err := r.receiver.Ack(inbound); err != nil {
		return err
	}
	inbound.Dispose()
	return nil
}

func (r *PersistentReceiver) Fail(inbound message.InboundMessage) error {
	if inbound == nil {
		return errors.New("inbound message is required")
	}
	if err := r.receiver.Settle(inbound, config.PersistentReceiverFailedOutcome); err != nil {
		return err
	}
	inbound.Dispose()
	return nil
}

func (r *PersistentReceiver) Close() error { return r.receiver.Terminate(defaultCloseGracePeriod) }

// receiverService is the narrow seam needed to build native consumers.
type receiverService interface {
	IsConnected() bool
	CreatePersistentMessageReceiverBuilder() solace.PersistentMessageReceiverBuilder
}

// ConsumerFactory implements subscriber.ConsumerFactory over native durable
// queue receivers. It never provisions or mutates the managed destination.
type consumerPreparer func(context.Context, receiverService, shimSubscriber.Binding, bool, func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error)

type ConsumerFactory struct {
	Services            map[string]solace.MessagingService
	ExclusiveGroups     map[string]bool
	ConsumersPerBinding map[string]int
}

func defaultConsumerPreparer(ctx context.Context, service receiverService, binding shimSubscriber.Binding, exclusive bool, deliver func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
	return prepareConsumer(ctx, service, binding, exclusive, deliver)
}

func (f ConsumerFactory) Prepare(ctx context.Context, binding shimSubscriber.Binding, deliver func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	service := f.Services[binding.BrokerID]
	if service == nil {
		return nil, fmt.Errorf("integration: no connected service for broker %q", binding.BrokerID)
	}
	return f.prepareConsumers(ctx, service, binding, deliver, defaultConsumerPreparer)
}

func (f ConsumerFactory) prepareConsumers(ctx context.Context, service receiverService, binding shimSubscriber.Binding, deliver func(context.Context, shimSubscriber.Delivery) error, prepare consumerPreparer) (shimSubscriber.Consumer, error) {
	count, configured := f.ConsumersPerBinding[binding.Group]
	if !configured {
		count = 1
	} else if count < 1 {
		return nil, fmt.Errorf("integration: group %q requires a positive consumer count", binding.Group)
	}
	if f.ExclusiveGroups[binding.Group] && count != 1 {
		return nil, fmt.Errorf("integration: exclusive group %q requires exactly one consumer per binding", binding.Group)
	}
	consumers := make([]shimSubscriber.Consumer, 0, count)
	for range count {
		consumer, err := prepare(ctx, service, binding, f.ExclusiveGroups[binding.Group], deliver)
		if err != nil {
			var cleanup []error
			for _, prepared := range consumers {
				cleanup = append(cleanup, prepared.Close(context.WithoutCancel(ctx)))
			}
			return nil, errors.Join(err, errors.Join(cleanup...))
		}
		consumers = append(consumers, consumer)
	}
	if len(consumers) == 1 {
		return consumers[0], nil
	}
	return &consumerGroup{consumers: consumers}, nil
}

type consumerGroup struct {
	mu        sync.Mutex
	consumers []shimSubscriber.Consumer
	active    bool
	closed    bool
	closeErr  error
}

func (g *consumerGroup) Activate(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrConsumerClosed
	}
	if g.active {
		return nil
	}
	activated := make([]shimSubscriber.Consumer, 0, len(g.consumers))
	for _, consumer := range g.consumers {
		if err := consumer.Activate(ctx); err != nil {
			var rollback []error
			for index := len(activated) - 1; index >= 0; index-- {
				rollback = append(rollback, activated[index].Pause(context.WithoutCancel(ctx)))
			}
			if rollbackErr := errors.Join(rollback...); rollbackErr != nil {
				return errors.Join(err, rollbackErr, g.closeLocked(context.WithoutCancel(ctx)))
			}
			return err
		}
		activated = append(activated, consumer)
	}
	g.active = true
	return nil
}

func (g *consumerGroup) Pause(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrConsumerClosed
	}
	if !g.active {
		return nil
	}
	paused := make([]shimSubscriber.Consumer, 0, len(g.consumers))
	for _, consumer := range g.consumers {
		if err := consumer.Pause(ctx); err != nil {
			var rollback []error
			for index := len(paused) - 1; index >= 0; index-- {
				rollback = append(rollback, paused[index].Activate(context.WithoutCancel(ctx)))
			}
			if rollbackErr := errors.Join(rollback...); rollbackErr != nil {
				return errors.Join(err, rollbackErr, g.closeLocked(context.WithoutCancel(ctx)))
			}
			return err
		}
		paused = append(paused, consumer)
	}
	g.active = false
	return nil
}

func (g *consumerGroup) Close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closeLocked(ctx)
}

func (g *consumerGroup) closeLocked(ctx context.Context) error {
	if g.closed {
		return g.closeErr
	}
	g.closed = true
	ctx = context.WithoutCancel(ctx)
	var errs []error
	for _, consumer := range g.consumers {
		errs = append(errs, consumer.Close(ctx))
	}
	g.closeErr = errors.Join(errs...)
	return g.closeErr
}

func prepareConsumer(ctx context.Context, service receiverService, binding shimSubscriber.Binding, exclusive bool, deliver func(context.Context, shimSubscriber.Delivery) error) (*Consumer, error) {
	if service == nil || !service.IsConnected() || deliver == nil {
		return nil, errors.New("integration: connected service and delivery callback are required")
	}
	var queue *resource.Queue
	if exclusive {
		queue = resource.QueueDurableExclusive(binding.Destination)
	} else {
		queue = resource.QueueDurableNonExclusive(binding.Destination)
	}
	receiver, err := service.CreatePersistentMessageReceiverBuilder().
		WithMessageClientAcknowledgement().
		WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome).
		Build(queue)
	if err != nil {
		return nil, fmt.Errorf("build persistent consumer for %q: %w", binding.Destination, err)
	}
	return prepareNativeConsumer(ctx, receiver, binding, deliver)
}

func prepareNativeConsumer(ctx context.Context, receiver nativeReceiver, binding shimSubscriber.Binding, deliver func(context.Context, shimSubscriber.Delivery) error) (*Consumer, error) {
	consumer := newConsumer(receiver, binding, deliver)
	if err := receiver.Start(); err != nil {
		return nil, terminatePreparedReceiver(receiver, fmt.Errorf("start persistent consumer for %q: %w", binding.Destination, err))
	}
	if err := receiver.Pause(); err != nil {
		return nil, terminatePreparedReceiver(receiver, fmt.Errorf("pause prepared consumer for %q: %w", binding.Destination, err))
	}
	if err := receiver.ReceiveAsync(consumer.receive); err != nil {
		return nil, terminatePreparedReceiver(receiver, fmt.Errorf("register asynchronous consumer for %q: %w", binding.Destination, err))
	}
	if err := ctx.Err(); err != nil {
		return nil, terminatePreparedReceiver(receiver, err)
	}
	return consumer, nil
}

func terminatePreparedReceiver(receiver nativeReceiver, cause error) error {
	return errors.Join(cause, receiver.Terminate(0))
}

// nativeReceiver is deliberately small so lifecycle and delivery ownership are
// unit-testable without a broker.
type nativeReceiver interface {
	ReceiveAsync(solace.MessageHandler) error
	Start() error
	Pause() error
	Resume() error
	Ack(message.InboundMessage) error
	Settle(message.InboundMessage, config.MessageSettlementOutcome) error
	Terminate(time.Duration) error
}

type consumerState uint8

const (
	consumerPaused consumerState = iota
	consumerActive
	consumerClosed
)

// Consumer is a prepared native asynchronous receiver. Lifecycle methods are
// serialized and idempotent even if transition reconciliation replays them.
type Consumer struct {
	mu       chan struct{}
	receiver nativeReceiver
	binding  shimSubscriber.Binding
	deliver  func(context.Context, shimSubscriber.Delivery) error
	state    consumerState
	ctx      context.Context
	cancel   context.CancelFunc
	closeErr error
}

func newConsumer(receiver nativeReceiver, binding shimSubscriber.Binding, deliver func(context.Context, shimSubscriber.Delivery) error) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{mu: make(chan struct{}, 1), receiver: receiver, binding: binding, deliver: deliver, state: consumerPaused, ctx: ctx, cancel: cancel}
	c.mu <- struct{}{}
	return c
}

func (c *Consumer) Activate(ctx context.Context) error {
	return c.changeState(ctx, consumerActive)
}

func (c *Consumer) Pause(ctx context.Context) error {
	return c.changeState(ctx, consumerPaused)
}

func (c *Consumer) Close(ctx context.Context) error {
	return c.changeState(ctx, consumerClosed)
}

func (c *Consumer) changeState(ctx context.Context, target consumerState) error {
	select {
	case <-c.mu:
	default:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.mu:
		}
	}
	defer func() { c.mu <- struct{}{} }()
	if c.state == consumerClosed {
		if target == consumerClosed {
			return c.closeErr
		}
		return ErrConsumerClosed
	}
	if c.state == target {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var err error
	switch target {
	case consumerActive:
		err = c.receiver.Resume()
	case consumerPaused:
		err = c.receiver.Pause()
	case consumerClosed:
		err = c.receiver.Terminate(defaultCloseGracePeriod)
	}
	if target == consumerClosed {
		// A termination attempt irrevocably closes the wrapper even when the native
		// receiver reports an error: no later callback may be treated as active and
		// repeated Close calls must not retry an indeterminate termination.
		c.state = consumerClosed
		c.closeErr = err
		c.cancel()
		return c.closeErr
	}
	if err != nil {
		return err
	}
	c.state = target
	return nil
}

func (c *Consumer) receive(inbound message.InboundMessage) {
	delivery, err := newDelivery(c.receiver, inbound)
	if err != nil {
		// Malformed messages are terminal poison for this consumer contract. Reject
		// when supported rather than invoking application code with untrusted data.
		_ = c.receiver.Settle(inbound, config.PersistentReceiverRejectedOutcome)
		inbound.Dispose()
		return
	}
	if err := c.deliver(c.ctx, delivery); err != nil {
		if shimSubscriber.RetainedDelivery(err) {
			return
		}
		// Validation errors are not retained by the shim. Reject them so the
		// callback cannot leak native message ownership.
		if settleErr := delivery.reject(); settleErr != nil {
			delivery.Dispose()
		}
		return
	}
	// A successful callback should have ACKed. If a non-conforming callback did
	// not, leave it unsettled but release local native resources.
	delivery.Dispose()
}

// Delivery owns one inbound native message until Ack, Fail, or Dispose. All
// terminal operations are safe to replay, and settlement disposes exactly once.
type Delivery struct {
	mu       chan struct{}
	receiver interface {
		Ack(message.InboundMessage) error
		Settle(message.InboundMessage, config.MessageSettlementOutcome) error
	}
	inbound  message.InboundMessage
	view     customer.MessageView
	metadata shimSubscriber.RoutingMetadata
	done     bool
}

func newDelivery(receiver interface {
	Ack(message.InboundMessage) error
	Settle(message.InboundMessage, config.MessageSettlementOutcome) error
}, inbound message.InboundMessage) (*Delivery, error) {
	if receiver == nil || inbound == nil {
		return nil, errors.New("integration: receiver and inbound message are required")
	}
	parsed, err := parseInbound(inbound)
	if err != nil {
		return nil, err
	}
	metadata, err := parsed.RoutingMetadata("", nil)
	if err != nil {
		return nil, err
	}
	headers := applicationHeaders(parsed.Properties)
	d := &Delivery{
		mu:       make(chan struct{}, 1),
		receiver: receiver,
		inbound:  inbound,
		view: customer.MessageView{
			Topic: parsed.Topic, Headers: headers, Payload: parsed.Payload, EventID: parsed.EventID,
		},
		metadata: metadata,
	}
	d.mu <- struct{}{}
	return d, nil
}

func (d *Delivery) Message() customer.MessageView {
	return customer.MessageView{
		Topic: d.view.Topic, EventID: d.view.EventID,
		Headers: cloneStringMap(d.view.Headers),
		Payload: append([]byte(nil), d.view.Payload.([]byte)...),
	}
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func (d *Delivery) RoutingMetadata() (shimSubscriber.RoutingMetadata, error) {
	return d.metadata, nil
}

func (d *Delivery) Ack(ctx context.Context) error {
	return d.settle(ctx, config.PersistentReceiverAcceptedOutcome)
}

func (d *Delivery) Fail(ctx context.Context) error {
	return d.settle(ctx, config.PersistentReceiverFailedOutcome)
}

func (d *Delivery) Reject(ctx context.Context) error {
	return d.settle(ctx, config.PersistentReceiverRejectedOutcome)
}

func (d *Delivery) reject() error {
	return d.Reject(context.Background())
}

func (d *Delivery) settle(ctx context.Context, outcome config.MessageSettlementOutcome) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.mu:
	}
	defer func() { d.mu <- struct{}{} }()
	if d.done {
		return nil
	}
	var err error
	if outcome == config.PersistentReceiverAcceptedOutcome {
		err = d.receiver.Ack(d.inbound)
	} else {
		err = d.receiver.Settle(d.inbound, outcome)
	}
	if err != nil {
		return err
	}
	d.done = true
	d.inbound.Dispose()
	return nil
}

func (d *Delivery) Dispose() {
	<-d.mu
	defer func() { d.mu <- struct{}{} }()
	if d.done {
		return
	}
	d.done = true
	d.inbound.Dispose()
}

func parseInbound(inbound message.InboundMessage) (ReceivedMessage, error) {
	payload, ok := inbound.GetPayloadAsBytes()
	if !ok {
		return ReceivedMessage{}, errors.New("inbound message has no byte payload")
	}
	properties := make(map[string]any)
	for key, value := range inbound.GetProperties() {
		properties[key] = value
	}
	result := ReceivedMessage{
		Topic:          stringProperty(inbound, PropertyOriginalTopic),
		Payload:        append([]byte(nil), payload...),
		Properties:     properties,
		EventID:        stringProperty(inbound, PropertyEventID),
		Group:          stringProperty(inbound, PropertyScalingGroup),
		Hash:           stringProperty(inbound, PropertyBusinessHash),
		HashContract:   stringProperty(inbound, PropertyHashContract),
		LibraryVersion: stringProperty(inbound, PropertyLibraryVersion),
	}
	applicationID, _ := inbound.GetApplicationMessageID()
	if result.EventID == "" {
		return ReceivedMessage{}, fmt.Errorf("%w: event ID property is missing", ErrInvalidInboundMetadata)
	}
	if applicationID != "" && applicationID != result.EventID {
		return ReceivedMessage{}, fmt.Errorf("%w: event ID property and application message ID disagree", ErrInvalidInboundMetadata)
	}
	epochText := stringProperty(inbound, PropertyRoutingEpoch)
	epoch, err := strconv.ParseUint(epochText, 10, 64)
	if err != nil || epoch == 0 || strconv.FormatUint(epoch, 10) != epochText {
		return ReceivedMessage{}, fmt.Errorf("%w: invalid routing epoch", ErrInvalidInboundMetadata)
	}
	result.Epoch = epoch
	if _, err := result.RoutingMetadata("", nil); err != nil {
		return ReceivedMessage{}, err
	}
	return result, nil
}

func stringProperty(inbound message.InboundMessage, key string) string {
	value, ok := inbound.GetProperty(key)
	if !ok {
		return ""
	}
	text, _ := value.(string)
	return text
}
