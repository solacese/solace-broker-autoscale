package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/message/rgmid"
	"solace.dev/go/messaging/pkg/solace/message/sdt"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
)

func TestConnectRequiresIdentity(t *testing.T) {
	if _, err := Connect(Connection{}); err == nil {
		t.Fatal("Connect accepted incomplete connection")
	}
}

func TestNewGuaranteedPublisherRejectsDisconnectedService(t *testing.T) {
	if _, err := NewGuaranteedPublisher(nil, time.Second); err == nil {
		t.Fatal("NewGuaranteedPublisher accepted nil service")
	}
}

func TestValidateRoutedMessageRejectsReservedProperties(t *testing.T) {
	tests := []string{PropertyEventID, PropertyLibraryVersion, config.QueuePartitionKey, "swlb.future_property"}
	for _, key := range tests {
		t.Run(key, func(t *testing.T) {
			routed := validRoutedMessage()
			routed.Properties = map[string]string{key: "application-value"}
			if err := validateRoutedMessage(routed); !errors.Is(err, ErrReservedProperty) {
				t.Fatalf("validateRoutedMessage error = %v, want ErrReservedProperty", err)
			}
		})
	}
}

func TestApplicationPropertiesRemoveAuthoritativeMetadata(t *testing.T) {
	properties := map[string]string{
		"application":                 "kept",
		shimPublisher.PropertyEventID: "forged",
		"swlb.unknown":                "forged",
	}
	filtered := applicationProperties(properties)
	if filtered["application"] != "kept" || len(filtered) != 1 {
		t.Fatalf("applicationProperties() = %#v", filtered)
	}
}

func TestPartitionPolicyIsPerGroup(t *testing.T) {
	adapter := PublisherAdapter{Partitioned: true, PartitionPolicy: PartitionPolicy{"flight-operations": true, "baggage-tracking": false}}
	if !adapter.partitioned("flight-operations") {
		t.Fatal("flight operations should be partitioned")
	}
	if adapter.partitioned("baggage-tracking") || adapter.partitioned("unknown") {
		t.Fatal("policy must not inherit another group's partition setting")
	}
}

func TestPublisherPolicySetsJMSXGroupIDOnlyForPartitionedGroup(t *testing.T) {
	adapter := PublisherAdapter{PartitionPolicy: PartitionPolicy{"flight-operations": true, "baggage-tracking": false}}
	for _, test := range []struct {
		group   string
		wantKey bool
	}{
		{group: "flight-operations", wantKey: true},
		{group: "baggage-tracking", wantKey: false},
	} {
		t.Run(test.group, func(t *testing.T) {
			brokerMessage := validBrokerMessage(7)
			brokerMessage.Properties[shimPublisher.PropertyScalingGroup] = test.group
			routed, err := adapter.routedMessage(brokerMessage)
			if err != nil {
				t.Fatal(err)
			}
			key, value, ok := partitionKeyProperty(routed)
			if ok != test.wantKey {
				t.Fatalf("JMSXGroupID present = %v, want %v", ok, test.wantKey)
			}
			if test.wantKey && (key != config.MessageProperty(config.QueuePartitionKey) || value != brokerMessage.Properties[shimPublisher.PropertyBusinessHash]) {
				t.Fatalf("partition property = (%q, %q), want JMSXGroupID and business hash", key, value)
			}
			if !test.wantKey && (key != "" || value != "") {
				t.Fatalf("non-partitioned property = (%q, %q)", key, value)
			}
		})
	}
}

func TestReceivedMessageRoutingMetadataValidatesAndCompares(t *testing.T) {
	digest := sha256.Sum256([]byte("key"))
	message := ReceivedMessage{
		Topic: "orders/created", EventID: "event-1", Group: "orders",
		Hash: routingHash(digest), HashContract: "orders-v1", LibraryVersion: "lib-v1", Epoch: 7,
	}
	metadata, err := message.RoutingMetadata("orders", &digest)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.BusinessHash != digest || metadata.OriginalTopic != message.Topic || metadata.Epoch != 7 {
		t.Fatalf("unexpected metadata: %#v", metadata)
	}
	other := sha256.Sum256([]byte("other"))
	if _, err := message.RoutingMetadata("orders", &other); !errors.Is(err, ErrInvalidInboundMetadata) {
		t.Fatalf("hash mismatch error = %v", err)
	}
	if _, err := message.RoutingMetadata("billing", nil); !errors.Is(err, ErrInvalidInboundMetadata) {
		t.Fatalf("group mismatch error = %v", err)
	}
	message.Hash = "ABC"
	if _, err := message.RoutingMetadata("", nil); !errors.Is(err, ErrInvalidInboundMetadata) {
		t.Fatalf("invalid hash error = %v", err)
	}
}

func TestAsyncPublisherBackpressureAndReceiptCorrelation(t *testing.T) {
	native := &fakeAsyncPublisher{}
	publisher, err := newAsyncPersistentPublisher(func(RoutedMessage) (message.OutboundMessage, error) {
		return &fakeOutboundMessage{}, nil
	}, native, 1)
	if err != nil {
		t.Fatal(err)
	}
	first, err := publisher.PublishAsync(context.Background(), validRoutedMessage(), "correlation-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.PublishAsync(context.Background(), validRoutedMessage(), "correlation-2"); !errors.Is(err, ErrPublisherBackpressure) {
		t.Fatalf("second PublishAsync error = %v, want backpressure", err)
	}
	native.receipt(nil, true)
	result, err := first.Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Correlation != "correlation-1" || !result.Persisted || result.Err != nil {
		t.Fatalf("unexpected result: %#v", result)
	}
	if native.lastMessage == nil || !native.lastMessage.IsDisposed() {
		t.Fatal("outbound message was not disposed after receipt")
	}
	if _, err := publisher.PublishAsync(context.Background(), validRoutedMessage(), "correlation-3"); err != nil {
		t.Fatalf("capacity was not released after receipt: %v", err)
	}
}

func TestAsyncPublisherImmediateFailureReleasesCapacity(t *testing.T) {
	native := &fakeAsyncPublisher{publishErr: errors.New("rejected")}
	publisher, err := newAsyncPersistentPublisher(func(RoutedMessage) (message.OutboundMessage, error) {
		return &fakeOutboundMessage{}, nil
	}, native, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.PublishAsync(context.Background(), validRoutedMessage(), nil); err == nil {
		t.Fatal("PublishAsync succeeded")
	}
	native.publishErr = nil
	if _, err := publisher.PublishAsync(context.Background(), validRoutedMessage(), nil); err != nil {
		t.Fatalf("capacity was not released: %v", err)
	}
}

func TestAsyncPublisherCloseIsIdempotent(t *testing.T) {
	closeErr := errors.New("terminate")
	native := &fakeAsyncPublisher{terminateErr: closeErr}
	publisher, err := newAsyncPersistentPublisher(func(RoutedMessage) (message.OutboundMessage, error) {
		return &fakeOutboundMessage{}, nil
	}, native, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("first Close error = %v", err)
	}
	if err := publisher.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("second Close error = %v", err)
	}
	if native.terminates != 1 {
		t.Fatalf("Terminate calls = %d, want 1", native.terminates)
	}
	if _, err := publisher.PublishAsync(context.Background(), validRoutedMessage(), nil); !errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("publish after close error = %v", err)
	}
}

func TestAsyncPublisherAdapterConvertsMessageAttemptAndReceipt(t *testing.T) {
	var built RoutedMessage
	native := &fakeAsyncPublisher{}
	publisher, err := newAsyncPersistentPublisher(func(routed RoutedMessage) (message.OutboundMessage, error) {
		built = routed
		return &fakeOutboundMessage{}, nil
	}, native, 1)
	if err != nil {
		t.Fatal(err)
	}
	adapter := AsyncPublisherAdapter{
		Publishers:      map[string]*AsyncPersistentPublisher{"broker-a": publisher},
		PartitionPolicy: PartitionPolicy{"orders": true},
	}
	attempt := shimPublisher.PublishAttempt{EventID: "event-1", Number: 3, Epoch: 7}
	future, err := adapter.PublishAsync(context.Background(), "broker-a", validBrokerMessage(7), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if built.ScalingGroup != "orders" || !built.Partitioned || built.LibraryVersion != "lib-v1" {
		t.Fatalf("unexpected routed message: %#v", built)
	}
	if len(built.Properties) != 1 || built.Properties["application"] != "value" {
		t.Fatalf("application properties = %#v", built.Properties)
	}
	native.receipt(nil, true)
	result, err := future.Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempt != attempt || result.Outcome != shimPublisher.OutcomeAcknowledged || result.Err != nil {
		t.Fatalf("unexpected async adapter result: %#v", result)
	}
}

func TestAsyncPublisherAdapterKeepsTransportFailureUnknown(t *testing.T) {
	native := &fakeAsyncPublisher{}
	publisher, err := newAsyncPersistentPublisher(func(RoutedMessage) (message.OutboundMessage, error) {
		return &fakeOutboundMessage{}, nil
	}, native, 1)
	if err != nil {
		t.Fatal(err)
	}
	adapter := AsyncPublisherAdapter{Publishers: map[string]*AsyncPersistentPublisher{"broker-a": publisher}}
	attempt := shimPublisher.PublishAttempt{EventID: "event-1", Number: 1, Epoch: 7}
	future, err := adapter.PublishAsync(context.Background(), "broker-a", validBrokerMessage(7), attempt)
	if err != nil {
		t.Fatal(err)
	}
	receiptErr := errors.New("connection lost after submission")
	native.receipt(receiptErr, false)
	result, err := future.Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempt != attempt || result.Outcome != shimPublisher.OutcomeUnknown || !errors.Is(result.Err, receiptErr) {
		t.Fatalf("unexpected async adapter result: %#v", result)
	}
}

func TestAsyncPublisherAdapterMapsBrokerNACKToRejected(t *testing.T) {
	for name, code := range map[string]subcode.Code{
		"no subscription":   subcode.NoSubscriptionMatch,
		"endpoint shutdown": subcode.EndpointShutdown,
	} {
		t.Run(name, func(t *testing.T) {
			native := &fakeAsyncPublisher{}
			publisher, err := newAsyncPersistentPublisher(func(RoutedMessage) (message.OutboundMessage, error) {
				return &fakeOutboundMessage{}, nil
			}, native, 1)
			if err != nil {
				t.Fatal(err)
			}
			adapter := AsyncPublisherAdapter{Publishers: map[string]*AsyncPersistentPublisher{"broker-a": publisher}}
			attempt := shimPublisher.PublishAttempt{EventID: "event-1", Number: 1, Epoch: 7}
			future, err := adapter.PublishAsync(context.Background(), "broker-a", validBrokerMessage(7), attempt)
			if err != nil {
				t.Fatal(err)
			}
			receiptErr := solace.NewNativeError("negative acknowledgement", code)
			native.receipt(receiptErr, false)
			result, err := future.Await(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if result.Attempt != attempt || result.Outcome != shimPublisher.OutcomeRejected || !errors.Is(result.Err, receiptErr) {
				t.Fatalf("unexpected async adapter result: %#v", result)
			}
		})
	}
}

func TestAsyncPublisherAdapterRejectsMissingPublisherAndAttemptMismatch(t *testing.T) {
	adapter := AsyncPublisherAdapter{}
	attempt := shimPublisher.PublishAttempt{EventID: "event-1", Number: 1, Epoch: 7}
	if future, err := adapter.PublishAsync(context.Background(), "missing", validBrokerMessage(7), attempt); err == nil || future != nil {
		t.Fatalf("missing publisher result = %#v, %v", future, err)
	}

	native := &fakeAsyncPublisher{}
	publisher, err := newAsyncPersistentPublisher(func(RoutedMessage) (message.OutboundMessage, error) {
		return &fakeOutboundMessage{}, nil
	}, native, 1)
	if err != nil {
		t.Fatal(err)
	}
	adapter.Publishers = map[string]*AsyncPersistentPublisher{"broker-a": publisher}
	attempt.Epoch++
	if future, err := adapter.PublishAsync(context.Background(), "broker-a", validBrokerMessage(7), attempt); err == nil || future != nil {
		t.Fatalf("mismatched attempt result = %#v, %v", future, err)
	}
}

func TestAsyncPublisherFutureRejectsCorrelationMismatch(t *testing.T) {
	results := make(chan PublishResult, 1)
	results <- PublishResult{Correlation: shimPublisher.PublishAttempt{EventID: "other", Number: 1, Epoch: 1}, Persisted: true}
	close(results)
	future := asyncPublisherFuture{
		future:  PublishFuture{result: results},
		attempt: shimPublisher.PublishAttempt{EventID: "expected", Number: 1, Epoch: 1},
	}
	if _, err := future.Await(context.Background()); err == nil {
		t.Fatal("Await accepted mismatched native correlation")
	}
}

func TestConsumerLifecycleIsIdempotent(t *testing.T) {
	receiver := &fakeNativeReceiver{}
	consumer := newConsumer(receiver, shimSubscriber.Binding{Group: "orders", BrokerID: "broker-a", Epoch: 1, Destination: "queue"}, func(context.Context, shimSubscriber.Delivery) error { return nil })
	ctx := context.Background()
	if err := consumer.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if receiver.resumes != 1 || receiver.pauses != 1 || receiver.terminates != 1 {
		t.Fatalf("lifecycle counts resume=%d pause=%d terminate=%d", receiver.resumes, receiver.pauses, receiver.terminates)
	}
}

func TestConsumerFactoryUsesConfiguredFlowCount(t *testing.T) {
	binding := testBinding()
	service := &fakeReceiverService{connected: true}
	var prepared []*fakeLifecycleConsumer
	factory := ConsumerFactory{ConsumersPerBinding: map[string]int{"orders": 3}}
	consumer, err := factory.prepareConsumers(context.Background(), service, binding, func(context.Context, shimSubscriber.Delivery) error { return nil }, func(_ context.Context, gotService receiverService, gotBinding shimSubscriber.Binding, exclusive bool, deliver func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
		if gotService != service || gotBinding != binding || exclusive || deliver == nil {
			t.Fatalf("unexpected prepare arguments: service=%T binding=%#v exclusive=%v deliverNil=%v", gotService, gotBinding, exclusive, deliver == nil)
		}
		prepared = append(prepared, &fakeLifecycleConsumer{})
		return prepared[len(prepared)-1], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	group, ok := consumer.(*consumerGroup)
	if !ok || len(group.consumers) != 3 || len(prepared) != 3 {
		t.Fatalf("prepared consumer = %#v, calls=%d", consumer, len(prepared))
	}
}

func TestConsumerFactoryFlowCountDefaultsAndValidation(t *testing.T) {
	binding := testBinding()
	service := &fakeReceiverService{connected: true}
	t.Run("unconfigured defaults to one", func(t *testing.T) {
		calls := 0
		consumer, err := (ConsumerFactory{}).prepareConsumers(context.Background(), service, binding, func(context.Context, shimSubscriber.Delivery) error { return nil }, func(context.Context, receiverService, shimSubscriber.Binding, bool, func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
			calls++
			return &fakeLifecycleConsumer{}, nil
		})
		if err != nil || consumer == nil || calls != 1 {
			t.Fatalf("default flow result = %#v, %v, calls=%d", consumer, err, calls)
		}
	})

	tests := map[string]int{"zero": 0, "negative": -1}
	for name, count := range tests {
		t.Run(name, func(t *testing.T) {
			factory := ConsumerFactory{ConsumersPerBinding: map[string]int{"orders": count}}
			if _, err := factory.prepareConsumers(context.Background(), service, binding, func(context.Context, shimSubscriber.Delivery) error { return nil }, func(context.Context, receiverService, shimSubscriber.Binding, bool, func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
				t.Fatal("prepare called for invalid flow count")
				return nil, nil
			}); err == nil {
				t.Fatalf("configured flow count %d accepted", count)
			}
		})
	}
}

func TestConsumerFactoryCleansUpPartialSetup(t *testing.T) {
	binding := testBinding()
	service := &fakeReceiverService{connected: true}
	setupErr := errors.New("setup")
	closeErr := errors.New("cleanup")
	prepared := []*fakeLifecycleConsumer{{closeErr: closeErr}, {}}
	calls := 0
	factory := ConsumerFactory{ConsumersPerBinding: map[string]int{"orders": 3}}
	consumer, err := factory.prepareConsumers(context.Background(), service, binding, func(context.Context, shimSubscriber.Delivery) error { return nil }, func(context.Context, receiverService, shimSubscriber.Binding, bool, func(context.Context, shimSubscriber.Delivery) error) (shimSubscriber.Consumer, error) {
		if calls == len(prepared) {
			return nil, setupErr
		}
		consumer := prepared[calls]
		calls++
		return consumer, nil
	})
	if consumer != nil || !errors.Is(err, setupErr) || !errors.Is(err, closeErr) {
		t.Fatalf("Prepare result = %#v, %v", consumer, err)
	}
	for index, preparedConsumer := range prepared {
		if preparedConsumer.closes != 1 {
			t.Fatalf("prepared consumer %d Close calls = %d, want 1", index, preparedConsumer.closes)
		}
	}
}

func TestConsumerGroupActivationRollbackAndCompensation(t *testing.T) {
	t.Run("rollback succeeds", func(t *testing.T) {
		activateErr := errors.New("activate")
		first := &fakeLifecycleConsumer{}
		second := &fakeLifecycleConsumer{activateErr: activateErr}
		group := &consumerGroup{consumers: []shimSubscriber.Consumer{first, second}}
		if err := group.Activate(context.Background()); !errors.Is(err, activateErr) {
			t.Fatalf("Activate error = %v", err)
		}
		if first.activates != 1 || first.pauses != 1 || first.closes != 0 || group.active || group.closed {
			t.Fatalf("first=%#v group active=%v closed=%v", first, group.active, group.closed)
		}
		second.activateErr = nil
		if err := group.Activate(context.Background()); err != nil {
			t.Fatalf("Activate retry: %v", err)
		}
		if !group.active || first.activates != 2 || second.activates != 2 {
			t.Fatalf("retry lifecycle first=%#v second=%#v active=%v", first, second, group.active)
		}
	})

	t.Run("rollback failure closes every consumer", func(t *testing.T) {
		activateErr := errors.New("activate")
		rollbackErr := errors.New("rollback pause")
		closeErr := errors.New("cleanup")
		first := &fakeLifecycleConsumer{pauseErr: rollbackErr}
		second := &fakeLifecycleConsumer{activateErr: activateErr, closeErr: closeErr}
		group := &consumerGroup{consumers: []shimSubscriber.Consumer{first, second}}
		err := group.Activate(context.Background())
		if !errors.Is(err, activateErr) || !errors.Is(err, rollbackErr) || !errors.Is(err, closeErr) {
			t.Fatalf("Activate error = %v", err)
		}
		if !group.closed || first.closes != 1 || second.closes != 1 {
			t.Fatalf("compensation first=%#v second=%#v closed=%v", first, second, group.closed)
		}
		if err := group.Activate(context.Background()); !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("Activate after compensation = %v", err)
		}
		if err := group.Close(context.Background()); !errors.Is(err, closeErr) || first.closes != 1 || second.closes != 1 {
			t.Fatalf("replayed Close = %v, closes=(%d,%d)", err, first.closes, second.closes)
		}
	})
}

func TestConsumerGroupPauseRollbackAndCompensation(t *testing.T) {
	t.Run("rollback succeeds", func(t *testing.T) {
		pauseErr := errors.New("pause")
		first := &fakeLifecycleConsumer{}
		second := &fakeLifecycleConsumer{pauseErr: pauseErr}
		group := &consumerGroup{consumers: []shimSubscriber.Consumer{first, second}, active: true}
		if err := group.Pause(context.Background()); !errors.Is(err, pauseErr) {
			t.Fatalf("Pause error = %v", err)
		}
		if first.pauses != 1 || first.activates != 1 || first.closes != 0 || !group.active || group.closed {
			t.Fatalf("first=%#v group active=%v closed=%v", first, group.active, group.closed)
		}
		second.pauseErr = nil
		if err := group.Pause(context.Background()); err != nil {
			t.Fatalf("Pause retry: %v", err)
		}
		if group.active || first.pauses != 2 || second.pauses != 2 {
			t.Fatalf("retry lifecycle first=%#v second=%#v active=%v", first, second, group.active)
		}
	})

	t.Run("rollback failure closes every consumer", func(t *testing.T) {
		pauseErr := errors.New("pause")
		rollbackErr := errors.New("rollback activate")
		closeErr := errors.New("cleanup")
		first := &fakeLifecycleConsumer{activateErr: rollbackErr}
		second := &fakeLifecycleConsumer{pauseErr: pauseErr, closeErr: closeErr}
		group := &consumerGroup{consumers: []shimSubscriber.Consumer{first, second}, active: true}
		err := group.Pause(context.Background())
		if !errors.Is(err, pauseErr) || !errors.Is(err, rollbackErr) || !errors.Is(err, closeErr) {
			t.Fatalf("Pause error = %v", err)
		}
		if !group.closed || first.closes != 1 || second.closes != 1 {
			t.Fatalf("compensation first=%#v second=%#v closed=%v", first, second, group.closed)
		}
		if err := group.Pause(context.Background()); !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("Pause after compensation = %v", err)
		}
	})
}

func TestConsumerGroupClosedStateAndIdempotentClose(t *testing.T) {
	closeErr := errors.New("terminate")
	first := &fakeLifecycleConsumer{closeErr: closeErr}
	second := &fakeLifecycleConsumer{}
	group := &consumerGroup{consumers: []shimSubscriber.Consumer{first, second}, active: true}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := group.Close(canceled); !errors.Is(err, closeErr) {
		t.Fatalf("first Close error = %v", err)
	}
	if err := group.Close(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("second Close error = %v", err)
	}
	if first.closes != 1 || second.closes != 1 {
		t.Fatalf("Close calls = (%d,%d), want (1,1)", first.closes, second.closes)
	}
	if err := group.Activate(context.Background()); !errors.Is(err, ErrConsumerClosed) {
		t.Fatalf("Activate after close = %v", err)
	}
	if err := group.Pause(context.Background()); !errors.Is(err, ErrConsumerClosed) {
		t.Fatalf("Pause after close = %v", err)
	}
	if err := group.Close(canceled); !errors.Is(err, closeErr) {
		t.Fatalf("replayed Close with canceled context = %v", err)
	}
}

func TestPrepareConsumerPausesBeforeRegisteringDelivery(t *testing.T) {
	receiver := &fakeNativeReceiver{deliverOnRegister: validInboundMessage()}
	var deliveries int
	consumer, err := prepareNativeConsumer(context.Background(), receiver, shimSubscriber.Binding{Group: "orders", BrokerID: "broker-a", Epoch: 1, Destination: "queue"}, func(context.Context, shimSubscriber.Delivery) error {
		deliveries++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if consumer == nil {
		t.Fatal("prepareConsumer returned nil consumer")
	}
	want := []string{"start", "pause", "register"}
	if !reflect.DeepEqual(receiver.calls, want) {
		t.Fatalf("native calls = %v, want %v", receiver.calls, want)
	}
	if deliveries != 1 {
		t.Fatalf("registered callback deliveries = %d, want 1", deliveries)
	}
}

func TestConsumerCloseMarksClosedWhenTerminateFails(t *testing.T) {
	terminateErr := errors.New("terminate")
	receiver := &fakeNativeReceiver{terminateErr: terminateErr}
	consumer := newConsumer(receiver, shimSubscriber.Binding{Group: "orders", BrokerID: "broker-a", Epoch: 1, Destination: "queue"}, func(context.Context, shimSubscriber.Delivery) error { return nil })
	if err := consumer.Close(context.Background()); !errors.Is(err, terminateErr) {
		t.Fatalf("first Close error = %v", err)
	}
	select {
	case <-consumer.ctx.Done():
	default:
		t.Fatal("consumer context was not canceled after termination attempt")
	}
	if err := consumer.Close(context.Background()); !errors.Is(err, terminateErr) {
		t.Fatalf("second Close error = %v", err)
	}
	if receiver.terminates != 1 {
		t.Fatalf("Terminate calls = %d, want 1", receiver.terminates)
	}
	if err := consumer.Activate(context.Background()); !errors.Is(err, ErrConsumerClosed) || receiver.resumes != 0 {
		t.Fatalf("Activate after failed close = %v, resumes=%d", err, receiver.resumes)
	}
	if err := consumer.Pause(context.Background()); !errors.Is(err, ErrConsumerClosed) || receiver.pauses != 0 {
		t.Fatalf("Pause after failed close = %v, pauses=%d", err, receiver.pauses)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumer.Activate(canceled); !errors.Is(err, ErrConsumerClosed) {
		t.Fatalf("Activate after close with canceled context = %v", err)
	}
}

func TestPrepareConsumerReturnsCleanupError(t *testing.T) {
	setupErr := errors.New("pause")
	cleanupErr := errors.New("terminate")
	receiver := &fakeNativeReceiver{pauseErr: setupErr, terminateErr: cleanupErr}
	consumer, err := prepareNativeConsumer(context.Background(), receiver, testBinding(), func(context.Context, shimSubscriber.Delivery) error { return nil })
	if consumer != nil || !errors.Is(err, setupErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("prepareNativeConsumer result = %#v, %v", consumer, err)
	}
	if receiver.terminates != 1 {
		t.Fatalf("Terminate calls = %d, want 1", receiver.terminates)
	}
}

func TestConsumerCallbackDisposesUnretainedErrors(t *testing.T) {
	tests := []struct {
		name    string
		deliver func(context.Context, shimSubscriber.Delivery) error
		outcome config.MessageSettlementOutcome
	}{
		{name: "metadata validation", deliver: func(context.Context, shimSubscriber.Delivery) error { return errors.New("invalid metadata") }, outcome: config.PersistentReceiverRejectedOutcome},
		{name: "blocked lane", deliver: func(context.Context, shimSubscriber.Delivery) error {
			return &shimSubscriber.BlockedError{Group: "orders"}
		}, outcome: config.PersistentReceiverRejectedOutcome},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receiver := &fakeNativeReceiver{}
			consumer := newConsumer(receiver, shimSubscriber.Binding{Group: "orders", BrokerID: "broker-a", Epoch: 1, Destination: "queue"}, test.deliver)
			inbound := validInboundMessage()
			consumer.receive(inbound)
			if receiver.settles != 1 || receiver.lastOutcome != test.outcome || !inbound.disposed {
				t.Fatalf("settles=%d outcome=%v disposed=%v", receiver.settles, receiver.lastOutcome, inbound.disposed)
			}
		})
	}
}

func TestConsumerCallbackPreservesRetainedDelivery(t *testing.T) {
	receiver := &fakeNativeReceiver{}
	consumer := newConsumer(receiver, shimSubscriber.Binding{Group: "orders", BrokerID: "broker-a", Epoch: 1, Destination: "queue"}, func(context.Context, shimSubscriber.Delivery) error {
		return &shimSubscriber.DeliveryError{Err: errors.New("application failed"), Retained: true}
	})
	inbound := validInboundMessage()
	consumer.receive(inbound)
	if receiver.settles != 0 || inbound.disposed {
		t.Fatalf("retained delivery settles=%d disposed=%v", receiver.settles, inbound.disposed)
	}
}

func TestDeliveryHeadersExcludeRoutingEnvelope(t *testing.T) {
	inbound := validInboundMessage()
	inbound.properties["application"] = "kept"
	inbound.properties[config.QueuePartitionKey] = "partition"
	delivery, err := newDelivery(&fakeNativeReceiver{}, inbound)
	if err != nil {
		t.Fatal(err)
	}
	headers := delivery.Message().Headers
	if !reflect.DeepEqual(headers, map[string]string{"application": "kept"}) {
		t.Fatalf("headers = %#v", headers)
	}
}

func TestConsumerGroupDeliverySettlesOnOriginatingNativeFlow(t *testing.T) {
	tests := []struct {
		name    string
		settle  func(shimSubscriber.Delivery) error
		outcome config.MessageSettlementOutcome
	}{
		{name: "ack", settle: func(delivery shimSubscriber.Delivery) error { return delivery.Ack(context.Background()) }, outcome: config.PersistentReceiverAcceptedOutcome},
		{name: "fail", settle: func(delivery shimSubscriber.Delivery) error { return delivery.(*Delivery).Fail(context.Background()) }, outcome: config.PersistentReceiverFailedOutcome},
		{name: "reject", settle: func(delivery shimSubscriber.Delivery) error {
			return delivery.(shimSubscriber.RejectableDelivery).Reject(context.Background())
		}, outcome: config.PersistentReceiverRejectedOutcome},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first := &fakeNativeReceiver{}
			second := &fakeNativeReceiver{}
			var delivery shimSubscriber.Delivery
			firstConsumer := newConsumer(first, testBinding(), func(_ context.Context, received shimSubscriber.Delivery) error {
				delivery = received
				return &shimSubscriber.DeliveryError{Err: errors.New("retain"), Retained: true}
			})
			_ = newConsumer(second, testBinding(), func(context.Context, shimSubscriber.Delivery) error { return nil })

			inbound := validInboundMessage()
			firstConsumer.receive(inbound)
			if delivery == nil {
				t.Fatal("delivery callback was not invoked")
			}
			if err := test.settle(delivery); err != nil {
				t.Fatal(err)
			}
			if test.outcome == config.PersistentReceiverAcceptedOutcome {
				if first.acks != 1 || first.settles != 0 {
					t.Fatalf("originating flow acks=%d settles=%d", first.acks, first.settles)
				}
			} else if first.acks != 0 || first.settles != 1 || first.lastOutcome != test.outcome {
				t.Fatalf("originating flow acks=%d settles=%d outcome=%v", first.acks, first.settles, first.lastOutcome)
			}
			if second.acks != 0 || second.settles != 0 || !inbound.disposed {
				t.Fatalf("other flow acks=%d settles=%d disposed=%v", second.acks, second.settles, inbound.disposed)
			}
		})
	}
}

func TestDeliveryAckFailAndDisposeLifecycle(t *testing.T) {
	t.Run("ack retries after settlement error", func(t *testing.T) {
		receiver := &fakeNativeReceiver{ackErr: errors.New("transport")}
		inbound := validInboundMessage()
		delivery, err := newDelivery(receiver, inbound)
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(context.Background()); err == nil {
			t.Fatal("Ack succeeded")
		}
		if inbound.disposed {
			t.Fatal("failed Ack disposed message needed for retry")
		}
		receiver.ackErr = nil
		if err := delivery.Ack(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !inbound.disposed || receiver.acks != 2 {
			t.Fatalf("disposed=%v acks=%d", inbound.disposed, receiver.acks)
		}
		if err := delivery.Ack(context.Background()); err != nil || receiver.acks != 2 {
			t.Fatalf("replayed Ack = %v, calls=%d", err, receiver.acks)
		}
	})

	t.Run("fail settles failed once", func(t *testing.T) {
		receiver := &fakeNativeReceiver{}
		inbound := validInboundMessage()
		delivery, err := newDelivery(receiver, inbound)
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Fail(context.Background()); err != nil {
			t.Fatal(err)
		}
		delivery.Dispose()
		if receiver.settles != 1 || receiver.lastOutcome != config.PersistentReceiverFailedOutcome || !inbound.disposed {
			t.Fatalf("settles=%d outcome=%v disposed=%v", receiver.settles, receiver.lastOutcome, inbound.disposed)
		}
	})
}

func TestNewDeliveryRejectsInvalidMetadata(t *testing.T) {
	inbound := validInboundMessage()
	delete(inbound.properties, PropertyOriginalTopic)
	if _, err := newDelivery(&fakeNativeReceiver{}, inbound); !errors.Is(err, ErrInvalidInboundMetadata) {
		t.Fatalf("newDelivery error = %v", err)
	}
}

func testBinding() shimSubscriber.Binding {
	return shimSubscriber.Binding{Group: "orders", BrokerID: "broker-a", Epoch: 1, Destination: "queue"}
}

func validBrokerMessage(epoch uint64) shimPublisher.BrokerMessage {
	digest := sha256.Sum256([]byte("key"))
	return shimPublisher.BrokerMessage{
		EventID: "event-1", Payload: []byte("payload"), Topic: "orders/created",
		Destination: "_swlb/v1/data/orders/epoch/7", Epoch: epoch,
		Properties: map[string]string{
			"application":                        "value",
			shimPublisher.PropertyEventID:        "event-1",
			shimPublisher.PropertyScalingGroup:   "orders",
			shimPublisher.PropertyBusinessHash:   routingHash(digest),
			shimPublisher.PropertyHashContract:   "orders-v1",
			shimPublisher.PropertyLibraryVersion: "lib-v1",
			shimPublisher.PropertyEpoch:          "7",
		},
	}
}

func validRoutedMessage() RoutedMessage {
	return RoutedMessage{
		Destination: "_swlb/v1/data/orders/epoch/1", OriginalTopic: "orders/created",
		Payload: []byte("payload"), EventID: "event-1", ScalingGroup: "orders",
		BusinessHash: sha256.Sum256([]byte("key")), HashContract: "orders-v1",
		LibraryVersion: "lib-v1", Epoch: 1,
	}
}

func routingHash(digest [32]byte) string {
	const hex = "0123456789abcdef"
	encoded := make([]byte, len(digest)*2)
	for i, b := range digest {
		encoded[i*2] = hex[b>>4]
		encoded[i*2+1] = hex[b&15]
	}
	return string(encoded)
}

type fakeOutboundMessage struct {
	disposed bool
}

func (m *fakeOutboundMessage) Dispose()                                  { m.disposed = true }
func (m *fakeOutboundMessage) IsDisposed() bool                          { return m.disposed }
func (m *fakeOutboundMessage) GetProperties() sdt.Map                    { return nil }
func (m *fakeOutboundMessage) GetProperty(string) (sdt.Data, bool)       { return nil, false }
func (m *fakeOutboundMessage) HasProperty(string) bool                   { return false }
func (m *fakeOutboundMessage) GetPayloadAsBytes() ([]byte, bool)         { return nil, false }
func (m *fakeOutboundMessage) GetPayloadAsString() (string, bool)        { return "", false }
func (m *fakeOutboundMessage) GetPayloadAsMap() (sdt.Map, bool)          { return nil, false }
func (m *fakeOutboundMessage) GetPayloadAsStream() (sdt.Stream, bool)    { return nil, false }
func (m *fakeOutboundMessage) GetCorrelationID() (string, bool)          { return "", false }
func (m *fakeOutboundMessage) GetExpiration() time.Time                  { return time.Time{} }
func (m *fakeOutboundMessage) GetSequenceNumber() (int64, bool)          { return 0, false }
func (m *fakeOutboundMessage) GetPriority() (int, bool)                  { return 0, false }
func (m *fakeOutboundMessage) GetHTTPContentType() (string, bool)        { return "", false }
func (m *fakeOutboundMessage) GetHTTPContentEncoding() (string, bool)    { return "", false }
func (m *fakeOutboundMessage) GetApplicationMessageID() (string, bool)   { return "", false }
func (m *fakeOutboundMessage) GetApplicationMessageType() (string, bool) { return "", false }
func (m *fakeOutboundMessage) GetClassOfService() int                    { return 0 }
func (m *fakeOutboundMessage) String() string                            { return "fake outbound" }

type fakeAsyncPublisher struct {
	listener     solace.MessagePublishReceiptListener
	context      any
	lastMessage  message.OutboundMessage
	publishErr   error
	startErr     error
	terminateErr error
	terminates   int
}

func (f *fakeAsyncPublisher) Publish(outbound message.OutboundMessage, _ *resource.Topic, _ config.MessagePropertiesConfigurationProvider, correlation interface{}) error {
	f.lastMessage = outbound
	f.context = correlation
	return f.publishErr
}
func (f *fakeAsyncPublisher) SetMessagePublishReceiptListener(listener solace.MessagePublishReceiptListener) {
	f.listener = listener
}
func (f *fakeAsyncPublisher) Start() error                  { return f.startErr }
func (f *fakeAsyncPublisher) Terminate(time.Duration) error { f.terminates++; return f.terminateErr }
func (f *fakeAsyncPublisher) receipt(err error, persisted bool) {
	f.listener(fakePublishReceipt{context: f.context, outbound: f.lastMessage, err: err, persisted: persisted})
}

type fakePublishReceipt struct {
	context   any
	outbound  message.OutboundMessage
	err       error
	persisted bool
}

func (r fakePublishReceipt) GetUserContext() interface{}         { return r.context }
func (r fakePublishReceipt) GetTimeStamp() time.Time             { return time.Unix(123, 0) }
func (r fakePublishReceipt) GetMessage() message.OutboundMessage { return r.outbound }
func (r fakePublishReceipt) GetError() error                     { return r.err }
func (r fakePublishReceipt) IsPersisted() bool                   { return r.persisted }

type fakeReceiverService struct {
	connected bool
}

func (f *fakeReceiverService) IsConnected() bool { return f.connected }
func (f *fakeReceiverService) CreatePersistentMessageReceiverBuilder() solace.PersistentMessageReceiverBuilder {
	return nil
}

type fakeLifecycleConsumer struct {
	activateErr error
	pauseErr    error
	closeErr    error
	activates   int
	pauses      int
	closes      int
}

func (f *fakeLifecycleConsumer) Activate(context.Context) error {
	f.activates++
	return f.activateErr
}
func (f *fakeLifecycleConsumer) Pause(context.Context) error {
	f.pauses++
	return f.pauseErr
}
func (f *fakeLifecycleConsumer) Close(ctx context.Context) error {
	f.closes++
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.closeErr
}

// fakeNativeReceiver exposes the narrow lifecycle seam used by Consumer.
type fakeNativeReceiver struct {
	handler                                            solace.MessageHandler
	startErr, pauseErr, resumeErr, terminateErr        error
	ackErr, settleErr                                  error
	starts, pauses, resumes, terminates, acks, settles int
	lastOutcome                                        config.MessageSettlementOutcome
	calls                                              []string
	deliverOnRegister                                  message.InboundMessage
}

func (f *fakeNativeReceiver) ReceiveAsync(handler solace.MessageHandler) error {
	f.calls = append(f.calls, "register")
	f.handler = handler
	if f.deliverOnRegister != nil {
		handler(f.deliverOnRegister)
	}
	return nil
}
func (f *fakeNativeReceiver) Start() error {
	f.calls = append(f.calls, "start")
	f.starts++
	return f.startErr
}
func (f *fakeNativeReceiver) Pause() error {
	f.calls = append(f.calls, "pause")
	f.pauses++
	return f.pauseErr
}
func (f *fakeNativeReceiver) Resume() error                    { f.resumes++; return f.resumeErr }
func (f *fakeNativeReceiver) Ack(message.InboundMessage) error { f.acks++; return f.ackErr }
func (f *fakeNativeReceiver) Settle(_ message.InboundMessage, outcome config.MessageSettlementOutcome) error {
	f.settles++
	f.lastOutcome = outcome
	return f.settleErr
}
func (f *fakeNativeReceiver) Terminate(time.Duration) error { f.terminates++; return f.terminateErr }

func validInboundMessage() *fakeInboundMessage {
	digest := sha256.Sum256([]byte("key"))
	return &fakeInboundMessage{
		payload:              []byte("payload"),
		applicationMessageID: "event-1",
		properties: sdt.Map{
			PropertyOriginalTopic:  "orders/created",
			PropertyEventID:        "event-1",
			PropertyScalingGroup:   "orders",
			PropertyBusinessHash:   routingHash(digest),
			PropertyHashContract:   "orders-v1",
			PropertyLibraryVersion: "lib-v1",
			PropertyRoutingEpoch:   "1",
		},
	}
}

type fakeInboundMessage struct {
	properties           sdt.Map
	payload              []byte
	applicationMessageID string
	disposed             bool
}

func (m *fakeInboundMessage) Dispose()               { m.disposed = true }
func (m *fakeInboundMessage) IsDisposed() bool       { return m.disposed }
func (m *fakeInboundMessage) GetProperties() sdt.Map { return m.properties }
func (m *fakeInboundMessage) GetProperty(key string) (sdt.Data, bool) {
	v, ok := m.properties[key]
	return v, ok
}
func (m *fakeInboundMessage) HasProperty(key string) bool            { _, ok := m.properties[key]; return ok }
func (m *fakeInboundMessage) GetPayloadAsBytes() ([]byte, bool)      { return m.payload, m.payload != nil }
func (m *fakeInboundMessage) GetPayloadAsString() (string, bool)     { return "", false }
func (m *fakeInboundMessage) GetPayloadAsMap() (sdt.Map, bool)       { return nil, false }
func (m *fakeInboundMessage) GetPayloadAsStream() (sdt.Stream, bool) { return nil, false }
func (m *fakeInboundMessage) GetCorrelationID() (string, bool)       { return "", false }
func (m *fakeInboundMessage) GetExpiration() time.Time               { return time.Time{} }
func (m *fakeInboundMessage) GetSequenceNumber() (int64, bool)       { return 0, false }
func (m *fakeInboundMessage) GetPriority() (int, bool)               { return 0, false }
func (m *fakeInboundMessage) GetHTTPContentType() (string, bool)     { return "", false }
func (m *fakeInboundMessage) GetHTTPContentEncoding() (string, bool) { return "", false }
func (m *fakeInboundMessage) GetApplicationMessageID() (string, bool) {
	return m.applicationMessageID, m.applicationMessageID != ""
}
func (m *fakeInboundMessage) GetApplicationMessageType() (string, bool) { return "", false }
func (m *fakeInboundMessage) GetClassOfService() int                    { return 0 }
func (m *fakeInboundMessage) String() string                            { return "fake inbound" }
func (m *fakeInboundMessage) GetDestinationName() string                { return "queue" }
func (m *fakeInboundMessage) GetTimeStamp() (time.Time, bool)           { return time.Time{}, false }
func (m *fakeInboundMessage) GetSenderTimestamp() (time.Time, bool)     { return time.Time{}, false }
func (m *fakeInboundMessage) GetSenderID() (string, bool)               { return "", false }
func (m *fakeInboundMessage) GetReplicationGroupMessageID() (rgmid.ReplicationGroupMessageID, bool) {
	return nil, false
}
func (m *fakeInboundMessage) GetMessageDiscardNotification() message.MessageDiscardNotification {
	return fakeDiscardNotification{}
}
func (m *fakeInboundMessage) IsRedelivered() bool                               { return false }
func (m *fakeInboundMessage) GetCacheRequestID() (message.CacheRequestID, bool) { return 0, false }
func (m *fakeInboundMessage) GetCacheStatus() message.CacheStatus               { return message.Live }

type fakeDiscardNotification struct{}

func (fakeDiscardNotification) HasBrokerDiscardIndication() bool   { return false }
func (fakeDiscardNotification) HasInternalDiscardIndication() bool { return false }
