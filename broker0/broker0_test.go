package broker0

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
)

func activeSnapshot(revision uint64) control.MembershipSnapshot {
	return control.MembershipSnapshot{
		Version:           control.SnapshotVersion,
		ScalingGroup:      "orders",
		Revision:          revision,
		Epoch:             1,
		Phase:             control.PhaseActive,
		HashContract:      "orders-v1",
		Algorithm:         control.AlgorithmSHA256BigEndianModulo,
		CurrentMembership: control.Membership{"broker-b", "broker-a"},
		Queue:             control.QueueInfo{Name: "orders-q", Durable: true},
		Destination:       control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders"},
	}
}

func commandEnvelope(messageID, participant string) control.CommandEnvelope {
	issuedAt := time.Unix(99, 0).UTC()
	return control.CommandEnvelope{
		Version: control.ProtocolVersion, MessageID: messageID, Namespace: "swlb", Group: "orders",
		TransitionID: "move-1", Epoch: 2, Phase: control.PhasePaused,
		Participant: participant, Role: control.RolePublisher, IssuedAt: issuedAt, Deadline: issuedAt.Add(time.Minute),
	}
}

func acknowledgementEnvelope(messageID string) control.AcknowledgementEnvelope {
	return control.AcknowledgementEnvelope{
		Version: control.ProtocolVersion, MessageID: messageID, CommandID: "command-1", Namespace: "swlb",
		Group: "orders", TransitionID: "move-1", Epoch: 2, Phase: control.PhasePrepare,
		Participant: "subscriber-1", Role: control.RoleSubscriber, Ready: true,
		ObservedAt: time.Unix(100, 0).UTC(),
	}
}

func telemetryEnvelope(messageID string) control.TelemetryEnvelope {
	return control.TelemetryEnvelope{
		Version: control.ProtocolVersion, MessageID: messageID, Namespace: "swlb", Group: "orders",
		TransitionID: "move-1", Epoch: 1, Phase: control.PhaseDrain, Participant: "observer-1",
		Role: control.RoleObserver, SourceBroker: "broker-a", SourceQueue: "orders.e1", ObservedAt: time.Unix(101, 0).UTC(),
		Queued: control.MetricCount{Known: true}, Stored: control.MetricCount{Known: true}, Unacked: control.MetricCount{Known: true},
	}
}

func TestSharedEnvelopesAreStrictlyParsed(t *testing.T) {
	command := commandEnvelope("command-1", "publisher-1")
	commandData, err := encodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	commandMessage, err := parseMessage(KindCommand, "command-1", commandData)
	if err != nil {
		t.Fatal(err)
	}
	if commandMessage.Command == nil || *commandMessage.Command != command {
		t.Fatalf("parsed command = %#v", commandMessage.Command)
	}
	if _, err := parseMessage(KindCommand, "different", commandData); err == nil {
		t.Fatal("accepted mismatched command transport operation ID")
	}

	acknowledgement := acknowledgementEnvelope("ack-1")
	data, err := encodeAcknowledgement(acknowledgement)
	if err != nil {
		t.Fatal(err)
	}
	message, err := parseMessage(KindAcknowledgement, "ack-1", data)
	if err != nil {
		t.Fatal(err)
	}
	if message.Acknowledgement == nil || *message.Acknowledgement != acknowledgement {
		t.Fatalf("parsed acknowledgement = %#v", message.Acknowledgement)
	}
	if _, err := parseMessage(KindAcknowledgement, "different", data); err == nil {
		t.Fatal("accepted mismatched transport operation ID")
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	raw["unexpected"] = true
	unknown, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseMessage(KindAcknowledgement, "ack-1", unknown); err == nil {
		t.Fatal("accepted unknown envelope field")
	}

	snapshotData, err := encodeMembershipSnapshot(activeSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	snapshotMessage, err := parseMessage(KindMembershipSnapshot, "snapshot-1", snapshotData)
	if err != nil {
		t.Fatal(err)
	}
	if snapshotMessage.Snapshot == nil || !snapshotMessage.Snapshot.CurrentMembership.Equal(control.Membership{"broker-b", "broker-a"}) {
		t.Fatalf("snapshot changed authoritative order: %#v", snapshotMessage.Snapshot)
	}
}

type memoryOperations struct {
	mu          sync.Mutex
	operations  map[string]bool
	containsErr error
	recordErr   error
	recordErrs  []error
	records     []string
}

func newMemoryOperations() *memoryOperations {
	return &memoryOperations{operations: make(map[string]bool)}
}

func (operations *memoryOperations) Contains(ctx context.Context, operationID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	operations.mu.Lock()
	defer operations.mu.Unlock()
	if operations.containsErr != nil {
		return false, operations.containsErr
	}
	return operations.operations[operationID], nil
}

func (operations *memoryOperations) Record(ctx context.Context, operationID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	operations.mu.Lock()
	defer operations.mu.Unlock()
	operations.records = append(operations.records, operationID)
	if len(operations.recordErrs) != 0 {
		err := operations.recordErrs[0]
		operations.recordErrs = operations.recordErrs[1:]
		if err != nil {
			return err
		}
	}
	if operations.recordErr != nil {
		return operations.recordErr
	}
	operations.operations[operationID] = true
	return nil
}

type fakeNativePublisher struct {
	mu           sync.Mutex
	publications []Publication
	err          error
}

func (publisher *fakeNativePublisher) PublishPersistent(_ context.Context, publication Publication) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	publication.Payload = append([]byte(nil), publication.Payload...)
	publisher.publications = append(publisher.publications, publication)
	return publisher.err
}

func TestPersistentPublisherDeduplicatesCompletedOperations(t *testing.T) {
	operations := newMemoryOperations()
	native := &fakeNativePublisher{}
	publisher, err := NewPersistentControlPublisher(native, operations, DestinationResolverFunc(func(kind Kind, group string) (string, error) {
		return "swlb/control/" + group + "/" + string(kind), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	update := controller.ControlUpdate{OperationID: "publish-1", Group: "orders", Snapshot: activeSnapshot(1)}
	if err := publisher.Publish(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if len(native.publications) != 1 {
		t.Fatalf("native publication count = %d, want 1", len(native.publications))
	}
	publication := native.publications[0]
	if publication.Kind != KindMembershipSnapshot || publication.OperationID != update.OperationID {
		t.Fatalf("publication metadata = %#v", publication)
	}
	parsed, err := control.ParseMembershipSnapshot(publication.Payload)
	if err != nil || parsed.Revision != 1 {
		t.Fatalf("published snapshot = %#v, %v", parsed, err)
	}
}

func TestRegistrationEnvelopeTransport(t *testing.T) {
	registration := control.RegistrationEnvelope{
		Version: control.ProtocolVersion, MessageID: "registration-1", Namespace: "swlb", Group: "orders",
		Epoch: 1, Phase: control.PhaseActive, Participant: "publisher-1", Role: control.RolePublisher,
		LibraryVersion: "routing-v1", ObservedAt: time.Unix(98, 0).UTC(),
	}
	data, err := encodeRegistration(registration)
	if err != nil {
		t.Fatal(err)
	}
	message, err := parseMessage(KindRegistration, registration.MessageID, data)
	if err != nil {
		t.Fatal(err)
	}
	if message.Registration == nil || *message.Registration != registration {
		t.Fatalf("parsed registration = %#v", message.Registration)
	}
}

func TestParticipantPublicationsRequireScopedResolver(t *testing.T) {
	publisher, err := NewPersistentControlPublisher(&fakeNativePublisher{}, newMemoryOperations(), DestinationResolverFunc(func(Kind, string) (string, error) {
		return "shared", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	registration := control.RegistrationEnvelope{
		Version: control.ProtocolVersion, MessageID: "registration-scoped", Namespace: "swlb", Group: "orders",
		Epoch: 1, Phase: control.PhaseActive, Participant: "publisher-1", Role: control.RolePublisher,
		LibraryVersion: "routing-v1", ObservedAt: time.Unix(98, 0).UTC(),
	}
	if err := publisher.PublishRegistration(context.Background(), registration); err == nil {
		t.Fatal("registration fell back to shared destination")
	}
	ack := acknowledgementEnvelope("ack-scoped")
	if err := publisher.PublishAcknowledgement(context.Background(), ack); err == nil {
		t.Fatal("acknowledgement fell back to shared destination")
	}
	telemetry := telemetryEnvelope("telemetry-scoped")
	if err := publisher.PublishTelemetry(context.Background(), telemetry); err == nil {
		t.Fatal("telemetry fell back to shared destination")
	}
}

func TestPersistentPublisherPublishesCommandToParticipantTopicOnce(t *testing.T) {
	operations := newMemoryOperations()
	native := &fakeNativePublisher{}
	resolverCalled := false
	publisher, err := NewPersistentControlPublisher(native, operations, DestinationResolverFunc(func(Kind, string) (string, error) {
		resolverCalled = true
		return "", errors.New("shared resolver must not route participant commands")
	}))
	if err != nil {
		t.Fatal(err)
	}
	command := commandEnvelope("command-pause-1", "publisher-1")
	if err := publisher.PublishCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if resolverCalled {
		t.Fatal("shared destination resolver was used for participant command")
	}
	if len(native.publications) != 1 {
		t.Fatalf("native publication count = %d, want 1", len(native.publications))
	}
	publication := native.publications[0]
	if publication.Destination != "swlb/control/orders/commands/publisher/publisher-1" || publication.Kind != KindCommand || publication.OperationID != command.MessageID {
		t.Fatalf("command publication metadata = %#v", publication)
	}
	parsed, err := control.ParseCommandEnvelope(publication.Payload)
	if err != nil || parsed != command {
		t.Fatalf("published command = %#v, %v", parsed, err)
	}
}

func TestPersistentPublisherRetriesSameOperationAfterRecordFailure(t *testing.T) {
	operations := newMemoryOperations()
	operations.recordErr = errors.New("disk full")
	native := &fakeNativePublisher{}
	publisher, err := NewPersistentControlPublisher(native, operations, DestinationResolverFunc(func(Kind, string) (string, error) { return "updates", nil }))
	if err != nil {
		t.Fatal(err)
	}
	update := controller.ControlUpdate{OperationID: "publish-2", Group: "orders", Snapshot: activeSnapshot(1)}
	if err := publisher.Publish(context.Background(), update); err == nil {
		t.Fatal("record failure was hidden")
	}
	operations.recordErr = nil
	if err := publisher.Publish(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if len(native.publications) != 2 || native.publications[0].OperationID != native.publications[1].OperationID {
		t.Fatalf("retry did not preserve operation ID: %#v", native.publications)
	}
}

func TestJSONOperationStorePersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "operations.json")
	store, err := OpenJSONOperationStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Record(context.Background(), "operation-1"); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenJSONOperationStore(path)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := restarted.Contains(context.Background(), "operation-1")
	if err != nil || !completed {
		t.Fatalf("persisted operation = %v, %v", completed, err)
	}
}

func TestJSONOperationStoreBoundsRetentionWithoutEarlyEviction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.json")
	now := time.Unix(1_000, 0).UTC()
	store, err := OpenJSONOperationStoreWithOptions(path, OperationStoreOptions{
		MaxEntries: 2, RedeliveryHorizon: time.Hour, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, operationID := range []string{"operation-1", "operation-2"} {
		if err := store.Record(context.Background(), operationID); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Record(context.Background(), "operation-3"); !errors.Is(err, ErrOperationStoreFull) {
		t.Fatalf("full store error = %v", err)
	}
	for _, operationID := range []string{"operation-1", "operation-2"} {
		if completed, _ := store.Contains(context.Background(), operationID); !completed {
			t.Fatalf("unexpired operation %q was evicted", operationID)
		}
	}

	now = now.Add(time.Hour)
	if err := store.Record(context.Background(), "operation-3"); err != nil {
		t.Fatal(err)
	}
	if completed, _ := store.Contains(context.Background(), "operation-1"); completed {
		t.Fatal("expired operation remained retained")
	}
	restarted, err := OpenJSONOperationStoreWithOptions(path, OperationStoreOptions{
		MaxEntries: 2, RedeliveryHorizon: time.Hour, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed, _ := restarted.Contains(context.Background(), "operation-3"); !completed {
		t.Fatal("replacement operation was not durable")
	}
}

type fakeDelivery struct {
	kind                 Kind
	operationID          string
	payload              []byte
	authenticated        string
	authenticatedPresent bool
	scopeKind            Kind
	scopeGroup           string
	scopeRole            control.ParticipantRole
	mu                   sync.Mutex
	acks                 int
	rejections           int
	ackErr               error
	rejectErr            error
}

func (delivery *fakeDelivery) Kind() Kind          { return delivery.kind }
func (delivery *fakeDelivery) OperationID() string { return delivery.operationID }
func (delivery *fakeDelivery) Payload() []byte     { return delivery.payload }
func (delivery *fakeDelivery) AuthenticatedParticipant() (string, bool) {
	return delivery.authenticated, delivery.authenticatedPresent
}
func (delivery *fakeDelivery) AuthenticatedScope() (Kind, string, control.ParticipantRole) {
	kind, group, role := delivery.scopeKind, delivery.scopeGroup, delivery.scopeRole
	if kind == "" {
		kind = delivery.kind
	}
	if group == "" {
		group = "orders"
	}
	if role == "" {
		switch kind {
		case KindRegistration:
			role = control.RolePublisher
		case KindAcknowledgement:
			role = control.RoleSubscriber
		case KindTelemetry:
			role = control.RoleObserver
		}
	}
	return kind, group, role
}
func (delivery *fakeDelivery) Ack(context.Context) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	delivery.acks++
	return delivery.ackErr
}

func (delivery *fakeDelivery) Reject(context.Context) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	delivery.rejections++
	return delivery.rejectErr
}

func (delivery *fakeDelivery) ackCount() int {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	return delivery.acks
}

func (delivery *fakeDelivery) rejectionCount() int {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	return delivery.rejections
}

type fakeReceiver struct{ closed bool }

func (*fakeReceiver) Receive(context.Context) (Delivery, error) { return nil, errors.New("unused") }
func (receiver *fakeReceiver) Close() error {
	receiver.closed = true
	return nil
}

type sequenceControllerReceiver struct {
	deliveries []Delivery
	next       int
}

func (receiver *sequenceControllerReceiver) Receive(ctx context.Context) (Delivery, error) {
	if receiver.next < len(receiver.deliveries) {
		delivery := receiver.deliveries[receiver.next]
		receiver.next++
		return delivery, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*sequenceControllerReceiver) Close() error { return nil }

type inboxTestFence struct{}

func (inboxTestFence) Fence(context.Context, controller.FenceRequest) error   { return nil }
func (inboxTestFence) Unfence(context.Context, controller.FenceRequest) error { return nil }
func (inboxTestFence) VerifyFence(context.Context, controller.FenceRequest) (controller.FenceStatus, error) {
	return controller.FenceStatus{}, nil
}
func (inboxTestFence) EnableIngress(context.Context, controller.FenceRequest) error { return nil }
func (inboxTestFence) VerifyIngress(context.Context, controller.FenceRequest) (controller.IngressStatus, error) {
	return controller.IngressStatus{}, nil
}

type inboxTestPublisher struct{}

func (inboxTestPublisher) Publish(context.Context, controller.ControlUpdate) error { return nil }

type fakeControllerHandler struct {
	acknowledgements []controller.Acknowledgement
	telemetry        []controller.Telemetry
	err              error
	errs             []error
}

func (handler *fakeControllerHandler) nextError() error {
	if len(handler.errs) == 0 {
		return handler.err
	}
	err := handler.errs[0]
	handler.errs = handler.errs[1:]
	return err
}

func (handler *fakeControllerHandler) Acknowledge(acknowledgement controller.Acknowledgement) error {
	handler.acknowledgements = append(handler.acknowledgements, acknowledgement)
	return handler.nextError()
}
func (handler *fakeControllerHandler) Observe(telemetry controller.Telemetry) error {
	handler.telemetry = append(handler.telemetry, telemetry)
	return handler.nextError()
}

func TestControllerInboxAppliesRecordsThenAcknowledges(t *testing.T) {
	envelope := acknowledgementEnvelope("ack-operation")
	payload, err := encodeAcknowledgement(envelope)
	if err != nil {
		t.Fatal(err)
	}
	delivery := &fakeDelivery{kind: KindAcknowledgement, operationID: envelope.MessageID, payload: payload, authenticated: envelope.Participant, authenticatedPresent: true}
	handler := &fakeControllerHandler{}
	operations := newMemoryOperations()
	inbox, err := NewControllerInbox(&fakeReceiver{}, handler, operations)
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Handle(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if len(handler.acknowledgements) != 1 || len(operations.records) != 1 || delivery.ackCount() != 1 {
		t.Fatalf("processing = handler:%d records:%v acks:%d", len(handler.acknowledgements), operations.records, delivery.ackCount())
	}
	if got := handler.acknowledgements[0]; got.Phase != controller.PhasePrepare || got.Role != control.RoleSubscriber ||
		got.CommandID != envelope.CommandID || got.Namespace != envelope.Namespace {
		t.Fatalf("controller acknowledgement = %#v", got)
	}

	replay := &fakeDelivery{kind: KindAcknowledgement, operationID: envelope.MessageID, payload: payload, authenticated: envelope.Participant, authenticatedPresent: true}
	if err := inbox.Handle(context.Background(), replay); err != nil {
		t.Fatal(err)
	}
	if len(handler.acknowledgements) != 1 || replay.ackCount() != 1 {
		t.Fatalf("replay was reapplied or not ACKed: handler:%d ack:%d", len(handler.acknowledgements), replay.ackCount())
	}
}

func TestControllerInboxRequiresAuthenticatedRegistration(t *testing.T) {
	registration := control.RegistrationEnvelope{
		Version: control.ProtocolVersion, MessageID: "registration-missing-auth", Namespace: "swlb", Group: "orders",
		Epoch: 1, Phase: control.PhaseActive, Participant: "publisher-1", Role: control.RolePublisher,
		LibraryVersion: "routing-v1", ObservedAt: time.Unix(99, 0).UTC(),
	}
	payload, err := encodeRegistration(registration)
	if err != nil {
		t.Fatal(err)
	}
	handler := &registrationTestHandler{}
	inbox, err := NewControllerInbox(&fakeReceiver{}, handler, newMemoryOperations())
	if err != nil {
		t.Fatal(err)
	}
	delivery := &fakeDelivery{kind: KindRegistration, operationID: registration.MessageID, payload: payload}
	if err := inbox.Handle(context.Background(), delivery); err == nil {
		t.Fatal("unauthenticated registration was accepted")
	}
	if len(handler.registrations) != 0 || delivery.ackCount() != 0 {
		t.Fatal("unauthenticated registration reached handler or was acknowledged")
	}
}

func TestControllerInboxRejectsSpoofedGroupAndRole(t *testing.T) {
	envelope := acknowledgementEnvelope("ack-scope-spoof")
	payload, err := encodeAcknowledgement(envelope)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := NewControllerInbox(&fakeReceiver{}, &fakeControllerHandler{}, newMemoryOperations())
	if err != nil {
		t.Fatal(err)
	}
	for name, delivery := range map[string]*fakeDelivery{
		"group": {kind: KindAcknowledgement, operationID: envelope.MessageID, payload: payload, authenticated: envelope.Participant, authenticatedPresent: true, scopeGroup: "other", scopeRole: envelope.Role},
		"role":  {kind: KindAcknowledgement, operationID: envelope.MessageID, payload: payload, authenticated: envelope.Participant, authenticatedPresent: true, scopeGroup: envelope.Group, scopeRole: control.RolePublisher},
	} {
		t.Run(name, func(t *testing.T) {
			if err := inbox.Handle(context.Background(), delivery); err == nil {
				t.Fatal("payload scope overrode trusted queue binding")
			}
			if delivery.ackCount() != 0 {
				t.Fatal("spoofed delivery was acknowledged")
			}
		})
	}
}

func TestControllerInboxRejectsSpoofedRegistration(t *testing.T) {
	registration := control.RegistrationEnvelope{
		Version: control.ProtocolVersion, MessageID: "registration-authenticated", Namespace: "swlb", Group: "orders",
		Epoch: 1, Phase: control.PhaseActive, Participant: "publisher-1", Role: control.RolePublisher,
		LibraryVersion: "routing-v1", ObservedAt: time.Unix(99, 0).UTC(),
	}
	payload, err := encodeRegistration(registration)
	if err != nil {
		t.Fatal(err)
	}
	handler := &registrationTestHandler{}
	inbox, err := NewControllerInbox(&fakeReceiver{}, handler, newMemoryOperations())
	if err != nil {
		t.Fatal(err)
	}
	delivery := &fakeDelivery{kind: KindRegistration, operationID: registration.MessageID, payload: payload, authenticated: "intruder", authenticatedPresent: true}
	if err := inbox.Handle(context.Background(), delivery); err == nil {
		t.Fatal("spoofed registration was accepted")
	}
	if len(handler.registrations) != 0 || delivery.ackCount() != 0 {
		t.Fatal("spoofed registration reached handler or was acknowledged")
	}
}

type registrationTestHandler struct {
	fakeControllerHandler
	registrations []control.RegistrationEnvelope
}

func (h *registrationTestHandler) Register(registration control.RegistrationEnvelope) error {
	h.registrations = append(h.registrations, registration)
	return nil
}

func TestControllerInboxBindsAuthenticatedParticipant(t *testing.T) {
	envelope := acknowledgementEnvelope("ack-authenticated")
	payload, err := encodeAcknowledgement(envelope)
	if err != nil {
		t.Fatal(err)
	}
	handler := &fakeControllerHandler{}
	inbox, err := NewControllerInbox(&fakeReceiver{}, handler, newMemoryOperations())
	if err != nil {
		t.Fatal(err)
	}
	spoofed := &fakeDelivery{
		kind: KindAcknowledgement, operationID: envelope.MessageID, payload: payload,
		authenticated: "subscriber-2", authenticatedPresent: true,
	}
	if err := inbox.Handle(context.Background(), spoofed); err == nil {
		t.Fatal("acknowledgement from a different authenticated participant was accepted")
	}
	if len(handler.acknowledgements) != 0 || spoofed.ackCount() != 0 {
		t.Fatal("spoofed acknowledgement reached controller or was acknowledged")
	}

	valid := &fakeDelivery{
		kind: KindAcknowledgement, operationID: envelope.MessageID, payload: payload,
		authenticated: envelope.Participant, authenticatedPresent: true,
	}
	if err := inbox.Handle(context.Background(), valid); err != nil {
		t.Fatal(err)
	}
	if len(handler.acknowledgements) != 1 || handler.acknowledgements[0].AuthenticatedParticipant != envelope.Participant {
		t.Fatalf("authenticated acknowledgement = %#v", handler.acknowledgements)
	}
}

func TestControllerInboxLeavesFailedDeliveryUnacknowledged(t *testing.T) {
	envelope := telemetryEnvelope("telemetry-operation")
	payload, err := encodeTelemetry(envelope)
	if err != nil {
		t.Fatal(err)
	}
	delivery := &fakeDelivery{kind: KindTelemetry, operationID: envelope.MessageID, payload: payload, authenticated: envelope.Participant, authenticatedPresent: true}
	handler := &fakeControllerHandler{err: errors.New("state disk unavailable")}
	operations := newMemoryOperations()
	inbox, err := NewControllerInbox(&fakeReceiver{}, handler, operations)
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Handle(context.Background(), delivery); err == nil {
		t.Fatal("controller failure was hidden")
	}
	if delivery.ackCount() != 0 || delivery.rejectionCount() != 0 || len(operations.records) != 0 {
		t.Fatalf("failed delivery was settled: records:%v acks:%d rejects:%d", operations.records, delivery.ackCount(), delivery.rejectionCount())
	}
}

func TestControllerInboxRejectsStaleAcknowledgementAfterPhaseAdvanceAndContinues(t *testing.T) {
	now := time.Unix(5_000, 0).UTC()
	coordinator, err := controller.Open(
		controller.JSONStore{Path: filepath.Join(t.TempDir(), "controller.json")},
		inboxTestFence{}, inboxTestPublisher{}, controller.Options{Now: func() time.Time { return now }},
	)
	if err != nil {
		t.Fatal(err)
	}
	spec := controller.TransitionSpec{
		ID: "move-1", Namespace: "swlb", Group: "orders", Revision: 10, FromEpoch: 1, ToEpoch: 2,
		Current: []controller.Broker{{ID: "old", Destination: "orders"}}, Proposed: []controller.Broker{{ID: "new", Destination: "orders"}},
		Queue: control.QueueInfo{Name: "orders-q", Durable: true}, Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "orders"},
		HashContract: "orders-v1", Algorithm: control.AlgorithmSHA256BigEndianModulo,
		RoleRequirements: map[controller.Phase][]controller.ParticipantRequirement{
			controller.PhasePrepare:  {{Participant: "subscriber-1", Role: control.RoleSubscriber}},
			controller.PhasePause:    {{Participant: "publisher-1", Role: control.RolePublisher}},
			controller.PhaseDrain:    {{Participant: "subscriber-1", Role: control.RoleSubscriber}},
			controller.PhaseActivate: {{Participant: "subscriber-1", Role: control.RoleSubscriber}},
		},
	}
	if _, err := coordinator.Begin(spec); err != nil {
		t.Fatal(err)
	}
	if changed, err := coordinator.Reconcile(context.Background(), spec.Group); err != nil || !changed {
		t.Fatalf("publish PREPARE: changed=%v err=%v", changed, err)
	}
	prepareState, _ := coordinator.Group(spec.Group)
	staleEnvelope := acknowledgementEnvelope("ack-stale-after-phase")
	staleEnvelope.CommandID = prepareState.IssuedCommandIDs[controller.PhasePrepare][staleEnvelope.Participant]
	staleEnvelope.ObservedAt = now
	accepted := controller.Acknowledgement{
		CommandID: staleEnvelope.CommandID, Namespace: staleEnvelope.Namespace, Group: staleEnvelope.Group,
		TransitionID: staleEnvelope.TransitionID, Epoch: staleEnvelope.Epoch, Phase: controller.PhasePrepare,
		Participant: staleEnvelope.Participant, AuthenticatedParticipant: staleEnvelope.Participant,
		Role: staleEnvelope.Role, ObservedAt: staleEnvelope.ObservedAt,
	}
	if err := coordinator.Acknowledge(accepted); err != nil {
		t.Fatal(err)
	}
	if changed, err := coordinator.Reconcile(context.Background(), spec.Group); err != nil || !changed {
		t.Fatalf("advance to PAUSE: changed=%v err=%v", changed, err)
	}
	if changed, err := coordinator.Reconcile(context.Background(), spec.Group); err != nil || !changed {
		t.Fatalf("publish PAUSE: changed=%v err=%v", changed, err)
	}
	stalePayload, err := encodeAcknowledgement(staleEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	validEnvelope := staleEnvelope
	validEnvelope.MessageID = "ack-current-phase"
	validEnvelope.CommandID = "orders/move-1/forward/command/PAUSE/publisher/publisher-1"
	validEnvelope.Phase = control.PhasePaused
	validEnvelope.Participant = "publisher-1"
	validEnvelope.Role = control.RolePublisher
	validPayload, err := encodeAcknowledgement(validEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	stale := &fakeDelivery{kind: KindAcknowledgement, operationID: staleEnvelope.MessageID, payload: stalePayload, authenticated: staleEnvelope.Participant, authenticatedPresent: true}
	valid := &fakeDelivery{kind: KindAcknowledgement, operationID: validEnvelope.MessageID, payload: validPayload, authenticated: validEnvelope.Participant, authenticatedPresent: true, scopeRole: validEnvelope.Role}
	receiver := &sequenceControllerReceiver{deliveries: []Delivery{stale, valid}}
	operations := newMemoryOperations()
	inbox, err := NewControllerInbox(receiver, coordinator, operations)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- inbox.Run(ctx) }()
	deadline := time.After(time.Second)
	for valid.ackCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("valid acknowledgement after stale delivery was not processed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("inbox run error = %v", err)
	}
	if stale.ackCount() != 0 || stale.rejectionCount() != 1 {
		t.Fatalf("stale acknowledgement settlements: acks=%d rejects=%d", stale.ackCount(), stale.rejectionCount())
	}
	if completed, _ := operations.Contains(context.Background(), staleEnvelope.MessageID); completed {
		t.Fatal("rejected stale acknowledgement was recorded as completed")
	}
	if completed, _ := operations.Contains(context.Background(), validEnvelope.MessageID); !completed {
		t.Fatal("valid acknowledgement after stale delivery was not recorded")
	}
}

func TestControllerInboxDuplicateOperationOnlyAcknowledges(t *testing.T) {
	envelope := acknowledgementEnvelope("ack-already-completed")
	payload, err := encodeAcknowledgement(envelope)
	if err != nil {
		t.Fatal(err)
	}
	operations := newMemoryOperations()
	if err := operations.Record(context.Background(), envelope.MessageID); err != nil {
		t.Fatal(err)
	}
	handler := &fakeControllerHandler{err: errors.New("must not be called")}
	delivery := &fakeDelivery{kind: KindAcknowledgement, operationID: envelope.MessageID, payload: payload, authenticated: envelope.Participant, authenticatedPresent: true}
	inbox, err := NewControllerInbox(&fakeReceiver{}, handler, operations)
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Handle(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if len(handler.acknowledgements) != 0 || delivery.ackCount() != 1 || delivery.rejectionCount() != 0 || len(operations.records) != 1 {
		t.Fatalf("duplicate handling: calls=%d records=%v acks=%d rejects=%d", len(handler.acknowledgements), operations.records, delivery.ackCount(), delivery.rejectionCount())
	}
}

func TestControllerInboxRejectsMalformedAndSpoofedWithoutAcknowledging(t *testing.T) {
	validEnvelope := acknowledgementEnvelope("ack-terminal")
	validPayload, err := encodeAcknowledgement(validEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	deliveries := []*fakeDelivery{
		{kind: KindAcknowledgement, operationID: validEnvelope.MessageID, payload: []byte(`{"unknown":true}`), authenticated: validEnvelope.Participant, authenticatedPresent: true},
		{kind: KindAcknowledgement, operationID: validEnvelope.MessageID, payload: validPayload, authenticated: "intruder", authenticatedPresent: true},
	}
	receiver := &sequenceControllerReceiver{deliveries: []Delivery{deliveries[0], deliveries[1]}}
	handler := &fakeControllerHandler{}
	inbox, err := NewControllerInbox(receiver, handler, newMemoryOperations())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- inbox.Run(ctx) }()
	deadline := time.After(time.Second)
	for deliveries[1].rejectionCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("terminal deliveries did not get rejected")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	<-result
	for index, delivery := range deliveries {
		if delivery.ackCount() != 0 || delivery.rejectionCount() != 1 {
			t.Fatalf("delivery %d settlements: acks=%d rejects=%d", index, delivery.ackCount(), delivery.rejectionCount())
		}
	}
	if len(handler.acknowledgements) != 0 {
		t.Fatal("malformed or spoofed acknowledgement reached controller")
	}
}

func TestControllerInboxRetriesTransientFailureWithoutRedelivery(t *testing.T) {
	envelope := telemetryEnvelope("telemetry-transient-retry")
	payload, err := encodeTelemetry(envelope)
	if err != nil {
		t.Fatal(err)
	}
	delivery := &fakeDelivery{kind: KindTelemetry, operationID: envelope.MessageID, payload: payload, authenticated: envelope.Participant, authenticatedPresent: true}
	receiver := &sequenceControllerReceiver{deliveries: []Delivery{delivery}}
	handler := &fakeControllerHandler{errs: []error{errors.New("state disk unavailable"), nil}}
	operations := newMemoryOperations()
	inbox, err := NewControllerInbox(receiver, handler, operations)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- inbox.Run(ctx) }()
	deadline := time.After(time.Second)
	for delivery.ackCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("transient controller failure was not retried")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	<-result
	if receiver.next != 1 || len(handler.telemetry) != 2 || delivery.rejectionCount() != 0 {
		t.Fatalf("retry behavior: receives=%d attempts=%d rejects=%d", receiver.next, len(handler.telemetry), delivery.rejectionCount())
	}
	if completed, _ := operations.Contains(context.Background(), envelope.MessageID); !completed {
		t.Fatal("retried telemetry was not recorded")
	}
}

func TestControllerInboxRetriesTransientRecordFailureWithoutRedelivery(t *testing.T) {
	envelope := acknowledgementEnvelope("ack-record-retry")
	payload, err := encodeAcknowledgement(envelope)
	if err != nil {
		t.Fatal(err)
	}
	delivery := &fakeDelivery{kind: KindAcknowledgement, operationID: envelope.MessageID, payload: payload, authenticated: envelope.Participant, authenticatedPresent: true}
	receiver := &sequenceControllerReceiver{deliveries: []Delivery{delivery}}
	handler := &fakeControllerHandler{}
	operations := newMemoryOperations()
	operations.recordErrs = []error{errors.New("operation store unavailable"), nil}
	inbox, err := NewControllerInbox(receiver, handler, operations)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- inbox.Run(ctx) }()
	deadline := time.After(time.Second)
	for delivery.ackCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("transient operation-store failure was not retried")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	<-result
	if receiver.next != 1 || len(handler.acknowledgements) != 2 || delivery.rejectionCount() != 0 || len(operations.records) != 2 {
		t.Fatalf("retry behavior: receives=%d attempts=%d records=%v rejects=%d", receiver.next, len(handler.acknowledgements), operations.records, delivery.rejectionCount())
	}
}

type fakeSubscriber struct {
	mu             sync.Mutex
	subscribed     bool
	groups         []string
	participants   []string
	deliveries     chan Delivery
	reconnects     chan struct{}
	errors         chan error
	subscribeEvent chan struct{}
}

func newFakeSubscriber() *fakeSubscriber {
	return &fakeSubscriber{
		deliveries: make(chan Delivery, 8), reconnects: make(chan struct{}, 8), errors: make(chan error, 1), subscribeEvent: make(chan struct{}),
	}
}

func (subscriber *fakeSubscriber) Subscribe(_ context.Context, group, participant string) (UpdateSubscription, error) {
	subscriber.mu.Lock()
	subscriber.subscribed = true
	subscriber.groups = append(subscriber.groups, group)
	subscriber.participants = append(subscriber.participants, participant)
	select {
	case <-subscriber.subscribeEvent:
	default:
		close(subscriber.subscribeEvent)
	}
	subscriber.mu.Unlock()
	return UpdateSubscription{Deliveries: subscriber.deliveries, Reconnects: subscriber.reconnects, Errors: subscriber.errors}, nil
}

type browseResponse struct {
	messages []BrowsedMessage
	err      error
}

type fakeBrowser struct {
	mu         sync.Mutex
	subscriber *fakeSubscriber
	responses  chan browseResponse
	calls      int
	called     chan int
}

func (browser *fakeBrowser) Browse(ctx context.Context, _ string) ([]BrowsedMessage, error) {
	browser.mu.Lock()
	browser.calls++
	call := browser.calls
	subscribed := browser.subscriber.subscribed
	browser.mu.Unlock()
	if !subscribed {
		return nil, errors.New("browse occurred before subscribe")
	}
	select {
	case browser.called <- call:
	default:
	}
	select {
	case response := <-browser.responses:
		return response.messages, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type fakeApplier struct {
	mu          sync.Mutex
	applied     []Message
	failures    []error
	applyErr    error
	applyCalled chan Message
	applyGate   chan struct{}
}

func (applier *fakeApplier) Apply(ctx context.Context, message Message) error {
	if applier.applyCalled != nil {
		select {
		case applier.applyCalled <- message:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if applier.applyGate != nil {
		select {
		case <-applier.applyGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	applier.mu.Lock()
	defer applier.mu.Unlock()
	if applier.applyErr != nil {
		return applier.applyErr
	}
	applier.applied = append(applier.applied, message)
	return nil
}

func (applier *fakeApplier) FailClosed(_ context.Context, _ string, cause error) error {
	applier.mu.Lock()
	defer applier.mu.Unlock()
	applier.failures = append(applier.failures, cause)
	return nil
}

func snapshotBrowseMessage(t *testing.T, operationID string, snapshot control.MembershipSnapshot) BrowsedMessage {
	t.Helper()
	payload, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return BrowsedMessage{Kind: KindMembershipSnapshot, OperationID: operationID, Payload: payload}
}

func snapshotDelivery(t *testing.T, operationID string, snapshot control.MembershipSnapshot) *fakeDelivery {
	t.Helper()
	message := snapshotBrowseMessage(t, operationID, snapshot)
	return &fakeDelivery{kind: message.Kind, operationID: message.OperationID, payload: message.Payload}
}

func newOrchestratorTest(t *testing.T, browser *fakeBrowser, subscriber *fakeSubscriber, applier *fakeApplier, operations OperationStore, interval, staleness time.Duration) *GroupOrchestrator {
	t.Helper()
	orchestrator, err := NewGroupOrchestrator(subscriber, browser, applier, operations, OrchestratorOptions{
		Group: "orders", Participant: "publisher-1", RebrowseInterval: interval, MaxStaleness: staleness,
	})
	if err != nil {
		t.Fatal(err)
	}
	return orchestrator
}

func waitMessage(t *testing.T, channel <-chan Message) Message {
	t.Helper()
	select {
	case message := <-channel:
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for applied message")
		return Message{}
	}
}

func waitCall(t *testing.T, channel <-chan int) int {
	t.Helper()
	select {
	case call := <-channel:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for browse")
		return 0
	}
}

func TestOrchestratorSubscribesBuffersAppliesThenACKs(t *testing.T) {
	subscriber := newFakeSubscriber()
	browser := &fakeBrowser{subscriber: subscriber, responses: make(chan browseResponse, 2), called: make(chan int, 2)}
	applier := &fakeApplier{applyCalled: make(chan Message, 2), applyGate: make(chan struct{})}
	operations := newMemoryOperations()
	orchestrator := newOrchestratorTest(t, browser, subscriber, applier, operations, time.Hour, 2*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- orchestrator.Run(ctx) }()
	if call := waitCall(t, browser.called); call != 1 {
		t.Fatalf("initial browse call = %d", call)
	}

	update := activeSnapshot(2)
	delivery := snapshotDelivery(t, "snapshot-2", update)
	subscriber.deliveries <- delivery
	browser.responses <- browseResponse{messages: []BrowsedMessage{snapshotBrowseMessage(t, "snapshot-1", activeSnapshot(1))}}
	applied := waitMessage(t, applier.applyCalled)
	if applied.Snapshot == nil || applied.Snapshot.Revision != 2 {
		t.Fatalf("reconciled snapshot = %#v", applied.Snapshot)
	}
	if delivery.ackCount() != 0 {
		t.Fatal("buffered update ACKed before state application completed")
	}
	close(applier.applyGate)

	deadline := time.After(time.Second)
	for delivery.ackCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("buffered update was not ACKed after apply")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	snapshot, ok := orchestrator.Snapshot()
	if !ok || snapshot.Revision != 2 {
		t.Fatalf("orchestrator snapshot = %#v, %v", snapshot, ok)
	}
	if len(applier.failures) != 1 || !errors.Is(applier.failures[0], ErrNoAuthoritativeState) {
		t.Fatalf("initial fail-closed calls = %#v", applier.failures)
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() cancellation error = %v", err)
	}
}

func TestOrchestratorReconcilesCompletedLiveDeliveryBeforeACK(t *testing.T) {
	subscriber := newFakeSubscriber()
	browser := &fakeBrowser{subscriber: subscriber, responses: make(chan browseResponse, 1), called: make(chan int, 1)}
	applier := &fakeApplier{applyCalled: make(chan Message, 1), applyGate: make(chan struct{})}
	operations := newMemoryOperations()
	operations.operations["snapshot-2"] = true
	orchestrator := newOrchestratorTest(t, browser, subscriber, applier, operations, time.Hour, 2*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- orchestrator.Run(ctx) }()
	waitCall(t, browser.called)
	replayed := snapshotDelivery(t, "snapshot-2", activeSnapshot(2))
	subscriber.deliveries <- replayed
	browser.responses <- browseResponse{messages: []BrowsedMessage{snapshotBrowseMessage(t, "snapshot-1", activeSnapshot(1))}}
	applied := waitMessage(t, applier.applyCalled)
	if applied.Snapshot.Revision != 2 {
		t.Fatalf("completed live delivery did not participate in reconciliation: %#v", applied.Snapshot)
	}
	if replayed.ackCount() != 0 {
		t.Fatal("completed live delivery was ACKed before reconciliation completed")
	}
	close(applier.applyGate)
	deadline := time.After(time.Second)
	for replayed.ackCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("completed live delivery was not ACKed after reconciliation")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() cancellation error = %v", err)
	}
}

func TestOrchestratorDoesNotACKWhenApplyFails(t *testing.T) {
	subscriber := newFakeSubscriber()
	browser := &fakeBrowser{subscriber: subscriber, responses: make(chan browseResponse, 1), called: make(chan int, 1)}
	applier := &fakeApplier{applyErr: errors.New("local state unavailable")}
	operations := newMemoryOperations()
	orchestrator := newOrchestratorTest(t, browser, subscriber, applier, operations, time.Hour, 2*time.Hour)
	buffered := snapshotDelivery(t, "snapshot-2", activeSnapshot(2))

	result := make(chan error, 1)
	go func() { result <- orchestrator.Run(context.Background()) }()
	waitCall(t, browser.called)
	subscriber.deliveries <- buffered
	browser.responses <- browseResponse{messages: []BrowsedMessage{snapshotBrowseMessage(t, "snapshot-1", activeSnapshot(1))}}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("apply failure was hidden")
		}
	case <-time.After(time.Second):
		t.Fatal("orchestrator did not return after apply failure")
	}
	if buffered.ackCount() != 0 || len(operations.records) != 0 {
		t.Fatalf("failed apply was completed: acks:%d records:%v", buffered.ackCount(), operations.records)
	}
	if _, ok := orchestrator.Snapshot(); ok {
		t.Fatal("failed state application became usable")
	}
}

func TestOrchestratorDoesNotACKWhenDedupeRecordFails(t *testing.T) {
	subscriber := newFakeSubscriber()
	browser := &fakeBrowser{subscriber: subscriber, responses: make(chan browseResponse, 1), called: make(chan int, 1)}
	applier := &fakeApplier{applyCalled: make(chan Message, 1)}
	operations := newMemoryOperations()
	operations.recordErr = errors.New("dedupe disk full")
	orchestrator := newOrchestratorTest(t, browser, subscriber, applier, operations, time.Hour, 2*time.Hour)
	browser.responses <- browseResponse{messages: []BrowsedMessage{snapshotBrowseMessage(t, "snapshot-1", activeSnapshot(1))}}

	result := make(chan error, 1)
	go func() { result <- orchestrator.Run(context.Background()) }()
	waitMessage(t, applier.applyCalled)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("operation-store failure was hidden")
		}
	case <-time.After(time.Second):
		t.Fatal("orchestrator did not return after operation-store failure")
	}
	if _, ok := orchestrator.Snapshot(); ok {
		t.Fatal("state stayed usable after operation-store failure")
	}
}

func TestOrchestratorRebrowsesOnReconnect(t *testing.T) {
	subscriber := newFakeSubscriber()
	browser := &fakeBrowser{subscriber: subscriber, responses: make(chan browseResponse, 2), called: make(chan int, 2)}
	applier := &fakeApplier{applyCalled: make(chan Message, 3)}
	orchestrator := newOrchestratorTest(t, browser, subscriber, applier, newMemoryOperations(), time.Hour, 2*time.Hour)
	browser.responses <- browseResponse{messages: []BrowsedMessage{snapshotBrowseMessage(t, "snapshot-1", activeSnapshot(1))}}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- orchestrator.Run(ctx) }()
	waitCall(t, browser.called)
	first := waitMessage(t, applier.applyCalled)
	if first.Snapshot.Revision != 1 {
		t.Fatalf("first revision = %d", first.Snapshot.Revision)
	}

	browser.responses <- browseResponse{messages: []BrowsedMessage{snapshotBrowseMessage(t, "snapshot-2", activeSnapshot(2))}}
	subscriber.reconnects <- struct{}{}
	if call := waitCall(t, browser.called); call != 2 {
		t.Fatalf("reconnect browse call = %d", call)
	}
	second := waitMessage(t, applier.applyCalled)
	if second.Snapshot.Revision != 2 {
		t.Fatalf("reconnect revision = %d", second.Snapshot.Revision)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() cancellation error = %v", err)
	}
}

func TestOrchestratorIgnoresPeriodicBrowseSupersededByLiveUpdate(t *testing.T) {
	subscriber := newFakeSubscriber()
	browser := &fakeBrowser{subscriber: subscriber, responses: make(chan browseResponse, 2), called: make(chan int, 2)}
	applier := &fakeApplier{applyCalled: make(chan Message, 2)}
	orchestrator := newOrchestratorTest(t, browser, subscriber, applier, newMemoryOperations(), time.Hour, 2*time.Hour)
	browser.responses <- browseResponse{messages: []BrowsedMessage{snapshotBrowseMessage(t, "snapshot-1", activeSnapshot(1))}}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- orchestrator.Run(ctx) }()
	waitCall(t, browser.called)
	waitMessage(t, applier.applyCalled)

	subscriber.reconnects <- struct{}{}
	if call := waitCall(t, browser.called); call != 2 {
		t.Fatalf("periodic browse call = %d", call)
	}
	live := snapshotDelivery(t, "snapshot-2", activeSnapshot(2))
	subscriber.deliveries <- live
	if applied := waitMessage(t, applier.applyCalled); applied.Snapshot.Revision != 2 {
		t.Fatalf("live revision = %d", applied.Snapshot.Revision)
	}
	browser.responses <- browseResponse{messages: []BrowsedMessage{snapshotBrowseMessage(t, "snapshot-1", activeSnapshot(1))}}

	deadline := time.After(time.Second)
	for {
		if snapshot, ok := orchestrator.Snapshot(); ok && snapshot.Revision == 2 {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("stale browse failed orchestrator: %v", err)
		case <-deadline:
			t.Fatal("orchestrator did not retain live revision")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() cancellation error = %v", err)
	}
}

func TestOrchestratorFailsClosedWhenAuthoritativeBrowseStaysStale(t *testing.T) {
	subscriber := newFakeSubscriber()
	browser := &fakeBrowser{subscriber: subscriber, responses: make(chan browseResponse, 32), called: make(chan int, 32)}
	applier := &fakeApplier{applyCalled: make(chan Message, 2)}
	orchestrator := newOrchestratorTest(t, browser, subscriber, applier, newMemoryOperations(), 5*time.Millisecond, 40*time.Millisecond)
	browser.responses <- browseResponse{messages: []BrowsedMessage{snapshotBrowseMessage(t, "snapshot-1", activeSnapshot(1))}}
	for index := 0; index < 20; index++ {
		browser.responses <- browseResponse{err: errors.New("Broker 0 unavailable")}
	}

	result := make(chan error, 1)
	go func() { result <- orchestrator.Run(context.Background()) }()
	waitMessage(t, applier.applyCalled)
	select {
	case err := <-result:
		if !errors.Is(err, ErrMembershipStale) {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("orchestrator did not fail closed after staleness limit")
	}
	if _, ok := orchestrator.Snapshot(); ok {
		t.Fatal("stale membership remained usable")
	}
	applier.mu.Lock()
	defer applier.mu.Unlock()
	if len(applier.failures) < 2 || !errors.Is(applier.failures[len(applier.failures)-1], ErrMembershipStale) {
		t.Fatalf("fail-closed causes = %#v", applier.failures)
	}
}

func TestSelectBrowsedRejectsConflictingState(t *testing.T) {
	first := activeSnapshot(1)
	second := activeSnapshot(1)
	second.Queue.Name = "different"
	messages := []Message{
		{Kind: KindMembershipSnapshot, OperationID: "one", Group: first.ScalingGroup, Snapshot: &first},
		{Kind: KindMembershipSnapshot, OperationID: "two", Group: second.ScalingGroup, Snapshot: &second},
	}
	if _, err := selectBrowsed(messages); !errors.Is(err, control.ErrConflictingUpdate) {
		t.Fatalf("selectBrowsed() error = %v", err)
	}
}

func TestTelemetryConversionPreservesKnownUnknownCounts(t *testing.T) {
	envelope := telemetryEnvelope("telemetry-1")
	envelope.Queued = control.MetricCount{Known: false}
	envelope.Stored = control.MetricCount{Known: true, Value: 7}
	got := telemetryForController(envelope)
	want := controller.Telemetry{
		MessageID: envelope.MessageID, Namespace: envelope.Namespace, Group: envelope.Group,
		TransitionID: envelope.TransitionID, Epoch: envelope.Epoch, Participant: envelope.Participant,
		Role: envelope.Role, SourceBroker: envelope.SourceBroker, SourceQueue: envelope.SourceQueue, ObservedAt: envelope.ObservedAt,
		Queued: controller.Count{Known: false}, Stored: controller.Count{Known: true, Value: 7}, Unacked: controller.Count{Known: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("telemetry conversion = %#v, want %#v", got, want)
	}
}
