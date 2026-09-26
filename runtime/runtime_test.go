package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
)

type reconcileResult struct {
	changed bool
	err     error
}

type fakeReconciler struct {
	mu      sync.Mutex
	state   controller.PersistentState
	results map[string][]reconcileResult
	calls   chan string
}

func (f *fakeReconciler) Reconcile(_ context.Context, group string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls <- group
	queue := f.results[group]
	if len(queue) == 0 {
		return false, nil
	}
	result := queue[0]
	f.results[group] = queue[1:]
	return result.changed, result.err
}

func (f *fakeReconciler) Snapshot() controller.PersistentState { return f.state }

type rollbackReconciler struct {
	mu             sync.Mutex
	state          controller.PersistentState
	rollbackProofs []controller.RollbackProof
	requested      chan struct{}
}

func (r *rollbackReconciler) Reconcile(context.Context, string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Groups["a"].RollingBack {
		return false, nil
	}
	return false, controller.ErrPhaseDeadline
}
func (r *rollbackReconciler) Snapshot() controller.PersistentState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}
func (r *rollbackReconciler) RequestRollback(group string, proof controller.RollbackProof) error {
	r.mu.Lock()
	r.rollbackProofs = append(r.rollbackProofs, proof)
	r.state.Groups[group].RollingBack = true
	requested := r.requested
	r.mu.Unlock()
	if requested != nil {
		close(requested)
	}
	return nil
}
func (r *rollbackReconciler) proofs() []controller.RollbackProof {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.rollbackProofs)
}

type blockingComponent struct {
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (c *blockingComponent) Run(ctx context.Context) error {
	close(c.started)
	<-ctx.Done()
	return ctx.Err()
}
func (c *blockingComponent) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func newTestProcess(t *testing.T, reconciler Reconciler, components []Component, triggers <-chan Trigger, mutate func(*ControllerOptions)) *ControllerProcess {
	t.Helper()
	options := ControllerOptions{
		Groups:            []string{"a", "b"},
		ReconcileInterval: time.Hour,
		ActionTimeout:     time.Second,
		RetryInitial:      time.Millisecond,
		RetryMaximum:      2 * time.Millisecond,
		ShutdownTimeout:   time.Second,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if mutate != nil {
		mutate(&options)
	}
	process, err := NewControllerProcess(reconciler, components, triggers, options)
	if err != nil {
		t.Fatal(err)
	}
	return process
}

func TestControllerProcessInitialReconciliationAndShutdown(t *testing.T) {
	reconciler := &fakeReconciler{state: controller.PersistentState{Groups: map[string]*controller.GroupState{}}, results: map[string][]reconcileResult{}, calls: make(chan string, 10)}
	component := &blockingComponent{started: make(chan struct{}), closed: make(chan struct{})}
	process := newTestProcess(t, reconciler, []Component{component}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()

	select {
	case <-component.started:
	case <-time.After(time.Second):
		t.Fatal("component did not start")
	}
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case group := <-reconciler.calls:
			seen[group] = true
		case <-time.After(time.Second):
			t.Fatalf("initial reconciliation missing groups: %v", seen)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	select {
	case <-component.closed:
	default:
		t.Fatal("component was not closed")
	}
}

func TestControllerProcessGroupsAreIndependent(t *testing.T) {
	blocked := errors.New("group a unavailable")
	reconciler := &fakeReconciler{
		state: controller.PersistentState{Groups: map[string]*controller.GroupState{}},
		results: map[string][]reconcileResult{
			"a": {{err: blocked}},
			"b": {{changed: true}, {}},
		},
		calls: make(chan string, 20),
	}
	process := newTestProcess(t, reconciler, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	var bCalls int
	deadline := time.After(time.Second)
	for bCalls < 2 {
		select {
		case group := <-reconciler.calls:
			if group == "b" {
				bCalls++
			}
		case <-deadline:
			t.Fatal("group b did not continue while group a retried")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestControllerProcessRetries(t *testing.T) {
	temporary := errors.New("temporary")
	reconciler := &fakeReconciler{
		state:   controller.PersistentState{Groups: map[string]*controller.GroupState{}},
		results: map[string][]reconcileResult{"a": {{err: temporary}, {err: temporary}, {}}},
		calls:   make(chan string, 20),
	}
	process := newTestProcess(t, reconciler, nil, nil, func(options *ControllerOptions) { options.Groups = []string{"a"} })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	for count := 0; count < 3; count++ {
		select {
		case <-reconciler.calls:
		case <-time.After(time.Second):
			t.Fatal("retry did not occur")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestControllerProcessAutomaticallyRollsBackExpiredExactPrecommitState(t *testing.T) {
	spec := controller.TransitionSpec{
		ID: "move", Group: "a", Revision: 2, FromEpoch: 1, ToEpoch: 2,
		Current: []controller.Broker{{ID: "source", Destination: "topic"}}, Proposed: []controller.Broker{{ID: "target", Destination: "topic"}},
		HashContract: "a-v1", Algorithm: "sha256-unsigned-big-endian-modulo",
		Queue: control.QueueInfo{Name: "q", Durable: true}, Destination: control.DestinationInfo{Kind: control.DestinationTopic, Name: "topic"},
	}
	state := &controller.GroupState{Spec: spec, Phase: controller.PhasePrepare}
	reconciler := &rollbackReconciler{state: controller.PersistentState{Groups: map[string]*controller.GroupState{"a": state}}, requested: make(chan struct{})}
	catalog := NewMembershipCatalog()
	if err := catalog.Put(sourceActiveSnapshot(spec)); err != nil {
		t.Fatal(err)
	}
	process := newTestProcess(t, reconciler, nil, nil, func(options *ControllerOptions) {
		options.Groups = []string{"a"}
		options.Catalog = catalog
		options.RollbackFenceReversible = true
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	select {
	case <-reconciler.requested:
	case <-time.After(time.Second):
		t.Fatal("expired pre-commit transition did not request rollback")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	proof := reconciler.proofs()[0]
	if proof.TransitionID != spec.ID || !proof.SourceMembershipIntact || !proof.ProposedNotActivated || !proof.FenceReversible {
		t.Fatalf("rollback proof = %#v", proof)
	}
}

func TestControllerProcessDoesNotRollbackExpiredPostcommitState(t *testing.T) {
	spec := controller.TransitionSpec{ID: "move", Group: "a", Revision: 2, FromEpoch: 1, ToEpoch: 2}
	reconciler := &rollbackReconciler{state: controller.PersistentState{Groups: map[string]*controller.GroupState{"a": {Spec: spec, Phase: controller.PhaseCommit, CommitPublished: true}}}}
	catalog := NewMembershipCatalog()
	process := &ControllerProcess{reconciler: reconciler, options: ControllerOptions{Catalog: catalog, RollbackFenceReversible: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	if process.requestSafeTimeoutRollback("a") {
		t.Fatal("post-commit transition requested rollback")
	}
	if proofs := reconciler.proofs(); len(proofs) != 0 {
		t.Fatalf("post-commit rollback proofs = %#v", proofs)
	}
}

func TestParticipantProcessStartsIndependentGroupsAndStops(t *testing.T) {
	a := &blockingComponent{started: make(chan struct{}), closed: make(chan struct{})}
	b := &blockingComponent{started: make(chan struct{}), closed: make(chan struct{})}
	participant, err := NewParticipantProcess(map[string]Component{"a": a, "b": b}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- participant.Run(ctx) }()
	for group, started := range map[string]<-chan struct{}{"a": a.started, "b": b.started} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("group %s did not start", group)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for group, closed := range map[string]<-chan struct{}{"a": a.closed, "b": b.closed} {
		select {
		case <-closed:
		default:
			t.Fatalf("group %s did not close", group)
		}
	}
}
