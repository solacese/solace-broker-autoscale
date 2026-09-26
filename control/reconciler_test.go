package control

import (
	"errors"
	"sync"
	"testing"
)

func revision(snapshot MembershipSnapshot, revision, epoch uint64) MembershipSnapshot {
	snapshot.Revision = revision
	snapshot.Epoch = epoch
	return snapshot
}

func TestReconcilerRequiresSubscribeFirst(t *testing.T) {
	reconciler := NewReconciler()
	if err := reconciler.ApplyBrowse(validActiveSnapshot()); !errors.Is(err, ErrNotSubscribed) {
		t.Fatalf("ApplyBrowse() error = %v", err)
	}
	if _, err := reconciler.ApplyUpdate(validActiveSnapshot()); !errors.Is(err, ErrNotSubscribed) {
		t.Fatalf("ApplyUpdate() error = %v", err)
	}
	if _, err := reconciler.FinishBrowse(); !errors.Is(err, ErrNotSubscribed) {
		t.Fatalf("FinishBrowse() error = %v", err)
	}
	if err := reconciler.BeginSubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.BeginSubscribe(); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second BeginSubscribe() error = %v", err)
	}
	if _, err := reconciler.FinishBrowse(); !errors.Is(err, ErrBrowseRequired) {
		t.Fatalf("FinishBrowse() without browse error = %v", err)
	}
}

func TestReconcilerSubscribeBrowseBufferedUpdates(t *testing.T) {
	reconciler := NewReconciler()
	if err := reconciler.BeginSubscribe(); err != nil {
		t.Fatal(err)
	}

	base := revision(validActiveSnapshot(), 10, 3)
	stale := revision(validActiveSnapshot(), 9, 3)
	newer := revision(validActiveSnapshot(), 12, 4)
	newer.CurrentMembership = Membership{"broker-z", "broker-a", "broker-m"}
	middle := revision(validTransitionSnapshot(PhasePrepare), 11, 3)

	// Deliveries can race with browse and can arrive out of order.
	for _, update := range []MembershipSnapshot{newer, stale, middle} {
		changed, err := reconciler.ApplyUpdate(update)
		if err != nil {
			t.Fatal(err)
		}
		if changed {
			t.Fatal("buffered update reported an installed state change")
		}
	}
	if _, ok := reconciler.Snapshot(); ok {
		t.Fatal("snapshot became visible before browse reconciliation")
	}
	if err := reconciler.ApplyBrowse(base); err != nil {
		t.Fatal(err)
	}
	got, err := reconciler.FinishBrowse()
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 12 || got.Epoch != 4 || !got.CurrentMembership.Equal(newer.CurrentMembership) {
		t.Fatalf("FinishBrowse() = %+v", got)
	}
}

func TestReconcilerRejectsStaleLiveUpdate(t *testing.T) {
	reconciler := readyReconciler(t, revision(validActiveSnapshot(), 10, 3))
	for name, stale := range map[string]MembershipSnapshot{
		"older revision":   revision(validActiveSnapshot(), 9, 3),
		"epoch regression": revision(validActiveSnapshot(), 11, 2),
	} {
		t.Run(name, func(t *testing.T) {
			changed, err := reconciler.ApplyUpdate(stale)
			if changed || !errors.Is(err, ErrStaleUpdate) {
				t.Fatalf("ApplyUpdate() = %v, %v", changed, err)
			}
		})
	}
	got, ok := reconciler.Snapshot()
	if !ok || got.Revision != 10 || got.Epoch != 3 {
		t.Fatalf("stale update changed state: %+v, %v", got, ok)
	}
}

func TestReconcilerReplaysAndConflicts(t *testing.T) {
	base := revision(validActiveSnapshot(), 10, 3)
	reconciler := readyReconciler(t, base)
	changed, err := reconciler.ApplyUpdate(base.Clone())
	if err != nil || changed {
		t.Fatalf("exact replay = %v, %v", changed, err)
	}

	conflict := base.Clone()
	conflict.Queue.Name = "different"
	changed, err = reconciler.ApplyUpdate(conflict)
	if changed || !errors.Is(err, ErrConflictingUpdate) {
		t.Fatalf("conflict = %v, %v", changed, err)
	}

	conflict = base.Clone()
	conflict.Epoch++
	changed, err = reconciler.ApplyUpdate(conflict)
	if changed || !errors.Is(err, ErrConflictingUpdate) {
		t.Fatalf("same revision/different epoch = %v, %v", changed, err)
	}
}

func TestReconcilerRejectsSameRevisionContractConflict(t *testing.T) {
	base := revision(validActiveSnapshot(), 10, 3)
	reconciler := readyReconciler(t, base)
	conflict := base.Clone()
	conflict.HashContract = "flight-operations-v2"
	if _, err := reconciler.ApplyUpdate(conflict); !errors.Is(err, ErrConflictingUpdate) {
		t.Fatalf("ApplyUpdate() error = %v, want conflict", err)
	}
}

func TestReconcilerRejectsUncoordinatedActiveMembershipChange(t *testing.T) {
	base := revision(validActiveSnapshot(), 10, 3)
	reconciler := readyReconciler(t, base)
	changed := revision(validActiveSnapshot(), 11, 3)
	changed.CurrentMembership = Membership{"broker-a", "broker-z"}
	if _, err := reconciler.ApplyUpdate(changed); !errors.Is(err, ErrConflictingUpdate) {
		t.Fatalf("ApplyUpdate() error = %v, want conflict", err)
	}
}

func TestReconcilerRejectsUncoordinatedRoutingIdentityChanges(t *testing.T) {
	base := revision(validActiveSnapshot(), 10, 3)
	base.Namespace = "acme"
	base.LibraryVersion = "v1.2.3"
	base.CurrentResources = []EpochResourceIdentity{{
		Epoch: 3, BrokerID: "broker-a", ConsumerSet: "workers", QueueName: "acme.data.orders.workers.e3", IngressTopic: "acme/data/orders/epoch/3/>",
	}}

	tests := map[string]func(*MembershipSnapshot){
		"namespace":       func(s *MembershipSnapshot) { s.Namespace = "other" },
		"library version": func(s *MembershipSnapshot) { s.LibraryVersion = "v2.0.0" },
		"hash contract":   func(s *MembershipSnapshot) { s.HashContract = "flight-operations-v2" },
		"queue":           func(s *MembershipSnapshot) { s.Queue.Name = "different.queue" },
		"destination":     func(s *MembershipSnapshot) { s.Destination.Name = "different/>" },
		"resource queue":  func(s *MembershipSnapshot) { s.CurrentResources[0].QueueName = "different.queue" },
		"resource topic":  func(s *MembershipSnapshot) { s.CurrentResources[0].IngressTopic = "different/epoch/3/>" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			reconciler := readyReconciler(t, base)
			changed := base.Clone()
			changed.Revision++
			mutate(&changed)
			if _, err := reconciler.ApplyUpdate(changed); !errors.Is(err, ErrConflictingUpdate) {
				t.Fatalf("ApplyUpdate() error = %v, want conflict", err)
			}
			got, ok := reconciler.Snapshot()
			if !ok || !snapshotsEqual(got, base) {
				t.Fatalf("rejected update changed installed snapshot: %+v, %v", got, ok)
			}
		})
	}
}

func TestReconcilerRejectsRoutingIdentityChangeDuringTransition(t *testing.T) {
	prepare := revision(validTransitionSnapshot(PhasePrepare), 10, 3)
	prepare.Namespace = "acme"
	prepare.LibraryVersion = "v1.2.3"
	prepare.CurrentResources = []EpochResourceIdentity{{
		Epoch: 3, BrokerID: "broker-a", ConsumerSet: "workers", QueueName: "acme.data.orders.workers.e3", IngressTopic: "acme/data/orders/epoch/3/>",
	}}
	prepare.ProposedResources = []EpochResourceIdentity{{
		Epoch: 4, BrokerID: "broker-a", ConsumerSet: "workers", QueueName: "acme.data.orders.workers.e4", IngressTopic: "acme/data/orders/epoch/4/>",
	}}

	tests := map[string]func(*MembershipSnapshot){
		"queue":               func(s *MembershipSnapshot) { s.Queue.Name = "different.queue" },
		"destination":         func(s *MembershipSnapshot) { s.Destination.Name = "different/>" },
		"current resource":    func(s *MembershipSnapshot) { s.CurrentResources[0].QueueName = "different.e3" },
		"proposed resource":   func(s *MembershipSnapshot) { s.ProposedResources[0].QueueName = "different.e4" },
		"proposed membership": func(s *MembershipSnapshot) { s.ProposedMembership = Membership{"broker-z", "broker-m"} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			reconciler := readyReconciler(t, prepare)
			paused := prepare.Clone()
			paused.Revision++
			paused.Phase = PhasePaused
			mutate(&paused)
			if _, err := reconciler.ApplyUpdate(paused); !errors.Is(err, ErrConflictingUpdate) {
				t.Fatalf("ApplyUpdate() error = %v, want conflict", err)
			}
		})
	}
}

func TestReconcilerPreservesResourceTransitionRollbackAndCatchUp(t *testing.T) {
	active := revision(validActiveSnapshot(), 10, 3)
	active.Namespace = "acme"
	active.LibraryVersion = "v1.2.3"
	active.CurrentResources = []EpochResourceIdentity{{
		Epoch: 3, BrokerID: "broker-a", ConsumerSet: "workers", QueueName: "acme.data.orders.workers.e3", IngressTopic: "acme/data/orders/epoch/3/>",
	}}
	prepare := revision(validTransitionSnapshot(PhasePrepare), 11, 3)
	prepare.Namespace = active.Namespace
	prepare.LibraryVersion = active.LibraryVersion
	prepare.CurrentResources = append([]EpochResourceIdentity(nil), active.CurrentResources...)
	prepare.ProposedResources = []EpochResourceIdentity{{
		Epoch: 4, BrokerID: "broker-a", ConsumerSet: "workers", QueueName: "acme.data.orders.workers.e4", IngressTopic: "acme/data/orders/epoch/4/>",
	}}

	t.Run("coordinated activation", func(t *testing.T) {
		reconciler := readyReconciler(t, active)
		for _, update := range []MembershipSnapshot{
			prepare,
			func() MembershipSnapshot { s := revision(prepare.Clone(), 12, 3); s.Phase = PhaseDrain; return s }(),
			func() MembershipSnapshot {
				s := revision(prepare.Clone(), 13, 4)
				s.Phase = PhaseCommitted
				s.CurrentMembership = s.ProposedMembership.Clone()
				s.CurrentResources = append([]EpochResourceIdentity(nil), s.ProposedResources...)
				s.ProposedMembership = nil
				s.ProposedResources = nil
				return s
			}(),
			func() MembershipSnapshot {
				s := revision(validActiveSnapshot(), 14, 4)
				s.Namespace = active.Namespace
				s.LibraryVersion = active.LibraryVersion
				s.CurrentMembership = prepare.ProposedMembership.Clone()
				s.CurrentResources = append([]EpochResourceIdentity(nil), prepare.ProposedResources...)
				return s
			}(),
		} {
			changed, err := reconciler.ApplyUpdate(update)
			if err != nil || !changed {
				t.Fatalf("ApplyUpdate(%s) = %v, %v", update.Phase, changed, err)
			}
		}
		got, _ := reconciler.Snapshot()
		if got.Epoch != 4 || !got.CurrentMembership.Equal(prepare.ProposedMembership) || !resourcesEqual(got.CurrentResources, prepare.ProposedResources) {
			t.Fatalf("activated snapshot = %+v", got)
		}
	})

	t.Run("same-epoch rollback", func(t *testing.T) {
		reconciler := readyReconciler(t, prepare)
		rollback := active.Clone()
		rollback.Revision = 12
		changed, err := reconciler.ApplyUpdate(rollback)
		if err != nil || !changed {
			t.Fatalf("ApplyUpdate() = %v, %v", changed, err)
		}
	})

	t.Run("later-epoch catch-up", func(t *testing.T) {
		reconciler := readyReconciler(t, active)
		catchUp := active.Clone()
		catchUp.Revision = 20
		catchUp.Epoch = 5
		catchUp.CurrentMembership = Membership{"broker-m", "broker-z"}
		catchUp.CurrentResources = []EpochResourceIdentity{{
			Epoch: 5, BrokerID: "broker-a", ConsumerSet: "workers", QueueName: "acme.data.orders.workers.e5", IngressTopic: "acme/data/orders/epoch/5/>",
		}}
		changed, err := reconciler.ApplyUpdate(catchUp)
		if err != nil || !changed {
			t.Fatalf("ApplyUpdate() = %v, %v", changed, err)
		}
	})
}

func TestReconcilerRejectsCrossedRevisionAndEpoch(t *testing.T) {
	reconciler := readyReconciler(t, revision(validActiveSnapshot(), 10, 3))
	crossed := revision(validActiveSnapshot(), 9, 4)
	if _, err := reconciler.ApplyUpdate(crossed); !errors.Is(err, ErrConflictingUpdate) {
		t.Fatalf("ApplyUpdate() error = %v", err)
	}
}

func TestReconcilerRejectsCrossGroupUpdate(t *testing.T) {
	reconciler := readyReconciler(t, validActiveSnapshot())
	other := revision(validActiveSnapshot(), 8, 3)
	other.ScalingGroup = "baggage"
	if _, err := reconciler.ApplyUpdate(other); err == nil {
		t.Fatal("cross-group update accepted")
	}
}

func TestReconcilerCopiesInputsAndOutputs(t *testing.T) {
	base := validActiveSnapshot()
	reconciler := readyReconciler(t, base)
	base.CurrentMembership[0] = "input-mutated"

	first, ok := reconciler.Snapshot()
	if !ok || first.CurrentMembership[0] != "broker-z" {
		t.Fatalf("input mutation reached state: %+v", first)
	}
	first.CurrentMembership[0] = "output-mutated"
	second, _ := reconciler.Snapshot()
	if second.CurrentMembership[0] != "broker-z" {
		t.Fatalf("output mutation reached state: %+v", second)
	}
}

func TestReconcilerConcurrentBufferedUpdates(t *testing.T) {
	reconciler := NewReconciler()
	if err := reconciler.BeginSubscribe(); err != nil {
		t.Fatal(err)
	}
	base := revision(validActiveSnapshot(), 10, 3)
	if err := reconciler.ApplyBrowse(base); err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	for i := uint64(11); i <= 100; i++ {
		update := revision(validActiveSnapshot(), i, 3)
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := reconciler.ApplyUpdate(update); err != nil {
				t.Errorf("ApplyUpdate(%d): %v", update.Revision, err)
			}
		}()
	}
	wait.Wait()
	got, err := reconciler.FinishBrowse()
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 100 {
		t.Fatalf("final revision = %d, want 100", got.Revision)
	}
}

func TestReconcilerAppliesFreshLiveUpdate(t *testing.T) {
	reconciler := readyReconciler(t, revision(validActiveSnapshot(), 10, 3))
	fresh := revision(validTransitionSnapshot(PhasePrepare), 11, 3)
	changed, err := reconciler.ApplyUpdate(fresh)
	if err != nil || !changed {
		t.Fatalf("ApplyUpdate() = %v, %v", changed, err)
	}
	got, _ := reconciler.Snapshot()
	if got.Revision != 11 || got.Phase != PhasePrepare {
		t.Fatalf("Snapshot() = %+v", got)
	}
}

func readyReconciler(t *testing.T, snapshot MembershipSnapshot) *Reconciler {
	t.Helper()
	reconciler := NewReconciler()
	if err := reconciler.BeginSubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ApplyBrowse(snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.FinishBrowse(); err != nil {
		t.Fatal(err)
	}
	return reconciler
}
