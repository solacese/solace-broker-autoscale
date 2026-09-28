package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/solacese/solace-workload-balancer/control"
)

type membershipOrderStore struct {
	state PersistentState
	fail  bool
	saves int
}

func (s *membershipOrderStore) Load() (PersistentState, error) { return clonePersistent(s.state), nil }
func (s *membershipOrderStore) Save(state PersistentState) error {
	s.saves++
	if s.fail {
		return errors.New("save failed")
	}
	s.state = clonePersistent(state)
	return nil
}

type membershipOrderPublisher struct {
	store        *membershipOrderStore
	published    int
	sawPersisted bool
}

func (p *membershipOrderPublisher) Publish(_ context.Context, update ControlUpdate) error {
	p.published++
	snapshot, ok := p.store.state.Membership[update.Group]
	p.sawPersisted = ok && snapshot.Revision == update.Snapshot.Revision
	return nil
}

func TestMembershipPersistsBeforePublicationAndSaveFailurePublishesNothing(t *testing.T) {
	for _, fail := range []bool{false, true} {
		store := &membershipOrderStore{state: PersistentState{Version: stateVersion, Membership: map[string]control.MembershipSnapshot{}, Groups: map[string]*GroupState{}, History: map[string][]TransitionRecord{}, CleanupEvidence: map[string][]CleanupEvidence{}}}
		publisher := &membershipOrderPublisher{store: store}
		controller, err := Open(store, &fakeFence{}, publisher, Options{Now: func() time.Time { return time.Unix(1, 0).UTC() }})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = controller.Begin(testSpec("orders", "move")); err != nil {
			t.Fatal(err)
		}
		store.fail = fail
		_, err = controller.Reconcile(context.Background(), "orders")
		if fail {
			if err == nil || publisher.published != 0 {
				t.Fatalf("save failure err=%v publishes=%d", err, publisher.published)
			}
		} else if err != nil || publisher.published != 1 || !publisher.sawPersisted {
			t.Fatalf("err=%v published=%d persisted=%t", err, publisher.published, publisher.sawPersisted)
		}
	}
}

func TestSaveMembershipRejectsConflictsAndEpochRegression(t *testing.T) {
	store := &membershipOrderStore{state: PersistentState{Version: stateVersion, Membership: map[string]control.MembershipSnapshot{}, Groups: map[string]*GroupState{}, History: map[string][]TransitionRecord{}, CleanupEvidence: map[string][]CleanupEvidence{}}}
	controller, err := Open(store, &fakeFence{}, &fakePublisher{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	base := controlSnapshot(testSpec("orders", "move"), PhasePrepare, false)
	if err := controller.SaveMembership(base); err != nil {
		t.Fatal(err)
	}
	conflict := base.Clone()
	conflict.Queue.Name = "other"
	if err := controller.SaveMembership(conflict); err == nil {
		t.Fatal("accepted same-revision conflict")
	}
	regressed := base.Clone()
	regressed.Revision++
	regressed.Epoch = 0
	if err := controller.SaveMembership(regressed); err == nil {
		t.Fatal("accepted invalid epoch regression")
	}
}
