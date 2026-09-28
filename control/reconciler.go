package control

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	// ErrNotSubscribed means snapshot or update processing was attempted before
	// the caller established its update subscription.
	ErrNotSubscribed = errors.New("control: update subscription must be established before requesting a snapshot")
	// ErrSnapshotRequired means bootstrap cannot finish without an authoritative
	// snapshot response from the controller.
	ErrSnapshotRequired = errors.New("control: an authoritative controller snapshot is required")
	// ErrStaleUpdate means an update cannot advance both monotonic revision and
	// nondecreasing epoch state.
	ErrStaleUpdate = errors.New("control: stale membership update")
	// ErrConflictingUpdate means an update contradicts the installed routing
	// contract or coordinated transition state.
	ErrConflictingUpdate = errors.New("control: conflicting membership update")
	// ErrAlreadyStarted means BeginSubscribe was called more than once.
	ErrAlreadyStarted = errors.New("control: reconciliation already started")
)

// Reconciler implements subscribe-first bootstrap:
//
//  1. establish the update subscription and call BeginSubscribe;
//  2. request the authoritative controller snapshot and pass it to ApplyBaseline;
//  3. concurrently pass subscription deliveries to ApplyUpdate (they buffer);
//  4. call FinishBootstrap to atomically install the response and reconcile
//     buffered updates in revision order.
//
// Thereafter ApplyUpdate installs fresh full snapshots immediately. Reconciler
// copies snapshots at its boundaries and is safe for concurrent use.
type Reconciler struct {
	mu       sync.RWMutex
	started  bool
	ready    bool
	baseline *MembershipSnapshot
	buffered []MembershipSnapshot
	current  *MembershipSnapshot
	group    string
}

// NewReconciler creates an unstarted reconciler.
func NewReconciler() *Reconciler {
	return &Reconciler{}
}

// BeginSubscribe records that the caller established the live update
// subscription. It intentionally precedes baseline to close the snapshot/update
// race.
func (r *Reconciler) BeginSubscribe() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return ErrAlreadyStarted
	}
	r.started = true
	return nil
}

// ApplyBaseline offers the authoritative snapshot returned by the controller.
// Duplicate responses are allowed only when their state is identical.
func (r *Reconciler) ApplyBaseline(snapshot MembershipSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	snapshot = snapshot.Clone()

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return ErrNotSubscribed
	}
	if r.group != "" && snapshot.ScalingGroup != r.group {
		return fmt.Errorf("control: scaling group changed from %q to %q", r.group, snapshot.ScalingGroup)
	}
	if r.group == "" {
		r.group = snapshot.ScalingGroup
	}
	if r.ready {
		return errors.New("control: baseline already finished")
	}
	if r.baseline == nil {
		r.baseline = &snapshot
		return nil
	}

	comparison, err := compareSnapshots(*r.baseline, snapshot)
	if err != nil {
		return err
	}
	if comparison < 0 {
		r.baseline = &snapshot
		return nil
	}
	if comparison > 0 && snapshot.Revision > r.baseline.Revision {
		return fmt.Errorf("%w: baselined epoch regressed from %d to %d", ErrStaleUpdate, r.baseline.Epoch, snapshot.Epoch)
	}
	return nil
}

// ApplyUpdate buffers a valid update until FinishBootstrap, then applies valid
// fresh updates immediately. The returned bool reports whether the installed
// snapshot changed; it is false while buffering or for an exact replay.
func (r *Reconciler) ApplyUpdate(snapshot MembershipSnapshot) (bool, error) {
	if err := snapshot.Validate(); err != nil {
		return false, err
	}
	snapshot = snapshot.Clone()

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return false, ErrNotSubscribed
	}
	if r.group != "" && snapshot.ScalingGroup != r.group {
		return false, fmt.Errorf("control: scaling group changed from %q to %q", r.group, snapshot.ScalingGroup)
	}
	if r.group == "" {
		r.group = snapshot.ScalingGroup
	}
	if !r.ready {
		r.buffered = append(r.buffered, snapshot)
		return false, nil
	}
	return r.applyLocked(snapshot)
}

// FinishBootstrap atomically installs the controller baseline, reconciles
// buffered updates in ascending revision/epoch order, and returns the result.
// Updates older than the response are expected race artifacts and are discarded.
func (r *Reconciler) FinishBootstrap() (MembershipSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return MembershipSnapshot{}, ErrNotSubscribed
	}
	if r.ready {
		return r.current.Clone(), nil
	}
	if r.baseline == nil {
		return MembershipSnapshot{}, ErrSnapshotRequired
	}

	current := r.baseline.Clone()
	r.current = &current
	sort.SliceStable(r.buffered, func(i, j int) bool {
		if r.buffered[i].Revision != r.buffered[j].Revision {
			return r.buffered[i].Revision < r.buffered[j].Revision
		}
		return r.buffered[i].Epoch < r.buffered[j].Epoch
	})
	for _, update := range r.buffered {
		comparison, err := compareSnapshots(*r.current, update)
		if err != nil {
			r.current = nil
			return MembershipSnapshot{}, err
		}
		if comparison > 0 {
			if update.Revision > r.current.Revision {
				r.current = nil
				return MembershipSnapshot{}, fmt.Errorf("%w: buffered epoch regressed from %d to %d at revision %d", ErrStaleUpdate, current.Epoch, update.Epoch, update.Revision)
			}
			// An older buffered delivery is the normal consequence of requesting
			// a controller snapshot after subscribing.
			continue
		}
		if comparison == 0 {
			continue
		}
		updated := update.Clone()
		r.current = &updated
	}
	r.buffered = nil
	r.baseline = nil
	r.ready = true
	return r.current.Clone(), nil
}

// Snapshot returns an independent copy of the installed state. No snapshot is
// visible until FinishBootstrap has reconciled the buffer.
func (r *Reconciler) Snapshot() (MembershipSnapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.ready || r.current == nil {
		return MembershipSnapshot{}, false
	}
	return r.current.Clone(), true
}

func (r *Reconciler) applyLocked(snapshot MembershipSnapshot) (bool, error) {
	comparison, err := compareSnapshots(*r.current, snapshot)
	if err != nil {
		return false, err
	}
	switch {
	case comparison > 0:
		return false, fmt.Errorf("%w: current epoch/revision is %d/%d, update is %d/%d", ErrStaleUpdate, r.current.Epoch, r.current.Revision, snapshot.Epoch, snapshot.Revision)
	case comparison == 0:
		return false, nil
	default:
		updated := snapshot.Clone()
		r.current = &updated
		return true, nil
	}
}

// compareSnapshots compares left to right. It returns -1 when right is a valid
// advance, 0 for an exact replay, and 1 when right is stale. Revision is globally
// monotonic while epoch is nondecreasing. Equal revisions must be identical;
// crossed revision/epoch ordering is a conflict.
func compareSnapshots(left, right MembershipSnapshot) (int, error) {
	if left.Version != right.Version {
		return 0, fmt.Errorf("control: snapshot version changed from %d to %d", left.Version, right.Version)
	}
	if left.ScalingGroup != right.ScalingGroup {
		return 0, fmt.Errorf("control: scaling group changed from %q to %q", left.ScalingGroup, right.ScalingGroup)
	}
	if right.Revision == left.Revision {
		if snapshotsEqual(left, right) {
			return 0, nil
		}
		return 0, fmt.Errorf("%w at revision %d", ErrConflictingUpdate, left.Revision)
	}
	if right.Revision < left.Revision {
		if right.Epoch > left.Epoch {
			return 0, fmt.Errorf("%w: lower revision %d carries higher epoch %d", ErrConflictingUpdate, right.Revision, right.Epoch)
		}
		return 1, nil
	}
	if right.Epoch < left.Epoch {
		return 1, nil
	}
	if err := validateAdvance(left, right); err != nil {
		return 0, err
	}
	return -1, nil
}

func validateRoutingCompatibility(left, right MembershipSnapshot) error {
	switch {
	case left.Namespace != right.Namespace:
		return fmt.Errorf("%w: namespace changed from %q to %q", ErrConflictingUpdate, left.Namespace, right.Namespace)
	case left.LibraryVersion != right.LibraryVersion:
		return fmt.Errorf("%w: library version changed from %q to %q", ErrConflictingUpdate, left.LibraryVersion, right.LibraryVersion)
	case left.HashContract != right.HashContract:
		return fmt.Errorf("%w: hash contract changed from %q to %q", ErrConflictingUpdate, left.HashContract, right.HashContract)
	case left.Algorithm != right.Algorithm:
		return fmt.Errorf("%w: routing algorithm changed from %q to %q", ErrConflictingUpdate, left.Algorithm, right.Algorithm)
	case left.Queue != right.Queue:
		return fmt.Errorf("%w: queue identity or delivery semantics changed", ErrConflictingUpdate)
	case left.Destination != right.Destination:
		return fmt.Errorf("%w: destination identity changed", ErrConflictingUpdate)
	default:
		return nil
	}
}

func validateAdvance(left, right MembershipSnapshot) error {
	if err := validateRoutingCompatibility(left, right); err != nil {
		return err
	}
	if right.Phase == PhaseActive {
		if left.Phase == PhaseActive {
			if right.Epoch == left.Epoch && (!right.CurrentMembership.Equal(left.CurrentMembership) || !resourcesEqual(right.CurrentResources, left.CurrentResources)) {
				return fmt.Errorf("%w: ACTIVE state changed without an epoch transition", ErrConflictingUpdate)
			}
			return nil // A later epoch may be authoritative catch-up after missed updates.
		}
		if left.Transition == nil {
			return fmt.Errorf("%w: activation has no preceding transition", ErrConflictingUpdate)
		}
		if right.Epoch == left.Epoch {
			if !right.CurrentMembership.Equal(left.CurrentMembership) || !resourcesEqual(right.CurrentResources, left.CurrentResources) {
				return fmt.Errorf("%w: rollback changed current state", ErrConflictingUpdate)
			}
			return nil
		}
		if right.Epoch != left.Transition.ToEpoch || !right.CurrentMembership.Equal(left.ProposedMembership) || !resourcesEqual(right.CurrentResources, left.ProposedResources) {
			return fmt.Errorf("%w: activation does not match proposed membership", ErrConflictingUpdate)
		}
		return nil
	}
	if right.Phase == PhaseCommitted {
		if left.Phase != PhaseDrain || left.Transition == nil || right.Transition == nil || *left.Transition != *right.Transition ||
			right.Epoch != left.Transition.ToEpoch || !right.CurrentMembership.Equal(left.ProposedMembership) ||
			!resourcesEqual(right.CurrentResources, left.ProposedResources) {
			return fmt.Errorf("%w: COMMITTED state does not match drained proposal", ErrConflictingUpdate)
		}
		return nil
	}
	if left.Phase == PhaseCommitted {
		return fmt.Errorf("%w: COMMITTED state may only advance to ACTIVE", ErrConflictingUpdate)
	}
	if left.Phase == PhaseActive {
		if right.Phase != PhasePrepare || right.Epoch != left.Epoch || !right.CurrentMembership.Equal(left.CurrentMembership) ||
			!resourcesEqual(right.CurrentResources, left.CurrentResources) {
			return fmt.Errorf("%w: transition did not begin from current ACTIVE membership", ErrConflictingUpdate)
		}
		return nil
	}
	if left.Transition == nil || right.Transition == nil || *left.Transition != *right.Transition ||
		right.Epoch != left.Epoch || !right.CurrentMembership.Equal(left.CurrentMembership) ||
		!right.ProposedMembership.Equal(left.ProposedMembership) ||
		!resourcesEqual(right.CurrentResources, left.CurrentResources) ||
		!resourcesEqual(right.ProposedResources, left.ProposedResources) {
		return fmt.Errorf("%w: transition identity or membership changed", ErrConflictingUpdate)
	}
	rank := func(phase Phase) int {
		switch phase {
		case PhasePrepare:
			return 1
		case PhasePaused:
			return 2
		case PhaseDrain:
			return 3
		default:
			return 0
		}
	}
	if rank(right.Phase) < rank(left.Phase) {
		return fmt.Errorf("%w: phase regressed from %s to %s", ErrStaleUpdate, left.Phase, right.Phase)
	}
	return nil
}
