package integration

import (
	"errors"
	"fmt"
	"slices"

	"github.com/solacese/solace-workload-balancer/control"
)

type SmokeReceipt struct {
	EventID         string `json:"event_id"`
	DurablyAccepted bool   `json:"durably_accepted"`
	Group           string `json:"group"`
	Epoch           uint64 `json:"epoch"`
	Broker          string `json:"broker"`
}

type SmokeDelivery struct {
	EventID string
	Group   string
	Order   int
}

type SmokeEvidence struct {
	Submitted   map[string][]string
	Receipts    map[string]SmokeReceipt
	Deliveries  []SmokeDelivery
	Memberships []control.MembershipSnapshot
}

type SmokeReport struct{ Duplicates map[string]int }

// RecordActiveMembership samples only ACTIVE controller states. Transitional
// snapshots remain valid runtime state, but they are not members of the ACTIVE
// membership sequence verified by the live smoke.
func RecordActiveMembership(e *SmokeEvidence, snapshots map[string]control.MembershipSnapshot) {
	for _, group := range []string{"events-a", "events-b"} {
		snapshot, ok := snapshots[group]
		if !ok || snapshot.Revision == 0 || snapshot.Phase != control.PhaseActive {
			continue
		}
		duplicate := false
		for _, recorded := range e.Memberships {
			if recorded.ScalingGroup == group && recorded.Revision == snapshot.Revision {
				duplicate = true
				break
			}
		}
		if !duplicate {
			e.Memberships = append(e.Memberships, snapshot.Clone())
		}
	}
}

func VerifySmokeEvidence(e SmokeEvidence, eventsASequence [][]string, eventsBMembership []string) (SmokeReport, error) {
	report := SmokeReport{Duplicates: make(map[string]int)}
	var errs []error
	for group, submitted := range e.Submitted {
		for _, id := range submitted {
			receipt, ok := e.Receipts[id]
			if !ok || !receipt.DurablyAccepted || receipt.EventID != id || receipt.Group != group || receipt.Epoch == 0 || receipt.Broker == "" {
				errs = append(errs, fmt.Errorf("invalid or missing durable receipt for %s", id))
			}
		}
		var observed []string
		counts := make(map[string]int)
		for _, delivery := range e.Deliveries {
			if delivery.Group == group {
				counts[delivery.EventID]++
				if counts[delivery.EventID] == 1 {
					observed = append(observed, delivery.EventID)
				}
			}
		}
		for id, count := range counts {
			if count > 1 {
				report.Duplicates[id] = count - 1
			}
		}
		if !slices.Equal(observed, submitted) {
			errs = append(errs, fmt.Errorf("%s delivery order/completeness mismatch: got %v want %v", group, observed, submitted))
		}
	}
	var seen [][]string
	eventsBSeen := false
	for _, snapshot := range e.Memberships {
		if snapshot.Phase != control.PhaseActive {
			errs = append(errs, fmt.Errorf("membership %s revision %d is not ACTIVE", snapshot.ScalingGroup, snapshot.Revision))
			continue
		}
		membership := []string(snapshot.CurrentMembership)
		switch snapshot.ScalingGroup {
		case "events-a":
			if len(seen) == 0 || !slices.Equal(seen[len(seen)-1], membership) {
				seen = append(seen, slices.Clone(membership))
			}
		case "events-b":
			eventsBSeen = true
			if !slices.Equal(membership, eventsBMembership) {
				errs = append(errs, fmt.Errorf("eventsB membership changed: %v", membership))
			}
		}
	}
	if !eventsBSeen {
		errs = append(errs, errors.New("missing ACTIVE EventsB membership evidence"))
	}
	if len(seen) < len(eventsASequence) {
		errs = append(errs, fmt.Errorf("incomplete EventsA membership sequence: %v", seen))
	} else {
		tail := seen[len(seen)-len(eventsASequence):]
		for i := range eventsASequence {
			if !slices.Equal(tail[i], eventsASequence[i]) {
				errs = append(errs, fmt.Errorf("wrong EventsA membership sequence: %v", tail))
				break
			}
		}
	}
	return report, errors.Join(errs...)
}
