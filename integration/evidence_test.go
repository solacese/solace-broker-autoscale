package integration

import (
	"testing"

	"github.com/solacese/solace-workload-balancer/control"
)

func validSmokeEvidence() SmokeEvidence {
	submitted := map[string][]string{"events-a": {"f1", "f2"}, "events-b": {"b1", "b2"}}
	receipts := map[string]SmokeReceipt{}
	for group, ids := range submitted {
		for _, id := range ids {
			receipts[id] = SmokeReceipt{EventID: id, DurablyAccepted: true, Group: group, Epoch: 1, Broker: "A"}
		}
	}
	members := []control.MembershipSnapshot{}
	for revision, ids := range [][]string{{"A"}, {"A", "B"}, {"A", "B", "C"}, {"A", "B"}} {
		members = append(members, control.MembershipSnapshot{ScalingGroup: "events-a", Revision: uint64(revision + 1), Phase: control.PhaseActive, CurrentMembership: ids})
	}
	members = append(members, control.MembershipSnapshot{ScalingGroup: "events-b", Revision: 1, Phase: control.PhaseActive, CurrentMembership: control.Membership{"A"}})
	return SmokeEvidence{Submitted: submitted, Receipts: receipts, Deliveries: []SmokeDelivery{{"f1", "events-a", 0}, {"f2", "events-a", 1}, {"b1", "events-b", 0}, {"b2", "events-b", 1}}, Memberships: members}
}
func sequence() [][]string { return [][]string{{"A"}, {"A", "B"}, {"A", "B", "C"}, {"A", "B"}} }
func TestVerifySmokeEvidence(t *testing.T) {
	if report, err := VerifySmokeEvidence(validSmokeEvidence(), sequence(), []string{"A"}); err != nil || len(report.Duplicates) != 0 {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}
func TestVerifySmokeEvidenceRejectsMissingWrongOrderAndReceipt(t *testing.T) {
	for name, mutate := range map[string]func(*SmokeEvidence){"missing": func(e *SmokeEvidence) { e.Deliveries = e.Deliveries[1:] }, "order": func(e *SmokeEvidence) { e.Deliveries[0], e.Deliveries[1] = e.Deliveries[1], e.Deliveries[0] }, "receipt": func(e *SmokeEvidence) { r := e.Receipts["f1"]; r.DurablyAccepted = false; e.Receipts["f1"] = r }, "membership": func(e *SmokeEvidence) { e.Memberships[2].CurrentMembership = []string{"A", "C", "B"} }} {
		t.Run(name, func(t *testing.T) {
			e := validSmokeEvidence()
			mutate(&e)
			if _, err := VerifySmokeEvidence(e, sequence(), []string{"A"}); err == nil {
				t.Fatal("invalid evidence passed")
			}
		})
	}
}
func TestVerifySmokeEvidenceReportsDuplicatesSeparately(t *testing.T) {
	e := validSmokeEvidence()
	e.Deliveries = append(e.Deliveries, SmokeDelivery{"f2", "events-a", 2})
	report, err := VerifySmokeEvidence(e, sequence(), []string{"A"})
	if err != nil || report.Duplicates["f2"] != 1 {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}

func TestRecordActiveMembershipFiltersTransitions(t *testing.T) {
	e := SmokeEvidence{}
	RecordActiveMembership(&e, map[string]control.MembershipSnapshot{
		"events-a": {ScalingGroup: "events-a", Revision: 2, Phase: control.PhasePrepare, CurrentMembership: control.Membership{"A"}},
		"events-b": {ScalingGroup: "events-b", Revision: 1, Phase: control.PhaseActive, CurrentMembership: control.Membership{"A"}},
	})
	if len(e.Memberships) != 1 || e.Memberships[0].ScalingGroup != "events-b" {
		t.Fatalf("memberships=%#v", e.Memberships)
	}
	RecordActiveMembership(&e, map[string]control.MembershipSnapshot{"events-a": {ScalingGroup: "events-a", Revision: 3, Phase: control.PhaseActive, CurrentMembership: control.Membership{"A", "B"}}})
	if len(e.Memberships) != 2 || e.Memberships[1].Phase != control.PhaseActive {
		t.Fatalf("memberships=%#v", e.Memberships)
	}
}

func TestVerifySmokeEvidenceRequiresEventsBMembership(t *testing.T) {
	e := validSmokeEvidence()
	e.Memberships = e.Memberships[:len(e.Memberships)-1]
	if _, err := VerifySmokeEvidence(e, sequence(), []string{"A"}); err == nil {
		t.Fatal("missing EventsB evidence passed")
	}
}
