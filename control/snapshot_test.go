package control

import (
	"encoding/json"
	"strings"
	"testing"
)

func validActiveSnapshot() MembershipSnapshot {
	return MembershipSnapshot{
		Version:           SnapshotVersion,
		ScalingGroup:      "events-a",
		Revision:          7,
		Epoch:             3,
		Phase:             PhaseActive,
		HashContract:      "entity-affinity-v1",
		Algorithm:         AlgorithmRendezvousV1,
		CurrentMembership: Membership{"broker-z", "broker-a"},
		CurrentBrokers:    []BrokerDescriptor{{ID: "broker-z", Endpoint: "amqps://z.example:5671"}, {ID: "broker-a", Endpoint: "amqps://a.example:5671"}},
		CurrentResources: []EpochResourceIdentity{
			{Epoch: 3, BrokerID: "broker-z", ConsumerSet: "default", QueueName: "eventsA.operations.broker-z", IngressTopic: "events/>"},
			{Epoch: 3, BrokerID: "broker-a", ConsumerSet: "default", QueueName: "eventsA.operations.broker-a", IngressTopic: "events/>"},
		},
		Queue:       QueueInfo{Name: "eventsA.operations", Durable: true},
		Destination: DestinationInfo{Kind: DestinationTopic, Name: "events/>"},
	}
}

func validTransitionSnapshot(phase Phase) MembershipSnapshot {
	snapshot := validActiveSnapshot()
	snapshot.Revision++
	snapshot.Phase = phase
	snapshot.ProposedMembership = Membership{"broker-z", "broker-a", "broker-m"}
	snapshot.ProposedBrokers = []BrokerDescriptor{{ID: "broker-z", Endpoint: "amqps://z.example:5671"}, {ID: "broker-a", Endpoint: "amqps://a.example:5671"}, {ID: "broker-m", Endpoint: "amqps://m.example:5671"}}
	snapshot.ProposedResources = []EpochResourceIdentity{
		{Epoch: 4, BrokerID: "broker-z", ConsumerSet: "default", QueueName: "eventsA.operations.broker-z.e4", IngressTopic: "events/e4/>"},
		{Epoch: 4, BrokerID: "broker-a", ConsumerSet: "default", QueueName: "eventsA.operations.broker-a.e4", IngressTopic: "events/e4/>"},
		{Epoch: 4, BrokerID: "broker-m", ConsumerSet: "default", QueueName: "eventsA.operations.broker-m.e4", IngressTopic: "events/e4/>"},
	}
	snapshot.Transition = &Transition{ID: "scale-20260925-1", FromEpoch: 3, ToEpoch: 4}
	return snapshot
}

func TestMembershipSnapshotAcceptsOpaqueRoutingAlgorithm(t *testing.T) {
	snapshot := validActiveSnapshot()
	snapshot.Algorithm = "customer-xxhash-score-v2"
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMembershipSnapshotValidPhases(t *testing.T) {
	for _, snapshot := range []MembershipSnapshot{
		validActiveSnapshot(),
		validTransitionSnapshot(PhasePrepare),
		validTransitionSnapshot(PhasePaused),
		validTransitionSnapshot(PhaseDrain),
		func() MembershipSnapshot {
			snapshot := validTransitionSnapshot(PhaseCommitted)
			snapshot.Epoch = snapshot.Transition.ToEpoch
			snapshot.CurrentMembership = snapshot.ProposedMembership.Clone()
			snapshot.CurrentBrokers = append([]BrokerDescriptor(nil), snapshot.ProposedBrokers...)
			snapshot.CurrentResources = append([]EpochResourceIdentity(nil), snapshot.ProposedResources...)
			snapshot.ProposedMembership = nil
			snapshot.ProposedBrokers = nil
			snapshot.ProposedResources = nil
			return snapshot
		}(),
	} {
		if err := snapshot.Validate(); err != nil {
			t.Fatalf("Validate(%s) = %v", snapshot.Phase, err)
		}
	}
}

func TestMembershipSnapshotValidation(t *testing.T) {
	tests := map[string]func(*MembershipSnapshot){
		"version":              func(s *MembershipSnapshot) { s.Version++ },
		"empty group":          func(s *MembershipSnapshot) { s.ScalingGroup = "" },
		"zero revision":        func(s *MembershipSnapshot) { s.Revision = 0 },
		"zero epoch":           func(s *MembershipSnapshot) { s.Epoch = 0 },
		"empty hash contract":  func(s *MembershipSnapshot) { s.HashContract = "" },
		"empty algorithm":      func(s *MembershipSnapshot) { s.Algorithm = "" },
		"invalid algorithm":    func(s *MembershipSnapshot) { s.Algorithm = "bad\x00algorithm" },
		"unknown phase":        func(s *MembershipSnapshot) { s.Phase = "UNKNOWN" },
		"empty membership":     func(s *MembershipSnapshot) { s.CurrentMembership = nil },
		"duplicate membership": func(s *MembershipSnapshot) { s.CurrentMembership = Membership{"b1", "b1"} },
		"empty broker":         func(s *MembershipSnapshot) { s.CurrentMembership = Membership{"b1", ""} },
		"credential URL":       func(s *MembershipSnapshot) { s.CurrentMembership = Membership{"amqp://user:password@broker"} },
		"non-durable queue":    func(s *MembershipSnapshot) { s.Queue.Durable = false },
		"unknown destination": func(s *MembershipSnapshot) {
			s.Destination.Kind = "EXCHANGE"
		},
		"empty destination": func(s *MembershipSnapshot) { s.Destination.Name = "" },
		"active proposal": func(s *MembershipSnapshot) {
			s.ProposedMembership = Membership{"b2"}
		},
		"active transition": func(s *MembershipSnapshot) {
			s.Transition = &Transition{ID: "t", FromEpoch: 3, ToEpoch: 4}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := validActiveSnapshot()
			mutate(&snapshot)
			if err := snapshot.Validate(); err == nil {
				t.Fatal("Validate() succeeded")
			}
		})
	}
}

func TestTransitionValidation(t *testing.T) {
	tests := map[string]func(*MembershipSnapshot){
		"missing proposal":   func(s *MembershipSnapshot) { s.ProposedMembership = nil },
		"same proposal":      func(s *MembershipSnapshot) { s.ProposedMembership = s.CurrentMembership.Clone() },
		"duplicate proposal": func(s *MembershipSnapshot) { s.ProposedMembership = Membership{"broker-a", "broker-a"} },
		"missing transition": func(s *MembershipSnapshot) { s.Transition = nil },
		"empty transition ID": func(s *MembershipSnapshot) {
			s.Transition.ID = ""
		},
		"wrong from epoch": func(s *MembershipSnapshot) { s.Transition.FromEpoch-- },
		"wrong to epoch":   func(s *MembershipSnapshot) { s.Transition.ToEpoch++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := validTransitionSnapshot(PhasePrepare)
			mutate(&snapshot)
			if err := snapshot.Validate(); err == nil {
				t.Fatal("Validate() succeeded")
			}
		})
	}
}

func TestReorderOnlyTransitionIsExplicitAndValid(t *testing.T) {
	snapshot := validTransitionSnapshot(PhasePrepare)
	snapshot.ProposedMembership = Membership{"broker-a", "broker-z"}
	snapshot.ProposedBrokers = descriptors(snapshot.ProposedMembership)
	snapshot.ProposedResources = snapshot.ProposedResources[:2]
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("Validate() rejected coordinated reorder: %v", err)
	}
	if snapshot.CurrentMembership.Equal(snapshot.ProposedMembership) {
		t.Fatal("ordered memberships unexpectedly equal")
	}
}

func TestMembershipOrderIsPreserved(t *testing.T) {
	snapshot := validActiveSnapshot()
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseMembershipSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.CurrentMembership.Equal(Membership{"broker-z", "broker-a"}) {
		t.Fatalf("membership order changed: %v", parsed.CurrentMembership)
	}
	if parsed.CurrentMembership.Equal(Membership{"broker-a", "broker-z"}) {
		t.Fatal("order-sensitive membership equality accepted reorder")
	}
}

func TestCloneOwnsMutableState(t *testing.T) {
	snapshot := validTransitionSnapshot(PhasePrepare)
	clone := snapshot.Clone()
	clone.CurrentMembership[0] = "changed"
	clone.ProposedMembership[0] = "changed"
	clone.Transition.ID = "changed"
	if snapshot.CurrentMembership[0] != "broker-z" || snapshot.ProposedMembership[0] != "broker-z" || snapshot.Transition.ID != "scale-20260925-1" {
		t.Fatalf("Clone shares mutable state with source: %+v", snapshot)
	}
}

func TestParseMembershipSnapshotStrictJSON(t *testing.T) {
	valid, err := json.Marshal(validActiveSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseMembershipSnapshot(valid); err != nil {
		t.Fatalf("valid JSON rejected: %v", err)
	}

	tests := map[string]string{
		"unknown field":     strings.TrimSuffix(string(valid), "}") + `,"endpoint":"amqp://broker"}`,
		"duplicate field":   strings.Replace(string(valid), `"version":3`, `"version":3,"version":3`, 1),
		"trailing value":    string(valid) + `{}`,
		"secret field":      strings.TrimSuffix(string(valid), "}") + `,"password":"do-not-accept"}`,
		"nested secret":     strings.TrimSuffix(string(valid), "}") + `,"metadata":{"api_token":"do-not-accept"}}`,
		"wrong scalar type": strings.Replace(string(valid), `"revision":7`, `"revision":"7"`, 1),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseMembershipSnapshot([]byte(data)); err == nil {
				t.Fatal("ParseMembershipSnapshot() succeeded")
			}
		})
	}
}

func TestParseMembershipSnapshotSizeLimit(t *testing.T) {
	if _, err := ParseMembershipSnapshot(make([]byte, maxSnapshotBytes+1)); err == nil {
		t.Fatal("oversized snapshot accepted")
	}
}
