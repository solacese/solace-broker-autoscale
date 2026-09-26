package main

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestNewPlanIsExactAndUnique(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 25, 10, 11, 12, 0, time.FixedZone("offset", 3600))
	first, err := newPlan(now, bytes.NewReader([]byte{0, 1, 2, 3, 4, 5, 6, 7}))
	if err != nil {
		t.Fatal(err)
	}
	second, err := newPlan(now, bytes.NewReader([]byte{7, 6, 5, 4, 3, 2, 1, 0}))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatal("plan IDs are not unique")
	}
	if first.Created.Location() != time.UTC {
		t.Fatal("plan time is not normalized to UTC")
	}
	wantRoles := []string{"broker-0", "broker-a", "broker-b", "broker-c"}
	roles := make([]string, 0, 4)
	for _, service := range first.Services {
		roles = append(roles, service.Role)
		if service.Datacenter != qualificationDatacenter || service.Release != qualificationRelease {
			t.Fatalf("wrong placement: %#v", service)
		}
	}
	if !reflect.DeepEqual(roles, wantRoles) {
		t.Fatalf("roles = %v, want %v", roles, wantRoles)
	}
	if got := first.Services[0]; got.ServiceClass != "enterprise" || got.Capacity != "250" || !got.HighAvailable {
		t.Fatalf("Broker 0 = %#v", got)
	}
	for _, service := range first.Services[1:] {
		if service.ServiceClass != "enterprise" || service.HighAvailable || service.Capacity != "5k" {
			t.Fatalf("data service = %#v", service)
		}
	}
}

func TestPlanValueDoesNotExposeMutableServiceSlice(t *testing.T) {
	t.Parallel()
	plan, err := newPlan(time.Unix(0, 0), bytes.NewReader(make([]byte, 8)))
	if err != nil {
		t.Fatal(err)
	}
	copyOfPlan := plan
	copyOfPlan.Services[0].Name = "changed"
	if plan.Services[0].Name == copyOfPlan.Services[0].Name {
		t.Fatal("plan copy shares mutable service backing storage")
	}
}
