package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

const (
	qualificationDatacenter = "gke-gcp-us-east4-a"
	qualificationRelease    = "10.26.0.8894-14"
	controlServiceClass     = "ENTERPRISE_250_HIGHAVAILABILITY"
	dataServiceClass        = "ENTERPRISE_5K_STANDALONE"
)

type ServiceSpec struct {
	Role            string
	Name            string
	ServiceClass    string
	ServiceClassID  string
	Capacity        string
	HighAvailable   bool
	Datacenter      string
	DatacenterID    string
	Release         string
	BrokerVersionID string
}

type Plan struct {
	ID       string
	Created  time.Time
	Services [4]ServiceSpec
}

func newPlan(now time.Time, random io.Reader) (Plan, error) {
	if random == nil {
		random = rand.Reader
	}
	now = now.UTC()
	var entropy [8]byte
	if _, err := io.ReadFull(random, entropy[:]); err != nil {
		return Plan{}, fmt.Errorf("generate plan entropy: %w", err)
	}
	suffix := now.Format("060102t150405") + "-" + hex.EncodeToString(entropy[:4])
	base := "swlb-q-" + suffix
	plan := Plan{
		ID:      suffix,
		Created: now,
		Services: [4]ServiceSpec{
			{Role: "broker-0", Name: base + "-broker-0", ServiceClass: "enterprise", Capacity: "250", HighAvailable: true, Datacenter: qualificationDatacenter, Release: qualificationRelease},
			{Role: "broker-a", Name: base + "-broker-a", ServiceClass: "enterprise", Capacity: "5k", Datacenter: qualificationDatacenter, Release: qualificationRelease},
			{Role: "broker-b", Name: base + "-broker-b", ServiceClass: "enterprise", Capacity: "5k", Datacenter: qualificationDatacenter, Release: qualificationRelease},
			{Role: "broker-c", Name: base + "-broker-c", ServiceClass: "enterprise", Capacity: "5k", Datacenter: qualificationDatacenter, Release: qualificationRelease},
		},
	}
	if err := plan.validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (p Plan) validate() error {
	seenRoles := make(map[string]struct{}, len(p.Services))
	seenNames := make(map[string]struct{}, len(p.Services))
	for _, service := range p.Services {
		if service.Role == "" || service.Name == "" {
			return fmt.Errorf("qualification plan contains an unnamed service")
		}
		if service.Datacenter != qualificationDatacenter || service.Release != qualificationRelease {
			return fmt.Errorf("qualification plan contains an unsupported placement")
		}
		resolved := service.DatacenterID != "" || service.ServiceClassID != "" || service.BrokerVersionID != ""
		if resolved && (service.DatacenterID == "" || service.ServiceClassID == "" || service.BrokerVersionID == "") {
			return fmt.Errorf("qualification plan contains a partially resolved service")
		}
		if _, exists := seenRoles[service.Role]; exists {
			return fmt.Errorf("qualification plan contains duplicate role %q", service.Role)
		}
		if _, exists := seenNames[service.Name]; exists {
			return fmt.Errorf("qualification plan contains a duplicate service name")
		}
		seenRoles[service.Role] = struct{}{}
		seenNames[service.Name] = struct{}{}
	}
	broker0 := p.Services[0]
	if broker0.Role != "broker-0" || broker0.ServiceClass != "enterprise" || broker0.Capacity != "250" || !broker0.HighAvailable {
		return fmt.Errorf("qualification plan requires Broker 0 Enterprise 250 HA")
	}
	for index, role := range []string{"broker-a", "broker-b", "broker-c"} {
		service := p.Services[index+1]
		if service.Role != role || service.ServiceClass != "enterprise" || service.Capacity != "5k" || service.HighAvailable {
			return fmt.Errorf("qualification plan requires Brokers A, B, and C as Enterprise 5K standalone services")
		}
	}
	return nil
}
