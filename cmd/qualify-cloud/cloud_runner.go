package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	cloudapi "github.com/solacese/solace-workload-balancer/cloud"
)

const (
	cloudPollInterval   = 2 * time.Second
	cloudDefaultBaseURL = cloudapi.DefaultBaseURL
)

type missionControlRunner struct{}

func (missionControlRunner) Preflight(ctx context.Context, jwt, baseURL string, plan Plan) (Plan, error) {
	client, err := cloudapi.NewClient(jwt, cloudapi.WithBaseURL(baseURL))
	if err != nil {
		return Plan{}, err
	}
	resolved, err := resolvePlan(ctx, client, plan)
	if err != nil {
		return Plan{}, err
	}
	if _, err := cloudPlan(resolved); err != nil {
		return Plan{}, err
	}
	if _, err := tokenSubject(jwt); err != nil {
		return Plan{}, err
	}
	return resolved, nil
}

func (missionControlRunner) Provision(ctx context.Context, jwt, baseURL string, plan Plan, journalPath string) ([]ResourceRecord, error) {
	client, cloudPlan, owner, err := lifecycleForResolvedPlan(jwt, baseURL, plan)
	if err != nil {
		return nil, err
	}
	journal, err := cloudapi.OpenJournal(journalPath, cloudPlan, owner)
	if err != nil {
		return nil, err
	}
	defer journal.Close()
	lifecycle, err := cloudapi.NewLifecycle(client, journal, cloudPlan, cloudPollInterval)
	if err != nil {
		return nil, err
	}
	services, err := lifecycle.Ensure(ctx)
	return recordsFromServices(services), err
}

func (missionControlRunner) Cleanup(ctx context.Context, jwt, baseURL string, journalRecord Journal, journalPath string) error {
	if _, err := os.Stat(journalPath); err != nil {
		if os.IsNotExist(err) && len(journalRecord.Resources) == 0 {
			return nil
		}
		if os.IsNotExist(err) {
			return fmt.Errorf("cloud lifecycle journal is missing")
		}
		return err
	}
	client, cloudPlan, owner, err := lifecycleForResolvedPlan(jwt, baseURL, journalRecord.Plan)
	if err != nil {
		return err
	}
	journal, err := cloudapi.OpenJournal(journalPath, cloudPlan, owner)
	if err != nil {
		return err
	}
	defer journal.Close()
	lifecycle, err := cloudapi.NewLifecycle(client, journal, cloudPlan, cloudPollInterval)
	if err != nil {
		return err
	}
	if err := lifecycle.Cleanup(ctx); err != nil {
		return err
	}
	for _, resource := range journal.Services() {
		if resource.ServiceID == "" && resource.State != "ABSENT" {
			return fmt.Errorf("cleanup journal contains an unresolved create outcome")
		}
	}
	return nil
}

func lifecycleForResolvedPlan(jwt, baseURL string, plan Plan) (*cloudapi.Client, cloudapi.QualificationPlan, string, error) {
	cloudPlan, err := cloudPlan(plan)
	if err != nil {
		return nil, cloudapi.QualificationPlan{}, "", err
	}
	owner, err := tokenSubject(jwt)
	if err != nil {
		return nil, cloudapi.QualificationPlan{}, "", err
	}
	client, err := cloudapi.NewClient(jwt, cloudapi.WithBaseURL(baseURL), cloudapi.WithTokenOwner(owner))
	if err != nil {
		return nil, cloudapi.QualificationPlan{}, "", err
	}
	return client, cloudPlan, owner, nil
}

func resolvePlan(ctx context.Context, client *cloudapi.Client, plan Plan) (Plan, error) {
	datacenters, err := client.ListDatacenters(ctx)
	if err != nil {
		return Plan{}, fmt.Errorf("list datacenters: %w", err)
	}
	classes, err := client.ListServiceClasses(ctx)
	if err != nil {
		return Plan{}, fmt.Errorf("list service classes: %w", err)
	}
	datacenterID := ""
	for _, datacenter := range datacenters {
		if datacenter.ID == qualificationDatacenter || datacenter.Name == qualificationDatacenter {
			if datacenterID != "" && datacenterID != datacenter.ID {
				return Plan{}, fmt.Errorf("datacenter selection is ambiguous")
			}
			if !datacenter.Available || !datacenter.Visible {
				return Plan{}, fmt.Errorf("required datacenter is unavailable")
			}
			datacenterID = datacenter.ID
		}
	}
	if datacenterID == "" {
		return Plan{}, fmt.Errorf("required datacenter was not found")
	}
	versions, err := client.ListCompatibleBrokerVersions(ctx, datacenterID)
	if err != nil {
		return Plan{}, fmt.Errorf("list compatible broker versions: %w", err)
	}

	dataID, controlID := "", ""
	var dataClass, controlClass cloudapi.ServiceClass
	for _, class := range classes {
		switch class.ID {
		case dataServiceClass:
			if dataID != "" {
				return Plan{}, fmt.Errorf("data service class selection is ambiguous")
			}
			dataID, dataClass = class.ID, class
		case controlServiceClass:
			if controlID != "" {
				return Plan{}, fmt.Errorf("control service class selection is ambiguous")
			}
			controlID, controlClass = class.ID, class
		}
	}
	if dataID == "" || controlID == "" {
		return Plan{}, fmt.Errorf("required service classes were not found")
	}
	if !contains(datacenterSupportedClasses(datacenters, datacenterID), dataID) ||
		!contains(datacenterSupportedClasses(datacenters, datacenterID), controlID) {
		return Plan{}, fmt.Errorf("required service classes are not supported by the selected datacenter")
	}
	if availableQuota(dataClass) < 3 {
		return Plan{}, fmt.Errorf("Enterprise 5K standalone quota cannot accommodate three data services")
	}
	if availableQuota(controlClass) < 1 {
		return Plan{}, fmt.Errorf("Enterprise 250 HA control-pool quota cannot accommodate Broker 0")
	}

	versionFound := false
	for _, version := range versions {
		if version.Version == qualificationRelease && version.Recommended && isLTS(version.ReleaseChannel) &&
			contains(version.SupportedServiceClasses, dataID) && contains(version.SupportedServiceClasses, controlID) {
			if versionFound {
				return Plan{}, fmt.Errorf("broker release selection is ambiguous")
			}
			versionFound = true
		}
	}
	if !versionFound {
		return Plan{}, fmt.Errorf("required broker release was not found")
	}

	for index := range plan.Services {
		plan.Services[index].DatacenterID = datacenterID
		plan.Services[index].BrokerVersionID = qualificationRelease
		plan.Services[index].ServiceClassID = dataID
		if plan.Services[index].Role == "broker-0" {
			plan.Services[index].ServiceClassID = controlID
		}
	}
	if err := plan.validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func cloudPlan(plan Plan) (cloudapi.QualificationPlan, error) {
	var services [4]cloudapi.PlannedService
	for index, service := range plan.Services {
		if service.DatacenterID == "" || service.ServiceClassID == "" || service.BrokerVersionID == "" {
			return cloudapi.QualificationPlan{}, fmt.Errorf("qualification plan has not been resolved by preflight")
		}
		services[index] = cloudapi.PlannedService{
			Role: service.Role,
			Request: cloudapi.CreateServiceRequest{
				Name:           service.Name,
				DatacenterID:   service.DatacenterID,
				ServiceClassID: service.ServiceClassID,
				BrokerVersion:  service.BrokerVersionID,
				MessageVPN:     messageVPNName(service.Role, plan.ID),
				Locked:         false,
				DMREnabled:     false,
				ServiceConnectionEndpoints: []cloudapi.ConnectionEndpoint{{
					Name:       "public",
					AccessType: "PUBLIC",
					Ports: []cloudapi.ConnectionPort{
						{Protocol: "serviceSmfTlsListenPort", Port: 55443},
						{Protocol: "serviceManagementTlsListenPort", Port: 943},
					},
				}},
			},
		}
		if service.Role == "broker-0" {
			services[index].Request.RedundancyGroupSSLEnabled = true
		}
	}
	return cloudapi.NewQualificationPlan(services)
}

func datacenterSupportedClasses(datacenters []cloudapi.Datacenter, id string) []string {
	for _, datacenter := range datacenters {
		if datacenter.ID == id {
			return datacenter.SupportedServiceClasses
		}
	}
	return nil
}

func availableQuota(class cloudapi.ServiceClass) int {
	if len(class.Limits) == 0 {
		return -1
	}
	available := class.Limits[0].Limit - class.Limits[0].InUse
	for _, limit := range class.Limits[1:] {
		if limit.Limit-limit.InUse != available {
			return -1
		}
	}
	return available
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func isLTS(channel string) bool {
	normalized := normalize(channel)
	return strings.Contains(normalized, "lts") || strings.Contains(normalized, "longtermsupport")
}

func tokenSubject(jwt string) (string, error) {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("JWT must contain three segments")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("decode JWT claims: %w", err)
	}
	var claims struct {
		Subject string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("decode JWT claims: %w", err)
	}
	if strings.TrimSpace(claims.Subject) == "" {
		return "", fmt.Errorf("JWT subject is required for safe lifecycle ownership")
	}
	return claims.Subject, nil
}

func recordsFromServices(services map[string]cloudapi.Service) []ResourceRecord {
	roles := make([]string, 0, len(services))
	for role := range services {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	resources := make([]ResourceRecord, 0, len(roles))
	for _, role := range roles {
		resources = append(resources, ResourceRecord{Role: role, ID: services[role].ID})
	}
	return resources
}

func messageVPNName(role, planID string) string {
	value := normalize(role + planID)
	if len(value) > 26 {
		value = value[:26]
	}
	return value
}

func normalize(value string) string {
	return strings.ToLower(strings.NewReplacer("_", "", "-", "", " ", "").Replace(value))
}
