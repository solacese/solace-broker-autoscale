// Package cloud implements the narrow Solace Cloud Mission Control lifecycle
// needed by qualification runs. It never provisions anything unless Create is
// called explicitly.
package cloud

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const DefaultBaseURL = "https://api.solace.cloud"

// ServiceIdentity is the complete identity used to reconcile an uncertain
// create. Name alone is deliberately insufficient.
type ServiceIdentity struct {
	Name                       string               `json:"name"`
	DatacenterID               string               `json:"datacenterId"`
	ServiceClassID             string               `json:"serviceClassId"`
	BrokerVersion              string               `json:"eventBrokerVersion"`
	MessageVPN                 string               `json:"msgVpnName"`
	MaxSpoolUsageGB            int                  `json:"maxSpoolUsage,omitempty"`
	EnvironmentID              string               `json:"environmentId,omitempty"`
	Locked                     bool                 `json:"locked"`
	RedundancyGroupSSLEnabled  bool                 `json:"redundancyGroupSslEnabled"`
	DMREnabled                 bool                 `json:"dmrEnabled"`
	ServiceConnectionEndpoints []ConnectionEndpoint `json:"serviceConnectionEndpoints,omitempty"`
}

func (i ServiceIdentity) validate() error {
	if strings.TrimSpace(i.Name) == "" || strings.TrimSpace(i.DatacenterID) == "" || strings.TrimSpace(i.ServiceClassID) == "" ||
		strings.TrimSpace(i.BrokerVersion) == "" || strings.TrimSpace(i.MessageVPN) == "" {
		return errors.New("service identity requires name, datacenter ID, service class ID, broker version, and message VPN")
	}
	return nil
}

func (i ServiceIdentity) matches(s Service) bool { return len(i.mismatches(s)) == 0 }

// matchesPlan permits journals written by the previous schema, which did not
// persist optional provisioning-contract fields, while requiring every field
// actually present in the journal to match the immutable plan.
func (i ServiceIdentity) matchesPlan(planned ServiceIdentity) bool {
	if i.Name != planned.Name || i.DatacenterID != planned.DatacenterID || i.ServiceClassID != planned.ServiceClassID ||
		i.BrokerVersion != planned.BrokerVersion || i.MessageVPN != planned.MessageVPN || i.EnvironmentID != planned.EnvironmentID ||
		i.Locked != planned.Locked || i.RedundancyGroupSSLEnabled != planned.RedundancyGroupSSLEnabled {
		return false
	}
	if i.MaxSpoolUsageGB != 0 && i.MaxSpoolUsageGB != planned.MaxSpoolUsageGB {
		return false
	}
	if i.DMREnabled && i.DMREnabled != planned.DMREnabled {
		return false
	}
	if len(i.ServiceConnectionEndpoints) != 0 && !reflect.DeepEqual(i.ServiceConnectionEndpoints, planned.ServiceConnectionEndpoints) {
		return false
	}
	return true
}

func (i ServiceIdentity) mismatches(s Service) []string {
	var fields []string
	if s.Name != i.Name {
		fields = append(fields, "name")
	}
	if s.DatacenterID != i.DatacenterID {
		fields = append(fields, "datacenterId")
	}
	if s.ServiceClassID != i.ServiceClassID {
		fields = append(fields, "serviceClassId")
	}
	if s.BrokerVersion != i.BrokerVersion {
		fields = append(fields, "eventBrokerServiceVersion")
	}
	if s.MessageVPN != i.MessageVPN {
		fields = append(fields, "msgVpnName")
	}
	if i.EnvironmentID != "" && s.EnvironmentID != i.EnvironmentID {
		fields = append(fields, "environmentId")
	}
	if s.Locked != i.Locked {
		fields = append(fields, "locked")
	}
	if i.MaxSpoolUsageGB != 0 && s.Broker.MaxSpoolUsageGB != i.MaxSpoolUsageGB {
		fields = append(fields, "maxSpoolUsage")
	}
	if s.Broker.RedundancyGroupSSLEnabled != i.RedundancyGroupSSLEnabled {
		fields = append(fields, "redundancyGroupSslEnabled")
	}
	for _, wanted := range i.ServiceConnectionEndpoints {
		found := false
		for _, endpoint := range s.ConnectionEndpoints {
			if endpointIdentityMatches(wanted, endpoint) {
				found = true
				break
			}
		}
		if !found {
			fields = append(fields, "serviceConnectionEndpoints")
			break
		}
	}
	return fields
}

func cloneConnectionEndpoints(source []ConnectionEndpoint) []ConnectionEndpoint {
	if source == nil {
		return nil
	}
	result := make([]ConnectionEndpoint, len(source))
	for index, endpoint := range source {
		result[index] = endpoint
		result[index].Hostnames = append([]string(nil), endpoint.Hostnames...)
		result[index].Ports = append([]ConnectionPort(nil), endpoint.Ports...)
	}
	return result
}

func endpointIdentityMatches(wanted, actual ConnectionEndpoint) bool {
	if wanted.Name != actual.Name || wanted.AccessType != actual.AccessType {
		return false
	}
	for _, wantedPort := range wanted.Ports {
		matched := false
		for _, actualPort := range actual.Ports {
			if wantedPort.Protocol == actualPort.Protocol && wantedPort.Port == actualPort.Port {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// CreateServiceRequest is the supported subset of the Mission Control create
// request. Identity returns the fields used for crash reconciliation.
type CreateServiceRequest struct {
	Name                       string               `json:"name"`
	DatacenterID               string               `json:"datacenterId"`
	ServiceClassID             string               `json:"serviceClassId"`
	BrokerVersion              string               `json:"eventBrokerVersion,omitempty"`
	MessageVPN                 string               `json:"msgVpnName,omitempty"`
	MaxSpoolUsageGB            int                  `json:"maxSpoolUsage,omitempty"`
	EnvironmentID              string               `json:"environmentId,omitempty"`
	Locked                     bool                 `json:"locked"`
	RedundancyGroupSSLEnabled  bool                 `json:"redundancyGroupSslEnabled,omitempty"`
	DMREnabled                 bool                 `json:"dmrEnabled"`
	ServiceConnectionEndpoints []ConnectionEndpoint `json:"serviceConnectionEndpoints,omitempty"`
}

func (r CreateServiceRequest) Identity() ServiceIdentity {
	return ServiceIdentity{
		Name: r.Name, DatacenterID: r.DatacenterID, ServiceClassID: r.ServiceClassID,
		BrokerVersion: r.BrokerVersion, MessageVPN: r.MessageVPN, MaxSpoolUsageGB: r.MaxSpoolUsageGB, EnvironmentID: r.EnvironmentID,
		Locked: r.Locked, RedundancyGroupSSLEnabled: r.RedundancyGroupSSLEnabled, DMREnabled: r.DMREnabled,
		ServiceConnectionEndpoints: cloneConnectionEndpoints(r.ServiceConnectionEndpoints),
	}
}

// Service is an event broker service summary/detail.
type Service struct {
	ID                  string               `json:"id"`
	Name                string               `json:"name"`
	BrokerVersion       string               `json:"eventBrokerServiceVersion"`
	CreatedBy           string               `json:"createdBy"`
	OwnedBy             string               `json:"ownedBy"`
	DatacenterID        string               `json:"datacenterId"`
	ServiceClassID      string               `json:"serviceClassId"`
	MessageVPN          string               `json:"msgVpnName"`
	EnvironmentID       string               `json:"environmentId"`
	Locked              bool                 `json:"locked"`
	CreationState       string               `json:"creationState"`
	AdminState          string               `json:"adminState"`
	DMREnabled          bool                 `json:"dmrEnabled"`
	OngoingOperationIDs []string             `json:"ongoingOperationIds,omitempty"`
	Broker              Broker               `json:"broker,omitempty"`
	ConnectionEndpoints []ConnectionEndpoint `json:"serviceConnectionEndpoints,omitempty"`
}

// Broker contains expanded broker details returned by expand=broker.
type Broker struct {
	Version                      string       `json:"version"`
	MaxSpoolUsageGB              int          `json:"maxSpoolUsage"`
	RedundancyGroupSSLEnabled    bool         `json:"redundancyGroupSslEnabled"`
	ManagementReadOnlyCredential Credential   `json:"managementReadOnlyLoginCredential,omitempty"`
	MessageVPNs                  []MessageVPN `json:"msgVpns,omitempty"`
}

type MessageVPN struct {
	Name                            string     `json:"msgVpnName"`
	Subdomain                       string     `json:"subDomainName"`
	ServiceCredential               Credential `json:"serviceLoginCredential,omitempty"`
	ManagementAdminCredential       Credential `json:"managementAdminLoginCredential,omitempty"`
	MissionControlManagerCredential Credential `json:"missionControlManagerLoginCredential,omitempty"`
}

type Credential struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Token    string `json:"token,omitempty"`
}

type ConnectionEndpoint struct {
	ID            string           `json:"id,omitempty"`
	Name          string           `json:"name"`
	Description   string           `json:"description,omitempty"`
	AccessType    string           `json:"accessType"`
	CreationState string           `json:"creationState,omitempty"`
	Hostnames     []string         `json:"hostNames,omitempty"`
	Ports         []ConnectionPort `json:"ports"`
}

type ConnectionPort struct {
	Protocol string `json:"protocol"`
	Port     int    `json:"port"`
}

// ConnectionBundle is a normalized expanded service response suitable for
// configuring clients. Credentials are intentionally retained only in memory.
type ConnectionBundle struct {
	ServiceID            string
	ServiceName          string
	MessageVPN           string
	SMFHosts             []string
	ManagementURLs       []string
	WebMessagingURLs     []string
	AMQPURLs             []string
	MQTTURLs             []string
	RESTURLs             []string
	ServiceCredential    Credential
	ManagementCredential Credential
}

// Datacenter, ServiceClass, and BrokerVersion are typed list resources.
type Datacenter struct {
	ID                      string   `json:"id"`
	Name                    string   `json:"name"`
	Type                    string   `json:"datacenterType"`
	Provider                string   `json:"provider"`
	OperationalState        string   `json:"operState"`
	Available               bool     `json:"available"`
	Visible                 bool     `json:"visible"`
	SupportedServiceClasses []string `json:"supportedServiceClasses,omitempty"`
}

type ServiceClass struct {
	ID                      string              `json:"id"`
	Name                    string              `json:"name"`
	VPNConnections          int                 `json:"vpnConnections"`
	BrokerScalingTier       string              `json:"brokerScalingTier"`
	MaximumSpoolSizeGB      int                 `json:"vpnMaxSpoolSize"`
	MaximumVPNs             int                 `json:"maxNumberVpns"`
	HighAvailabilityCapable bool                `json:"highAvailabilityCapable"`
	Limits                  []ServiceClassLimit `json:"limits,omitempty"`
}

// ServiceClassLimit is the organization-specific quota and current usage
// returned for a service class.
type ServiceClassLimit struct {
	Limit int `json:"limit"`
	InUse int `json:"inUse"`
}

type BrokerVersion struct {
	ID                      string   `json:"id"`
	Version                 string   `json:"version"`
	ReleaseChannel          string   `json:"releaseChannel"`
	Recommended             bool     `json:"recommended"`
	SupportedServiceClasses []string `json:"supportedServiceClasses,omitempty"`
	Capabilities            []string `json:"capabilities,omitempty"`
}

type OperationError struct {
	Message string `json:"message"`
	ErrorID string `json:"errorId"`
}

type Operation struct {
	ID            string          `json:"id"`
	OperationType string          `json:"operationType"`
	CreatedBy     string          `json:"createdBy"`
	ResourceID    string          `json:"resourceId"`
	ResourceType  string          `json:"resourceType"`
	Status        string          `json:"status"`
	Error         *OperationError `json:"error,omitempty"`
}

func (o Operation) Terminal() bool  { return o.Status == "SUCCEEDED" || o.Status == "FAILED" }
func (o Operation) Succeeded() bool { return o.Status == "SUCCEEDED" }

// PlannedService is one immutable qualification-plan entry.
type PlannedService struct {
	Role    string               `json:"role"`
	Request CreateServiceRequest `json:"request"`
}

// Identity returns the full immutable service identity derived from the
// creation request.
func (s PlannedService) Identity() ServiceIdentity { return s.Request.Identity() }

// QualificationPlan always contains Broker 0 and three data services. Its
// fields are private so callers cannot mutate the plan after validation.
type QualificationPlan struct {
	services [4]PlannedService
	sha256   string
}

func NewQualificationPlan(services [4]PlannedService) (QualificationPlan, error) {
	roles := map[string]bool{"broker-0": false, "broker-a": false, "broker-b": false, "broker-c": false}
	names := make(map[string]bool, 4)
	for n := range services {
		services[n] = clonePlannedService(services[n])
		s := services[n]
		if _, ok := roles[s.Role]; !ok || roles[s.Role] {
			return QualificationPlan{}, fmt.Errorf("qualification plan has invalid or duplicate role %q", s.Role)
		}
		roles[s.Role] = true
		identity := s.Request.Identity()
		if err := identity.validate(); err != nil {
			return QualificationPlan{}, fmt.Errorf("qualification plan %q: %w", s.Role, err)
		}
		if names[identity.Name] {
			return QualificationPlan{}, fmt.Errorf("qualification plan duplicate service name %q", identity.Name)
		}
		names[identity.Name] = true
		services[n] = s
	}
	sort.Slice(services[:], func(i, j int) bool { return services[i].Role < services[j].Role })
	canonical, err := json.Marshal(struct {
		SchemaVersion int               `json:"schemaVersion"`
		Services      [4]PlannedService `json:"services"`
	}{1, services})
	if err != nil {
		return QualificationPlan{}, fmt.Errorf("marshal canonical qualification plan: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return QualificationPlan{services: services, sha256: hex.EncodeToString(digest[:])}, nil
}

func (p QualificationPlan) Services() [4]PlannedService {
	var result [4]PlannedService
	for index, service := range p.services {
		result[index] = clonePlannedService(service)
	}
	return result
}

func (p QualificationPlan) SHA256() string { return p.sha256 }

func clonePlannedService(service PlannedService) PlannedService {
	clone := service
	if service.Request.ServiceConnectionEndpoints != nil {
		clone.Request.ServiceConnectionEndpoints = make([]ConnectionEndpoint, len(service.Request.ServiceConnectionEndpoints))
		for index, endpoint := range service.Request.ServiceConnectionEndpoints {
			clone.Request.ServiceConnectionEndpoints[index] = endpoint
			clone.Request.ServiceConnectionEndpoints[index].Hostnames = append([]string(nil), endpoint.Hostnames...)
			clone.Request.ServiceConnectionEndpoints[index].Ports = append([]ConnectionPort(nil), endpoint.Ports...)
		}
	}
	return clone
}
