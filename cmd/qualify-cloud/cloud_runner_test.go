package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cloudapi "github.com/solacese/solace-workload-balancer/cloud"
)

func TestResolvePlanUsesExactCloudResources(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v2/missionControl/datacenters":
			_ = json.NewEncoder(writer).Encode(map[string]any{"data": []cloudapi.Datacenter{{ID: "dc-id", Name: qualificationDatacenter, Available: true, Visible: true, SupportedServiceClasses: []string{"ENTERPRISE_5K_STANDALONE", "ENTERPRISE_250_HIGHAVAILABILITY"}}}})
		case "/api/v2/missionControl/serviceClasses":
			_ = json.NewEncoder(writer).Encode(map[string]any{"data": []cloudapi.ServiceClass{
				{ID: "ENTERPRISE_5K_STANDALONE", Name: "Enterprise 5K Standalone", Limits: []cloudapi.ServiceClassLimit{{Limit: 4, InUse: 1}}},
				{ID: "ENTERPRISE_250_HIGHAVAILABILITY", Name: "Enterprise 250 HA", HighAvailabilityCapable: true, Limits: []cloudapi.ServiceClassLimit{{Limit: 2, InUse: 1}}},
			}})
		case "/api/v2/missionControl/eventBrokerServiceVersions":
			if request.URL.Query().Get("datacenterId") != "dc-id" || request.URL.Query().Get("filterIncompatibleVersions") != "true" {
				t.Errorf("missing compatibility query: %s", request.URL.RawQuery)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"data": []cloudapi.BrokerVersion{{ID: "version-id", Version: qualificationRelease, ReleaseChannel: "LTS", Recommended: true, SupportedServiceClasses: []string{"ENTERPRISE_5K_STANDALONE", "ENTERPRISE_250_HIGHAVAILABILITY"}}}})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client, err := cloudapi.NewClient("token", cloudapi.WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := newPlan(time.Unix(0, 0), bytes.NewReader(make([]byte, 8)))
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolvePlan(context.Background(), client, plan)
	if err != nil {
		t.Fatal(err)
	}
	converted, err := cloudPlan(resolved)
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range converted.Services() {
		request := service.Request
		if request.DatacenterID != "dc-id" || request.BrokerVersion != qualificationRelease {
			t.Fatalf("unexpected resolved request: %#v", request)
		}
		wantClass := "ENTERPRISE_5K_STANDALONE"
		if service.Role == "broker-0" {
			wantClass = "ENTERPRISE_250_HIGHAVAILABILITY"
		}
		if request.ServiceClassID != wantClass {
			t.Fatalf("%s class = %q, want %q", service.Role, request.ServiceClassID, wantClass)
		}
		if request.Locked || request.DMREnabled {
			t.Fatalf("%s must be unlocked with DMR disabled", service.Role)
		}
		if request.RedundancyGroupSSLEnabled != (service.Role == "broker-0") {
			t.Fatalf("%s redundancy SSL = %t", service.Role, request.RedundancyGroupSSLEnabled)
		}
		if len(request.ServiceConnectionEndpoints) != 1 || request.ServiceConnectionEndpoints[0].Name != "public" || request.ServiceConnectionEndpoints[0].AccessType != "PUBLIC" {
			t.Fatalf("%s endpoint = %#v", service.Role, request.ServiceConnectionEndpoints)
		}
		ports := request.ServiceConnectionEndpoints[0].Ports
		if len(ports) != 2 || ports[0].Protocol != "serviceSmfTlsListenPort" || ports[1].Protocol != "serviceManagementTlsListenPort" {
			t.Fatalf("%s ports = %#v", service.Role, ports)
		}
	}
}

func TestResolvePlanRejectsUnsupportedClassQuotaAndNonLTS(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		dcClasses   []string
		devLimit    int
		entLimit    int
		channel     string
		recommended bool
	}{
		{name: "unsupported class", dcClasses: []string{"DEVELOPER"}, devLimit: 3, entLimit: 1, channel: "LTS", recommended: true},
		{name: "data quota", dcClasses: []string{"ENTERPRISE_5K_STANDALONE", "ENTERPRISE_250_HIGHAVAILABILITY"}, devLimit: 2, entLimit: 1, channel: "LTS", recommended: true},
		{name: "control quota", dcClasses: []string{"ENTERPRISE_5K_STANDALONE", "ENTERPRISE_250_HIGHAVAILABILITY"}, devLimit: 3, entLimit: 0, channel: "LTS", recommended: true},
		{name: "non lts", dcClasses: []string{"ENTERPRISE_5K_STANDALONE", "ENTERPRISE_250_HIGHAVAILABILITY"}, devLimit: 3, entLimit: 1, channel: "standard", recommended: true},
		{name: "not recommended", dcClasses: []string{"ENTERPRISE_5K_STANDALONE", "ENTERPRISE_250_HIGHAVAILABILITY"}, devLimit: 3, entLimit: 1, channel: "LTS", recommended: false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				switch request.URL.Path {
				case "/api/v2/missionControl/datacenters":
					_ = json.NewEncoder(writer).Encode(map[string]any{"data": []cloudapi.Datacenter{{ID: "dc-id", Name: qualificationDatacenter, Available: true, Visible: true, SupportedServiceClasses: test.dcClasses}}})
				case "/api/v2/missionControl/serviceClasses":
					_ = json.NewEncoder(writer).Encode(map[string]any{"data": []cloudapi.ServiceClass{
						{ID: "ENTERPRISE_5K_STANDALONE", Name: "Enterprise 5K Standalone", Limits: []cloudapi.ServiceClassLimit{{Limit: test.devLimit}}},
						{ID: "ENTERPRISE_250_HIGHAVAILABILITY", Name: "Enterprise 250 HA", HighAvailabilityCapable: true, Limits: []cloudapi.ServiceClassLimit{{Limit: test.entLimit}}},
					}})
				case "/api/v2/missionControl/eventBrokerServiceVersions":
					_ = json.NewEncoder(writer).Encode(map[string]any{"data": []cloudapi.BrokerVersion{{Version: qualificationRelease, ReleaseChannel: test.channel, Recommended: test.recommended, SupportedServiceClasses: []string{"ENTERPRISE_5K_STANDALONE", "ENTERPRISE_250_HIGHAVAILABILITY"}}}})
				default:
					http.NotFound(writer, request)
				}
			}))
			defer server.Close()
			client, err := cloudapi.NewClient("token", cloudapi.WithBaseURL(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := newPlan(time.Unix(0, 0), bytes.NewReader(make([]byte, 8)))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := resolvePlan(context.Background(), client, plan); err == nil {
				t.Fatal("unsafe plan unexpectedly passed preflight")
			}
		})
	}
}

func TestTokenSubjectReadsJWTWithoutExposingIt(t *testing.T) {
	t.Parallel()
	claims := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"owner-123"}`))
	jwt := "header." + claims + ".signature"
	owner, err := tokenSubject(jwt)
	if err != nil {
		t.Fatal(err)
	}
	if owner != "owner-123" {
		t.Fatalf("owner = %q", owner)
	}
	_, err = tokenSubject("secret.invalid")
	if err == nil {
		t.Fatal("invalid JWT unexpectedly accepted")
	}
	if strings.Contains(err.Error(), "secret.invalid") {
		t.Fatal("JWT leaked in validation error")
	}
}
