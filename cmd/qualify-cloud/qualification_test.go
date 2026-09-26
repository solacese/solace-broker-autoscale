package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	cloudapi "github.com/solacese/solace-workload-balancer/cloud"
	"github.com/solacese/solace-workload-balancer/qualification"
)

func TestResolveBundlesUsesExactResourceIDs(t *testing.T) {
	t.Parallel()
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requested = append(requested, request.URL.Path)
		parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
		id := parts[len(parts)-1]
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"data": cloudapi.Service{
			ID: id, Name: id, MessageVPN: "vpn",
			ConnectionEndpoints: []cloudapi.ConnectionEndpoint{{Hostnames: []string{"host.test"}, Ports: []cloudapi.ConnectionPort{{Protocol: "serviceSmfTlsListenPort", Port: 55443}, {Protocol: "serviceManagementTlsListenPort", Port: 943}}}},
			Broker:              cloudapi.Broker{MessageVPNs: []cloudapi.MessageVPN{{Name: "vpn", ServiceCredential: cloudapi.Credential{Username: "user", Password: "secret"}, ManagementAdminCredential: cloudapi.Credential{Username: "admin", Password: "private"}}}},
		}})
	}))
	defer server.Close()
	resources := []ResourceRecord{{Role: "broker-c", ID: "id-c"}, {Role: "broker-0", ID: "id-0"}, {Role: "broker-b", ID: "id-b"}, {Role: "broker-a", ID: "id-a"}}
	bundles, err := resolveBundles(context.Background(), "jwt", server.URL, resources)
	if err != nil {
		t.Fatal(err)
	}
	if bundles.Control.ServiceID != "id-0" || bundles.Data[0].ServiceID != "id-a" || bundles.Data[1].ServiceID != "id-b" || bundles.Data[2].ServiceID != "id-c" {
		t.Fatalf("bundles were not ordered by exact role: %#v", bundles)
	}
	want := []string{"/api/v2/missionControl/eventBrokerServices/id-c", "/api/v2/missionControl/eventBrokerServices/id-0", "/api/v2/missionControl/eventBrokerServices/id-b", "/api/v2/missionControl/eventBrokerServices/id-a"}
	if !reflect.DeepEqual(requested, want) {
		t.Fatalf("requested paths = %v, want %v", requested, want)
	}
}

func TestQualificationSummaryContainsOnlyAggregateEvidence(t *testing.T) {
	t.Parallel()
	result := qualification.Result{
		Duration: 3 * time.Second,
		Cleanup:  "complete",
		Scenarios: []qualification.ScenarioResult{
			{Name: qualification.ScenarioFence, Status: "PASS", Duration: time.Second},
			{
				Name:     qualification.ScenarioStatic,
				Status:   "FAIL",
				Reason:   "delivery invariant failed",
				Duration: 2 * time.Second,
				Validation: qualification.ValidationResult{
					Expected: 10, Deliveries: 9, UniqueEventIDs: 9, Missing: 1,
					ThroughputPerSecond:     4.5,
					BrokerDistribution:      map[string]int{"broker-b": 4, "broker-a": 5},
					Accepted:                10,
					PositiveACKs:            9,
					OutboxHighWaterMessages: 7,
					OutboxHighWaterBytes:    4096,
					DurableAcceptanceLatency: qualification.Quantiles{
						P50: time.Millisecond, P95: 2 * time.Millisecond,
						P99: 3 * time.Millisecond, Max: 4 * time.Millisecond,
					},
					BrokerPositiveACKLatency: qualification.Quantiles{
						P50: 5 * time.Millisecond, P95: 6 * time.Millisecond,
						P99: 7 * time.Millisecond, Max: 8 * time.Millisecond,
					},
					EndToEndConsumedLatency: qualification.Quantiles{
						P50: 9 * time.Millisecond, P95: 10 * time.Millisecond,
						P99: 11 * time.Millisecond, Max: 12 * time.Millisecond,
					},
				},
			},
		},
	}
	summary := qualificationSummary(result)
	for _, fragment := range []string{
		"fence-nack-semantics=PASS duration=1s",
		"static-3-broker-data-flow=FAIL duration=2s",
		"expected=10 delivered=9 unique=9 missing=1",
		"accepted=10 positive_acks=9 outbox_high_water_messages=7 outbox_high_water_bytes=4096",
		"throughput=4.50/s",
		"durable_accept_p50=1ms durable_accept_p95=2ms durable_accept_p99=3ms durable_accept_max=4ms",
		"broker_positive_ack_p50=5ms broker_positive_ack_p95=6ms broker_positive_ack_p99=7ms broker_positive_ack_max=8ms",
		"end_to_end_consumed_p50=9ms end_to_end_consumed_p95=10ms end_to_end_consumed_p99=11ms end_to_end_consumed_max=12ms",
		"broker_distribution=broker-a:5,broker-b:4",
		"reason=delivery invariant failed",
		"cleanup=complete duration=3s",
	} {
		if !strings.Contains(summary, fragment) {
			t.Fatalf("summary %q does not contain %q", summary, fragment)
		}
	}
}

func TestQualificationNamespaceAndStrictOptions(t *testing.T) {
	t.Parallel()
	plan, err := newPlan(time.Unix(0, 0), bytes.NewReader(make([]byte, 8)))
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := qualificationNamespace(plan)
	if err != nil {
		t.Fatal(err)
	}
	if namespace != "swlb-q-700101t000000-00000000" {
		t.Fatalf("namespace = %q", namespace)
	}
	options := qualificationOptions(2*time.Minute, 20*time.Second)
	if options.AllowSkipped || options.Keys < 1 || options.EventsPerKey < 1 || options.TargetDuration <= 0 || options.CleanupTimeout != 20*time.Second {
		t.Fatalf("options are not strict: %#v", options)
	}
}
