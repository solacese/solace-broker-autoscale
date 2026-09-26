package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const testOwner = "owner-123"

func testPlan(t *testing.T) QualificationPlan {
	t.Helper()
	roles := [4]string{"broker-0", "broker-a", "broker-b", "broker-c"}
	var services [4]PlannedService
	for index, role := range roles {
		request := CreateServiceRequest{
			Name: "qual-" + role, DatacenterID: "dc-1", ServiceClassID: "DEVELOPER",
			BrokerVersion: "10.10.1.1-1", MessageVPN: "vpn-" + role,
			RedundancyGroupSSLEnabled:  role == "broker-0",
			ServiceConnectionEndpoints: []ConnectionEndpoint{{Name: "public", AccessType: "PUBLIC", Ports: []ConnectionPort{{Protocol: "serviceSmfTlsListenPort", Port: 55443}}}},
		}
		services[index] = PlannedService{Role: role, Request: request}
	}
	plan, err := NewQualificationPlan(services)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func newTestLifecycle(t *testing.T, server *httptest.Server, plan QualificationPlan) (*Lifecycle, *Journal) {
	t.Helper()
	client, err := NewClient("raw-test-token", WithBaseURL(server.URL), WithTokenOwner(testOwner), WithPageSize(2), WithIdempotencyHeader("Idempotency-Key"))
	if err != nil {
		t.Fatal(err)
	}
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "lifecycle.json"), plan, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	lifecycle, err := NewLifecycle(client, journal, plan, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return lifecycle, journal
}

func writeJSON(t *testing.T, writer http.ResponseWriter, status int, value any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

func operationEnvelope(id, resourceID, status string) map[string]any {
	return map[string]any{"data": Operation{ID: id, ResourceID: resourceID, Status: status}, "meta": map[string]any{}}
}

func serviceEnvelope(service Service) map[string]any {
	return map[string]any{"data": service, "meta": map[string]any{}}
}

func readyService(id string, planned PlannedService, owner string) Service {
	identity := planned.Identity()
	return Service{
		ID: id, Name: identity.Name, DatacenterID: identity.DatacenterID, ServiceClassID: identity.ServiceClassID,
		BrokerVersion: identity.BrokerVersion, MessageVPN: identity.MessageVPN, EnvironmentID: identity.EnvironmentID,
		Locked: identity.Locked, DMREnabled: identity.DMREnabled, OwnedBy: owner, CreationState: "COMPLETED", AdminState: "START",
		Broker: Broker{MaxSpoolUsageGB: identity.MaxSpoolUsageGB, RedundancyGroupSSLEnabled: identity.RedundancyGroupSSLEnabled},
		ConnectionEndpoints: func() []ConnectionEndpoint {
			endpoints := cloneConnectionEndpoints(identity.ServiceConnectionEndpoints)
			for index := range endpoints {
				endpoints[index].ID = "endpoint-" + id
				endpoints[index].CreationState = "completed"
			}
			return endpoints
		}(),
	}
}

func TestHappyLifecycle(t *testing.T) {
	plan := testPlan(t)
	var mu sync.Mutex
	created := make(map[string]Service)
	operations := make(map[string]string)
	var createKeys []string
	var deletes []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer raw-test-token" {
			t.Errorf("unexpected authorization header")
		}
		parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
		if request.Method == http.MethodPost && request.URL.Path == apiRoot+"/eventBrokerServices" {
			var body CreateServiceRequest
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode create: %v", err)
			}
			id, opID := "id-"+body.Name, "op-create-"+body.Name
			mu.Lock()
			created[id] = Service{
				ID: id, Name: body.Name, DatacenterID: body.DatacenterID, ServiceClassID: body.ServiceClassID,
				BrokerVersion: body.BrokerVersion, MessageVPN: body.MessageVPN, EnvironmentID: body.EnvironmentID,
				Locked: body.Locked, DMREnabled: body.DMREnabled, OwnedBy: testOwner, CreationState: "COMPLETED", AdminState: "START",
				Broker: Broker{MaxSpoolUsageGB: body.MaxSpoolUsageGB, RedundancyGroupSSLEnabled: body.RedundancyGroupSSLEnabled},
				ConnectionEndpoints: func() []ConnectionEndpoint {
					endpoints := cloneConnectionEndpoints(body.ServiceConnectionEndpoints)
					for index := range endpoints {
						endpoints[index].ID = "endpoint-" + body.Name
						endpoints[index].CreationState = "completed"
					}
					return endpoints
				}(),
			}
			operations[opID] = id
			createKeys = append(createKeys, request.Header.Get("Idempotency-Key"))
			mu.Unlock()
			writeJSON(t, writer, http.StatusAccepted, operationEnvelope(opID, id, "PENDING"))
			return
		}
		if len(parts) == 7 && parts[3] == "eventBrokerServices" && parts[5] == "operations" && request.Method == http.MethodGet {
			writeJSON(t, writer, http.StatusOK, operationEnvelope(parts[6], parts[4], "SUCCEEDED"))
			return
		}
		if len(parts) == 5 && parts[3] == "eventBrokerServices" {
			id := parts[4]
			mu.Lock()
			service, exists := created[id]
			mu.Unlock()
			switch request.Method {
			case http.MethodGet:
				if !exists {
					writeJSON(t, writer, http.StatusNotFound, map[string]any{"error": "missing"})
					return
				}
				if got := request.URL.Query()["expand"]; len(got) != 2 {
					t.Errorf("expected expanded service request, got %v", got)
				}
				writeJSON(t, writer, http.StatusOK, serviceEnvelope(service))
				return
			case http.MethodDelete:
				if !exists {
					writeJSON(t, writer, http.StatusNotFound, map[string]any{"error": "missing"})
					return
				}
				mu.Lock()
				delete(created, id)
				deletes = append(deletes, id)
				mu.Unlock()
				writeJSON(t, writer, http.StatusAccepted, operationEnvelope("op-delete-"+id, id, "PENDING"))
				return
			}
		}
		http.Error(writer, "unexpected path", http.StatusBadRequest)
	}))
	defer server.Close()

	lifecycle, journal := newTestLifecycle(t, server, plan)
	services, err := lifecycle.Ensure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 4 {
		t.Fatalf("got %d services, want 4", len(services))
	}
	for _, key := range createKeys {
		if key == "" {
			t.Error("create omitted configured idempotency header")
		}
	}
	info, err := os.Stat(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("journal mode = %04o, want 0600", info.Mode().Perm())
	}
	if err := lifecycle.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	sort.Strings(deletes)
	want := []string{"id-qual-broker-0", "id-qual-broker-a", "id-qual-broker-b", "id-qual-broker-c"}
	sort.Strings(want)
	if fmt.Sprint(deletes) != fmt.Sprint(want) {
		t.Fatalf("deleted IDs %v, want %v", deletes, want)
	}
}

func TestUncertainCreateAdoptsExactIdentityAndOwner(t *testing.T) {
	plan := testPlan(t)
	planned := plan.Services()[0]
	service := readyService("adopt-me", planned, testOwner)
	var posts int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == apiRoot+"/eventBrokerServices":
			writeJSON(t, writer, http.StatusOK, map[string]any{"data": []Service{service}, "meta": map[string]any{}})
		case request.Method == http.MethodGet && request.URL.Path == apiRoot+"/eventBrokerServices/adopt-me":
			writeJSON(t, writer, http.StatusOK, serviceEnvelope(service))
		case request.Method == http.MethodPost:
			posts++
			http.Error(writer, "must not create", http.StatusConflict)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	lifecycle, journal := newTestLifecycle(t, server, plan)
	if err := journal.put(JournalService{Role: planned.Role, Identity: planned.Request.Identity(), State: stateCreateIntent, IdempotencyKey: "same-key"}); err != nil {
		t.Fatal(err)
	}
	got, err := lifecycle.ensureOne(context.Background(), planned)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != service.ID || posts != 0 {
		t.Fatalf("adoption got ID %q and %d POSTs", got.ID, posts)
	}
	record, _ := journal.Service(planned.Role)
	if record.State != stateReady || record.ServiceID != service.ID {
		t.Fatalf("unexpected adopted journal record: %+v", record)
	}
}

func TestUncertainCreateRefusesCollision(t *testing.T) {
	plan := testPlan(t)
	planned := plan.Services()[0]
	collision := readyService("not-ours", planned, "someone-else")
	collision.ServiceClassID = "ENTERPRISE_1K_HIGHAVAILABILITY"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == apiRoot+"/eventBrokerServices" {
			writeJSON(t, writer, http.StatusOK, map[string]any{"data": []Service{collision}, "meta": map[string]any{}})
			return
		}
		if request.URL.Path == apiRoot+"/eventBrokerServices/not-ours" {
			writeJSON(t, writer, http.StatusOK, serviceEnvelope(collision))
			return
		}
		http.NotFound(writer, request)
	}))
	defer server.Close()
	lifecycle, journal := newTestLifecycle(t, server, plan)
	if err := journal.put(JournalService{Role: planned.Role, Identity: planned.Request.Identity(), State: stateCreateIntent, IdempotencyKey: "same-key"}); err != nil {
		t.Fatal(err)
	}
	_, err := lifecycle.ensureOne(context.Background(), planned)
	if err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("expected collision refusal, got %v", err)
	}
}

func TestHTTPErrorRedactsSecretsAndRejectsRedirects(t *testing.T) {
	token := "top-secret-bearer"
	var redirected bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirect-target" {
			redirected = true
			http.Error(writer, "followed", http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Location", "/redirect-target")
		writer.Header().Set("X-Request-Id", "req-7")
		writeJSON(t, writer, http.StatusFound, map[string]any{
			"access_token": token, "password": "p4ss", "nested": map[string]any{"authorization": "Bearer " + token},
		})
	}))
	defer server.Close()
	client, err := NewClient(token, WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ListServices(context.Background())
	if err == nil {
		t.Fatal("expected strict redirect error")
	}
	text := err.Error()
	for _, secret := range []string{token, "p4ss"} {
		if strings.Contains(text, secret) {
			t.Fatalf("error leaked %q: %s", secret, text)
		}
	}
	if !strings.Contains(text, "[REDACTED]") || !strings.Contains(text, "req-7") {
		t.Fatalf("error lacks redaction/request ID: %s", text)
	}
	if redirected {
		t.Fatal("client followed redirect")
	}
}

func TestListServicesPaginates(t *testing.T) {
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		pages = append(pages, request.URL.RawQuery)
		page := request.URL.Query().Get("pageNumber")
		data := []Service{{ID: page + "a"}, {ID: page + "b"}}
		if page == "2" {
			data = data[:1]
		}
		writeJSON(t, writer, http.StatusOK, map[string]any{"data": data, "meta": map[string]any{}})
	}))
	defer server.Close()
	client, err := NewClient("token", WithBaseURL(server.URL), WithPageSize(2))
	if err != nil {
		t.Fatal(err)
	}
	services, err := client.ListServices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 3 || len(pages) != 2 {
		t.Fatalf("got %d services over %d pages", len(services), len(pages))
	}
}

func TestCompatibleBrokerVersionsAreDatacenterFilteredAndTyped(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		queries = append(queries, request.URL.RawQuery)
		if request.URL.Query().Get("datacenterId") != "dc/a" || request.URL.Query().Get("filterIncompatibleVersions") != "true" {
			t.Errorf("missing compatibility filters: %s", request.URL.RawQuery)
		}
		page := request.URL.Query().Get("pageNumber")
		versions := []BrokerVersion{{ID: "version-" + page, Version: "10.10.1.1-1", Capabilities: []string{"portDisabling"}}}
		writeJSON(t, writer, http.StatusOK, map[string]any{"data": versions, "meta": map[string]any{}})
	}))
	defer server.Close()
	client, err := NewClient("token", WithBaseURL(server.URL), WithPageSize(2))
	if err != nil {
		t.Fatal(err)
	}
	versions, err := client.ListCompatibleBrokerVersions(context.Background(), "dc/a")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0].Capabilities[0] != "portDisabling" || len(queries) != 1 {
		t.Fatalf("unexpected compatible versions: %+v, queries %v", versions, queries)
	}
	if _, err := client.ListCompatibleBrokerVersions(context.Background(), ""); err == nil {
		t.Fatal("accepted empty datacenter")
	}
}

func TestServiceClassLimitsDecode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writeJSON(t, writer, http.StatusOK, map[string]any{"data": []ServiceClass{{
			ID: "DEVELOPER", Name: "Developer", BrokerScalingTier: "developer", MaximumVPNs: 1,
			Limits: []ServiceClassLimit{{Limit: 8, InUse: 3}},
		}}, "meta": map[string]any{}})
	}))
	defer server.Close()
	client, err := NewClient("token", WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	classes, err := client.ListServiceClasses(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(classes) != 1 || len(classes[0].Limits) != 1 || classes[0].Limits[0].Limit != 8 || classes[0].Limits[0].InUse != 3 {
		t.Fatalf("service class limits not decoded: %+v", classes)
	}
}

func TestCleanupUsesOnlyJournaledExactIDAnd404IsSuccess(t *testing.T) {
	plan := testPlan(t)
	planned := plan.Services()[0]
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests = append(requests, request.Method+" "+request.URL.Path)
		if request.Method == http.MethodGet && request.URL.Path == apiRoot+"/eventBrokerServices/exact-id" {
			writeJSON(t, writer, http.StatusNotFound, map[string]any{"message": "already gone"})
			return
		}
		http.Error(writer, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()
	lifecycle, journal := newTestLifecycle(t, server, plan)
	if err := journal.put(JournalService{Role: planned.Role, Identity: planned.Request.Identity(), State: stateReady, ServiceID: "exact-id", IdempotencyKey: "key"}); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0] != "GET "+apiRoot+"/eventBrokerServices/exact-id" {
		t.Fatalf("unexpected requests: %v", requests)
	}
	record, _ := journal.Service(planned.Role)
	if record.State != stateDeleted {
		t.Fatalf("cleanup state = %q", record.State)
	}
}

func TestCleanupRefusesJournaledServiceWithChangedOwner(t *testing.T) {
	plan := testPlan(t)
	planned := plan.Services()[0]
	service := readyService("exact-id", planned, "different-owner")
	deletes := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			writeJSON(t, writer, http.StatusOK, serviceEnvelope(service))
		case http.MethodDelete:
			deletes++
			writeJSON(t, writer, http.StatusAccepted, operationEnvelope("delete-op", service.ID, "PENDING"))
		}
	}))
	defer server.Close()
	lifecycle, journal := newTestLifecycle(t, server, plan)
	if err := journal.put(JournalService{Role: planned.Role, Identity: planned.Identity(), State: stateReady, ServiceID: service.ID, IdempotencyKey: "key"}); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Cleanup(context.Background()); err == nil || !strings.Contains(err.Error(), "owner differs") {
		t.Fatalf("cleanup error = %v", err)
	}
	if deletes != 0 {
		t.Fatalf("cleanup attempted %d deletes for a differently owned service", deletes)
	}
}

func TestCleanupReconcilesAcceptedCreateAfterLostResponse(t *testing.T) {
	plan := testPlan(t)
	planned := plan.Services()[0]
	service := readyService("accepted-id", planned, testOwner)
	var posts, deletes int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == apiRoot+"/eventBrokerServices":
			writeJSON(t, writer, http.StatusOK, map[string]any{"data": []Service{{ID: service.ID, Name: service.Name}}, "meta": map[string]any{}})
		case request.Method == http.MethodGet && request.URL.Path == apiRoot+"/eventBrokerServices/accepted-id":
			writeJSON(t, writer, http.StatusOK, serviceEnvelope(service))
		case request.Method == http.MethodDelete && request.URL.Path == apiRoot+"/eventBrokerServices/accepted-id":
			deletes++
			writeJSON(t, writer, http.StatusNotFound, map[string]any{"message": "deleted"})
		case request.Method == http.MethodPost:
			posts++
			http.Error(writer, "must not retry create", http.StatusInternalServerError)
		default:
			http.Error(writer, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	lifecycle, journal := newTestLifecycle(t, server, plan)
	if err := journal.put(JournalService{Role: planned.Role, Identity: planned.Identity(), State: stateCreateIntent, IdempotencyKey: "key"}); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if posts != 0 || deletes != 1 {
		t.Fatalf("got %d create retries and %d deletes, want 0 and 1", posts, deletes)
	}
	record, _ := journal.Service(planned.Role)
	if record.State != stateDeleted || record.ServiceID != "accepted-id" {
		t.Fatalf("unexpected reconciled cleanup record: %+v", record)
	}
}

func TestCleanupRetainsUnmatchedCreateIntentForRetry(t *testing.T) {
	plan := testPlan(t)
	planned := plan.Services()[0]
	var lists, writes int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == apiRoot+"/eventBrokerServices" {
			lists++
			writeJSON(t, writer, http.StatusOK, map[string]any{"data": []Service{}, "meta": map[string]any{}})
			return
		}
		writes++
		http.Error(writer, "must not write", http.StatusInternalServerError)
	}))
	defer server.Close()
	lifecycle, journal := newTestLifecycle(t, server, plan)
	if err := journal.put(JournalService{Role: planned.Role, Identity: planned.Identity(), State: stateCreateIntent, IdempotencyKey: "key"}); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Cleanup(context.Background()); err == nil || !strings.Contains(err.Error(), "remains unresolved") {
		t.Fatalf("cleanup error = %v", err)
	}
	if lists != 3 || writes != 0 {
		t.Fatalf("got %d list attempts and %d writes, want 3 and 0", lists, writes)
	}
	record, _ := journal.Service(planned.Role)
	if record.State != stateCreateIntent || record.ServiceID != "" {
		t.Fatalf("unmatched intent was not retained for retry: %+v", record)
	}
}

func TestReadyVerificationRejectsServiceAndEndpointStates(t *testing.T) {
	plan := testPlan(t)
	planned := plan.Services()[0]
	tests := []struct {
		name   string
		mutate func(*Service)
		want   string
	}{
		{"creation", func(service *Service) { service.CreationState = "INPROGRESS" }, "creation state"},
		{"admin", func(service *Service) { service.AdminState = "STOP" }, "admin state"},
		{"endpoint", func(service *Service) { service.ConnectionEndpoints[0].CreationState = "inProgress" }, "endpoint"},
		{"missing endpoint", func(service *Service) { service.ConnectionEndpoints = nil }, "no connection endpoints"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := readyService("service-id", planned, testOwner)
			test.mutate(&service)
			err := verifyReadyService(service, planned)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q readiness error, got %v", test.want, err)
			}
		})
	}
}

func TestUncertainCreateWithoutIdempotencyDoesNotRetry(t *testing.T) {
	plan := testPlan(t)
	planned := plan.Services()[0]
	var getCalls, postCalls int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == apiRoot+"/eventBrokerServices" {
			getCalls++
			writeJSON(t, writer, http.StatusOK, map[string]any{"data": []Service{}, "meta": map[string]any{}})
			return
		}
		if request.Method == http.MethodPost {
			postCalls++
		}
		http.Error(writer, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()
	client, err := NewClient("token", WithBaseURL(server.URL), WithTokenOwner(testOwner), WithPageSize(2))
	if err != nil {
		t.Fatal(err)
	}
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "journal.json"), plan, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	lifecycle, err := NewLifecycle(client, journal, plan, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.put(JournalService{Role: planned.Role, Identity: planned.Identity(), State: stateCreateIntent, IdempotencyKey: "original-key"}); err != nil {
		t.Fatal(err)
	}
	_, err = lifecycle.ensureOne(context.Background(), planned)
	if err == nil || !strings.Contains(err.Error(), "refusing an unsafe retry") {
		t.Fatalf("expected unresolved no-retry error, got %v", err)
	}
	if getCalls != 3 || postCalls != 0 {
		t.Fatalf("got %d reconciliation GETs and %d POSTs, want 3 and 0", getCalls, postCalls)
	}
	record, _ := journal.Service(planned.Role)
	if record.State != stateCreateIntent || record.ServiceID != "" {
		t.Fatalf("uncertain create was not left unresolved: %+v", record)
	}
}

func TestWaitOperationReturnsRedactedTypedFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writeJSON(t, writer, http.StatusOK, map[string]any{"data": Operation{
			ID: "operation-private", ResourceID: "service-private", OperationType: "CREATE_SERVICE", Status: "FAILED",
			Error: &OperationError{Message: "capacity temporarily unavailable", ErrorID: "private-id"},
		}})
	}))
	defer server.Close()
	client, err := NewClient("raw-secret-token", WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.WaitOperation(context.Background(), "service-private", "operation-private", time.Millisecond)
	var failure *OperationFailure
	if !errors.As(err, &failure) {
		t.Fatalf("error = %v, want OperationFailure", err)
	}
	if !strings.Contains(err.Error(), "CREATE_SERVICE") || !strings.Contains(err.Error(), "capacity temporarily unavailable") {
		t.Fatalf("error = %v", err)
	}
	for _, private := range []string{"operation-private", "service-private", "private-id", "raw-secret-token"} {
		if strings.Contains(err.Error(), private) {
			t.Fatalf("error leaked %q: %v", private, err)
		}
	}
}

func TestServiceIdentityIncludesProvisioningContract(t *testing.T) {
	planned := testPlan(t).Services()[0]
	service := readyService("service-id", planned, testOwner)
	if mismatches := planned.Identity().mismatches(service); len(mismatches) != 0 {
		t.Fatalf("ready service mismatches identity: %v", mismatches)
	}
	mutations := map[string]func(*Service){
		"endpoint": func(service *Service) { service.ConnectionEndpoints[0].Ports[0].Port++ },
	}
	if planned.Identity().MaxSpoolUsageGB != 0 {
		mutations["max spool"] = func(service *Service) { service.Broker.MaxSpoolUsageGB++ }
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := service
			changed.ConnectionEndpoints = cloneConnectionEndpoints(service.ConnectionEndpoints)
			mutate(&changed)
			if planned.Identity().matches(changed) {
				t.Fatal("changed provisioning contract matched")
			}
		})
	}
}

func TestConnectionBundleDoesNotUseReadOnlyManagementCredential(t *testing.T) {
	bundle, err := ParseConnectionBundle(Service{
		ID: "service-1", MessageVPN: "vpn",
		ConnectionEndpoints: []ConnectionEndpoint{{Hostnames: []string{"example.test"}, Ports: []ConnectionPort{{Protocol: "serviceSmfTlsListenPort", Port: 55443}, {Protocol: "serviceManagementTlsListenPort", Port: 943}}}},
		Broker: Broker{
			ManagementReadOnlyCredential: Credential{Username: "readonly", Password: "secret"},
			MessageVPNs:                  []MessageVPN{{Name: "vpn", ServiceCredential: Credential{Username: "user", Password: "secret"}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ManagementCredential.Username != "" || bundle.ManagementCredential.Password != "" {
		t.Fatalf("read-only management credential was selected: %+v", bundle.ManagementCredential)
	}
}

func TestConnectionBundleParsesExpandedService(t *testing.T) {
	bundle, err := ParseConnectionBundle(Service{
		ID: "service-1", Name: "test", MessageVPN: "vpn",
		ConnectionEndpoints: []ConnectionEndpoint{{Hostnames: []string{"example.test", "2001:db8::1"}, Ports: []ConnectionPort{{Protocol: "serviceSmfTlsListenPort", Port: 55443}, {Protocol: "serviceManagementTlsListenPort", Port: 943}}}},
		Broker:              Broker{MessageVPNs: []MessageVPN{{Name: "vpn", ServiceCredential: Credential{Username: "user", Password: "secret"}, ManagementAdminCredential: Credential{Username: "admin", Password: "admin-secret"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(bundle.SMFHosts) != "[tcps://example.test:55443 tcps://[2001:db8::1]:55443]" {
		t.Fatalf("unexpected SMF hosts: %v", bundle.SMFHosts)
	}
	if bundle.ServiceCredential.Username != "user" || bundle.ManagementURLs[0] != "https://example.test:943" {
		t.Fatalf("unexpected bundle: %+v", bundle)
	}
}

func TestJournalLockAndPlanImmutability(t *testing.T) {
	plan := testPlan(t)
	copyOfServices := plan.Services()
	originalHash := plan.SHA256()
	copyOfServices[0].Request.Name = "mutated"
	copyOfServices[0].Request.ServiceConnectionEndpoints[0].Ports[0].Port = 2
	if plan.SHA256() != originalHash || plan.Services()[0].Request.Name == "mutated" || plan.Services()[0].Request.ServiceConnectionEndpoints[0].Ports[0].Port == 2 {
		t.Fatal("qualification plan is externally mutable")
	}
	journalPath := filepath.Join(t.TempDir(), "journal.json")
	journal, err := OpenJournal(journalPath, plan, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	second, err := OpenJournal(journalPath, plan, testOwner)
	if err == nil {
		second.Close()
		t.Fatal("second journal lock unexpectedly succeeded")
	}
	info, err := os.Stat(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("journal mode %04o", info.Mode().Perm())
	}
}

func TestCreateServiceSerializesExplicitFalseSafetyFlags(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSON(t, writer, http.StatusAccepted, operationEnvelope("op", "id", "PENDING"))
	}))
	defer server.Close()
	client, err := NewClient("raw", WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CreateService(context.Background(), testPlan(t).Services()[0].Request, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"locked", "dmrEnabled"} {
		value, present := body[field]
		if !present || value != false {
			t.Errorf("%s = %#v, present %t; want explicit false", field, value, present)
		}
	}
}

func TestRawBearerValidationAndNoDefaultIdempotencyHeader(t *testing.T) {
	if _, err := NewClient("Bearer already-prefixed"); err == nil {
		t.Fatal("accepted prefixed bearer token")
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		for key := range request.Header {
			if strings.Contains(strings.ToLower(key), "idempotency") {
				t.Errorf("unexpected idempotency header %q", key)
			}
		}
		_, _ = io.Copy(io.Discard, request.Body)
		writeJSON(t, writer, http.StatusAccepted, operationEnvelope("op", "id", "PENDING"))
	}))
	defer server.Close()
	client, err := NewClient("raw", WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CreateService(context.Background(), testPlan(t).Services()[0].Request, "caller-key")
	if err != nil {
		t.Fatal(err)
	}
}

func TestHTTPStatusHelper(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &HTTPError{StatusCode: http.StatusNotFound})
	if !IsHTTPStatus(err, http.StatusNotFound) || IsHTTPStatus(errors.New("other"), http.StatusNotFound) {
		t.Fatal("HTTP status matching failed")
	}
}
