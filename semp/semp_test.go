package semp

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, server *httptest.Server, options ...func(*ClientOptions)) *Client {
	t.Helper()
	config := ClientOptions{
		BaseURL:        server.URL,
		Username:       "admin-user",
		Password:       "do-not-leak-secret",
		Retries:        1,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	}
	for _, option := range options {
		option(&config)
	}
	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func jsonResponse(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func notFound(writer http.ResponseWriter) {
	jsonResponse(writer, http.StatusBadRequest, map[string]any{"meta": map[string]any{"error": map[string]any{"status": "NOT_FOUND"}}})
}

func TestRedirectRefusedWithoutCredentialForwarding(t *testing.T) {
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetRequests.Add(1)
	}))
	defer target.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Basic "+base64.StdEncoding.EncodeToString([]byte("admin-user:do-not-leak-secret")) {
			t.Errorf("missing basic authentication: %q", got)
		}
		http.Redirect(writer, request, target.URL+"/stolen", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	err := testClient(t, origin).DeleteQueue(context.Background(), "vpn", "queue")
	if err == nil || targetRequests.Load() != 0 {
		t.Fatalf("redirect followed or not rejected: requests=%d err=%v", targetRequests.Load(), err)
	}
	if strings.Contains(err.Error(), "do-not-leak-secret") {
		t.Fatalf("secret leaked in error: %v", err)
	}
}

func TestDefaultTLSVerificationRejectsUntrustedServer(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	client := testClient(t, server)
	err := client.DeleteQueue(context.Background(), "vpn", "queue")
	if err == nil {
		t.Fatal("untrusted TLS certificate was accepted")
	}
}

func TestCustomTransportCannotDisableTLSVerification(t *testing.T) {
	_, err := NewClient(ClientOptions{
		BaseURL:   "https://broker.example",
		Username:  "admin",
		Password:  "secret",
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec -- deliberate negative test
	})
	if err == nil {
		t.Fatal("insecure transport accepted")
	}
}

func TestCustomTransportCannotBypassTLSVerificationWithDialers(t *testing.T) {
	tests := []struct {
		name      string
		transport *http.Transport
	}{
		{
			name: "DialTLS",
			transport: &http.Transport{DialTLS: func(_, _ string) (net.Conn, error) {
				return nil, errors.New("not called")
			}},
		},
		{
			name: "DialTLSContext",
			transport: &http.Transport{DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("not called")
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewClient(ClientOptions{
				BaseURL:   "https://broker.example",
				Username:  "admin",
				Password:  "secret",
				Transport: test.transport,
			})
			if err == nil {
				t.Fatal("custom TLS dialer accepted")
			}
		})
	}
}

func TestCustomTransportAndTLSConfigAreCloned(t *testing.T) {
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	client, err := NewClient(ClientOptions{
		BaseURL:   "https://broker.example",
		Username:  "admin",
		Password:  "secret",
		Transport: transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	retained, ok := client.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client transport type = %T", client.http.Transport)
	}
	if retained == transport || retained.TLSClientConfig == transport.TLSClientConfig {
		t.Fatal("client retained caller-owned transport state")
	}

	transport.Proxy = func(*http.Request) (*url.URL, error) {
		return nil, errors.New("mutated proxy")
	}
	transport.TLSClientConfig.InsecureSkipVerify = true
	if retained.Proxy != nil {
		t.Fatal("post-construction transport mutation reached client")
	}
	if retained.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("post-construction TLS config mutation reached client")
	}
}

func TestEscapedPathSegments(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		got = request.URL.EscapedPath()
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	if err := testClient(t, server).DeleteQueue(context.Background(), "vpn/one", "q/name ?#"); err != nil {
		t.Fatal(err)
	}
	want := "/SEMP/v2/config/msgVpns/vpn%2Fone/queues/q%2Fname%20%3F%23"
	if got != want {
		t.Fatalf("escaped path = %q, want %q", got, want)
	}
}

func TestQueueExistsUsesExactNonMutatingGet(t *testing.T) {
	var gotMethod, gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotMethod = request.Method
		gotPath = request.URL.EscapedPath()
		jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{"queueName": "q/name"}})
	}))
	defer server.Close()

	exists, err := testClient(t, server).QueueExists(context.Background(), "vpn/one", "q/name")
	if err != nil || !exists {
		t.Fatalf("QueueExists() = %v, %v", exists, err)
	}
	if gotMethod != http.MethodGet || gotPath != "/SEMP/v2/config/msgVpns/vpn%2Fone/queues/q%2Fname" {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
}

func TestQueueExistsTreatsNotFoundAsAbsent(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusBadRequest} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if status == http.StatusBadRequest {
					notFound(writer)
					return
				}
				writer.WriteHeader(status)
			}))
			defer server.Close()

			exists, err := testClient(t, server).QueueExists(context.Background(), "vpn", "queue")
			if err != nil || exists {
				t.Fatalf("QueueExists() = %v, %v", exists, err)
			}
		})
	}
}

func TestQueueExistsReturnsWrongError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	exists, err := testClient(t, server).QueueExists(context.Background(), "vpn", "queue")
	if err == nil || exists || errors.Is(err, ErrNotFound) {
		t.Fatalf("QueueExists() = %v, %v", exists, err)
	}
}

func TestStrictSolace400Parsing(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		target error
	}{
		{"not found", `{"meta":{"error":{"status":"NOT_FOUND"}}}`, ErrNotFound},
		{"already exists", `{"meta":{"error":{"status":"ALREADY_EXISTS"}}}`, ErrAlreadyExists},
		{"lowercase is not special", `{"meta":{"error":{"status":"not_found"}}}`, nil},
		{"message text is not special", `{"meta":{"error":{"status":"INVALID","description":"NOT_FOUND"}}}`, nil},
		{"malformed is not special", `not-json`, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusBadRequest)
				_, _ = writer.Write([]byte(test.body))
			}))
			defer server.Close()
			client := testClient(t, server)
			var response any
			err := client.request(context.Background(), http.MethodGet, "/SEMP/v2/config/test", nil, &response)
			if err == nil {
				t.Fatal("expected an error")
			}
			if test.target == nil && (errors.Is(err, ErrNotFound) || errors.Is(err, ErrAlreadyExists)) {
				t.Fatalf("loosely classified error: %v", err)
			}
			if test.target != nil && !errors.Is(err, test.target) {
				t.Fatalf("error %v does not wrap %v", err, test.target)
			}
		})
	}
}

func TestBoundedRetry(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attempts.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := testClient(t, server, func(options *ClientOptions) { options.Retries = 3 })
	if err := client.DeleteQueue(context.Background(), "vpn", "q"); err == nil {
		t.Fatal("expected retry exhaustion")
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempt count = %d, want 3", got)
	}
}

func managedQueue() QueueSpec {
	return QueueSpec{
		MessageVPN:                         "vpn/name",
		Name:                               "managed/q",
		AccessType:                         "exclusive",
		Permission:                         "consume",
		IngressEnabled:                     false,
		EgressEnabled:                      true,
		MaxMsgSpoolUsage:                   1000,
		MaxRedeliveryCount:                 5,
		DeadMsgQueue:                       "#DEAD_MSG_QUEUE",
		PartitionCount:                     0,
		ConsumerAckPropagationEnabled:      true,
		MaxDeliveredUnackedMsgsPerFlow:     100,
		RejectMsgToSenderOnDiscardBehavior: rejectAlways,
	}
}

func queuePayload(spec QueueSpec) map[string]any {
	body, _ := json.Marshal(configFromSpec(spec))
	var result map[string]any
	_ = json.Unmarshal(body, &result)
	return result
}

func TestCreateQueueRejectsExistingResource(t *testing.T) {
	spec := managedQueue()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", request.Method)
		}
		jsonResponse(writer, http.StatusBadRequest, map[string]any{"meta": map[string]any{"error": map[string]any{"status": "ALREADY_EXISTS"}}})
	}))
	defer server.Close()
	created, err := testClient(t, server).CreateQueue(context.Background(), spec)
	if created || !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("CreateQueue() = %v, %v", created, err)
	}
}

func TestCreateQueueTracksUncertainPostForCleanup(t *testing.T) {
	spec := managedQueue()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer server.Close()
	created, err := testClient(t, server).CreateQueue(context.Background(), spec)
	if !created || err == nil {
		t.Fatalf("CreateQueue() = %v, %v", created, err)
	}
}

func TestCreateQueueTracksSuccessfulPostWhenVerificationFails(t *testing.T) {
	spec := managedQueue()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if requests.Add(1) == 1 {
			writer.WriteHeader(http.StatusCreated)
			return
		}
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	created, err := testClient(t, server).CreateQueue(context.Background(), spec)
	if !created || err == nil {
		t.Fatalf("CreateQueue() = %v, %v", created, err)
	}
}

func TestEnsureQueueAdoptsExactConfiguration(t *testing.T) {
	spec := managedQueue()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		payload := queuePayload(spec)
		payload["runtimeOnlyField"] = "ignored"
		jsonResponse(writer, http.StatusOK, map[string]any{"data": payload})
	}))
	defer server.Close()
	if err := testClient(t, server).EnsureQueue(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want one GET", requests)
	}
}

func TestEnsureQueueRejectsIncompatibleConfiguration(t *testing.T) {
	spec := managedQueue()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		payload := queuePayload(spec)
		payload["maxRedeliveryCount"] = float64(6)
		jsonResponse(writer, http.StatusOK, map[string]any{"data": payload})
	}))
	defer server.Close()
	err := testClient(t, server).EnsureQueue(context.Background(), spec)
	if err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("expected incompatibility, got %v", err)
	}
}

func TestEnsureQueueCreatesAndVerifiesAfterAlreadyExistsRace(t *testing.T) {
	spec := managedQueue()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch requests.Add(1) {
		case 1:
			notFound(writer)
		case 2:
			jsonResponse(writer, http.StatusBadRequest, map[string]any{"meta": map[string]any{"error": map[string]any{"status": "ALREADY_EXISTS"}}})
		case 3:
			jsonResponse(writer, http.StatusOK, map[string]any{"data": queuePayload(spec)})
		default:
			t.Fatal("unexpected request")
		}
	}))
	defer server.Close()
	if err := testClient(t, server).EnsureQueue(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
}

func TestFenceRequiresGETVerification(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch requests.Add(1) {
		case 1:
			if request.Method != http.MethodPatch {
				t.Errorf("method = %s", request.Method)
			}
			writer.WriteHeader(http.StatusOK)
		case 2:
			jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
				"queueName": "q", "msgVpnName": "vpn", "ingressEnabled": true,
				"rejectMsgToSenderOnDiscardBehavior": rejectAlways,
			}})
		}
	}))
	defer server.Close()
	err := testClient(t, server).FenceQueue(context.Background(), "vpn", "q")
	if err == nil || !strings.Contains(err.Error(), "did not verify") || requests.Load() != 2 {
		t.Fatalf("expected failed GET verification after PATCH, requests=%d err=%v", requests.Load(), err)
	}
}

func TestFenceRejectsMissingOrNullIngressVerification(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "omitted"},
		{name: "null", value: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests++
				if request.Method == http.MethodPatch {
					writer.WriteHeader(http.StatusNoContent)
					return
				}
				data := map[string]any{"rejectMsgToSenderOnDiscardBehavior": rejectAlways}
				if test.name == "null" {
					data["ingressEnabled"] = test.value
				}
				jsonResponse(writer, http.StatusOK, map[string]any{"data": data})
			}))
			defer server.Close()

			err := testClient(t, server).FenceQueue(context.Background(), "vpn", "q")
			if err == nil || !strings.Contains(err.Error(), "did not verify") || requests != 2 {
				t.Fatalf("expected unverified ingress error, requests=%d err=%v", requests, err)
			}
		})
	}
}

func TestMonitorVPNUsesAuthenticatedPaginatedConnectionCount(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		username, password, ok := request.BasicAuth()
		if !ok || username != "admin-user" || password != "do-not-leak-secret" {
			t.Fatal("missing SEMP basic authentication")
		}
		switch {
		case request.URL.Path == "/SEMP/v2/monitor/msgVpns/vpn":
			jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
				"msgVpnName": "vpn", "rxByteRate": 101, "txByteRate": 202, "msgSpoolUsage": 303,
			}})
		case request.URL.Path == "/SEMP/v2/monitor/msgVpns/vpn/clients" && request.URL.Query().Get("cursor") == "next":
			jsonResponse(writer, http.StatusOK, map[string]any{"data": []any{
				map[string]any{"msgVpnName": "vpn", "clientName": "client-2"},
			}})
		case request.URL.Path == "/SEMP/v2/monitor/msgVpns/vpn/clients":
			jsonResponse(writer, http.StatusOK, map[string]any{
				"data": []any{map[string]any{"msgVpnName": "vpn", "clientName": "client-1"}},
				"meta": map[string]any{"paging": map[string]any{"nextPageUri": server.URL + request.URL.Path + "?cursor=next"}},
			})
		case strings.HasSuffix(request.URL.Path, "/clients/client-1/connections") && request.URL.Query().Get("cursor") == "connection-next":
			jsonResponse(writer, http.StatusOK, map[string]any{"data": []any{
				map[string]any{"msgVpnName": "vpn", "clientName": "client-1", "clientAddress": "192.0.2.1:1001"},
			}})
		case strings.HasSuffix(request.URL.Path, "/clients/client-1/connections"):
			jsonResponse(writer, http.StatusOK, map[string]any{
				"data": []any{map[string]any{"msgVpnName": "vpn", "clientName": "client-1", "clientAddress": "192.0.2.1:1000"}},
				"meta": map[string]any{"paging": map[string]any{"nextPageUri": server.URL + request.URL.Path + "?cursor=connection-next"}},
			})
		case strings.HasSuffix(request.URL.Path, "/clients/client-2/connections"):
			jsonResponse(writer, http.StatusOK, map[string]any{"data": []any{
				map[string]any{"msgVpnName": "vpn", "clientName": "client-2", "clientAddress": "192.0.2.2:1000"},
			}})
		default:
			t.Fatalf("unexpected request %s", request.URL.String())
		}
	}))
	defer server.Close()

	status, err := testClient(t, server).MonitorVPN(context.Background(), "vpn")
	if err != nil {
		t.Fatal(err)
	}
	if status.IngressBytesPerSecond != 101 || status.EgressBytesPerSecond != 202 || status.SpoolBytes != 303 || status.ObservedClients != 2 || status.Connections != 3 {
		t.Fatalf("unexpected VPN status: %+v", status)
	}
}

func TestMonitorVPNRejectsMissingMetricAndMismatchedConnection(t *testing.T) {
	t.Run("missing metric", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
				"msgVpnName": "vpn", "rxByteRate": 1, "msgSpoolUsage": 2,
			}})
		}))
		defer server.Close()
		_, err := testClient(t, server).MonitorVPN(context.Background(), "vpn")
		if err == nil || !strings.Contains(err.Error(), "txByteRate") {
			t.Fatalf("expected missing counter error, got %v", err)
		}
	})

	t.Run("mismatched connection", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			switch {
			case strings.HasSuffix(request.URL.Path, "/connections"):
				jsonResponse(writer, http.StatusOK, map[string]any{"data": []any{
					map[string]any{"msgVpnName": "other", "clientName": "client", "clientAddress": "192.0.2.1:1000"},
				}})
			case strings.HasSuffix(request.URL.Path, "/clients"):
				jsonResponse(writer, http.StatusOK, map[string]any{"data": []any{
					map[string]any{"msgVpnName": "vpn", "clientName": "client"},
				}})
			default:
				jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
					"msgVpnName": "vpn", "rxByteRate": 1, "txByteRate": 2, "msgSpoolUsage": 3,
				}})
			}
		}))
		defer server.Close()
		_, err := testClient(t, server).MonitorVPN(context.Background(), "vpn")
		if err == nil || !strings.Contains(err.Error(), "invalid identity") {
			t.Fatalf("expected connection identity error, got %v", err)
		}
	})
}

func TestMonitorQueueRejectsMissingCounter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
			"queueName": "q", "msgVpnName": "vpn", "rxByteRate": 10, "txByteRate": 20, "spooledMsgCount": 2,
			"msgSpoolUsage": 512, "txUnackedMsgCount": 1, "ingressEnabled": false,
		}})
	}))
	defer server.Close()
	_, err := testClient(t, server).MonitorQueue(context.Background(), "vpn", "q")
	if err == nil || !strings.Contains(err.Error(), "inProgressAckMsgCount") {
		t.Fatalf("expected missing counter error, got %v", err)
	}
}

func TestMonitorQueueRequiresThroughputCounter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
			"queueName": "q", "msgVpnName": "vpn", "txByteRate": 20, "spooledMsgCount": 2,
			"msgSpoolUsage": 512, "txUnackedMsgCount": 1,
			"inProgressAckMsgCount": 0, "ingressEnabled": false,
		}})
	}))
	defer server.Close()
	_, err := testClient(t, server).MonitorQueue(context.Background(), "vpn", "q")
	if err == nil || !strings.Contains(err.Error(), "rxByteRate") {
		t.Fatalf("expected missing throughput error, got %v", err)
	}
}

func TestMonitorQueueRejectsMismatchedFlowIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/queues/q") {
			jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
				"queueName": "q", "msgVpnName": "vpn", "rxByteRate": 10, "txByteRate": 20, "spooledMsgCount": 0,
				"msgSpoolUsage": 0, "txUnackedMsgCount": 0, "inProgressAckMsgCount": 0, "ingressEnabled": true,
			}})
			return
		}
		jsonResponse(writer, http.StatusOK, map[string]any{"data": []any{
			map[string]any{"msgVpnName": "other", "queueName": "q", "clientName": "client", "flowId": 1},
		}})
	}))
	defer server.Close()
	_, err := testClient(t, server).MonitorQueue(context.Background(), "vpn", "q")
	if err == nil || !strings.Contains(err.Error(), "mismatched identity") {
		t.Fatalf("expected flow identity error, got %v", err)
	}
}

func TestMonitorQueueRejectsNullOrEmptyCounter(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "null", value: nil},
		{name: "empty", value: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
					"queueName": "q", "msgVpnName": "vpn", "rxByteRate": 10, "txByteRate": 20, "spooledMsgCount": test.value,
					"msgSpoolUsage": 512, "txUnackedMsgCount": 1,
					"inProgressAckMsgCount": 0, "ingressEnabled": false,
				}})
			}))
			defer server.Close()

			_, err := testClient(t, server).MonitorQueue(context.Background(), "vpn", "q")
			if err == nil || !strings.Contains(err.Error(), "spooledMsgCount") {
				t.Fatalf("expected invalid counter error, got %v", err)
			}
		})
	}
}

func TestMonitorDrainDoesNotFetchFlows(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests = append(requests, request.URL.Path)
		jsonResponse(writer, http.StatusOK, map[string]any{
			"collections": map[string]any{"msgs": map[string]any{"count": 0}},
			"data": map[string]any{
				"queueName": "q", "msgVpnName": "vpn", "msgSpoolUsage": 0,
				"txUnackedMsgCount": 0, "inProgressAckMsgCount": 0,
			},
		})
	}))
	defer server.Close()
	status, err := testClient(t, server).MonitorDrain(context.Background(), "vpn", "q")
	if err != nil {
		t.Fatal(err)
	}
	if !status.Drained() || len(requests) != 1 || strings.Contains(requests[0], "txFlows") {
		t.Fatalf("status=%+v requests=%v", status, requests)
	}
}

func TestMonitorQueuePaginatesFlowsAndCountsConsumers(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/queues/q"):
			jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
				"queueName": "q", "msgVpnName": "vpn", "rxByteRate": 10, "txByteRate": 20, "spooledMsgCount": 2,
				"msgSpoolUsage": 512, "txUnackedMsgCount": 1,
				"inProgressAckMsgCount": 1, "ingressEnabled": false,
			}})
		case request.URL.Query().Get("cursor") == "page2":
			jsonResponse(writer, http.StatusOK, map[string]any{"data": []any{
				map[string]any{"msgVpnName": "vpn", "queueName": "q", "clientName": "client-2", "flowId": 3},
			}})
		default:
			jsonResponse(writer, http.StatusOK, map[string]any{
				"data": []any{
					map[string]any{"msgVpnName": "vpn", "queueName": "q", "clientName": "client-1", "sessionName": "session-1", "flowId": 1},
					map[string]any{"msgVpnName": "vpn", "queueName": "q", "clientName": "client-1", "sessionName": "session-1", "flowId": 2},
				},
				"meta": map[string]any{"paging": map[string]any{"nextPageUri": server.URL + request.URL.Path + "?cursor=page2"}},
			})
		}
	}))
	defer server.Close()
	status, err := testClient(t, server).MonitorQueue(context.Background(), "vpn", "q")
	if err != nil {
		t.Fatal(err)
	}
	if status.Flows != 3 || status.Consumers != 2 || status.IngressBytesPerSecond != 10 || status.EgressBytesPerSecond != 20 || !status.ThroughputKnown || status.SpooledMessages != 2 || status.Drained() {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestPaginationRejectsCrossHost(t *testing.T) {
	attacker := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		t.Fatal("cross-host pagination request was followed")
	}))
	defer attacker.Close()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/queues/q") {
			jsonResponse(writer, http.StatusOK, map[string]any{"data": map[string]any{
				"queueName": "q", "msgVpnName": "vpn", "rxByteRate": 10, "txByteRate": 20, "spooledMsgCount": 0,
				"msgSpoolUsage": 0, "txUnackedMsgCount": 0,
				"inProgressAckMsgCount": 0, "ingressEnabled": true,
			}})
			return
		}
		jsonResponse(writer, http.StatusOK, map[string]any{
			"data": []any{}, "meta": map[string]any{"paging": map[string]any{"nextPageUri": attacker.URL + "/steal"}},
		})
	}))
	defer server.Close()
	_, err := testClient(t, server).MonitorQueue(context.Background(), "vpn", "q")
	if err == nil || !strings.Contains(err.Error(), "changes origin") {
		t.Fatalf("expected cross-host rejection, got %v", err)
	}
}

type fakeMonitorClient struct {
	vpn        VPNStatus
	queue      QueueStatus
	vpnErr     error
	queueErr   error
	vpnCalls   int
	queueCalls int
}

func (client *fakeMonitorClient) MonitorVPN(context.Context, string) (VPNStatus, error) {
	client.vpnCalls++
	return client.vpn, client.vpnErr
}

func (client *fakeMonitorClient) MonitorQueue(context.Context, string, string) (QueueStatus, error) {
	client.queueCalls++
	return client.queue, client.queueErr
}

func TestPolicyCollectorEmitsSeparateResourceAndBacklogSamples(t *testing.T) {
	client := &fakeMonitorClient{
		vpn: VPNStatus{MessageVPN: "vpn", IngressBytesPerSecond: 100, EgressBytesPerSecond: 200, SpoolBytes: 300, Connections: 4},
		queue: QueueStatus{
			MessageVPN: "vpn", Name: "swlb.orders.7.broker-a", ThroughputKnown: true,
			IngressBytesPerSecond: 10, EgressBytesPerSecond: 20, SpooledMessages: 30,
			SpoolUsageBytes: 40, UnackedMessages: 5, InProgressAckMessages: 2, Consumers: 3,
		},
	}
	times := []time.Time{
		time.Date(2026, 9, 26, 12, 0, 1, 0, time.FixedZone("offset", 3600)),
		time.Date(2026, 9, 26, 12, 0, 2, 0, time.FixedZone("offset", 3600)),
		time.Date(2026, 9, 26, 12, 0, 3, 0, time.FixedZone("offset", 3600)),
	}
	index := 0
	collector := PolicyCollector{
		Targets: PolicyTargetProviderFunc(func() ([]BrokerTarget, []GroupTarget, error) {
			return []BrokerTarget{{ID: "broker-a", MessageVPN: "vpn", ServiceClass: "class-a", BrokerVersion: "10.4.1", Client: client}},
				[]GroupTarget{{GroupID: "orders", BrokerID: "broker-a", Queue: "swlb.orders.7.broker-a"}}, nil
		}),
		Now: func() time.Time { value := times[index]; index++; return value },
	}
	snapshot, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Brokers) != 1 || len(snapshot.Groups) != 1 || client.vpnCalls != 1 || client.queueCalls != 1 {
		t.Fatalf("unexpected collection: %#v calls=%d/%d", snapshot, client.vpnCalls, client.queueCalls)
	}
	broker := snapshot.Brokers[0]
	if broker.ObservedAt != times[0].UTC() || broker.Resources.IngressBytesPerSecond.Value != 100 || broker.Resources.Connections.Value != 4 {
		t.Fatalf("unexpected broker sample: %#v", broker)
	}
	group := snapshot.Groups[0]
	if group.ObservedAt != times[1].UTC() || !group.Resources.IngressBytesPerSecond.Known || group.Resources.IngressBytesPerSecond.Value != 10 || group.Resources.SpoolBytes.Value != 40 || group.Resources.Connections.Value != 3 {
		t.Fatalf("unexpected group resources: %#v", group)
	}
	if group.Backlog.QueuedMessages.Value != 30 || group.Backlog.UnackedMessages.Value != 7 {
		t.Fatalf("backlog was not kept separate: %#v", group.Backlog)
	}
	if snapshot.CapturedAt != times[2].UTC() {
		t.Fatalf("captured at = %v, want %v", snapshot.CapturedAt, times[2].UTC())
	}
}

func TestPolicyCollectorFailsClosed(t *testing.T) {
	t.Run("unavailable queue metric remains unknown", func(t *testing.T) {
		client := &fakeMonitorClient{
			vpn:   VPNStatus{MessageVPN: "vpn"},
			queue: QueueStatus{MessageVPN: "vpn", Name: "queue"},
		}
		collector := PolicyCollector{
			Targets: PolicyTargetProviderFunc(func() ([]BrokerTarget, []GroupTarget, error) {
				return []BrokerTarget{{ID: "a", MessageVPN: "vpn", ServiceClass: "class", BrokerVersion: "1", Client: client}},
					[]GroupTarget{{GroupID: "g", BrokerID: "a", Queue: "queue"}}, nil
			}),
			Now: func() time.Time { return time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC) },
		}
		snapshot, err := collector.Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		resources := snapshot.Groups[0].Resources
		if resources.IngressBytesPerSecond.Known || resources.EgressBytesPerSecond.Known {
			t.Fatalf("unavailable throughput was invented: %#v", resources)
		}
	})

	t.Run("monitor failure aborts cycle", func(t *testing.T) {
		client := &fakeMonitorClient{vpn: VPNStatus{MessageVPN: "vpn"}, queueErr: errors.New("read failed")}
		collector := PolicyCollector{
			Targets: PolicyTargetProviderFunc(func() ([]BrokerTarget, []GroupTarget, error) {
				return []BrokerTarget{{ID: "a", MessageVPN: "vpn", ServiceClass: "class", BrokerVersion: "1", Client: client}},
					[]GroupTarget{{GroupID: "g", BrokerID: "a", Queue: "queue"}}, nil
			}),
			Now: time.Now,
		}
		if _, err := collector.Collect(context.Background()); err == nil || !strings.Contains(err.Error(), "read failed") {
			t.Fatalf("expected failed cycle, got %v", err)
		}
	})

	t.Run("invalid target blocks all requests", func(t *testing.T) {
		client := &fakeMonitorClient{}
		collector := PolicyCollector{
			Targets: PolicyTargetProviderFunc(func() ([]BrokerTarget, []GroupTarget, error) {
				return []BrokerTarget{{ID: "a", MessageVPN: "vpn", ServiceClass: "class", BrokerVersion: "1", Client: client}},
					[]GroupTarget{{GroupID: "g", BrokerID: "missing", Queue: "queue"}}, nil
			}),
		}
		if _, err := collector.Collect(context.Background()); err == nil || !strings.Contains(err.Error(), "unknown broker") {
			t.Fatalf("expected invalid target error, got %v", err)
		}
		if client.vpnCalls != 0 || client.queueCalls != 0 {
			t.Fatal("collector performed requests before validating the target snapshot")
		}
	})
}

func TestProvisionApplicationNarrowAndOrdered(t *testing.T) {
	vpn := "vpn"
	acl := ACLProfileSpec{vpn, "acl", "allow", "disallow", "disallow", "disallow"}
	profile := ClientProfileSpec{vpn, "profile", true, true, false, true}
	username := ClientUsernameSpec{vpn, "user", true, "profile", "acl"}
	posts := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			notFound(writer)
			return
		}
		if request.Method == http.MethodPost {
			posts = append(posts, request.URL.Path)
			var body map[string]any
			_ = json.NewDecoder(request.Body).Decode(&body)
			if _, exists := body["password"]; exists {
				t.Error("password was provisioned")
			}
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		t.Fatal("unexpected method")
	}))
	defer server.Close()
	// After each POST, verification GET must return the matching resource.
	client := testClient(t, server)
	responses := []any{aclProfileConfig{
		MessageVPN: vpn, Name: "acl", ClientConnectDefaultAction: "allow",
		PublishTopicDefaultAction: "disallow", SubscribeTopicDefaultAction: "disallow", SubscribeShareNameDefaultAction: "disallow",
	}, clientProfileConfig{
		MessageVPN: vpn, Name: "profile", AllowGuaranteedMsgSendEnabled: true,
		AllowGuaranteedMsgReceiveEnabled: true, RejectMsgToSenderOnNoSubscriptionMatchEnabled: true,
	}, clientUsernameConfig{MessageVPN: vpn, Name: "user", Enabled: true, ClientProfileName: "profile", ACLProfileName: "acl"}}
	var step atomic.Int32
	server.Config.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		index := int(step.Load())
		if request.Method == http.MethodGet {
			if index%3 == 0 {
				step.Add(1)
				notFound(writer)
				return
			}
			jsonResponse(writer, http.StatusOK, map[string]any{"data": responses[index/3]})
			step.Add(1)
			return
		}
		if request.Method == http.MethodPost {
			posts = append(posts, request.URL.Path)
			var body map[string]any
			_ = json.NewDecoder(request.Body).Decode(&body)
			if _, exists := body["password"]; exists {
				t.Error("password was provisioned")
			}
			step.Add(1)
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		t.Fatal("unexpected method")
	})
	if err := client.ProvisionApplication(context.Background(), acl, profile, username); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/SEMP/v2/config/msgVpns/vpn/aclProfiles",
		"/SEMP/v2/config/msgVpns/vpn/clientProfiles",
		"/SEMP/v2/config/msgVpns/vpn/clientUsernames",
	}
	if fmt.Sprint(posts) != fmt.Sprint(want) {
		t.Fatalf("POST order = %v, want %v", posts, want)
	}
}
