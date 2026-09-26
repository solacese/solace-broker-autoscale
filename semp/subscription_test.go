package semp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestListExactQueueSubscriptionsPaginatesAndValidatesIdentity(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", request.Method)
		}
		if request.URL.EscapedPath() != "/SEMP/v2/config/msgVpns/vpn%2Fone/queues/q%2Fname/subscriptions" {
			t.Fatalf("path = %q", request.URL.EscapedPath())
		}
		if request.URL.Query().Get("cursor") == "next" {
			jsonResponse(writer, http.StatusOK, map[string]any{"data": []any{
				map[string]any{"msgVpnName": "vpn/one", "queueName": "q/name", "subscriptionTopic": "new/epoch/>"},
			}})
			return
		}
		jsonResponse(writer, http.StatusOK, map[string]any{
			"data": []any{
				map[string]any{"msgVpnName": "vpn/one", "queueName": "q/name", "subscriptionTopic": "old/epoch/>"},
			},
			"meta": map[string]any{"paging": map[string]any{"nextPageUri": server.URL + request.URL.EscapedPath() + "?cursor=next"}},
		})
	}))
	defer server.Close()

	topics, err := testClient(t, server).ListExactQueueSubscriptions(context.Background(), "vpn/one", "q/name")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"old/epoch/>", "new/epoch/>"}; !reflect.DeepEqual(topics, want) {
		t.Fatalf("topics = %v, want %v", topics, want)
	}
}

func TestListExactQueueSubscriptionsRejectsMalformedIdentity(t *testing.T) {
	tests := []struct {
		name string
		item map[string]any
	}{
		{name: "missing VPN", item: map[string]any{"queueName": "queue", "subscriptionTopic": "topic/>"}},
		{name: "wrong VPN", item: map[string]any{"msgVpnName": "other", "queueName": "queue", "subscriptionTopic": "topic/>"}},
		{name: "missing queue", item: map[string]any{"msgVpnName": "vpn", "subscriptionTopic": "topic/>"}},
		{name: "wrong queue", item: map[string]any{"msgVpnName": "vpn", "queueName": "other", "subscriptionTopic": "topic/>"}},
		{name: "missing topic", item: map[string]any{"msgVpnName": "vpn", "queueName": "queue"}},
		{name: "empty topic", item: map[string]any{"msgVpnName": "vpn", "queueName": "queue", "subscriptionTopic": ""}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				jsonResponse(writer, http.StatusOK, map[string]any{"data": []any{test.item}})
			}))
			defer server.Close()

			topics, err := testClient(t, server).ListExactQueueSubscriptions(context.Background(), "vpn", "queue")
			if err == nil || topics != nil || !strings.Contains(err.Error(), "subscription 0 has missing or mismatched identity") {
				t.Fatalf("topics=%v err=%v", topics, err)
			}
		})
	}
}

func TestListExactQueueSubscriptionsRejectsMalformedPagination(t *testing.T) {
	var attackerRequests atomic.Int32
	attacker := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		attackerRequests.Add(1)
	}))
	defer attacker.Close()

	tests := []struct {
		name    string
		next    func(*httptest.Server) string
		wantErr string
	}{
		{name: "invalid URL", next: func(*httptest.Server) string { return "%" }, wantErr: "invalid pagination link"},
		{name: "cross host", next: func(*httptest.Server) string { return attacker.URL + "/steal" }, wantErr: "changes origin"},
		{name: "leaves base path", next: func(server *httptest.Server) string { return server.URL + "/outside" }, wantErr: "leaves configured base path"},
		{name: "loop", next: func(server *httptest.Server) string {
			return server.URL + "/root/SEMP/v2/config/msgVpns/vpn/queues/queue/subscriptions"
		}, wantErr: "pagination loop detected"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				jsonResponse(writer, http.StatusOK, map[string]any{
					"data": []any{},
					"meta": map[string]any{"paging": map[string]any{"nextPageUri": test.next(server)}},
				})
			}))
			defer server.Close()
			client := testClient(t, server, func(options *ClientOptions) { options.BaseURL = server.URL + "/root" })

			_, err := client.ListExactQueueSubscriptions(context.Background(), "vpn", "queue")
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want %q", err, test.wantErr)
			}
		})
	}
	if got := attackerRequests.Load(); got != 0 {
		t.Fatalf("cross-host pagination made %d requests", got)
	}
}
