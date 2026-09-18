package resolve

import (
	"context"
	"errors"
	"testing"
	"time"
)

const assignBody = `{"broker_id":"b1","msg_vpn":"vpn1","state":"active","lease_seconds":300,` +
	`"endpoints":{"amqp":"amqps://b1.example.com:5671"},"reused_existing":false}`

func TestResolveCachesAndReturnsFresh(t *testing.T) {
	calls := 0
	r := New("https://assign.example.com")
	r.Fetch = func(_ context.Context, _ string) ([]byte, error) {
		calls++
		return []byte(assignBody), nil
	}
	a, err := r.Resolve(context.Background(), "shard-a", "pub", "guaranteed", "amqp")
	if err != nil {
		t.Fatal(err)
	}
	if a.BrokerID != "b1" {
		t.Errorf("broker = %q", a.BrokerID)
	}
	uri, err := a.Endpoint("amqp")
	if err != nil || uri != "amqps://b1.example.com:5671" {
		t.Errorf("endpoint = %q, %v", uri, err)
	}
	if calls != 1 {
		t.Errorf("expected 1 fetch, got %d", calls)
	}
}

func TestResolveFailsOpenToCache(t *testing.T) {
	fail := false
	r := New("https://assign.example.com")
	r.Fetch = func(_ context.Context, _ string) ([]byte, error) {
		if fail {
			return nil, errors.New("connection refused")
		}
		return []byte(assignBody), nil
	}
	// prime the cache
	if _, err := r.Resolve(context.Background(), "shard-a", "pub", "direct", "amqp"); err != nil {
		t.Fatal(err)
	}
	// service now down: same key still resolves from cache
	fail = true
	a, err := r.Resolve(context.Background(), "shard-a", "pub", "direct", "amqp")
	if err != nil {
		t.Fatalf("expected fail-open, got error: %v", err)
	}
	if a.BrokerID != "b1" {
		t.Errorf("stale cache broker = %q", a.BrokerID)
	}
}

func TestResolveErrorsWhenDownAndUncached(t *testing.T) {
	r := New("https://assign.example.com")
	r.Fetch = func(_ context.Context, _ string) ([]byte, error) {
		return nil, errors.New("connection refused")
	}
	if _, err := r.Resolve(context.Background(), "shard-x", "pub", "direct", "amqp"); err == nil {
		t.Error("expected error with no cache and service down")
	}
}

func TestResolveMissingProtocol(t *testing.T) {
	r := New("https://assign.example.com")
	r.Now = func() time.Time { return time.Unix(0, 0) }
	r.Fetch = func(_ context.Context, _ string) ([]byte, error) { return []byte(assignBody), nil }
	a, _ := r.Resolve(context.Background(), "s", "c", "direct", "")
	if _, err := a.Endpoint("mqtt"); err == nil {
		t.Error("expected error for absent protocol endpoint")
	}
	if !a.FetchedAt.Equal(time.Unix(0, 0)) {
		t.Error("FetchedAt should use the injected clock")
	}
}

func TestResolveRejectsEmptyAssignment(t *testing.T) {
	r := New("https://assign.example.com")
	r.Fetch = func(_ context.Context, _ string) ([]byte, error) {
		return []byte(`{"broker_id":"","endpoints":{}}`), nil
	}
	if _, err := r.Resolve(context.Background(), "s", "c", "direct", ""); err == nil {
		t.Error("expected error for assignment with no broker/endpoints and no cache")
	}
}

func TestAuthorizationRejectionDoesNotReuseCachedAssignment(t *testing.T) {
	r := New("https://unused")
	r.Fetch = func(context.Context, string) ([]byte, error) { return []byte(assignBody), nil }
	if _, err := r.Resolve(context.Background(), "s", "c", "guaranteed", "amqp"); err != nil {
		t.Fatal(err)
	}
	r.Fetch = func(context.Context, string) ([]byte, error) { return nil, &HTTPError{Status: 401} }
	if _, err := r.Resolve(context.Background(), "s", "c", "guaranteed", "amqp"); err == nil {
		t.Fatal("authorization rejection reused cache")
	}
}
func TestProtocolCacheCannotSupplyAnotherProtocol(t *testing.T) {
	r := New("https://unused")
	r.Fetch = func(context.Context, string) ([]byte, error) { return []byte(assignBody), nil }
	r.Resolve(context.Background(), "s", "c", "guaranteed", "amqp")
	r.Fetch = func(context.Context, string) ([]byte, error) { return nil, errors.New("offline") }
	if _, err := r.Resolve(context.Background(), "s", "c", "guaranteed", "mqtt"); err == nil {
		t.Fatal("cross-protocol cached assignment")
	}
}
