package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseGroupsTrimsDeduplicatesAndPreservesOrder(t *testing.T) {
	got := parseGroups(" audit,ledger,audit, ,billing ")
	want := []string{"audit", "ledger", "billing"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseGroups() = %v, want %v", got, want)
	}
}

func TestResolveCredentialUsesBrokerSpecificValue(t *testing.T) {
	credentials := map[string]credential{
		"a": {Username: "cloud-a", Password: "secret-a"},
		"b": {Username: "cloud-b", Password: "secret-b"},
	}
	username, password, err := resolveCredential(credentials, "b", func(string) string {
		return "fallback"
	})
	if err != nil || username != "cloud-b" || password != "secret-b" {
		t.Fatalf("resolveCredential() = %q, %q, %v", username, password, err)
	}
}

func TestResolveCredentialFallsBackAndRejectsMissingValues(t *testing.T) {
	values := map[string]string{"SOLACE_USERNAME": "default", "SOLACE_PASSWORD": "password"}
	username, password, err := resolveCredential(nil, "a", func(key string) string {
		return values[key]
	})
	if err != nil || username != "default" || password != "password" {
		t.Fatalf("fallback = %q, %q, %v", username, password, err)
	}
	_, _, err = resolveCredential(nil, "missing", func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "credentials unavailable for broker missing") {
		t.Fatalf("missing credential error = %v", err)
	}
}
