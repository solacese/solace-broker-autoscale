package rules

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestInteropGolden proves the Go engine loads the identical portable spec the Python controller
// emits (testdata/interop_spec.json is written by scaling-controller/solace_autoscale/dispatch's
// to_spec) and routes every message to the same decision. The expected column is what the Python
// plan.decide produces for these same inputs; if the two engines ever diverge, this fails.
func TestInteropGolden(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "interop_spec.json"))
	if err != nil {
		t.Fatalf("read golden spec: %v", err)
	}
	plan, err := LoadSpec(data)
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	if plan.DefaultBroker != "broker-bulk" {
		t.Fatalf("default broker = %q", plan.DefaultBroker)
	}
	if got := plan.Targets(); len(got) != 3 {
		t.Fatalf("targets = %v (want 3: vip, big, bulk)", got)
	}

	type want struct {
		broker, address, key, rule string
		matched                    bool
	}
	cases := []struct {
		topic   string
		payload []byte
		want    want
	}{
		{"orders/eu/created", []byte(`{"priority":"high","region":"eu","amount":50}`),
			want{"broker-vip", "vip/orders/eu/created", "vip.eu", "vip-orders", true}},
		{"orders/us/created", []byte(`{"priority":"low","region":"us","amount":5000}`),
			want{"broker-big", "orders/us/created", "big.us", "large-orders", true}},
		{"orders/us/created", []byte(`{"priority":"low","region":"us","amount":10}`),
			want{"broker-bulk", "orders/us/created", "", "default", false}},
		{"telemetry/dev/1", bytes.Repeat([]byte("x"), 3000),
			want{"broker-bulk", "telemetry/dev/1", "", "telemetry-raw", true}},
		{"telemetry/dev/1", []byte(`{"small":true}`),
			want{"broker-bulk", "telemetry/dev/1", "", "default", false}},
		{"random/topic", []byte(`{}`),
			want{"broker-bulk", "random/topic", "", "default", false}},
		{"orders/eu/created", []byte("not-json"),
			want{"broker-bulk", "orders/eu/created", "", "default", false}},
	}
	for _, c := range cases {
		d, err := plan.Decide(c.topic, c.payload)
		if err != nil {
			t.Errorf("%s: %v", c.topic, err)
			continue
		}
		got := want{d.Broker, d.Address, d.Key, d.Rule, d.Matched}
		if got != c.want {
			t.Errorf("decide(%q):\n got  %+v\n want %+v", c.topic, got, c.want)
		}
	}
}

func TestLoadSpecRejectsUnknownOperatorAndVersion(t *testing.T) {
	if _, err := LoadSpec([]byte(`{"version":1,"default_broker":"b","rules":[{"name":"r","when":{"topic":">","payload":[{"op":"bogus","path":"x"}]},"route":{"broker":"b"}}]}`)); err == nil {
		t.Error("expected error for unknown operator")
	}
	if _, err := LoadSpec([]byte(`{"version":999,"default_broker":"b","rules":[]}`)); err == nil {
		t.Error("expected error for unsupported version")
	}
	if _, err := LoadSpec([]byte(`{"version":1,"rules":[{"name":"r","when":{"topic":">"},"route":{"key":"k"}}]}`)); err == nil {
		t.Error("expected error for rule missing route.broker")
	}
}
