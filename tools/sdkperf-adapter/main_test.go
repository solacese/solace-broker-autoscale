package main

import "testing"

func TestBelongsToRun(t *testing.T) {
	for _, test := range []struct {
		name, eventID, runID string
		want                 bool
	}{
		{"current run", "sdkperf-123-45-000001", "123-45", true},
		{"old run", "sdkperf-123-44-000001", "123-45", false},
		{"missing sequence", "sdkperf-123-45-", "123-45", false},
		{"synthetic event", "family-123-A-000001", "123-45", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := belongsToRun(test.eventID, test.runID); got != test.want {
				t.Fatalf("belongsToRun(%q, %q) = %t, want %t", test.eventID, test.runID, got, test.want)
			}
		})
	}
}
