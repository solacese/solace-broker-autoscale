package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/solacese/solace-workload-balancer/qualification"
	"github.com/solacese/solace-workload-balancer/routing"
)

const (
	defaultEvidencePath = "qualification-result.json"
	evidenceSchema      = "solace-workload-balancer/qualification-result/v1"
)

type qualificationEvidence struct {
	SchemaVersion         string                `json:"schema_version"`
	GeneratedAt           time.Time             `json:"generated_at"`
	Build                 buildEvidence         `json:"build"`
	Topology              topologyEvidence      `json:"topology"`
	PerformanceConditions performanceConditions `json:"performance_conditions"`
	Result                redactedResult        `json:"result"`
	Cleanup               cleanupEvidence       `json:"cleanup"`
	OverallStatus         string                `json:"overall_status"`
}

type buildEvidence struct {
	Module       string            `json:"module"`
	Version      string            `json:"version"`
	GoVersion    string            `json:"go_version"`
	GOOS         string            `json:"goos"`
	GOARCH       string            `json:"goarch"`
	Dependencies map[string]string `json:"dependencies,omitempty"`
}

type topologyEvidence struct {
	Datacenter    string                 `json:"datacenter"`
	BrokerRelease string                 `json:"broker_release"`
	Brokers       []topologyBroker       `json:"brokers"`
	ScalingGroups []topologyScalingGroup `json:"scaling_groups"`
}

type topologyBroker struct {
	Role         string `json:"role"`
	ServiceClass string `json:"service_class"`
	Capacity     string `json:"capacity"`
	Availability string `json:"availability"`
}

type topologyScalingGroup struct {
	Label                 string     `json:"label"`
	HashContract          string     `json:"hash_contract"`
	QueueMode             string     `json:"queue_mode"`
	MembershipTransitions [][]string `json:"membership_transitions"`
}

type performanceConditions struct {
	Keys                 int      `json:"keys"`
	EventsPerKey         int      `json:"events_per_key"`
	PayloadKinds         []string `json:"payload_kinds"`
	ExpectedMessages     int      `json:"expected_messages"`
	TargetDuration       string   `json:"target_duration"`
	PublishTimeout       string   `json:"publish_timeout"`
	ReceiveTimeout       string   `json:"receive_timeout"`
	DrainPollInterval    string   `json:"drain_poll_interval"`
	DrainGrace           string   `json:"drain_grace"`
	OutboxMaxMessages    int      `json:"outbox_max_messages"`
	OutboxMaxBytes       int64    `json:"outbox_max_bytes"`
	QueueMaxSpoolMB      uint64   `json:"queue_max_spool_mb"`
	QueueMaxRedeliveries uint64   `json:"queue_max_redeliveries"`
	PartitionCount       uint32   `json:"partition_count"`
	MaxInFlight          int      `json:"max_in_flight"`
	MaxInFlightPerBroker int      `json:"max_in_flight_per_broker"`
	ThroughputCriterion  string   `json:"throughput_criterion"`
}

type redactedResult struct {
	StartedAt time.Time                `json:"started_at"`
	Duration  time.Duration            `json:"duration"`
	Scenarios []redactedScenarioResult `json:"scenarios"`
}

type redactedScenarioResult struct {
	Name        string                         `json:"name"`
	Status      string                         `json:"status"`
	ReasonClass string                         `json:"reason_class,omitempty"`
	Duration    time.Duration                  `json:"duration"`
	Validation  qualification.ValidationResult `json:"validation,omitempty"`
}

type cleanupEvidence struct {
	Queues        string `json:"queues"`
	CloudServices string `json:"cloud_services"`
}

func newQualificationEvidence(generatedAt time.Time, result qualification.Result, cloudCleanup string, runErr error) qualificationEvidence {
	scenarios := make([]redactedScenarioResult, 0, len(result.Scenarios))
	for _, scenario := range result.Scenarios {
		entry := redactedScenarioResult{Name: scenario.Name, Status: scenario.Status, Duration: scenario.Duration, Validation: scenario.Validation}
		if scenario.Status == "FAIL" {
			entry.ReasonClass = "scenario failed"
		} else if scenario.Status == "SKIP" {
			entry.ReasonClass = "scenario skipped"
		}
		scenarios = append(scenarios, entry)
	}
	queueCleanup := result.Cleanup
	if queueCleanup == "" {
		queueCleanup = "not-started"
	}
	status := "PASS"
	if runErr != nil || cloudCleanup != "complete" || queueCleanup != "complete" {
		status = "FAIL"
	}
	return qualificationEvidence{
		SchemaVersion: evidenceSchema,
		GeneratedAt:   generatedAt.UTC(),
		Build:         currentBuildEvidence(),
		Topology: topologyEvidence{
			Datacenter: qualificationDatacenter, BrokerRelease: qualificationRelease,
			Brokers: []topologyBroker{
				{Role: "broker-0", ServiceClass: controlServiceClass, Capacity: "250", Availability: "high-availability"},
				{Role: "broker-a", ServiceClass: dataServiceClass, Capacity: "5k", Availability: "standalone"},
				{Role: "broker-b", ServiceClass: dataServiceClass, Capacity: "5k", Availability: "standalone"},
				{Role: "broker-c", ServiceClass: dataServiceClass, Capacity: "5k", Availability: "standalone"},
			},
			ScalingGroups: []topologyScalingGroup{
				{Label: qualification.FlightGroup, HashContract: routing.FlightOperationsDomain, QueueMode: "partitioned", MembershipTransitions: [][]string{{"broker-a"}, {"broker-a", "broker-b"}, {"broker-a", "broker-b", "broker-c"}, {"broker-a", "broker-b"}}},
				{Label: qualification.BaggageGroup, HashContract: routing.BaggageDomain, QueueMode: "exclusive", MembershipTransitions: [][]string{{"broker-a", "broker-b", "broker-c"}}},
			},
		},
		PerformanceConditions: performanceConditions{
			Keys: qualificationKeys, EventsPerKey: qualificationEventsPerKey, PayloadKinds: []string{"flight", "baggage"},
			ExpectedMessages: qualificationKeys * qualificationEventsPerKey * 2,
			TargetDuration:   qualificationTargetDuration.String(), PublishTimeout: qualificationPublishTimeout.String(), ReceiveTimeout: qualificationReceiveTimeout.String(),
			DrainPollInterval: qualificationDrainPoll.String(), DrainGrace: qualificationDrainGrace.String(),
			OutboxMaxMessages: qualificationOutboxMessages, OutboxMaxBytes: 512 << 20,
			QueueMaxSpoolMB: qualificationQueueSpoolMB, QueueMaxRedeliveries: qualificationMaxRedeliveries,
			PartitionCount: qualificationPartitionCount, MaxInFlight: qualificationMaxInFlight, MaxInFlightPerBroker: qualificationMaxInFlightBroker,
			ThroughputCriterion: "report-only; no pass threshold",
		},
		Result:        redactedResult{StartedAt: result.StartedAt, Duration: result.Duration, Scenarios: scenarios},
		Cleanup:       cleanupEvidence{Queues: queueCleanup, CloudServices: cloudCleanup},
		OverallStatus: status,
	}
}

func currentBuildEvidence() buildEvidence {
	result := buildEvidence{Module: "github.com/solacese/solace-workload-balancer", Version: "unknown", GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH}
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Path != "" {
			result.Module = info.Main.Path
		}
		if info.Main.Version != "" {
			result.Version = info.Main.Version
		}
		for _, dependency := range info.Deps {
			if dependency.Path == "solace.dev/go/messaging" {
				result.Dependencies = map[string]string{dependency.Path: dependency.Version}
				break
			}
		}
	}
	return result
}

func writeQualificationEvidence(path string, evidence qualificationEvidence) error {
	contents, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return fmt.Errorf("encode qualification evidence: %w", err)
	}
	contents = append(contents, '\n')
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create qualification evidence directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".qualification-result-*")
	if err != nil {
		return fmt.Errorf("create temporary qualification evidence: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o644); err != nil {
		temporary.Close()
		return fmt.Errorf("set qualification evidence permissions: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return fmt.Errorf("write qualification evidence: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync qualification evidence: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close qualification evidence: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("replace qualification evidence: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open qualification evidence directory: %w", err)
	}
	syncErr := directoryHandle.Sync()
	closeErr := directoryHandle.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("sync qualification evidence directory: %w", errors.Join(syncErr, closeErr))
	}
	return nil
}
