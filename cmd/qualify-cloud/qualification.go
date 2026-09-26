package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	cloudapi "github.com/solacese/solace-workload-balancer/cloud"
	"github.com/solacese/solace-workload-balancer/qualification"
)

const (
	qualificationKeys              = 4096
	qualificationEventsPerKey      = 8
	qualificationPublishTimeout    = 10 * time.Second
	qualificationReceiveTimeout    = 30 * time.Second
	qualificationDrainPoll         = 250 * time.Millisecond
	qualificationDrainGrace        = time.Second
	qualificationTargetDuration    = time.Minute
	qualificationOutboxMessages    = 300_000
	qualificationQueueSpoolMB      = 256
	qualificationMaxRedeliveries   = 5
	qualificationPartitionCount    = 8
	qualificationMaxInFlight       = 6144
	qualificationMaxInFlightBroker = 2048
)

// realQualificationHook resolves the exact four journaled IDs to connection
// bundles and invokes the strict qualification runner. execute blocks this hook
// before provisioning until all required Broker 0 capabilities can be built.
func realQualificationHook(ctx context.Context, jwt, baseURL string, plan Plan, resources []ResourceRecord, runTimeout, cleanupTimeout time.Duration) (qualification.Result, error) {
	bundles, err := resolveBundles(ctx, jwt, baseURL, resources)
	if err != nil {
		return qualification.Result{}, newQualificationStageError("connection-bundle-resolution", "not-started", err)
	}
	namespace, err := qualificationNamespace(plan)
	if err != nil {
		return qualification.Result{}, newQualificationStageError("namespace-validation", "not-started", err)
	}
	options := qualificationOptions(runTimeout, cleanupTimeout)
	if err := qualification.ConfigureDefaultHooks(&options); err != nil {
		return qualification.Result{}, newQualificationStageError("broker0-helper-setup", "not-started", err)
	}
	result, err := qualification.Run(ctx, bundles, namespace, options)
	fmt.Fprint(os.Stderr, qualificationSummary(result))
	if err != nil || !result.Passed() {
		failed := make([]string, 0, len(result.Scenarios))
		for _, scenario := range result.Scenarios {
			if scenario.Status != "PASS" {
				failed = append(failed, scenario.Name+"="+scenario.Status)
			}
		}
		stage := "setup"
		if len(failed) != 0 {
			stage = strings.Join(failed, ",")
		}
		return result, newQualificationStageError(stage, result.Cleanup, scenarioFailureCause(result, err))
	}
	return result, nil
}

func scenarioFailureCause(result qualification.Result, fallback error) error {
	failures := make([]string, 0, len(result.Scenarios))
	for _, scenario := range result.Scenarios {
		if scenario.Status != "PASS" && scenario.Reason != "" {
			failures = append(failures, scenario.Name+": "+scenario.Reason)
		}
	}
	if len(failures) != 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return fallback
}

func qualificationSummary(result qualification.Result) string {
	var summary strings.Builder
	for _, scenario := range result.Scenarios {
		fmt.Fprintf(&summary, "qualification result: %s=%s duration=%s", scenario.Name, scenario.Status, scenario.Duration.Round(time.Millisecond))
		if scenario.Name == qualification.ScenarioStatic {
			validation := scenario.Validation
			fmt.Fprintf(
				&summary,
				" expected=%d delivered=%d unique=%d missing=%d duplicates=%d out_of_order=%d unexpected_broker=%d accepted=%d positive_acks=%d outbox_high_water_messages=%d outbox_high_water_bytes=%d throughput=%.2f/s durable_accept_p50=%s durable_accept_p95=%s durable_accept_p99=%s durable_accept_max=%s broker_positive_ack_p50=%s broker_positive_ack_p95=%s broker_positive_ack_p99=%s broker_positive_ack_max=%s end_to_end_consumed_p50=%s end_to_end_consumed_p95=%s end_to_end_consumed_p99=%s end_to_end_consumed_max=%s",
				validation.Expected,
				validation.Deliveries,
				validation.UniqueEventIDs,
				validation.Missing,
				validation.Duplicates,
				validation.OutOfOrder,
				validation.UnexpectedBroker,
				validation.Accepted,
				validation.PositiveACKs,
				validation.OutboxHighWaterMessages,
				validation.OutboxHighWaterBytes,
				validation.ThroughputPerSecond,
				validation.DurableAcceptanceLatency.P50.Round(time.Microsecond),
				validation.DurableAcceptanceLatency.P95.Round(time.Microsecond),
				validation.DurableAcceptanceLatency.P99.Round(time.Microsecond),
				validation.DurableAcceptanceLatency.Max.Round(time.Microsecond),
				validation.BrokerPositiveACKLatency.P50.Round(time.Microsecond),
				validation.BrokerPositiveACKLatency.P95.Round(time.Microsecond),
				validation.BrokerPositiveACKLatency.P99.Round(time.Microsecond),
				validation.BrokerPositiveACKLatency.Max.Round(time.Microsecond),
				validation.EndToEndConsumedLatency.P50.Round(time.Microsecond),
				validation.EndToEndConsumedLatency.P95.Round(time.Microsecond),
				validation.EndToEndConsumedLatency.P99.Round(time.Microsecond),
				validation.EndToEndConsumedLatency.Max.Round(time.Microsecond),
			)
			if len(validation.BrokerDistribution) != 0 {
				roles := make([]string, 0, len(validation.BrokerDistribution))
				for role := range validation.BrokerDistribution {
					roles = append(roles, role)
				}
				sort.Strings(roles)
				summary.WriteString(" broker_distribution=")
				for index, role := range roles {
					if index > 0 {
						summary.WriteByte(',')
					}
					fmt.Fprintf(&summary, "%s:%d", role, validation.BrokerDistribution[role])
				}
			}
		}
		if scenario.Reason != "" {
			fmt.Fprintf(&summary, " reason=%s", scenario.Reason)
		}
		summary.WriteByte('\n')
	}
	fmt.Fprintf(&summary, "qualification result: cleanup=%s duration=%s\n", result.Cleanup, result.Duration.Round(time.Millisecond))
	return summary.String()
}

type qualificationStageError struct {
	stage   string
	cleanup string
	cause   error
}

func newQualificationStageError(stage, cleanup string, cause error) *qualificationStageError {
	return &qualificationStageError{stage: stage, cleanup: cleanup, cause: cause}
}

func (e *qualificationStageError) Error() string {
	return "qualification stage failed"
}

func (e *qualificationStageError) Unwrap() error { return e.cause }

func (e *qualificationStageError) SafeMessage() string {
	detail := safeFailureClass(e.cause)
	if detail != "" {
		detail = ", " + detail
	}
	return fmt.Sprintf("qualification failed at %s%s (queue cleanup: %s)", e.stage, detail, e.cleanup)
}

func safeFailureClass(err error) string {
	type roleError interface{ Role() string }
	var scoped roleError
	role := ""
	if errors.As(err, &scoped) {
		role = " for " + scoped.Role()
	}
	var httpError *cloudapi.HTTPError
	if errors.As(err, &httpError) {
		return fmt.Sprintf("cloud HTTP %d%s: %s", httpError.StatusCode, role, httpError.Body)
	}
	var operationError *cloudapi.OperationFailure
	if errors.As(err, &operationError) {
		return operationError.Error() + role
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "qualification deadline elapsed" + role
	}
	if errors.Is(err, context.Canceled) {
		return "qualification canceled" + role
	}
	raw := fmt.Sprint(err)
	text := strings.ToLower(raw)
	if marker := strings.LastIndex(raw, " failed: "); marker >= 0 {
		detail := raw[marker+len(" failed: "):]
		if end := strings.Index(detail, " (error ID "); end >= 0 {
			detail = detail[:end]
		}
		detail = strings.TrimSpace(detail)
		if detail != "" && len(detail) <= 240 {
			return "cloud operation failed" + role + ": " + detail
		}
	}
	if strings.Contains(text, "identity") && len(raw) <= 320 {
		return raw
	}
	if (strings.Contains(text, "failed") || strings.Contains(text, "deadline") || strings.Contains(text, "not met")) && len(raw) <= 500 {
		return raw
	}
	for fragment, label := range map[string]string{
		"ensure broker-0":        "Broker 0 lifecycle failed",
		"ensure broker-a":        "Broker A lifecycle failed",
		"ensure broker-b":        "Broker B lifecycle failed",
		"ensure broker-c":        "Broker C lifecycle failed",
		"creation state":         "service creation readiness check failed",
		"admin state":            "service administration readiness check failed",
		"endpoint":               "service endpoint readiness check failed",
		"operation":              "cloud operation failed or timed out",
		"identity fields differ": raw,
		"identity":               "created service identity check failed",
		"uncertain":              "cloud create outcome remained uncertain",
		"journal":                "lifecycle journal operation failed",
		"deadline":               "qualification deadline elapsed",
	} {
		if strings.Contains(text, fragment) {
			return label
		}
	}
	return "non-HTTP lifecycle failure"
}

func qualificationCapabilityCheck() error {
	return qualification.CheckDefaultCapabilities(qualificationOptions(defaultRunTimeout, defaultCleanupTimeout))
}

func qualificationOptions(runTimeout, cleanupTimeout time.Duration) qualification.Options {
	return qualification.Options{
		Keys:                 qualificationKeys,
		EventsPerKey:         qualificationEventsPerKey,
		PublishTimeout:       qualificationPublishTimeout,
		ReceiveTimeout:       qualificationReceiveTimeout,
		DrainPollInterval:    qualificationDrainPoll,
		DrainGrace:           qualificationDrainGrace,
		CleanupTimeout:       min(cleanupTimeout, qualificationCleanupLimit),
		TargetDuration:       min(qualificationTargetDuration, runTimeout),
		OutboxMaxMessages:    qualificationOutboxMessages,
		OutboxMaxBytes:       512 << 20,
		QueueMaxSpoolMB:      qualificationQueueSpoolMB,
		QueueMaxRedeliveries: qualificationMaxRedeliveries,
		PartitionCount:       qualificationPartitionCount,
		MaxInFlight:          qualificationMaxInFlight,
		MaxInFlightPerBroker: qualificationMaxInFlightBroker,
		AllowSkipped:         false,
		Progress: func(stage string) {
			fmt.Fprintf(os.Stderr, "qualification stage: %s\n", stage)
		},
	}
}

const qualificationCleanupLimit = 30 * time.Second

func qualificationNamespace(plan Plan) (string, error) {
	value := "swlb-q-" + strings.ToLower(plan.ID)
	if len(value) > 63 {
		value = value[:63]
	}
	if value == "" || strings.HasSuffix(value, "-") {
		return "", errors.New("qualification namespace is invalid")
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return "", errors.New("qualification namespace is invalid")
		}
	}
	return value, nil
}

func resolveBundles(ctx context.Context, jwt, baseURL string, resources []ResourceRecord) (qualification.Bundles, error) {
	if len(resources) != 4 {
		return qualification.Bundles{}, fmt.Errorf("qualification requires exactly four service IDs")
	}
	client, err := cloudapi.NewClient(jwt, cloudapi.WithBaseURL(baseURL))
	if err != nil {
		return qualification.Bundles{}, err
	}
	byRole := make(map[string]cloudapi.ConnectionBundle, 4)
	seenIDs := make(map[string]struct{}, 4)
	for _, resource := range resources {
		if resource.ID == "" {
			return qualification.Bundles{}, errors.New("qualification resource ID is empty")
		}
		if _, exists := seenIDs[resource.ID]; exists {
			return qualification.Bundles{}, errors.New("qualification service IDs must be distinct")
		}
		if _, exists := byRole[resource.Role]; exists {
			return qualification.Bundles{}, errors.New("qualification service roles must be distinct")
		}
		bundle, err := client.ConnectionBundle(ctx, resource.ID)
		if err != nil {
			return qualification.Bundles{}, fmt.Errorf("resolve connection bundle for %s: %w", resource.Role, err)
		}
		if bundle.ServiceID != resource.ID {
			return qualification.Bundles{}, errors.New("cloud returned a different service ID")
		}
		byRole[resource.Role] = bundle
		seenIDs[resource.ID] = struct{}{}
	}
	roles := []string{"broker-0", "broker-a", "broker-b", "broker-c"}
	for _, role := range roles {
		if _, exists := byRole[role]; !exists {
			return qualification.Bundles{}, fmt.Errorf("qualification resource role %s is missing", role)
		}
	}
	return qualification.Bundles{Control: byRole["broker-0"], Data: [3]cloudapi.ConnectionBundle{byRole["broker-a"], byRole["broker-b"], byRole["broker-c"]}}, nil
}
