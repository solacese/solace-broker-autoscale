package qualification

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/semp"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
)

// Run provisions only names in its immutable run plan, executes staged
// qualifications, and always attempts exact reverse-order queue cleanup. It does
// not create or delete Solace Cloud services.
func Run(ctx context.Context, bundles Bundles, namespace string, options Options) (result Result, runErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	options = options.withDefaults()
	if err := options.validate(); err != nil {
		return Result{}, err
	}
	if !options.AllowSkipped && (options.Broker0.SnapshotPublisher == nil || options.Broker0.Browser == nil || options.Broker0.IndependentBrowser == nil || options.Broker0.CommandAck == nil || options.Broker0.Transition == nil) {
		return Result{}, errors.New("qualification: Broker 0 snapshot, non-destructive browse, command/ack, and transition hooks are required")
	}
	started := options.Now()
	digest := sha256.Sum256([]byte(namespace))
	result = Result{NamespaceHash: hex.EncodeToString(digest[:8]), StartedAt: started, Cleanup: "not-started", allowSkipped: options.AllowSkipped}
	plan, err := buildPlan(bundles, namespace, options.RunID, options)
	if err != nil {
		return result, err
	}
	options.RunID = plan.public.RunID
	if configurer, ok := options.Broker0.SnapshotPublisher.(Broker0PlanConfigurer); ok {
		if err := configurer.Configure(ctx, plan.public); err != nil {
			return result, err
		}
	}
	if options.Broker0.Close != nil {
		defer func() { runErr = errors.Join(runErr, options.Broker0.Close()) }()
	}
	factory := options.QueueFactory
	if factory == nil {
		factory = sempFactory{}
	}
	options.report("semp-provision")
	managed, err := provision(ctx, plan, factory)
	if err != nil {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), options.CleanupTimeout)
		cleanupErr := managed.cleanup(cleanupCtx)
		cancelCleanup()
		if cleanupErr == nil {
			result.Cleanup = "complete"
		} else {
			result.Cleanup = "incomplete"
		}
		result.Duration = options.Now().Sub(started)
		return result, errors.Join(err, cleanupErr)
	}
	defer func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), options.CleanupTimeout)
		cleanupErr := managed.cleanup(cleanupCtx)
		cancelCleanup()
		if cleanupErr == nil {
			result.Cleanup = "complete"
		} else {
			result.Cleanup = "incomplete"
		}
		result.Duration = options.Now().Sub(started)
		runErr = errors.Join(runErr, cleanupErr)
	}()

	scenario := func(name string, execute func() (ValidationResult, error)) {
		options.report(name)
		begin := options.Now()
		validation, err := execute()
		entry := ScenarioResult{Name: name, Status: "PASS", Duration: options.Now().Sub(begin), Validation: validation}
		if err != nil {
			entry.Status, entry.Reason = "FAIL", redactedReason(err)
			runErr = errors.Join(runErr, fmt.Errorf("%s: %w", name, err))
		}
		result.Scenarios = append(result.Scenarios, entry)
	}

	scenario(ScenarioPartitioned, func() (ValidationResult, error) {
		return ValidationResult{}, checkPartitionCapability(ctx, managed, plan)
	})
	plane := options.DataPlane
	if plane == nil {
		plane = nativeDataPlane{timeout: options.PublishTimeout, maxInFlightPerBroker: options.MaxInFlightPerBroker, partitionCount: options.PartitionCount}
	}
	connectCtx, cancelConnect := context.WithTimeout(ctx, options.PublishTimeout)
	session, err := plane.Open(connectCtx, bundles, namespace)
	cancelConnect()
	if err != nil {
		for _, name := range []string{ScenarioFence, ScenarioStatic} {
			result.Scenarios = append(result.Scenarios, ScenarioResult{Name: name, Status: "FAIL", Reason: redactedReason(err)})
		}
		runErr = errors.Join(runErr, fmt.Errorf("open data plane: %w", err))
	} else {
		scenario(ScenarioFence, func() (ValidationResult, error) {
			return ValidationResult{}, checkFenceSemantics(ctx, managed, plan, session.AsyncBrokerPublisher())
		})
		staticCtx, cancelStatic := context.WithTimeout(ctx, staticScenarioTimeout(options))
		scenario(ScenarioStatic, func() (ValidationResult, error) {
			validation, trafficErr := runStaticTraffic(staticCtx, plan.public, options, session, factory)
			restoreCtx, cancelRestore := context.WithTimeout(context.WithoutCancel(ctx), options.PublishTimeout)
			defer cancelRestore()
			return validation, errors.Join(trafficErr, restoreStaticEpochs(restoreCtx, plan.public, factory))
		})
		cancelStatic()
		drainCtx, cancelDrain := context.WithTimeout(ctx, drainScenarioTimeout(options))
		scenario(ScenarioDrain, func() (ValidationResult, error) {
			return ValidationResult{}, checkDrain(drainCtx, managed, plan, options)
		})
		cancelDrain()
		if closeErr := session.Close(); closeErr != nil {
			runErr = errors.Join(runErr, closeErr)
		}
	}
	if session == nil {
		drainCtx, cancelDrain := context.WithTimeout(ctx, drainScenarioTimeout(options))
		scenario(ScenarioDrain, func() (ValidationResult, error) {
			return ValidationResult{}, checkDrain(drainCtx, managed, plan, options)
		})
		cancelDrain()
	}
	sidecarCtx, cancelSidecar := context.WithTimeout(ctx, sidecarScenarioTimeout(options))
	sidecarResults, sidecarErr := runSidecarScenarios(sidecarCtx, plan.public, options)
	cancelSidecar()
	result.Scenarios = append(result.Scenarios, sidecarResults...)
	runErr = errors.Join(runErr, sidecarErr)
	return result, runErr
}

func staticScenarioTimeout(options Options) time.Duration {
	return options.TargetDuration + options.ReceiveTimeout + 2*options.PublishTimeout
}

func drainScenarioTimeout(options Options) time.Duration {
	// A Cloud SEMP request may consume ReceiveTimeout before the first complete
	// sample. Reserve a second request budget so a later sample can prove that the
	// all-zero state persisted for the configured grace interval.
	return 2*options.ReceiveTimeout + options.DrainGrace + 2*options.DrainPollInterval
}

func sidecarScenarioTimeout(options Options) time.Duration {
	// Four bootstrap browses plus two independent final-state reads and one later
	// persistence read are all bounded by BrowserTimeout. The command/ack canary
	// additionally performs two bounded durable receives.
	return 7*options.BrowserTimeout + 8*options.ReceiveTimeout + 3*options.DrainGrace + 6*options.PublishTimeout
}

func checkPartitionCapability(ctx context.Context, managed *managedResources, plan resourcePlan) error {
	for _, resource := range plan.queues {
		if resource.ref.Role == "broker-0" || resource.spec.PartitionCount == 0 || !resource.spec.IngressEnabled {
			continue
		}
		if err := managed.clients[resource.ref.Role].EnsureQueue(ctx, resource.spec); err != nil {
			return err
		}
		status, err := managed.clients[resource.ref.Role].MonitorQueue(ctx, resource.ref.MessageVPN, resource.ref.Queue)
		if err != nil {
			return err
		}
		if !status.IngressEnabled {
			return errors.New("qualification: partitioned queue unexpectedly fenced")
		}
	}
	return nil
}

func checkFenceSemantics(ctx context.Context, managed *managedResources, plan resourcePlan, publisher shimPublisher.AsyncBrokerPublisher) error {
	var target queueResource
	for _, resource := range plan.queues {
		if resource.ref.Role == "broker-a" && resource.ref.Kind == "epoch-queue" {
			target = resource
			break
		}
	}
	if target.ref.Queue == "" {
		return errors.New("qualification: fence target unavailable")
	}
	client := managed.clients[target.ref.Role]
	if err := client.FenceQueue(ctx, target.ref.MessageVPN, target.ref.Queue); err != nil {
		return err
	}
	defer client.UnfenceQueue(context.WithoutCancel(ctx), target.ref.MessageVPN, target.ref.Queue)
	status, err := client.MonitorQueue(ctx, target.ref.MessageVPN, target.ref.Queue)
	if err != nil {
		return err
	}
	if status.IngressEnabled {
		return errors.New("qualification: SEMP did not retain disabled ingress")
	}
	if publisher == nil {
		return errors.New("qualification: fence probe publisher unavailable")
	}
	probeDestination := strings.TrimSuffix(target.subscription, ">") + "probe"
	attempt := shimPublisher.PublishAttempt{EventID: "fence-probe", Number: 1, Epoch: 1}
	future, publishErr := publisher.PublishAsync(ctx, target.ref.Role, shimPublisher.BrokerMessage{EventID: attempt.EventID, Payload: []byte("probe"), Topic: "qualification/fence", Properties: map[string]string{shimPublisher.PropertyScalingGroup: FlightGroup, shimPublisher.PropertyBusinessHash: "d343ff7173bbd7145ebd3be6658818295249dd84a84ed4f137b24d73de2279b8", shimPublisher.PropertyHashContract: "flight-operations-v1", shimPublisher.PropertyLibraryVersion: qualificationLibraryVersion}, Destination: probeDestination, Epoch: attempt.Epoch}, attempt)
	if publishErr != nil {
		return fmt.Errorf("qualification: fenced publish failed before broker receipt: %w", publishErr)
	}
	if future == nil {
		return errors.New("qualification: fenced publish returned no receipt future")
	}
	publishCtx, cancelPublish := context.WithTimeout(ctx, 10*time.Second)
	defer cancelPublish()
	result, publishErr := future.Await(publishCtx)
	if publishErr != nil || result.Attempt != attempt || result.Outcome != shimPublisher.OutcomeRejected || result.Err == nil {
		return errors.New("qualification: fenced publish did not receive a definitive negative acknowledgement")
	}
	return nil
}

func checkDrain(ctx context.Context, managed *managedResources, plan resourcePlan, options Options) error {
	type source struct {
		client QueueAPI
		vpn    string
		queue  string
	}
	var active []source
	for _, group := range plan.public.Groups {
		epoch, err := widestEpoch(group)
		if err != nil {
			return err
		}
		for _, broker := range epoch.BrokerIDs {
			active = append(active, source{client: managed.clients[broker], vpn: epoch.MessageVPNs[broker], queue: epoch.Queues[broker]})
		}
	}
	var zeroSince time.Time
	for {
		type result struct {
			status semp.QueueStatus
			err    error
		}
		results := make(chan result, len(active))
		for _, current := range active {
			current := current
			go func() {
				status, err := current.client.MonitorDrain(ctx, current.vpn, current.queue)
				results <- result{status: status, err: err}
			}()
		}
		allDrained := true
		for range active {
			result := <-results
			if result.err != nil {
				return result.err
			}
			if !result.status.Drained() {
				allDrained = false
			}
		}
		if allDrained {
			if zeroSince.IsZero() {
				zeroSince = options.Now()
			}
			if options.Now().Sub(zeroSince) >= options.DrainGrace {
				return nil
			}
		} else {
			zeroSince = time.Time{}
		}
		timer := time.NewTimer(options.DrainPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func runSidecarScenarios(ctx context.Context, plan ProvisionedPlan, options Options) ([]ScenarioResult, error) {
	if options.Broker0.Browser == nil {
		return []ScenarioResult{
			{Name: ScenarioBroker0Browse, Status: "SKIP", Reason: "non-destructive Broker 0 browser sidecar not configured"},
			{Name: ScenarioBroker0CommandAck, Status: "SKIP", Reason: "Broker 0 command/ack probe not configured"},
			{Name: ScenarioTransition, Status: "SKIP", Reason: "Broker 0 transition sidecar not configured"},
		}, nil
	}
	started := options.Now()
	browse := ScenarioResult{Name: ScenarioBroker0Browse, Status: "PASS"}
	var scenarioErr error
	for _, group := range plan.Groups {
		if options.Broker0.SnapshotPublisher == nil {
			scenarioErr = errors.New("qualification: Broker 0 snapshot publisher sidecar not configured")
			browse.Status, browse.Reason = "FAIL", redactedReason(scenarioErr)
			break
		}
		snapshot := activeSnapshot(group)
		snapshot.Namespace = plan.Namespace
		if err := options.Broker0.SnapshotPublisher.PublishSnapshot(ctx, snapshot); err != nil {
			scenarioErr = err
			browse.Status, browse.Reason = "FAIL", redactedReason(err)
			break
		}
		messages, err := options.Broker0.Browser.Browse(ctx, group.Group)
		if err != nil {
			scenarioErr = err
			browse.Status, browse.Reason = "FAIL", redactedReason(err)
			break
		}
		if err := validateBrowse(group.Group, messages); err != nil {
			scenarioErr = err
			browse.Status, browse.Reason = "FAIL", redactedReason(err)
			break
		}
		if options.Broker0.IndependentBrowser == nil {
			scenarioErr = errors.New("qualification: independent Broker 0 browser not configured")
			browse.Status, browse.Reason = "FAIL", redactedReason(scenarioErr)
			break
		}
		independent, err := options.Broker0.IndependentBrowser.Browse(ctx, group.Group)
		if err != nil {
			scenarioErr = err
			browse.Status, browse.Reason = "FAIL", redactedReason(err)
			break
		}
		if err := validateIndependentBrowse(group.Group, messages, independent); err != nil {
			scenarioErr = err
			browse.Status, browse.Reason = "FAIL", redactedReason(err)
			break
		}
	}
	browse.Duration = options.Now().Sub(started)
	commandAck := ScenarioResult{Name: ScenarioBroker0CommandAck, Status: "SKIP", Reason: "Broker 0 command/ack probe not configured"}
	var commandAckErr error
	if options.Broker0.CommandAck != nil {
		probeStarted := options.Now()
		commandAckErr = options.Broker0.CommandAck.ProveCommandAcknowledgement(ctx, plan)
		commandAck = ScenarioResult{Name: ScenarioBroker0CommandAck, Status: "PASS", Duration: options.Now().Sub(probeStarted)}
		if commandAckErr != nil {
			commandAck.Status, commandAck.Reason = "FAIL", redactedReason(commandAckErr)
		}
	}
	if options.Broker0.Transition == nil {
		return []ScenarioResult{browse, commandAck, {Name: ScenarioTransition, Status: "SKIP", Reason: "Broker 0 transition sidecar not configured"}}, errors.Join(scenarioErr, commandAckErr)
	}
	transitionStarted := options.Now()
	err := options.Broker0.Transition.RunTransition(ctx, plan, ObserverFunc(func(DeliveryObservation) error { return nil }))
	transition := ScenarioResult{Name: ScenarioTransition, Status: "PASS", Duration: options.Now().Sub(transitionStarted)}
	if err != nil {
		transition.Status, transition.Reason = "FAIL", redactedReason(err)
	}
	return []ScenarioResult{browse, commandAck, transition}, errors.Join(scenarioErr, commandAckErr, err)
}

func validateIndependentBrowse(group string, first, second []broker0.BrowsedMessage) error {
	if err := validateBrowse(group, second); err != nil {
		return err
	}
	for _, left := range first {
		if left.Kind != broker0.KindMembershipSnapshot {
			continue
		}
		for _, right := range second {
			if right.Kind == left.Kind && right.OperationID == left.OperationID && string(right.Payload) == string(left.Payload) {
				return nil
			}
		}
	}
	return errors.New("qualification: independent browsers did not observe the same retained snapshot")
}

func validateBrowse(group string, messages []broker0.BrowsedMessage) error {
	if len(messages) == 0 {
		return errors.New("qualification: browse returned no retained snapshot")
	}
	for _, message := range messages {
		if message.Kind != broker0.KindMembershipSnapshot {
			continue
		}
		var snapshot struct {
			ScalingGroup string `json:"scaling_group"`
		}
		if json.Unmarshal(message.Payload, &snapshot) == nil && snapshot.ScalingGroup == group {
			return nil
		}
	}
	return errors.New("qualification: browse did not return the requested group snapshot")
}

func redactedReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "scenario deadline elapsed"
	}
	if errors.Is(err, context.Canceled) {
		return "scenario canceled"
	}
	raw := fmt.Sprint(err)
	text := strings.ToLower(raw)
	classifications := []struct{ fragment, label string }{
		{"jcsmp browser", "JCSMP browser failed"},
		{"trust store", "TLS trust-store setup failed"},
		{"certificate", "TLS certificate validation failed"},
		{"ensure", "SEMP resource provisioning failed"},
		{"subscription", "SEMP subscription failed"},
		{"list flows", "SEMP flow telemetry failed"},
		{"fenced publish", "stale-publisher fence did not NACK"},
		{"backpressure", "publisher backpressure prevented target rate"},
		{"receive deadline", "subscriber receive deadline elapsed"},
		{"traffic invariants", "delivery correctness invariant failed"},
		{"publisher made no progress", "publisher made no progress"},
		{"acknowledgement became uncertain", "publisher acknowledgement became uncertain"},
		{"invalid inbound", "subscriber rejected routing metadata"},
		{"transition", "membership transition failed"},
		{"connect solace", "broker connection failed"},
	}
	for _, classification := range classifications {
		if strings.Contains(text, classification.fragment) {
			return classification.label
		}
	}
	if len(raw) <= 320 && !strings.Contains(text, "password") && !strings.Contains(text, "token") && !strings.Contains(text, "authorization") && !strings.Contains(text, "secret") && !strings.Contains(text, "http://") && !strings.Contains(text, "https://") && !strings.Contains(text, "tcps://") {
		return raw
	}
	return "operation failed; inspect private run logs"
}
