package broker0

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestJCSMPBrowserBrowseUsesExactQueueAndEnvironmentCredentials(t *testing.T) {
	var commandName string
	var commandArgs []string
	browser := newTestJCSMPBrowser(t, "success", MembershipQueueResolverFunc(func(_ context.Context, group string) (string, error) {
		if group != "flight-operations" {
			t.Fatalf("resolver group = %q", group)
		}
		return "acme.ctl.flight-operations.membership", nil
	}), func(name string, args ...string) {
		commandName = name
		commandArgs = append([]string(nil), args...)
	}, 0)

	if commandName != "test-java" {
		t.Fatalf("command name = %q", commandName)
	}
	wantArgs := []string{"-jar", "/private/lvq-browser.jar", "acme"}
	if !slices.Equal(commandArgs, wantArgs) {
		t.Fatalf("command args = %q, want %q", commandArgs, wantArgs)
	}
	for _, argument := range commandArgs {
		if strings.Contains(argument, "browser-user") || strings.Contains(argument, "browser-password") {
			t.Fatalf("credential leaked into command argument %q", argument)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	messages, err := browser.Browse(ctx, "flight-operations")
	if err != nil {
		t.Fatalf("Browse() error = %v", err)
	}
	if len(messages) != 1 || messages[0].Kind != KindMembershipSnapshot || messages[0].OperationID != "operation-42" || string(messages[0].Payload) != `{"revision":42}` {
		t.Fatalf("Browse() = %+v", messages)
	}
}

func TestJCSMPBrowserSerializesRequests(t *testing.T) {
	firstResolved := make(chan struct{})
	secondResolved := make(chan struct{})
	var calls atomic.Int32
	browser := newTestJCSMPBrowser(t, "delayed", MembershipQueueResolverFunc(func(_ context.Context, _ string) (string, error) {
		switch calls.Add(1) {
		case 1:
			close(firstResolved)
		case 2:
			close(secondResolved)
		}
		return "acme.ctl.group.membership", nil
	}), nil, 0)

	results := make(chan error, 2)
	go func() {
		_, err := browser.Browse(context.Background(), "group")
		results <- err
	}()
	select {
	case <-firstResolved:
	case <-time.After(time.Second):
		t.Fatal("first browse did not resolve its queue")
	}
	go func() {
		_, err := browser.Browse(context.Background(), "group")
		results <- err
	}()
	select {
	case <-secondResolved:
		t.Fatal("second request reached resolver while first request was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("Browse() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("serialized browse timed out")
		}
	}
}

func TestJCSMPBrowserContextCancellationAllowsRestart(t *testing.T) {
	var starts atomic.Int32
	browser := newTestJCSMPBrowserWithOptions(t, []string{"hang", "success"}, testQueueResolver(), func(string, ...string) {
		starts.Add(1)
	}, func(options *JCSMPBrowserOptions) {
		options.RestartBackoff = time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := browser.Browse(ctx, "group"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Browse() error = %v, want deadline exceeded", err)
	}

	restartCtx, restartCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer restartCancel()
	messages, err := browser.Browse(restartCtx, "group")
	if err != nil || len(messages) != 1 {
		t.Fatalf("Browse() after cancellation = %+v, %v", messages, err)
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("helper starts = %d, want 2", got)
	}
}

func TestJCSMPBrowserRejectsMalformedAndOversizedResponsesThenRestarts(t *testing.T) {
	tests := []struct {
		name        string
		scenario    string
		maxResponse int
	}{
		{name: "malformed", scenario: "malformed"},
		{name: "unknown field", scenario: "unknown"},
		{name: "invalid base64", scenario: "base64"},
		{name: "oversized", scenario: "oversized", maxResponse: 200},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var starts atomic.Int32
			browser := newTestJCSMPBrowserWithOptions(t, []string{test.scenario, "success"}, testQueueResolver(), func(string, ...string) {
				starts.Add(1)
			}, func(options *JCSMPBrowserOptions) {
				options.MaxResponseBytes = test.maxResponse
				options.RestartBackoff = time.Millisecond
			})
			if _, err := browser.Browse(context.Background(), "group"); !errors.Is(err, ErrJCSMPBrowserFailed) {
				t.Fatalf("Browse() error = %v, want helper failed", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			messages, err := browser.Browse(ctx, "group")
			if err != nil || len(messages) != 1 {
				t.Fatalf("Browse() after malformed response = %+v, %v", messages, err)
			}
			if got := starts.Load(); got != 2 {
				t.Fatalf("helper starts = %d, want 2", got)
			}
		})
	}
}

func TestDecodeJCSMPEmptyLVQReturnsTypedAbsence(t *testing.T) {
	_, err, fatal := decodeJCSMPResponse([]byte(`{"ok":false,"error":"membership LVQ is empty"}`))
	if fatal || !errors.Is(err, ErrNoAuthoritativeState) {
		t.Fatalf("decode error = %v, fatal=%t", err, fatal)
	}
}

func TestJCSMPBrowserHelperErrorDoesNotDesynchronizeStream(t *testing.T) {
	browser := newTestJCSMPBrowser(t, "reject-once", testQueueResolver(), nil, 0)
	if _, err := browser.Browse(context.Background(), "group"); err == nil || errors.Is(err, ErrJCSMPBrowserFailed) {
		t.Fatalf("first Browse() error = %v, want non-fatal rejection", err)
	}
	messages, err := browser.Browse(context.Background(), "group")
	if err != nil || len(messages) != 1 {
		t.Fatalf("second Browse() = %+v, %v", messages, err)
	}
}

func TestJCSMPBrowserNextBrowseRestartsAfterProcessExit(t *testing.T) {
	var starts atomic.Int32
	browser := newTestJCSMPBrowserWithOptions(t, []string{"exit", "success"}, testQueueResolver(), func(string, ...string) {
		starts.Add(1)
	}, func(options *JCSMPBrowserOptions) {
		options.RestartBackoff = time.Millisecond
	})
	waitForTestJCSMPHelperExit(t, browser)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	messages, err := browser.Browse(ctx, "group")
	if err != nil || len(messages) != 1 {
		t.Fatalf("Browse() after process exit = %+v, %v", messages, err)
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("helper starts = %d, want 2", got)
	}
}

func TestJCSMPBrowserRestartBackoffHonorsContext(t *testing.T) {
	var starts atomic.Int32
	browser := newTestJCSMPBrowserWithOptions(t, []string{"exit", "success"}, testQueueResolver(), func(string, ...string) {
		starts.Add(1)
	}, func(options *JCSMPBrowserOptions) {
		options.RestartBackoff = time.Second
	})
	waitForTestJCSMPHelperExit(t, browser)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := browser.Browse(ctx, "group"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Browse() during restart backoff error = %v, want deadline exceeded", err)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("helper starts = %d during backoff, want 1", got)
	}
}

func TestJCSMPBrowserConcurrentBrowseStartsOneReplacementHelper(t *testing.T) {
	var starts atomic.Int32
	browser := newTestJCSMPBrowserWithOptions(t, []string{"exit", "delayed"}, testQueueResolver(), func(string, ...string) {
		starts.Add(1)
	}, func(options *JCSMPBrowserOptions) {
		options.RestartBackoff = time.Millisecond
	})
	waitForTestJCSMPHelperExit(t, browser)

	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := browser.Browse(ctx, "group")
			results <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Browse() error = %v", err)
		}
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("helper starts = %d, want initial helper plus one replacement", got)
	}
}

func TestJCSMPBrowserFailureRedactsConfiguredSecrets(t *testing.T) {
	options := testJCSMPOptions()
	browser := newTestJCSMPBrowserWithOptions(t, []string{"exit-with-secrets"}, testQueueResolver(), nil, nil)
	waitForTestJCSMPHelperExit(t, browser)

	browser.stateMu.Lock()
	failure := browser.fatalErr
	browser.stateMu.Unlock()
	if failure == nil {
		t.Fatal("helper failure is nil")
	}
	got := failure.Error()
	for name, secret := range jcsmpEnvironmentValues(options) {
		if secret != "" && strings.Contains(got, secret) {
			t.Fatalf("helper failure contains configured value %s", name)
		}
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Fatal("helper failure does not contain a redaction marker")
	}
}

func TestJCSMPBrowserClosePreventsRestart(t *testing.T) {
	var starts atomic.Int32
	browser := newTestJCSMPBrowserWithOptions(t, []string{"exit", "success"}, testQueueResolver(), func(string, ...string) {
		starts.Add(1)
	}, func(options *JCSMPBrowserOptions) {
		options.RestartBackoff = time.Millisecond
	})
	waitForTestJCSMPHelperExit(t, browser)

	if err := browser.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := browser.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := browser.Browse(context.Background(), "group"); !errors.Is(err, ErrJCSMPBrowserClosed) {
		t.Fatalf("Browse() after Close() error = %v, want closed", err)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("helper starts = %d after Close, want 1", got)
	}
}

func TestJCSMPBrowserRejectsQueueOutsideNamespaceBeforeWriting(t *testing.T) {
	browser := newTestJCSMPBrowser(t, "success", MembershipQueueResolverFunc(func(context.Context, string) (string, error) {
		return "other.ctl.group.membership", nil
	}), nil, 0)
	if _, err := browser.Browse(context.Background(), "group"); err == nil || !strings.Contains(err.Error(), "outside the configured namespace") {
		t.Fatalf("Browse() error = %v", err)
	}
	// A local resolver error does not corrupt the protocol; a subsequent exact
	// queue can still use the same process.
	browser.resolver = testQueueResolver()
	if _, err := browser.Browse(context.Background(), "group"); err != nil {
		t.Fatalf("Browse() after local validation error = %v", err)
	}
}

func TestJCSMPEnvironmentReplacesInheritedCredentials(t *testing.T) {
	environment := jcsmpEnvironment([]string{
		"PATH=/bin",
		"SWLB_CONTROL_BROWSER_PASSWORD=old-secret",
		"SWLB_CONTROL_HOST=old-host",
	}, testJCSMPOptions())
	joined := strings.Join(environment, "\n")
	if strings.Contains(joined, "old-secret") || strings.Contains(joined, "old-host") {
		t.Fatalf("environment retained inherited credential: %q", joined)
	}
	if !strings.Contains(joined, "SWLB_CONTROL_BROWSER_PASSWORD=browser-password") || !strings.Contains(joined, "SWLB_CONTROL_HOST=tcp://broker.example:55555") || !strings.Contains(joined, "SWLB_CONTROL_TRUST_STORE=/private/truststore.p12") {
		t.Fatalf("environment does not contain configured values: %q", joined)
	}
}

func newTestJCSMPBrowser(t *testing.T, scenario string, resolver MembershipQueueResolver, observe func(string, ...string), maxResponse int) *JCSMPBrowser {
	t.Helper()
	return newTestJCSMPBrowserWithOptions(t, []string{scenario}, resolver, observe, func(options *JCSMPBrowserOptions) {
		options.MaxResponseBytes = maxResponse
	})
}

func newTestJCSMPBrowserWithOptions(t *testing.T, scenarios []string, resolver MembershipQueueResolver, observe func(string, ...string), configure func(*JCSMPBrowserOptions)) *JCSMPBrowser {
	t.Helper()
	options := testJCSMPOptions()
	if configure != nil {
		configure(&options)
	}
	var starts atomic.Int32
	browser, err := newJCSMPBrowser(resolver, options, func(name string, args ...string) *exec.Cmd {
		if observe != nil {
			observe(name, args...)
		}
		start := int(starts.Add(1)) - 1
		if start >= len(scenarios) {
			start = len(scenarios) - 1
		}
		return exec.Command(os.Args[0], "-test.run=^TestJCSMPBrowserHelperProcess$", "--", scenarios[start])
	})
	if err != nil {
		t.Fatalf("newJCSMPBrowser() error = %v", err)
	}
	t.Cleanup(func() {
		if err := browser.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return browser
}

func waitForTestJCSMPHelperExit(t *testing.T, browser *JCSMPBrowser) {
	t.Helper()
	select {
	case <-browser.done:
	case <-time.After(5 * time.Second):
		t.Fatal("helper did not exit")
	}
}

func testJCSMPOptions() JCSMPBrowserOptions {
	return JCSMPBrowserOptions{
		JavaExecutable: "test-java",
		JarPath:        "/private/lvq-browser.jar",
		Namespace:      "acme",
		Host:           "tcp://broker.example:55555",
		VPN:            "control-vpn",
		Username:       "browser-user",
		Password:       "browser-password",
		TrustStorePath: "/private/truststore.p12",
		Timeout:        500 * time.Millisecond,
	}
}

func testQueueResolver() MembershipQueueResolver {
	return MembershipQueueResolverFunc(func(context.Context, string) (string, error) {
		return "acme.ctl.group.membership", nil
	})
}

func TestJCSMPBrowserHelperProcess(t *testing.T) {
	separator := slices.Index(os.Args, "--")
	if separator < 0 || separator+1 >= len(os.Args) {
		return
	}
	scenario := os.Args[separator+1]
	switch scenario {
	case "exit":
		return
	case "exit-with-secrets":
		fmt.Fprintf(os.Stderr, "%s %s %s %s %s\n",
			os.Getenv("SWLB_CONTROL_HOST"),
			os.Getenv("SWLB_CONTROL_VPN"),
			os.Getenv("SWLB_CONTROL_BROWSER_USERNAME"),
			os.Getenv("SWLB_CONTROL_BROWSER_PASSWORD"),
			os.Getenv("SWLB_CONTROL_TRUST_STORE"))
		return
	}
	input := bufio.NewScanner(os.Stdin)
	requestNumber := 0
	for input.Scan() {
		requestNumber++
		var request jcsmpBrowseRequest
		if err := json.Unmarshal(input.Bytes(), &request); err != nil {
			fmt.Printf("{\"ok\":false,\"error\":\"bad test request\"}\n")
			continue
		}
		environmentOK := os.Getenv("SWLB_CONTROL_HOST") == "tcp://broker.example:55555" &&
			os.Getenv("SWLB_CONTROL_VPN") == "control-vpn" &&
			os.Getenv("SWLB_CONTROL_BROWSER_USERNAME") == "browser-user" &&
			os.Getenv("SWLB_CONTROL_BROWSER_PASSWORD") == "browser-password"
		queueOK := request.Queue == "acme.ctl.flight-operations.membership" || request.Queue == "acme.ctl.group.membership"
		if !environmentOK || request.Operation != "browse_latest" || !queueOK || request.TimeoutMS < 1 || request.TimeoutMS > 500 {
			fmt.Printf("{\"ok\":false,\"error\":\"bad test request (env=%t operation=%s queue=%s timeout=%d)\"}\n",
				environmentOK, request.Operation, request.Queue, request.TimeoutMS)
			continue
		}
		switch scenario {
		case "hang":
			time.Sleep(10 * time.Minute)
		case "delayed":
			time.Sleep(100 * time.Millisecond)
			writeTestJCSMPSuccess()
		case "malformed":
			fmt.Println("not-json")
		case "unknown":
			fmt.Println(`{"ok":true,"messages":[],"unexpected":true}`)
		case "base64":
			fmt.Println(`{"ok":true,"messages":[{"payloadBase64":"!!!!","applicationMessageId":"operation-42"}]}`)
		case "oversized":
			fmt.Printf("{\"ok\":true,\"messages\":[],\"padding\":\"%s\"}\n", strings.Repeat("x", 256))
		case "reject-once":
			if requestNumber == 1 {
				fmt.Println(`{"ok":false,"error":"temporary browse failure"}`)
			} else {
				writeTestJCSMPSuccess()
			}
		default:
			writeTestJCSMPSuccess()
		}
	}
}

func writeTestJCSMPSuccess() {
	fmt.Println(`{"ok":true,"messages":[{"payloadBase64":"eyJyZXZpc2lvbiI6NDJ9","applicationMessageId":"operation-42","timestamp":"2026-09-25T12:00:00Z"}]}`)
}
