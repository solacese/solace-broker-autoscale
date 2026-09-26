package broker0

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultJCSMPBrowseTimeout  = 10 * time.Second
	maxJCSMPBrowseTimeout      = 60 * time.Second
	maxJCSMPRequestBytes       = 16 * 1024
	maxJCSMPResponseBytes      = 24 * 1024 * 1024
	maxJCSMPMessages           = 16
	maxJCSMPPayloadBytes       = 1024 * 1024
	defaultJCSMPRestartBackoff = 100 * time.Millisecond
	maxJCSMPRestartBackoff     = 30 * time.Second
)

var (
	// ErrJCSMPBrowserClosed indicates that the helper has been explicitly closed.
	ErrJCSMPBrowserClosed = errors.New("broker0: JCSMP browser is closed")
	// ErrJCSMPBrowserFailed indicates that the current helper protocol or process
	// can no longer be trusted. A later Browse may restart the helper.
	ErrJCSMPBrowserFailed = errors.New("broker0: JCSMP browser helper failed")
)

// MembershipQueueResolver maps one scaling group to the exact, already
// provisioned membership LVQ. Implementations must not derive or provision a
// queue as a side effect.
type MembershipQueueResolver interface {
	MembershipQueue(context.Context, string) (string, error)
}

// MembershipQueueResolverFunc adapts a function to MembershipQueueResolver.
type MembershipQueueResolverFunc func(context.Context, string) (string, error)

func (f MembershipQueueResolverFunc) MembershipQueue(ctx context.Context, group string) (string, error) {
	return f(ctx, group)
}

// JCSMPBrowserOptions configures the long-running official-JCSMP helper. Host,
// VPN, username, and password are passed to the child only through its
// environment; credentials are never included in arguments or protocol data.
type JCSMPBrowserOptions struct {
	JavaExecutable   string
	JarPath          string
	Namespace        string
	Host             string
	VPN              string
	Username         string
	Password         string
	TrustStorePath   string
	Timeout          time.Duration
	MaxResponseBytes int
	RestartBackoff   time.Duration
}

// JCSMPBrowser supervises one long-running tools/lvq-browser process. Requests
// are serialized because the helper protocol has no request identifiers.
type JCSMPBrowser struct {
	resolver       MembershipQueueResolver
	options        JCSMPBrowserOptions
	command        jcsmpCommandFactory
	namespace      string
	browseTimeout  time.Duration
	maxResponse    int
	restartBackoff time.Duration

	requestSlot chan struct{}
	closedCh    chan struct{}

	stateMu      sync.Mutex
	closed       bool
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	stdout       io.ReadCloser
	reader       *bufio.Reader
	done         chan struct{}
	stderr       *bytes.Buffer
	generation   uint64
	fatalErr     error
	nextRestart  time.Time
	restartDelay time.Duration

	closeOnce sync.Once
	closeErr  error
}

var _ Browser = (*JCSMPBrowser)(nil)

type jcsmpCommandFactory func(string, ...string) *exec.Cmd

// NewJCSMPBrowser starts a helper process and returns a non-destructive Browser.
// If the helper later exits or corrupts its stream, the next Browse restarts it.
func NewJCSMPBrowser(resolver MembershipQueueResolver, options JCSMPBrowserOptions) (*JCSMPBrowser, error) {
	return newJCSMPBrowser(resolver, options, exec.Command)
}

func newJCSMPBrowser(resolver MembershipQueueResolver, options JCSMPBrowserOptions, command jcsmpCommandFactory) (*JCSMPBrowser, error) {
	if resolver == nil {
		return nil, errors.New("broker0: membership queue resolver is required")
	}
	if command == nil {
		return nil, errors.New("broker0: JCSMP helper command factory is required")
	}
	if options.JavaExecutable == "" {
		options.JavaExecutable = "java"
	}
	if options.JarPath == "" {
		return nil, errors.New("broker0: JCSMP browser JAR is required")
	}
	if err := validateJCSMPNamespace(options.Namespace); err != nil {
		return nil, err
	}
	if options.Host == "" || options.VPN == "" || options.Username == "" || options.Password == "" {
		return nil, errors.New("broker0: JCSMP host, VPN, username, and password are required")
	}
	if strings.IndexByte(options.Host, 0) >= 0 || strings.IndexByte(options.VPN, 0) >= 0 ||
		strings.IndexByte(options.Username, 0) >= 0 || strings.IndexByte(options.Password, 0) >= 0 {
		return nil, errors.New("broker0: JCSMP helper environment values must not contain NUL")
	}
	if options.Timeout == 0 {
		options.Timeout = defaultJCSMPBrowseTimeout
	}
	if options.Timeout < time.Millisecond || options.Timeout > maxJCSMPBrowseTimeout {
		return nil, errors.New("broker0: JCSMP browse timeout must be between 1ms and 60s")
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = maxJCSMPResponseBytes
	}
	if options.MaxResponseBytes < 1 || options.MaxResponseBytes > maxJCSMPResponseBytes {
		return nil, fmt.Errorf("broker0: JCSMP maximum response size must be between 1 and %d bytes", maxJCSMPResponseBytes)
	}
	if options.RestartBackoff == 0 {
		options.RestartBackoff = defaultJCSMPRestartBackoff
	}
	if options.RestartBackoff < time.Millisecond || options.RestartBackoff > maxJCSMPRestartBackoff {
		return nil, errors.New("broker0: JCSMP restart backoff must be between 1ms and 30s")
	}

	browser := &JCSMPBrowser{
		resolver:       resolver,
		options:        options,
		command:        command,
		namespace:      options.Namespace,
		browseTimeout:  options.Timeout,
		maxResponse:    options.MaxResponseBytes,
		restartBackoff: options.RestartBackoff,
		requestSlot:    make(chan struct{}, 1),
		closedCh:       make(chan struct{}),
		restartDelay:   options.RestartBackoff,
	}
	browser.requestSlot <- struct{}{}
	browser.stateMu.Lock()
	err := browser.startLocked()
	browser.stateMu.Unlock()
	if err != nil {
		return nil, err
	}
	return browser, nil
}

func jcsmpEnvironmentValues(options JCSMPBrowserOptions) map[string]string {
	return map[string]string{
		"SWLB_CONTROL_HOST":             options.Host,
		"SWLB_CONTROL_VPN":              options.VPN,
		"SWLB_CONTROL_BROWSER_USERNAME": options.Username,
		"SWLB_CONTROL_BROWSER_PASSWORD": options.Password,
		"SWLB_CONTROL_TRUST_STORE":      options.TrustStorePath,
	}
}

func jcsmpEnvironment(parent []string, options JCSMPBrowserOptions) []string {
	values := jcsmpEnvironmentValues(options)
	environment := make([]string, 0, len(parent)+len(values))
	for _, entry := range parent {
		name, _, found := strings.Cut(entry, "=")
		if _, replaced := values[name]; found && replaced {
			continue
		}
		environment = append(environment, entry)
	}
	for _, name := range []string{"SWLB_CONTROL_HOST", "SWLB_CONTROL_VPN", "SWLB_CONTROL_BROWSER_USERNAME", "SWLB_CONTROL_BROWSER_PASSWORD", "SWLB_CONTROL_TRUST_STORE"} {
		environment = append(environment, name+"="+values[name])
	}
	return environment
}

func (b *JCSMPBrowser) startLocked() error {
	if b.closed {
		return ErrJCSMPBrowserClosed
	}
	cmd := b.command(b.options.JavaExecutable, "-jar", b.options.JarPath, b.options.Namespace)
	if cmd == nil {
		return errors.New("broker0: JCSMP helper command factory returned nil")
	}
	cmd.Env = jcsmpEnvironment(os.Environ(), b.options)
	stderr := &bytes.Buffer{}
	cmd.Stderr = &limitedWriter{writer: stderr, remaining: 8 << 10}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("broker0: open JCSMP helper stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return fmt.Errorf("broker0: open JCSMP helper stdout: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return fmt.Errorf("broker0: start JCSMP browser helper: %w", err)
	}

	b.generation++
	generation := b.generation
	done := make(chan struct{})
	b.cmd = cmd
	b.stdin = stdin
	b.stdout = stdout
	b.reader = bufio.NewReaderSize(stdout, 32*1024)
	b.done = done
	b.stderr = stderr
	b.fatalErr = nil
	go b.wait(cmd, done, stderr, generation)
	return nil
}

func (b *JCSMPBrowser) wait(cmd *exec.Cmd, done chan struct{}, stderr *bytes.Buffer, generation uint64) {
	err := cmd.Wait()
	b.stateMu.Lock()
	if b.generation == generation && !b.closed && b.fatalErr == nil {
		detail := safeJCSMPStderr(stderr.String(), b.options)
		if err == nil {
			b.markFailedLocked(fmt.Errorf("%w: process exited unexpectedly%s", ErrJCSMPBrowserFailed, detail))
		} else {
			b.markFailedLocked(fmt.Errorf("%w: process exited: %v%s", ErrJCSMPBrowserFailed, err, detail))
		}
	}
	b.stateMu.Unlock()
	close(done)
}

type limitedWriter struct {
	writer    io.Writer
	remaining int
}

func (w *limitedWriter) Write(data []byte) (int, error) {
	original := len(data)
	if w.remaining > 0 {
		write := data
		if len(write) > w.remaining {
			write = write[:w.remaining]
		}
		_, _ = w.writer.Write(write)
		w.remaining -= len(write)
	}
	return original, nil
}

func safeJCSMPStderr(value string, options JCSMPBrowserOptions) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sensitive := jcsmpEnvironmentValues(options)
	secrets := make([]string, 0, len(sensitive))
	for _, secret := range sensitive {
		if secret != "" {
			secrets = append(secrets, secret)
		}
	}
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, secret := range secrets {
		value = strings.ReplaceAll(value, secret, "[REDACTED]")
	}
	if len(value) > 512 {
		value = value[:512]
	}
	return ": " + value
}

type jcsmpBrowseRequest struct {
	Operation string `json:"operation"`
	Queue     string `json:"queue"`
	TimeoutMS int64  `json:"timeoutMs"`
}

type jcsmpBrowseResponse struct {
	OK       *bool                  `json:"ok"`
	Messages *[]jcsmpBrowsedMessage `json:"messages"`
	Error    *string                `json:"error"`
}

type jcsmpBrowsedMessage struct {
	PayloadBase64        *string `json:"payloadBase64"`
	ApplicationMessageID *string `json:"applicationMessageId"`
	Timestamp            *string `json:"timestamp"`
}

type jcsmpExchangeResult struct {
	messages []BrowsedMessage
	err      error
	fatal    bool
}

// Browse resolves the exact membership queue, submits one JSON-lines request,
// and returns owned payload bytes. Cancelling an in-flight request terminates
// the helper because its untagged response could otherwise corrupt the next
// exchange.
func (b *JCSMPBrowser) Browse(ctx context.Context, group string) ([]BrowsedMessage, error) {
	if b == nil {
		return nil, ErrJCSMPBrowserClosed
	}
	if ctx == nil {
		return nil, errors.New("broker0: browse context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.closedCh:
		return nil, ErrJCSMPBrowserClosed
	case <-b.requestSlot:
	}
	defer func() { b.requestSlot <- struct{}{} }()

	stdin, reader, done, generation, err := b.ensureHelper(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateTransportIdentifier("scaling group", group); err != nil {
		return nil, err
	}
	queue, err := b.resolver.MembershipQueue(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("broker0: resolve membership queue for %q: %w", group, err)
	}
	if err := validateJCSMPQueue(b.namespace, queue); err != nil {
		return nil, fmt.Errorf("broker0: resolved membership queue for %q: %w", group, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	timeout := b.browseTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, ctx.Err()
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	timeoutMS := int64((timeout + time.Millisecond - 1) / time.Millisecond)
	if timeoutMS < 1 {
		timeoutMS = 1
	}

	result := make(chan jcsmpExchangeResult, 1)
	go func() {
		messages, exchangeErr, fatal := b.exchange(stdin, reader, jcsmpBrowseRequest{
			Operation: "browse_latest",
			Queue:     queue,
			TimeoutMS: timeoutMS,
		})
		result <- jcsmpExchangeResult{messages: messages, err: exchangeErr, fatal: fatal}
	}()

	select {
	case <-ctx.Done():
		b.fail(generation, fmt.Errorf("request cancelled: %w", ctx.Err()))
		return nil, ctx.Err()
	case <-done:
		return nil, b.failureFor(generation)
	case completed := <-result:
		if err := ctx.Err(); err != nil {
			b.fail(generation, fmt.Errorf("request cancelled: %w", err))
			return nil, err
		}
		if completed.fatal {
			return nil, b.fail(generation, completed.err)
		}
		if completed.err != nil {
			return nil, completed.err
		}
		return completed.messages, nil
	}
}

func (b *JCSMPBrowser) exchange(stdin io.Writer, reader *bufio.Reader, request jcsmpBrowseRequest) ([]BrowsedMessage, error, bool) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err), true
	}
	if len(encoded)+1 > maxJCSMPRequestBytes {
		return nil, errors.New("encoded request exceeds limit"), true
	}
	encoded = append(encoded, '\n')
	if _, err := stdin.Write(encoded); err != nil {
		return nil, fmt.Errorf("write request: %w", err), true
	}
	line, err := readJCSMPLine(reader, b.maxResponse)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err), true
	}
	messages, responseErr, malformed := decodeJCSMPResponse(line)
	return messages, responseErr, malformed
}

func readJCSMPLine(reader *bufio.Reader, maximum int) ([]byte, error) {
	line := make([]byte, 0, min(maximum, 32*1024))
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(line)+len(fragment) > maximum+1 {
			return nil, fmt.Errorf("response exceeds %d bytes", maximum)
		}
		line = append(line, fragment...)
		switch {
		case err == nil:
			line = line[:len(line)-1]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			if len(line) > maximum {
				return nil, fmt.Errorf("response exceeds %d bytes", maximum)
			}
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return nil, errors.New("helper exited before terminating its response line")
		default:
			return nil, err
		}
	}
}

func decodeJCSMPResponse(line []byte) ([]BrowsedMessage, error, bool) {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	var response jcsmpBrowseResponse
	if err := decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("malformed JSON: %w", err), true
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err, true
	}
	if response.OK == nil {
		return nil, errors.New("response omits ok"), true
	}
	if !*response.OK {
		if response.Error == nil || *response.Error == "" || response.Messages != nil {
			return nil, errors.New("invalid failure response"), true
		}
		if *response.Error == "membership LVQ is empty" {
			return nil, ErrNoAuthoritativeState, false
		}
		return nil, fmt.Errorf("broker0: JCSMP browse rejected: %s", *response.Error), false
	}
	if response.Error != nil || response.Messages == nil || len(*response.Messages) == 0 || len(*response.Messages) > maxJCSMPMessages {
		return nil, errors.New("invalid success response"), true
	}

	messages := make([]BrowsedMessage, 0, len(*response.Messages))
	for index, wire := range *response.Messages {
		if wire.PayloadBase64 == nil || wire.ApplicationMessageID == nil {
			return nil, fmt.Errorf("message %d omits payload or applicationMessageId", index), true
		}
		if err := validateOperationID(*wire.ApplicationMessageID); err != nil {
			return nil, fmt.Errorf("message %d applicationMessageId: %w", index, err), true
		}
		if wire.Timestamp != nil && *wire.Timestamp != "" {
			if _, err := time.Parse(time.RFC3339Nano, *wire.Timestamp); err != nil {
				return nil, fmt.Errorf("message %d has invalid timestamp", index), true
			}
		}
		if len(*wire.PayloadBase64) > base64.StdEncoding.EncodedLen(maxJCSMPPayloadBytes) {
			return nil, fmt.Errorf("message %d payload exceeds one MiB", index), true
		}
		payload, err := base64.StdEncoding.Strict().DecodeString(*wire.PayloadBase64)
		if err != nil {
			return nil, fmt.Errorf("message %d payload is not valid base64", index), true
		}
		if len(payload) > maxJCSMPPayloadBytes {
			return nil, fmt.Errorf("message %d payload exceeds one MiB", index), true
		}
		messages = append(messages, BrowsedMessage{
			Kind:        KindMembershipSnapshot,
			OperationID: *wire.ApplicationMessageID,
			Payload:     payload,
		})
	}
	return messages, nil, false
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("response contains trailing JSON")
		}
		return fmt.Errorf("response has malformed trailing data: %w", err)
	}
	return nil
}

func validateJCSMPNamespace(namespace string) error {
	if namespace == "" || len(namespace) > 253 {
		return errors.New("broker0: invalid JCSMP browser namespace")
	}
	for _, label := range strings.Split(namespace, ".") {
		if len(label) == 0 || len(label) > 63 || !isLowerAlphaNumeric(label[0]) || !isLowerAlphaNumeric(label[len(label)-1]) {
			return errors.New("broker0: invalid JCSMP browser namespace")
		}
		for index := 1; index < len(label)-1; index++ {
			if !isLowerAlphaNumeric(label[index]) && label[index] != '-' {
				return errors.New("broker0: invalid JCSMP browser namespace")
			}
		}
	}
	return nil
}

func isLowerAlphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
}

func validateJCSMPQueue(namespace, queue string) error {
	if len(queue) > 200 || !strings.HasPrefix(queue, namespace+".") {
		return errors.New("queue is outside the configured namespace")
	}
	for _, character := range queue {
		if (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') && character != '_' && character != '.' && character != '-' {
			return errors.New("queue is outside the configured namespace")
		}
	}
	return nil
}

func (b *JCSMPBrowser) ensureHelper(ctx context.Context) (io.WriteCloser, *bufio.Reader, <-chan struct{}, uint64, error) {
	for {
		b.stateMu.Lock()
		if b.closed {
			b.stateMu.Unlock()
			return nil, nil, nil, 0, ErrJCSMPBrowserClosed
		}
		if b.fatalErr == nil {
			stdin, reader, done, generation := b.stdin, b.reader, b.done, b.generation
			b.stateMu.Unlock()
			return stdin, reader, done, generation, nil
		}
		wait := time.Until(b.nextRestart)
		b.stateMu.Unlock()
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return nil, nil, nil, 0, ctx.Err()
			case <-b.closedCh:
				if !timer.Stop() {
					<-timer.C
				}
				return nil, nil, nil, 0, ErrJCSMPBrowserClosed
			case <-timer.C:
			}
		}
		b.stateMu.Lock()
		if b.closed {
			b.stateMu.Unlock()
			return nil, nil, nil, 0, ErrJCSMPBrowserClosed
		}
		if b.fatalErr == nil {
			b.stateMu.Unlock()
			continue
		}
		err := b.startLocked()
		if err != nil {
			failure := fmt.Errorf("%w: %v", ErrJCSMPBrowserFailed, err)
			b.markFailedLocked(failure)
			b.stateMu.Unlock()
			return nil, nil, nil, 0, failure
		}
		stdin, reader, done, generation := b.stdin, b.reader, b.done, b.generation
		b.stateMu.Unlock()
		return stdin, reader, done, generation, nil
	}
}

func (b *JCSMPBrowser) markFailedLocked(failure error) {
	b.fatalErr = failure
	b.nextRestart = time.Now().Add(b.restartDelay)
	b.restartDelay = min(b.restartDelay*2, maxJCSMPRestartBackoff)
}

func (b *JCSMPBrowser) failureFor(generation uint64) error {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.closed {
		return ErrJCSMPBrowserClosed
	}
	if b.generation != generation || b.fatalErr == nil {
		return fmt.Errorf("%w: helper generation ended", ErrJCSMPBrowserFailed)
	}
	return b.fatalErr
}

func (b *JCSMPBrowser) fail(generation uint64, cause error) error {
	b.stateMu.Lock()
	if b.closed {
		b.stateMu.Unlock()
		return ErrJCSMPBrowserClosed
	}
	if b.generation != generation {
		b.stateMu.Unlock()
		return fmt.Errorf("%w: stale helper generation", ErrJCSMPBrowserFailed)
	}
	if b.fatalErr == nil {
		b.markFailedLocked(fmt.Errorf("%w: %v", ErrJCSMPBrowserFailed, cause))
	}
	failure := b.fatalErr
	process := b.cmd.Process
	stdin, stdout := b.stdin, b.stdout
	b.stateMu.Unlock()

	_ = stdin.Close()
	_ = stdout.Close()
	if process != nil {
		_ = process.Kill()
	}
	return failure
}

// Close terminates the supervised helper. It is safe to call more than once.
func (b *JCSMPBrowser) Close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		b.stateMu.Lock()
		b.closed = true
		close(b.closedCh)
		cmd, stdin, stdout, done := b.cmd, b.stdin, b.stdout, b.done
		b.stateMu.Unlock()

		_ = stdin.Close()
		_ = stdout.Close()
		if cmd != nil && cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				b.closeErr = fmt.Errorf("broker0: terminate JCSMP browser helper: %w", err)
			}
		}
		if done != nil {
			<-done
		}
	})
	return b.closeErr
}
