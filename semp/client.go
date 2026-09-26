// Package semp provides a small, hardened SEMP v2 client for resources managed
// by the workload balancer. It intentionally exposes only the operations needed
// by this project instead of mirroring the complete broker API.
package semp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultTimeout    = 10 * time.Second
	defaultRetries    = 3
	defaultBackoff    = 100 * time.Millisecond
	defaultMaxBackoff = time.Second
	maxResponseBytes  = 4 << 20
	maxPages          = 1000
)

var (
	// ErrNotFound is returned only for Solace's exact HTTP 400 NOT_FOUND status,
	// or for an HTTP 404 response.
	ErrNotFound = errors.New("semp: resource not found")
	// ErrAlreadyExists is returned only for Solace's exact HTTP 400
	// ALREADY_EXISTS status, or for an HTTP 409 response.
	ErrAlreadyExists = errors.New("semp: resource already exists")
)

// HTTPError describes a failed SEMP request without retaining response bodies,
// credentials, or query strings.
type HTTPError struct {
	Method string
	Path   string
	Status int
	Code   string
}

func (e *HTTPError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("semp: %s %s returned HTTP %d (%s)", e.Method, e.Path, e.Status, e.Code)
	}
	return fmt.Sprintf("semp: %s %s returned HTTP %d", e.Method, e.Path, e.Status)
}

func (e *HTTPError) Unwrap() error {
	switch {
	case e.Status == http.StatusNotFound || (e.Status == http.StatusBadRequest && e.Code == "NOT_FOUND"):
		return ErrNotFound
	case e.Status == http.StatusConflict || (e.Status == http.StatusBadRequest && e.Code == "ALREADY_EXISTS"):
		return ErrAlreadyExists
	default:
		return nil
	}
}

// ClientOptions configures a Client. TLS verification is always enabled.
type ClientOptions struct {
	BaseURL  string
	Username string
	Password string
	Timeout  time.Duration
	// Retries is the total number of attempts. Zero selects the default.
	Retries        int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// Transport is intended for tests and must be a TLS-verifying *http.Transport
	// without custom TLS dialers. NewClient retains a clone.
	Transport http.RoundTripper
}

// Client is safe for concurrent use.
type Client struct {
	base           *url.URL
	username       string
	password       string
	http           *http.Client
	retries        int
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

// NewClient creates a SEMP v2 client. BaseURL must be an absolute http(s) URL
// without embedded credentials, a query, or a fragment. HTTPS always performs
// normal certificate and hostname verification.
func NewClient(options ClientOptions) (*Client, error) {
	if strings.TrimSpace(options.BaseURL) == "" || options.Username == "" || options.Password == "" {
		return nil, errors.New("semp: base URL, username, and password are required")
	}
	base, err := url.Parse(options.BaseURL)
	if err != nil || !base.IsAbs() || base.Host == "" || (base.Scheme != "https" && base.Scheme != "http") {
		return nil, errors.New("semp: base URL must be an absolute http or https URL")
	}
	if base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("semp: base URL must not contain credentials, a query, or a fragment")
	}
	base.Path = strings.TrimRight(base.Path, "/")
	base.RawPath = ""

	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	retries := options.Retries
	if retries == 0 {
		retries = defaultRetries
	}
	if retries < 0 {
		return nil, errors.New("semp: retries cannot be negative")
	}
	initial := options.InitialBackoff
	if initial <= 0 {
		initial = defaultBackoff
	}
	maximum := options.MaxBackoff
	if maximum <= 0 {
		maximum = defaultMaxBackoff
	}
	if maximum < initial {
		return nil, errors.New("semp: maximum backoff must not be shorter than initial backoff")
	}

	transport := options.Transport
	if transport != nil {
		configured, ok := transport.(*http.Transport)
		if !ok {
			return nil, errors.New("semp: custom transport must be *http.Transport")
		}
		configured = configured.Clone()
		if configured.TLSClientConfig != nil && configured.TLSClientConfig.InsecureSkipVerify {
			return nil, errors.New("semp: TLS certificate verification cannot be disabled")
		}
		if configured.DialTLS != nil || configured.DialTLSContext != nil {
			return nil, errors.New("semp: custom TLS dialers are not allowed")
		}
		transport = configured
	}
	if transport == nil {
		transport = &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          20,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: timeout,
			ExpectContinueTimeout: time.Second,
			TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		}
	}

	return &Client{
		base:     base,
		username: options.Username,
		password: options.Password,
		http: &http.Client{
			Transport: transport,
			Timeout:   timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		retries:        retries,
		initialBackoff: initial,
		maxBackoff:     maximum,
	}, nil
}

func escape(segment string) string { return url.PathEscape(segment) }

func pathSegments(parts ...string) string {
	var builder strings.Builder
	for _, part := range parts {
		builder.WriteByte('/')
		builder.WriteString(escape(part))
	}
	return builder.String()
}

func (c *Client) endpoint(path string) string {
	// path is assembled from escaped path segments. Concatenating its encoded
	// representation avoids url.URL escaping '%' a second time.
	return strings.TrimRight(c.base.String(), "/") + path
}

func (c *Client) request(ctx context.Context, method, path string, body any, result any) error {
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("semp: encode %s request: %w", method, err)
		}
	}

	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path), bytes.NewReader(encoded))
		if err != nil {
			return fmt.Errorf("semp: build %s request: %w", method, err)
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		req.SetBasicAuth(c.username, c.password)

		response, err := c.http.Do(req)
		if err != nil {
			if attempt+1 < c.retries && retryableTransport(err) {
				if waitErr := c.wait(ctx, attempt, ""); waitErr != nil {
					return waitErr
				}
				continue
			}
			return fmt.Errorf("semp: %s %s failed: %w", method, safePath(path), err)
		}

		payload, readErr := readResponse(response.Body)
		response.Body.Close()
		if readErr != nil {
			return fmt.Errorf("semp: read %s %s response: %w", method, safePath(path), readErr)
		}
		if response.StatusCode >= 300 && response.StatusCode <= 399 {
			return &HTTPError{Method: method, Path: safePath(path), Status: response.StatusCode}
		}
		if retryableStatus(response.StatusCode) && attempt+1 < c.retries {
			if waitErr := c.wait(ctx, attempt, response.Header.Get("Retry-After")); waitErr != nil {
				return waitErr
			}
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return responseError(method, path, response.StatusCode, payload)
		}
		if result != nil {
			if err := json.Unmarshal(payload, result); err != nil {
				return fmt.Errorf("semp: decode %s %s response: %w", method, safePath(path), err)
			}
		}
		return nil
	}
}

func readResponse(reader io.Reader) ([]byte, error) {
	limited := &io.LimitedReader{R: reader, N: maxResponseBytes + 1}
	payload, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxResponseBytes {
		return nil, errors.New("response exceeds size limit")
	}
	return payload, nil
}

func retryableTransport(err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func retryableStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func (c *Client) wait(ctx context.Context, attempt int, retryAfter string) error {
	delay := c.initialBackoff
	for i := 0; i < attempt && delay < c.maxBackoff; i++ {
		if delay > c.maxBackoff/2 {
			delay = c.maxBackoff
		} else {
			delay *= 2
		}
	}
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		serverDelay := time.Duration(seconds) * time.Second
		if serverDelay < delay {
			delay = serverDelay
		}
	}
	if delay > c.maxBackoff {
		delay = c.maxBackoff
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func safePath(path string) string {
	if index := strings.IndexByte(path, '?'); index >= 0 {
		return path[:index]
	}
	return path
}

type errorEnvelope struct {
	Meta struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	} `json:"meta"`
}

func responseError(method, path string, status int, payload []byte) error {
	code := ""
	if status == http.StatusBadRequest {
		var envelope errorEnvelope
		if json.Unmarshal(payload, &envelope) == nil {
			code = envelope.Meta.Error.Status
		}
	}
	return &HTTPError{Method: method, Path: safePath(path), Status: status, Code: code}
}

func isNotFound(err error) bool      { return errors.Is(err, ErrNotFound) }
func isAlreadyExists(err error) bool { return errors.Is(err, ErrAlreadyExists) }

// list follows SEMP nextPageUri links, accepting only same-origin links under
// the configured base path. The caller provides a collection element type.
func list[T any](ctx context.Context, c *Client, path string) ([]T, error) {
	var all []T
	seen := make(map[string]struct{})
	for pages := 0; path != ""; pages++ {
		if pages >= maxPages {
			return nil, errors.New("semp: pagination exceeded page limit")
		}
		if _, duplicate := seen[path]; duplicate {
			return nil, errors.New("semp: pagination loop detected")
		}
		seen[path] = struct{}{}

		var page struct {
			Data []T `json:"data"`
			Meta struct {
				Paging struct {
					NextPageURI string `json:"nextPageUri"`
				} `json:"paging"`
			} `json:"meta"`
		}
		if err := c.request(ctx, http.MethodGet, path, nil, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Data...)
		if page.Meta.Paging.NextPageURI == "" {
			break
		}
		next, err := c.paginationPath(page.Meta.Paging.NextPageURI)
		if err != nil {
			return nil, err
		}
		path = next
	}
	return all, nil
}

func (c *Client) paginationPath(raw string) (string, error) {
	next, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("semp: invalid pagination link")
	}
	resolved := c.base.ResolveReference(next)
	if !sameOrigin(c.base, resolved) {
		return "", errors.New("semp: pagination link changes origin")
	}
	basePath := strings.TrimRight(c.base.Path, "/")
	if resolved.Path != basePath && !strings.HasPrefix(resolved.Path, basePath+"/") {
		return "", errors.New("semp: pagination link leaves configured base path")
	}
	path := strings.TrimPrefix(resolved.EscapedPath(), c.base.EscapedPath())
	if path == "" {
		path = "/"
	}
	if resolved.RawQuery != "" {
		path += "?" + resolved.RawQuery
	}
	return path, nil
}

func sameOrigin(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}
