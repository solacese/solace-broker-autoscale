package cloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const apiRoot = "/api/v2/missionControl"

// ClientOption configures the Mission Control client.
type ClientOption func(*clientOptions) error

type clientOptions struct {
	baseURL           string
	httpClient        *http.Client
	tokenOwner        string
	idempotencyHeader string
	pageSize          int
}

func WithBaseURL(raw string) ClientOption {
	return func(o *clientOptions) error { o.baseURL = raw; return nil }
}

func WithHTTPClient(client *http.Client) ClientOption {
	return func(o *clientOptions) error {
		if client == nil {
			return errors.New("HTTP client must not be nil")
		}
		o.httpClient = client
		return nil
	}
}

// WithTokenOwner sets the Mission Control identity (the createdBy/ownedBy
// value) which is required for safe uncertain-create adoption.
func WithTokenOwner(owner string) ClientOption {
	return func(o *clientOptions) error {
		if strings.TrimSpace(owner) == "" {
			return errors.New("token owner must not be empty")
		}
		o.tokenOwner = owner
		return nil
	}
}

// WithIdempotencyHeader opts into an idempotency header supported by the API
// gateway in front of Mission Control. The public v2 specification does not
// currently advertise one, so the client sends none unless explicitly enabled.
func WithIdempotencyHeader(name string) ClientOption {
	return func(o *clientOptions) error {
		if strings.TrimSpace(name) == "" || http.CanonicalHeaderKey(name) == "" {
			return errors.New("idempotency header must not be empty")
		}
		o.idempotencyHeader = http.CanonicalHeaderKey(name)
		return nil
	}
}

func WithPageSize(size int) ClientOption {
	return func(o *clientOptions) error {
		if size < 1 || size > 100 {
			return errors.New("page size must be between 1 and 100")
		}
		o.pageSize = size
		return nil
	}
}

// Client is a Solace Cloud Mission Control v2 client. The bearer token is kept
// private and is never included in formatted errors.
type Client struct {
	baseURL           *url.URL
	httpClient        *http.Client
	bearerToken       string
	tokenOwner        string
	idempotencyHeader string
	pageSize          int
}

func NewClient(rawBearerToken string, options ...ClientOption) (*Client, error) {
	if strings.TrimSpace(rawBearerToken) == "" {
		return nil, errors.New("raw bearer token is required")
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(rawBearerToken)), "bearer ") {
		return nil, errors.New("supply the raw bearer token without a Bearer prefix")
	}
	o := clientOptions{baseURL: DefaultBaseURL, httpClient: http.DefaultClient, pageSize: 100}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("nil client option")
		}
		if err := option(&o); err != nil {
			return nil, err
		}
	}
	base, err := url.Parse(o.baseURL)
	if err != nil || base.Scheme != "https" && base.Scheme != "http" || base.Host == "" || base.RawQuery != "" || base.Fragment != "" || base.User != nil {
		return nil, errors.New("base URL must be an absolute HTTP(S) URL without user info, query, or fragment")
	}
	base.Path = strings.TrimRight(base.EscapedPath(), "/")
	base.RawPath = ""
	clone := *o.httpClient
	// ErrUseLastResponse prevents redirect following and returns the 3xx response
	// to the strict status checker below.
	clone.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{
		baseURL: base, httpClient: &clone, bearerToken: rawBearerToken,
		tokenOwner: o.tokenOwner, idempotencyHeader: o.idempotencyHeader, pageSize: o.pageSize,
	}, nil
}

func (c *Client) TokenOwner() string { return c.tokenOwner }

type responseEnvelope[T any] struct {
	Data T               `json:"data"`
	Meta json.RawMessage `json:"meta"`
}

func (c *Client) ListServices(ctx context.Context) ([]Service, error) {
	return listPages[Service](ctx, c, apiRoot+"/eventBrokerServices", nil, true)
}

func (c *Client) GetService(ctx context.Context, id string) (Service, error) {
	if strings.TrimSpace(id) == "" {
		return Service{}, errors.New("service ID is required")
	}
	var envelope responseEnvelope[Service]
	q := url.Values{}
	q.Add("expand", "broker")
	q.Add("expand", "serviceConnectionEndpoints")
	if err := c.get(ctx, apiRoot+"/eventBrokerServices/"+url.PathEscape(id), q, &envelope); err != nil {
		return Service{}, err
	}
	return envelope.Data, nil
}

func (c *Client) ListDatacenters(ctx context.Context) ([]Datacenter, error) {
	return listPages[Datacenter](ctx, c, apiRoot+"/datacenters", nil, true)
}

func (c *Client) ListServiceClasses(ctx context.Context) ([]ServiceClass, error) {
	return listPages[ServiceClass](ctx, c, apiRoot+"/serviceClasses", nil, false)
}

func (c *Client) ListBrokerVersions(ctx context.Context) ([]BrokerVersion, error) {
	return listPages[BrokerVersion](ctx, c, apiRoot+"/eventBrokerServiceVersions", nil, true)
}

// ListCompatibleBrokerVersions returns only versions compatible with the
// selected datacenter, using Mission Control's server-side compatibility
// filter. An empty datacenter is rejected rather than accidentally listing
// organization-wide versions.
func (c *Client) ListCompatibleBrokerVersions(ctx context.Context, datacenterID string) ([]BrokerVersion, error) {
	if strings.TrimSpace(datacenterID) == "" {
		return nil, errors.New("datacenter ID is required")
	}
	query := url.Values{}
	query.Set("datacenterId", datacenterID)
	query.Set("filterIncompatibleVersions", "true")
	return listPages[BrokerVersion](ctx, c, apiRoot+"/eventBrokerServiceVersions", query, true)
}

func (c *Client) ListOperations(ctx context.Context, serviceID string) ([]Operation, error) {
	if strings.TrimSpace(serviceID) == "" {
		return nil, errors.New("service ID is required")
	}
	return listPages[Operation](ctx, c, apiRoot+"/eventBrokerServices/"+url.PathEscape(serviceID)+"/operations", nil, true)
}

func listPages[T any](ctx context.Context, c *Client, endpoint string, query url.Values, paginated bool) ([]T, error) {
	if query == nil {
		query = make(url.Values)
	} else {
		query = cloneValues(query)
	}
	var result []T
	for pageNumber := 1; ; pageNumber++ {
		if pageNumber > 10000 {
			return nil, errors.New("pagination exceeded 10000 pages")
		}
		if paginated {
			query.Set("pageNumber", strconv.Itoa(pageNumber))
			query.Set("pageSize", strconv.Itoa(c.pageSize))
		}
		var envelope responseEnvelope[[]T]
		if err := c.get(ctx, endpoint, query, &envelope); err != nil {
			return nil, err
		}
		result = append(result, envelope.Data...)
		if !paginated || len(envelope.Data) < c.pageSize {
			return result, nil
		}
	}
}

func cloneValues(in url.Values) url.Values {
	out := make(url.Values, len(in))
	for key, values := range in {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func (c *Client) CreateService(ctx context.Context, request CreateServiceRequest, idempotencyKey string) (Operation, error) {
	if err := request.Identity().validate(); err != nil {
		return Operation{}, err
	}
	var envelope responseEnvelope[Operation]
	if err := c.write(ctx, http.MethodPost, apiRoot+"/eventBrokerServices", request, idempotencyKey, http.StatusAccepted, &envelope); err != nil {
		return Operation{}, err
	}
	return envelope.Data, nil
}

func (c *Client) DeleteService(ctx context.Context, serviceID, idempotencyKey string) (Operation, error) {
	if strings.TrimSpace(serviceID) == "" {
		return Operation{}, errors.New("service ID is required")
	}
	var envelope responseEnvelope[Operation]
	err := c.write(ctx, http.MethodDelete, apiRoot+"/eventBrokerServices/"+url.PathEscape(serviceID), nil, idempotencyKey, http.StatusAccepted, &envelope)
	return envelope.Data, err
}

func (c *Client) GetOperation(ctx context.Context, serviceID, operationID string) (Operation, error) {
	if strings.TrimSpace(serviceID) == "" || strings.TrimSpace(operationID) == "" {
		return Operation{}, errors.New("service ID and operation ID are required")
	}
	var envelope responseEnvelope[Operation]
	endpoint := apiRoot + "/eventBrokerServices/" + url.PathEscape(serviceID) + "/operations/" + url.PathEscape(operationID)
	if err := c.get(ctx, endpoint, nil, &envelope); err != nil {
		return Operation{}, err
	}
	return envelope.Data, nil
}

// WaitOperation polls until success, failure, or context cancellation.
func (c *Client) WaitOperation(ctx context.Context, serviceID, operationID string, interval time.Duration) (Operation, error) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for {
		op, err := c.GetOperation(ctx, serviceID, operationID)
		if err != nil {
			return Operation{}, err
		}
		switch op.Status {
		case "SUCCEEDED":
			return op, nil
		case "FAILED":
			failure := &OperationFailure{OperationType: op.OperationType}
			if failure.OperationType == "" {
				failure.OperationType = "unknown"
			}
			if op.Error != nil {
				failure.Message = redactText(op.Error.Message, c.bearerToken)
			}
			return op, failure
		case "PENDING", "INPROGRESS":
		default:
			return op, fmt.Errorf("Mission Control operation %q has unknown status %q", op.ID, op.Status)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return Operation{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) ConnectionBundle(ctx context.Context, serviceID string) (ConnectionBundle, error) {
	service, err := c.GetService(ctx, serviceID)
	if err != nil {
		return ConnectionBundle{}, err
	}
	return ParseConnectionBundle(service)
}

func ParseConnectionBundle(service Service) (ConnectionBundle, error) {
	if service.ID == "" {
		return ConnectionBundle{}, errors.New("expanded service has no ID")
	}
	bundle := ConnectionBundle{ServiceID: service.ID, ServiceName: service.Name, MessageVPN: service.MessageVPN}
	if len(service.Broker.MessageVPNs) > 0 {
		vpn := service.Broker.MessageVPNs[0]
		for _, candidate := range service.Broker.MessageVPNs {
			if candidate.Name == service.MessageVPN {
				vpn = candidate
				break
			}
		}
		if bundle.MessageVPN == "" {
			bundle.MessageVPN = vpn.Name
		}
		bundle.ServiceCredential = vpn.ServiceCredential
		bundle.ManagementCredential = vpn.ManagementAdminCredential
		if bundle.ManagementCredential.Username == "" && bundle.ManagementCredential.Token == "" {
			bundle.ManagementCredential = vpn.MissionControlManagerCredential
		}
	}
	for _, endpoint := range service.ConnectionEndpoints {
		for _, host := range endpoint.Hostnames {
			for _, port := range endpoint.Ports {
				if port.Port <= 0 {
					continue
				}
				address := func(scheme string) string { return scheme + "://" + netJoinHostPort(host, port.Port) }
				switch port.Protocol {
				case "serviceSmfPlainTextListenPort":
					bundle.SMFHosts = appendUnique(bundle.SMFHosts, address("tcp"))
				case "serviceSmfCompressedListenPort":
					bundle.SMFHosts = appendUnique(bundle.SMFHosts, address("tcp"))
				case "serviceSmfTlsListenPort":
					bundle.SMFHosts = appendUnique(bundle.SMFHosts, address("tcps"))
				case "serviceManagementTlsListenPort":
					bundle.ManagementURLs = appendUnique(bundle.ManagementURLs, address("https"))
				case "serviceWebPlainTextListenPort":
					bundle.WebMessagingURLs = appendUnique(bundle.WebMessagingURLs, address("ws"))
				case "serviceWebTlsListenPort":
					bundle.WebMessagingURLs = appendUnique(bundle.WebMessagingURLs, address("wss"))
				case "serviceAmqpPlainTextListenPort":
					bundle.AMQPURLs = appendUnique(bundle.AMQPURLs, address("amqp"))
				case "serviceAmqpTlsListenPort":
					bundle.AMQPURLs = appendUnique(bundle.AMQPURLs, address("amqps"))
				case "serviceMqttPlainTextListenPort", "serviceMqttWebSocketListenPort":
					bundle.MQTTURLs = appendUnique(bundle.MQTTURLs, address("mqtt"))
				case "serviceMqttTlsListenPort", "serviceMqttTlsWebSocketListenPort":
					bundle.MQTTURLs = appendUnique(bundle.MQTTURLs, address("mqtts"))
				case "serviceRestIncomingPlainTextListenPort":
					bundle.RESTURLs = appendUnique(bundle.RESTURLs, address("http"))
				case "serviceRestIncomingTlsListenPort":
					bundle.RESTURLs = appendUnique(bundle.RESTURLs, address("https"))
				}
			}
		}
	}
	if len(bundle.SMFHosts) == 0 {
		return ConnectionBundle{}, errors.New("expanded service has no enabled SMF endpoint")
	}
	if bundle.MessageVPN == "" {
		return ConnectionBundle{}, errors.New("expanded service has no message VPN")
	}
	return bundle, nil
}

func netJoinHostPort(host string, port int) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return host + ":" + strconv.Itoa(port)
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func (c *Client) get(ctx context.Context, endpoint string, query url.Values, target any) error {
	request, err := c.request(ctx, http.MethodGet, endpoint, query, nil, "")
	if err != nil {
		return err
	}
	return c.execute(request, http.StatusOK, target)
}

func (c *Client) write(ctx context.Context, method, endpoint string, body any, idempotencyKey string, expected int, target any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode Mission Control request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := c.request(ctx, method, endpoint, nil, reader, idempotencyKey)
	if err != nil {
		return err
	}
	return c.execute(request, expected, target)
}

func (c *Client) request(ctx context.Context, method, endpoint string, query url.Values, body io.Reader, idempotencyKey string) (*http.Request, error) {
	u := *c.baseURL
	u.Path = path.Join(c.baseURL.Path, endpoint)
	if query != nil {
		u.RawQuery = query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("build Mission Control request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.bearerToken)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" && c.idempotencyHeader != "" {
		request.Header.Set(c.idempotencyHeader, idempotencyKey)
	}
	return request, nil
}

func (c *Client) execute(request *http.Request, expected int, target any) error {
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("Mission Control %s request failed: %w", request.Method, err)
	}
	defer response.Body.Close()
	if response.StatusCode != expected {
		limited, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		if readErr != nil {
			limited = []byte("<unreadable response body>")
		}
		return newHTTPError(request.Method, request.URL, response.StatusCode, response.Header, limited, c.bearerToken)
	}
	limited := io.LimitReader(response.Body, 8<<20)
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode Mission Control %s response: %w", request.Method, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode Mission Control %s response: multiple JSON values", request.Method)
		}
		return fmt.Errorf("decode Mission Control %s response trailer: %w", request.Method, err)
	}
	return nil
}

// HTTPError is returned for every unexpected HTTP status, including redirects.
type HTTPError struct {
	Method     string
	URL        string
	StatusCode int
	RequestID  string
	Body       string
}

func (e *HTTPError) Error() string {
	requestID := ""
	if e.RequestID != "" {
		requestID = ", request ID " + e.RequestID
	}
	return fmt.Sprintf("Mission Control %s %s returned HTTP %d%s: %s", e.Method, e.URL, e.StatusCode, requestID, e.Body)
}

func newHTTPError(method string, requestURL *url.URL, status int, header http.Header, body []byte, token string) *HTTPError {
	cleanURL := *requestURL
	cleanURL.User = nil
	query := cleanURL.Query()
	for key := range query {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "token") || strings.Contains(lower, "secret") || strings.Contains(lower, "password") || strings.Contains(lower, "key") {
			query.Set(key, "[REDACTED]")
		}
	}
	cleanURL.RawQuery = query.Encode()
	return &HTTPError{
		Method: method, URL: cleanURL.String(), StatusCode: status,
		RequestID: firstHeader(header, "X-Request-Id", "X-Correlation-Id", "Traceparent"),
		Body:      redactBody(body, token),
	}
}

func firstHeader(header http.Header, names ...string) string {
	for _, name := range names {
		if value := header.Get(name); value != "" {
			return value
		}
	}
	return ""
}

var sensitiveText = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*bearer\s+|"?(?:access_?token|token|password|secret|api_?key)"?\s*[:=]\s*"?)[^"\s,}]+`)

func redactText(value, token string) string {
	if token != "" {
		value = strings.ReplaceAll(value, token, "[REDACTED]")
	}
	return sensitiveText.ReplaceAllString(value, `${1}[REDACTED]`)
}

func redactBody(body []byte, token string) string {
	var value any
	if json.Unmarshal(body, &value) == nil {
		redactJSON(value)
		if encoded, err := json.Marshal(value); err == nil {
			return redactText(string(encoded), token)
		}
	}
	text := strings.TrimSpace(string(body))
	if text == "" {
		text = http.StatusText(http.StatusInternalServerError)
	}
	return redactText(text, token)
}

func redactJSON(value any) {
	switch current := value.(type) {
	case map[string]any:
		for key, child := range current {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "token") || strings.Contains(lower, "password") || strings.Contains(lower, "secret") || strings.Contains(lower, "authorization") || strings.Contains(lower, "apikey") || strings.Contains(lower, "api_key") {
				current[key] = "[REDACTED]"
			} else {
				redactJSON(child)
			}
		}
	case []any:
		for _, child := range current {
			redactJSON(child)
		}
	}
}
