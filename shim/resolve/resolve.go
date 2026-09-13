// Package resolve is the Tier-1 resolver: it asks the assignment service which broker a
// (shard, client) belongs to, caches the answer, and never fails closed. The resolver returns a
// location (broker id + a per-protocol endpoint map). It does not vend credentials and never touches
// the message path.
//
// Fail-open is the contract: on a successful call the result is cached; if the service is later
// unreachable, the cached assignment is returned rather than erroring, because the assignment service
// being down must never take the application down. Only when there is no cache and the service is
// unreachable does Resolve return an error, because there is genuinely nothing to connect to.
package resolve

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/topology"
)

// Assignment is where a shard currently lives.
type Assignment struct {
	BrokerID       string            `json:"broker_id"`
	MsgVPN         string            `json:"msg_vpn"`
	State          string            `json:"state"`
	LeaseSeconds   int               `json:"lease_seconds"`
	Endpoints      map[string]string `json:"endpoints"`
	ReusedExisting bool              `json:"reused_existing"`
	FetchedAt      time.Time         `json:"-"`
}

// Endpoint returns the connection URI for a protocol ("amqp", "mqtt", "rest", "smf"), or an error if
// the assignment carries no endpoint for it.
func (a Assignment) Endpoint(protocol string) (string, error) {
	uri, ok := a.Endpoints[protocol]
	if !ok {
		return "", fmt.Errorf("protocol %q not in endpoint map for broker %q", protocol, a.BrokerID)
	}
	return uri, nil
}

// Fetcher fetches the raw assignment JSON for a URL. It is the seam that lets tests run without a
// network: the default uses net/http, tests pass a function returning canned bytes.
type Fetcher func(ctx context.Context, url string) ([]byte, error)

// Resolver resolves (shard, client, mode) to an Assignment with a fail-open cache.
type Resolver struct {
	BaseURL string
	Timeout time.Duration
	Fetch   Fetcher          // nil -> a default net/http fetcher
	Now     func() time.Time // nil -> time.Now

	mu    sync.Mutex
	cache map[cacheKey]Assignment
	//: per-shard live topology applied from the event spine; events win over HTTP by generation.
	topo map[string]*topology.Topology
}

type cacheKey struct{ shard, client, mode string }

// New returns a Resolver pointed at the assignment service base URL with sensible defaults.
func New(baseURL string) *Resolver {
	return &Resolver{BaseURL: baseURL, Timeout: 5 * time.Second}
}

func (r *Resolver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Resolve looks up where (shard, client) should connect for a delivery mode ("direct" or
// "guaranteed"), optionally hinting a protocol. On success it caches and returns the fresh
// assignment; if the service is unreachable it returns the last cached answer; with no cache it
// returns an error.
func (r *Resolver) Resolve(ctx context.Context, shard, client, mode, protocol string) (Assignment, error) {
	if mode == "" {
		mode = "direct"
	}
	key := cacheKey{shard, client, mode}

	body, err := r.fetch(ctx, shard, client, mode, protocol)
	if err == nil {
		var a Assignment
		if derr := json.Unmarshal(body, &a); derr != nil {
			err = fmt.Errorf("decode assignment: %w", derr)
		} else if a.BrokerID == "" || len(a.Endpoints) == 0 {
			err = fmt.Errorf("assignment for %v is missing broker_id or endpoints", key)
		} else {
			a.FetchedAt = r.now()
			r.store(key, a)
			return a, nil
		}
	}

	if cached, ok := r.cached(key); ok {
		return cached, nil // fail open
	}
	return Assignment{}, fmt.Errorf("assignment service unreachable and no cached assignment for %v: %w", key, err)
}

// Apply installs a topology snapshot from the event spine. Events win by generation: a snapshot is
// applied only if its generation is strictly greater than the one already held for the shard, so a
// late or duplicate event is harmless and can never regress the resolver. Returns true if applied.
//
// This is how the resolver becomes events-primary: once a spine event lands, ResolveKey answers from
// the applied topology with no network call. Apply is safe to call from the spine subscriber
// goroutine concurrently with ResolveKey.
func (r *Resolver) Apply(t *topology.Topology) bool {
	if t == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.topo == nil {
		r.topo = map[string]*topology.Topology{}
	}
	if cur, ok := r.topo[t.Shard]; ok && t.Gen() <= cur.Gen() {
		return false // do not regress: an equal or older generation is ignored
	}
	r.topo[t.Shard] = t
	return true
}

// KeyLocation is where a partition key currently lives: the owning broker and its endpoint for the
// requested protocol, plus the generation the answer was fenced at (so a publisher can stamp saas_gen
// and a handoff can be respected).
type KeyLocation struct {
	BrokerID string
	Endpoint string
	Protocol string
	Gen      int
	Shard    string
}

// ResolveKey maps (shard, partition key) to its owning broker, events-primary: it answers from the
// applied topology when one is present, and only cold-starts over HTTP (GET /topology) when the shard
// has no applied topology yet. After a cold start the snapshot is cached like a spine event (at its
// generation), so live events supersede it. Fail-open: if HTTP is unreachable and a topology is
// cached, that cached topology is used; only a cold shard with no cache errors.
func (r *Resolver) ResolveKey(ctx context.Context, shard, key, protocol string) (KeyLocation, error) {
	if protocol == "" {
		protocol = "amqp"
	}
	if t, ok := r.topology(shard); ok {
		return r.locate(t, shard, key, protocol)
	}

	// Cold start: fetch the shard snapshot once, apply it, then locate.
	body, err := r.fetchTopology(ctx, shard)
	if err == nil {
		if t, lerr := topology.LoadTopology(body); lerr == nil {
			r.Apply(t)
			return r.locate(t, shard, key, protocol)
		} else {
			err = fmt.Errorf("decode topology: %w", lerr)
		}
	}
	if t, ok := r.topology(shard); ok {
		return r.locate(t, shard, key, protocol) // fail open to whatever we have
	}
	return KeyLocation{}, fmt.Errorf("no topology for shard %q and cold-start fetch failed: %w", shard, err)
}

func (r *Resolver) locate(t *topology.Topology, shard, key, protocol string) (KeyLocation, error) {
	owner, ok := t.Owner(key)
	if !ok {
		return KeyLocation{}, fmt.Errorf("no ownable broker for shard %q key %q at gen %d", shard, key, t.Gen())
	}
	endpoint, err := t.EndpointFor(owner, protocol)
	if err != nil {
		return KeyLocation{}, err
	}
	return KeyLocation{BrokerID: owner, Endpoint: endpoint, Protocol: protocol, Gen: t.Gen(), Shard: shard}, nil
}

func (r *Resolver) topology(shard string) (*topology.Topology, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.topo[shard]
	return t, ok
}

func (r *Resolver) fetchTopology(ctx context.Context, shard string) ([]byte, error) {
	q := url.Values{}
	q.Set("shard", shard)
	full := strings.TrimRight(r.BaseURL, "/") + "/topology?" + q.Encode()
	if r.Fetch != nil {
		return r.Fetch(ctx, full)
	}
	return defaultFetch(ctx, full, r.Timeout)
}

func (r *Resolver) fetch(ctx context.Context, shard, client, mode, protocol string) ([]byte, error) {
	q := url.Values{}
	q.Set("shard", shard)
	q.Set("client_id", client)
	q.Set("mode", mode)
	if protocol != "" {
		q.Set("protocol", protocol)
	}
	full := strings.TrimRight(r.BaseURL, "/") + "/assignment?" + q.Encode()

	if r.Fetch != nil {
		return r.Fetch(ctx, full)
	}
	return defaultFetch(ctx, full, r.Timeout)
}

func defaultFetch(ctx context.Context, full string, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("assignment service returned %s", resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func (r *Resolver) store(key cacheKey, a Assignment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil {
		r.cache = map[cacheKey]Assignment{}
	}
	r.cache[key] = a
}

func (r *Resolver) cached(key cacheKey) (Assignment, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.cache[key]
	return a, ok
}
