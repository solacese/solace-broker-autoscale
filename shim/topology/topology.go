// Package topology is the Go side of the event spine (ADR 0009). It parses the self-describing
// topology snapshot the Python control plane publishes on _autoscale/shard/<shard>/topology and
// answers the one question the shim needs at publish time: which broker owns a partition key.
//
// Ownership uses rendezvous (highest-random-weight) hashing, computed identically to the Python
// control plane: for a key, the owner is the ACTIVE broker with the greatest sha256(broker_id +
// "\x00" + key), ties broken on the greater broker_id. This must match Python byte-for-byte, which
// is what TestTopologyInteropGolden proves against a Python-emitted golden file. Rendezvous makes
// scaling N->N+1 move only ~1/(N+1) of keys, which is what keeps handoffs (and the ordering risk
// they carry) minimal.
//
// This package is pure: it parses bytes and computes ownership. It does no I/O and holds no bus
// dependency. The spine subscriber (a later package) feeds snapshots in; the resolver applies them.
package topology

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
)

// Version is the topology event schema version this package understands. A snapshot with any other
// version is rejected rather than misparsed.
const Version = 1

// GenerationProperty is the message application property carrying the generation stamp. It matches
// the Python GENERATION_PROPERTY and the dispatch layer's saas_gen, so a subscriber can fence on the
// generation without decoding the body.
const GenerationProperty = "saas_gen"

// State mirrors the Python BrokerState. Only Active brokers own keys.
type State string

const (
	StateActive   State = "active"
	StateDraining State = "draining"
	StateDrained  State = "drained"
	StateDeleting State = "deleting"
	StateGone     State = "gone"
)

// BrokerRef is one broker in a snapshot: its id, state, and per-protocol endpoint map.
type BrokerRef struct {
	BrokerID  string            `json:"broker_id"`
	State     State             `json:"state"`
	Endpoints map[string]string `json:"endpoints"`
}

// Handoff records that a key range moved from one broker to another, fenced at EffectiveGen. The
// publisher keeps sending a moved key to FromBroker until it drains, then cuts to ToBroker stamping
// EffectiveGen; the listener holds gen-EffectiveGen traffic for a key until older traffic drains.
type Handoff struct {
	FromBroker   string `json:"from_broker"`
	ToBroker     string `json:"to_broker"`
	EffectiveGen int    `json:"effective_gen"`
	Reason       string `json:"reason"`
}

// Topology is a parsed snapshot for one shard. It is the wire event, plus ownership computation.
type Topology struct {
	Version    int         `json:"version"`
	Shard      string      `json:"shard"`
	Generation int         `json:"gen"`
	EmittedAt  string      `json:"emitted_at"`
	Brokers    []BrokerRef `json:"brokers"`
	Handoffs   []Handoff   `json:"handoffs"`
}

// LoadTopology parses a topology snapshot from its wire JSON, rejecting an unknown version.
func LoadTopology(data []byte) (*Topology, error) {
	var t Topology
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&t); err != nil {
		return nil, fmt.Errorf("decode topology event: %w", err)
	}
	if t.Version != Version {
		return nil, fmt.Errorf("unsupported topology version %d (expected %d)", t.Version, Version)
	}
	return &t, nil
}

// Gen returns the snapshot's generation.
func (t *Topology) Gen() int { return t.Generation }

// AllHandoffs returns the in-flight handoffs carried by this snapshot.
func (t *Topology) AllHandoffs() []Handoff { return t.Handoffs }

// ownableBrokers returns the ACTIVE broker ids, sorted, matching Python's ownable_brokers().
func (t *Topology) ownableBrokers() []string {
	ids := make([]string, 0, len(t.Brokers))
	for _, b := range t.Brokers {
		if b.State == StateActive {
			ids = append(ids, b.BrokerID)
		}
	}
	sort.Strings(ids)
	return ids
}

// Owner returns the broker id that owns the partition key, or "" (ok=false) when no broker is
// ownable. Rendezvous: greatest sha256(broker_id + 0x00 + key), ties broken on the greater
// broker_id. Identical to the Python ShardTopology.owner.
func (t *Topology) Owner(key string) (string, bool) {
	ids := t.ownableBrokers()
	if len(ids) == 0 {
		return "", false
	}
	var bestID string
	var bestDigest [sha256.Size]byte
	first := true
	for _, id := range ids {
		h := sha256.New()
		h.Write([]byte(id))
		h.Write([]byte{0})
		h.Write([]byte(key))
		var digest [sha256.Size]byte
		h.Sum(digest[:0])
		if first {
			bestID, bestDigest, first = id, digest, false
			continue
		}
		cmp := bytes.Compare(digest[:], bestDigest[:])
		if cmp > 0 || (cmp == 0 && id > bestID) {
			bestID, bestDigest = id, digest
		}
	}
	return bestID, true
}

// EndpointFor returns the connection URI for a broker id and protocol, or an error if the broker is
// not in the snapshot or has no endpoint for that protocol.
func (t *Topology) EndpointFor(brokerID, protocol string) (string, error) {
	for _, b := range t.Brokers {
		if b.BrokerID == brokerID {
			uri, ok := b.Endpoints[protocol]
			if !ok {
				return "", fmt.Errorf("broker %q has no endpoint for protocol %q", brokerID, protocol)
			}
			return uri, nil
		}
	}
	return "", fmt.Errorf("broker %q not in topology for shard %q", brokerID, t.Shard)
}
