// Package customer defines the small application-owned routing contract.
package customer

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

const (
	// DigestBytes is the digest width used by the v1 message metadata contract.
	// Customer implementations may use any deterministic hash and canonically
	// expand or pad it into this 32-byte wire container. Changing that mapping
	// requires a new hash and routing contract handshake.
	DigestBytes = 32
	// EntityAffinityContract is the default business-hash contract.
	EntityAffinityContract = "entity-affinity-v1"
	// RendezvousSHA256Contract is the default score contract. Its canonical input
	// and output are byte-identical to the original swlb-rendezvous-v1 mapping.
	RendezvousSHA256Contract = "swlb-rendezvous-v1"
	entityAffinityDomain     = "entity-affinity-v1"
)

// BusinessHash is the opaque customer-computed 256-bit business-key digest.
type BusinessHash [DigestBytes]byte

// RoutingScore is an opaque customer-computed rendezvous score. The router only
// compares scores as unsigned big-endian values; it never chooses a hash.
type RoutingScore [DigestBytes]byte

// ScoreInput is the complete endpoint- and epoch-independent input supplied by
// the router to the customer library for one candidate broker.
type ScoreInput struct {
	Algorithm    string
	ScalingGroup string
	BusinessHash BusinessHash
	BrokerID     string
}

// MessageView is a borrowed, read-only view of a message.
//
// Topic and EventID strings may be retained because strings are immutable.
// Headers, Payload, and any objects reachable through Payload remain owned by
// the caller. A CustomerLibrary implementation must not mutate or retain them
// after a method returns. Payload is intentionally any so applications can
// evaluate their native message before serialization.
type MessageView struct {
	Topic   string
	Headers map[string]string
	Payload any
	EventID string
}

// Header returns a borrowed header value without modifying the message.
func (m MessageView) Header(name string) (string, bool) {
	value, ok := m.Headers[name]
	return value, ok
}

// CustomerLibrary is implemented by application code and owns every hashing
// choice. Implementations must also satisfy RoutingHashPolicy. The router
// supplies candidates and performs highest-score selection; it does not import
// or select a cryptographic implementation.
//
// HashContract and RoutingAlgorithm are versioned handshakes. Changing either,
// GetBusinessHash, or GetRendezvousScore requires a coordinated migration after
// old durable records have been drained; an implementation must not hot-swap
// their meaning under an existing identifier.
type CustomerLibrary interface {
	GetScalingGroup(MessageView) (string, error)
	GetBusinessHash(MessageView) (BusinessHash, error)
}

// RoutingHashPolicy is the customer-owned score contract used by the router.
type RoutingHashPolicy interface {
	GetRendezvousScore(ScoreInput) (RoutingScore, error)
	RoutingAlgorithm() string
}

// CustomerLibraryFuncs adapts functions to CustomerLibrary. It is useful for
// small customer libraries while preserving the named method contract.
type CustomerLibraryFuncs struct {
	ScalingGroup    func(MessageView) (string, error)
	BusinessHash    func(MessageView) (BusinessHash, error)
	RendezvousScore func(ScoreInput) (RoutingScore, error)
	Algorithm       string
}

func (f CustomerLibraryFuncs) GetScalingGroup(message MessageView) (string, error) {
	if f.ScalingGroup == nil {
		return "", errors.New("customer: GetScalingGroup is not configured")
	}
	return f.ScalingGroup(message)
}

func (f CustomerLibraryFuncs) GetBusinessHash(message MessageView) (BusinessHash, error) {
	if f.BusinessHash == nil {
		return BusinessHash{}, errors.New("customer: GetBusinessHash is not configured")
	}
	return f.BusinessHash(message)
}

func (f CustomerLibraryFuncs) GetRendezvousScore(input ScoreInput) (RoutingScore, error) {
	if f.RendezvousScore == nil {
		return SHA256RendezvousScore(input)
	}
	return f.RendezvousScore(input)
}

func (f CustomerLibraryFuncs) RoutingAlgorithm() string {
	if f.Algorithm == "" {
		return RendezvousSHA256Contract
	}
	return f.Algorithm
}

func RoutingPolicy(library CustomerLibrary) (RoutingHashPolicy, error) {
	policy, ok := library.(RoutingHashPolicy)
	if !ok {
		return nil, errors.New("customer: library does not implement RoutingHashPolicy")
	}
	if policy.RoutingAlgorithm() == "" {
		return nil, errors.New("customer: routing algorithm is required")
	}
	return policy, nil
}

// EntityHash implements the default business hash over canonical, length-prefixed
// UTF-8 components.
func EntityHash(group, entityID string) (BusinessHash, error) {
	for index, component := range []string{group, entityID} {
		if component == "" {
			return BusinessHash{}, fmt.Errorf("customer: entity affinity component %d is required", index)
		}
		if strings.IndexByte(component, 0) >= 0 {
			return BusinessHash{}, fmt.Errorf("customer: entity affinity component %d contains NUL", index)
		}
	}
	encoded, err := canonicalStrings(entityAffinityDomain, group, entityID)
	if err != nil {
		return BusinessHash{}, err
	}
	return sha256.Sum256(encoded), nil
}

// SHA256RendezvousScore implements the default score mapping. The canonical
// sequence is algorithm ID, scaling group, raw business hash, then broker ID.
func SHA256RendezvousScore(input ScoreInput) (RoutingScore, error) {
	if input.Algorithm != RendezvousSHA256Contract {
		return RoutingScore{}, fmt.Errorf("customer: unsupported default routing algorithm %q", input.Algorithm)
	}
	if input.ScalingGroup == "" || input.BrokerID == "" || !utf8.ValidString(input.ScalingGroup) || !utf8.ValidString(input.BrokerID) {
		return RoutingScore{}, errors.New("customer: rendezvous group and broker ID must be non-empty valid UTF-8")
	}
	encoded, err := canonicalBytes(
		[]byte(input.Algorithm),
		[]byte(input.ScalingGroup),
		input.BusinessHash[:],
		[]byte(input.BrokerID),
	)
	if err != nil {
		return RoutingScore{}, err
	}
	return sha256.Sum256(encoded), nil
}

// EntityCustomerLibrary is the default generic application policy. It keeps
// events for one entity together within events-a or events-b and owns both v1
// SHA-256 mappings. Runtime code only injects this library; it chooses no hash.
type EntityCustomerLibrary struct{}

func (EntityCustomerLibrary) GetScalingGroup(message MessageView) (string, error) {
	group := message.Headers["scaling-group"]
	if group != "events-a" && group != "events-b" {
		return "", fmt.Errorf("customer: unsupported scaling group %q", group)
	}
	return group, nil
}

func (EntityCustomerLibrary) GetBusinessHash(message MessageView) (BusinessHash, error) {
	return EntityHash(message.Headers["scaling-group"], message.Headers["entity_id"])
}

func (EntityCustomerLibrary) GetRendezvousScore(input ScoreInput) (RoutingScore, error) {
	return SHA256RendezvousScore(input)
}

func (EntityCustomerLibrary) RoutingAlgorithm() string { return RendezvousSHA256Contract }

// ParseBusinessHash parses the lowercase wire and durable representation.
func ParseBusinessHash(value string) (BusinessHash, error) {
	var digest BusinessHash
	if len(value) != hex.EncodedLen(len(digest)) {
		return digest, fmt.Errorf("customer: business hash must contain exactly %d lowercase hex digits", hex.EncodedLen(len(digest)))
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || hex.EncodeToString(decoded) != value {
		return digest, fmt.Errorf("customer: business hash must contain exactly %d lowercase hex digits", hex.EncodedLen(len(digest)))
	}
	copy(digest[:], decoded)
	return digest, nil
}

func hashCanonicalStrings(components ...string) (BusinessHash, error) {
	encoded, err := canonicalStrings(components...)
	if err != nil {
		return BusinessHash{}, err
	}
	return sha256.Sum256(encoded), nil
}

func canonicalStrings(components ...string) ([]byte, error) {
	values := make([][]byte, len(components))
	for index, component := range components {
		if !utf8.ValidString(component) {
			return nil, errors.New("customer: canonical component is not valid UTF-8")
		}
		values[index] = []byte(component)
	}
	return canonicalBytes(values...)
}

func canonicalBytes(components ...[]byte) ([]byte, error) {
	total := 0
	for _, component := range components {
		if uint64(len(component)) > math.MaxUint32 || total > math.MaxInt-4-len(component) {
			return nil, errors.New("customer: canonical input is too large")
		}
		total += 4 + len(component)
	}
	encoded := make([]byte, 0, total)
	var length [4]byte
	for _, component := range components {
		binary.BigEndian.PutUint32(length[:], uint32(len(component)))
		encoded = append(encoded, length[:]...)
		encoded = append(encoded, component...)
	}
	return encoded, nil
}
