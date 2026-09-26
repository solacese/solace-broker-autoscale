// Package customer defines the small application-owned routing contract.
package customer

import (
	"crypto/sha256"
	"errors"
)

// BusinessHash is the exact SHA-256 value used by the routing contract.
// It is an alias so hashes produced by customer code can be passed to routing
// without conversion.
type BusinessHash = [sha256.Size]byte

// MessageView is a borrowed, read-only view of a message.
//
// Topic and EventID strings may be retained because strings are immutable.
// Headers, Payload, and any objects reachable through Payload remain owned by
// the caller. A CustomerLibrary implementation must not mutate or retain them
// after GetScalingGroup or GetBusinessHash returns. Payload is intentionally
// any so applications can evaluate their native message before serialization.
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

// CustomerLibrary is implemented by application code and must be deterministic.
// Both methods may inspect the whole borrowed message, but they must not select
// or otherwise depend on a broker. The same implementation and version must be
// deployed to publishers and subscribers.
type CustomerLibrary interface {
	GetScalingGroup(MessageView) (string, error)
	GetBusinessHash(MessageView) (BusinessHash, error)
}

// CustomerLibraryFuncs adapts two functions to CustomerLibrary. It is useful
// for small customer libraries while preserving the named method contract.
type CustomerLibraryFuncs struct {
	ScalingGroup func(MessageView) (string, error)
	BusinessHash func(MessageView) (BusinessHash, error)
}

// GetScalingGroup calls ScalingGroup.
func (f CustomerLibraryFuncs) GetScalingGroup(message MessageView) (string, error) {
	if f.ScalingGroup == nil {
		return "", errors.New("customer: GetScalingGroup is not configured")
	}
	return f.ScalingGroup(message)
}

// GetBusinessHash calls BusinessHash.
func (f CustomerLibraryFuncs) GetBusinessHash(message MessageView) (BusinessHash, error) {
	if f.BusinessHash == nil {
		return BusinessHash{}, errors.New("customer: GetBusinessHash is not configured")
	}
	return f.BusinessHash(message)
}
