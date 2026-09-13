// Package rules is the pure dispatch rule engine for the smart shim.
//
// Given a message (topic, payload) and an ordered list of rules, it decides which broker the
// message goes to, a stable partition/group key derived from the topic and payload, and the address
// to publish to. A rule fires when its topic pattern matches AND every payload predicate matches
// (AND semantics); rules are evaluated in order and the first match wins; if none match, the default
// broker applies. This is deliberately not round-robin: the same message always routes the same way,
// so per-key ordering holds and the listener side can reconstruct a coherent stream.
//
// The engine is a straight port of the Python engine under
// scaling-controller/solace_autoscale/dispatch/ and reads the identical portable rule spec, proven
// by a cross-language golden test (see spec_test.go). No I/O, no clock, no logging.
package rules

import "strings"

// TopicMatches reports whether topic matches a Solace-style pattern using "*" (exactly one level)
// and ">" (this level and every level after it; must be the final token). Levels are "/"-separated.
// A pattern with no wildcards is an exact match.
func TopicMatches(pattern, topic string) bool {
	if pattern == topic {
		return true
	}
	pat := strings.Split(pattern, "/")
	top := strings.Split(topic, "/")
	for i, seg := range pat {
		if seg == ">" {
			return i < len(top) // matches one-or-more remaining levels
		}
		if i >= len(top) {
			return false
		}
		if seg == "*" {
			continue
		}
		if seg != top[i] {
			return false
		}
	}
	return len(pat) == len(top)
}
