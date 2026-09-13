package rules

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// SpecVersion is the portable rule-spec shape this package understands. It matches SPEC_VERSION in
// scaling-controller/solace_autoscale/dispatch/spec.py; bump both together when the shape changes in
// a way loaders must gate on.
const SpecVersion = 1

// predicateSpec / matchSpec / routeSpec / ruleSpec / Spec mirror the JSON emitted by the Python
// to_spec. Numbers are kept as json.Number so integer predicate values stay exact and compare the
// same way on both sides.
type predicateSpec struct {
	Op    string `json:"op"`
	Path  string `json:"path,omitempty"`
	Value any    `json:"value,omitempty"` // any JSON type; numbers arrive as json.Number (UseNumber)
}

type matchSpec struct {
	Topic   string          `json:"topic"`
	Payload []predicateSpec `json:"payload,omitempty"`
}

type routeSpec struct {
	Broker string `json:"broker"`
	Key    string `json:"key,omitempty"`
	Topic  string `json:"topic,omitempty"`
}

type ruleSpec struct {
	Name  string    `json:"name"`
	When  matchSpec `json:"when"`
	Route routeSpec `json:"route"`
}

// Spec is a portable rule spec: a default broker plus an ordered list of rules.
type Spec struct {
	Version       int        `json:"version"`
	DefaultBroker string     `json:"default_broker"`
	Rules         []ruleSpec `json:"rules"`
}

// LoadSpec parses a portable rule spec (the JSON produced by the Python to_spec, or hand-authored to
// the same shape) into a ready-to-use Plan. Numeric values inside predicates are preserved as
// json.Number so they compare exactly against decoded payloads.
func LoadSpec(data []byte) (*Plan, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var s Spec
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("parse rule spec: %w", err)
	}
	if s.Version != 0 && s.Version != SpecVersion {
		return nil, fmt.Errorf("unsupported rule spec version %d (this shim understands %d)", s.Version, SpecVersion)
	}
	rs := make([]Rule, 0, len(s.Rules))
	for i, r := range s.Rules {
		if r.Route.Broker == "" {
			return nil, fmt.Errorf("rule %d (%q): route.broker is required", i, r.Name)
		}
		preds := make([]Predicate, 0, len(r.When.Payload))
		for j, p := range r.When.Payload {
			if !operators[p.Op] {
				return nil, fmt.Errorf("rule %q predicate %d: unknown operator %q", r.Name, j, p.Op)
			}
			preds = append(preds, Predicate{Path: p.Path, Op: p.Op, Value: p.Value})
		}
		topic := r.When.Topic
		if topic == "" {
			topic = ">"
		}
		rs = append(rs, Rule{
			Name:  r.Name,
			When:  Match{Topic: topic, Payload: preds},
			Route: Route{Broker: r.Route.Broker, Key: r.Route.Key, Topic: r.Route.Topic},
		})
	}
	return NewPlan(rs, s.DefaultBroker), nil
}
