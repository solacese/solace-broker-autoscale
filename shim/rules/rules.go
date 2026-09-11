package rules

import "fmt"

// Match is the condition half of a rule: a topic pattern and zero or more payload predicates (AND).
type Match struct {
	Topic   string
	Payload []Predicate
}

func (m Match) matches(topic string, decoded any, raw []byte) bool {
	if !TopicMatches(m.Topic, topic) {
		return false
	}
	for _, p := range m.Payload {
		if !evalPredicate(p, decoded, raw) {
			return false
		}
	}
	return true
}

// Route is the action half of a rule.
//
//	Broker - target broker name (resolved to an endpoint by the shim).
//	Key    - template for the partition/group key, e.g. "vip.{order.region}"; {topic} and
//	         {path.to.field} placeholders are filled from the topic and decoded payload. Empty means
//	         no key.
//	Topic  - optional template to rewrite the publish address (e.g. "vip/{topic}"); empty means the
//	         original topic is used.
type Route struct {
	Broker string
	Key    string
	Topic  string
}

// Rule is one named dispatch rule.
type Rule struct {
	Name  string
	When  Match
	Route Route
}

// Decision is the dispatch outcome for one message.
type Decision struct {
	Broker  string // target broker name
	Address string // topic to publish to (rewritten or original)
	Key     string // partition/group key, or "" for none
	Rule    string // name of the rule that fired, or "default"
	Matched bool   // whether a real rule fired (vs. the default fallback)
}

// Evaluate routes one message. First matching rule wins; else the default broker. Returns an error
// only when no rule matches and defaultBroker is empty (the message cannot be placed).
func Evaluate(topic string, payload []byte, rs []Rule, defaultBroker string) (Decision, error) {
	decoded := decodeJSON(payload)
	for _, r := range rs {
		if !r.When.matches(topic, decoded, payload) {
			continue
		}
		address := topic
		if r.Route.Topic != "" {
			address = renderTemplate(r.Route.Topic, topic, decoded)
		}
		key := ""
		if r.Route.Key != "" {
			key = renderTemplate(r.Route.Key, topic, decoded)
		}
		return Decision{
			Broker: r.Route.Broker, Address: address,
			Key: key, Rule: r.Name, Matched: true,
		}, nil
	}
	if defaultBroker == "" {
		return Decision{}, fmt.Errorf("no dispatch rule matched topic %q and no default_broker is configured", topic)
	}
	return Decision{Broker: defaultBroker, Address: topic, Rule: "default"}, nil
}

// TargetBrokers is the set of brokers these rules can ever route to, in first-seen order. This is
// what the listener shim must subscribe to.
func TargetBrokers(rs []Rule, defaultBroker string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(b string) {
		if b != "" && !seen[b] {
			seen[b] = true
			out = append(out, b)
		}
	}
	for _, r := range rs {
		add(r.Route.Broker)
	}
	add(defaultBroker)
	return out
}

// Plan is a compiled, reusable dispatch plan: the rules plus the default and the derived target set.
type Plan struct {
	Rules         []Rule
	DefaultBroker string
	targets       []string
}

// NewPlan builds a plan and precomputes its target broker set.
func NewPlan(rs []Rule, defaultBroker string) *Plan {
	return &Plan{
		Rules:         rs,
		DefaultBroker: defaultBroker,
		targets:       TargetBrokers(rs, defaultBroker),
	}
}

// Targets returns a copy of the broker set the plan can route to.
func (p *Plan) Targets() []string {
	out := make([]string, len(p.targets))
	copy(out, p.targets)
	return out
}

// Decide routes one message against the plan.
func (p *Plan) Decide(topic string, payload []byte) (Decision, error) {
	return Evaluate(topic, payload, p.Rules, p.DefaultBroker)
}
