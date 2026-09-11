package rules

import "testing"

func TestTopicMatches(t *testing.T) {
	cases := []struct {
		pattern, topic string
		want           bool
	}{
		{"orders/eu/new", "orders/eu/new", true},
		{"orders/eu/new", "orders/us/new", false},
		{"orders/*/new", "orders/eu/new", true},
		{"orders/*/new", "orders/eu/old", false},
		{"orders/*/new", "orders/eu/new/extra", false}, // * is exactly one level
		{"orders/>", "orders/eu/new", true},
		{"orders/>", "orders", false}, // > needs at least one more level
		{">", "anything/at/all", true},
		{"orders/*/>", "orders/eu/a/b", true},
		{"a/b", "a/b/c", false},
	}
	for _, c := range cases {
		if got := TopicMatches(c.pattern, c.topic); got != c.want {
			t.Errorf("TopicMatches(%q,%q)=%v want %v", c.pattern, c.topic, got, c.want)
		}
	}
}

func pred(path, op string, value any) Predicate { return Predicate{Path: path, Op: op, Value: value} }

func TestEvalPredicateOperators(t *testing.T) {
	payload := []byte(`{"n":10,"s":"hello","tag":"vip","list":["a","b"],"nested":{"x":1},"nul":null}`)
	decoded := decodeJSON(payload)
	cases := []struct {
		name string
		p    Predicate
		want bool
	}{
		{"eq-num", pred("n", "eq", 10), true},
		{"eq-num-miss", pred("n", "eq", 11), false},
		{"ne", pred("n", "ne", 11), true},
		{"in", pred("tag", "in", []any{"vip", "gold"}), true},
		{"nin", pred("tag", "nin", []any{"gold"}), true},
		{"gt", pred("n", "gt", 5), true},
		{"gte-eq", pred("n", "gte", 10), true},
		{"lt", pred("n", "lt", 5), false},
		{"lte", pred("n", "lte", 10), true},
		{"gt-typemismatch", pred("s", "gt", 5), false}, // string vs number -> false, not error
		{"exists", pred("nested.x", "exists", nil), true},
		{"exists-absent", pred("nope", "exists", nil), false},
		{"missing", pred("nope", "missing", nil), true},
		{"missing-present", pred("n", "missing", nil), false},
		{"absent-eq-false", pred("nope", "eq", 1), false}, // missing field -> false for eq
		{"prefix", pred("s", "prefix", "hel"), true},
		{"prefix-nonstring", pred("n", "prefix", "1"), false},
		{"contains-str", pred("s", "contains", "ell"), true},
		{"contains-list", pred("list", "contains", "a"), true},
		{"regex", pred("s", "regex", "^h.*o$"), true},
		{"regex-badpattern", pred("s", "regex", "("), false},
		{"nested-path", pred("nested.x", "eq", 1), true},
	}
	for _, c := range cases {
		if got := evalPredicate(c.p, decoded, payload); got != c.want {
			t.Errorf("%s: evalPredicate(%+v)=%v want %v", c.name, c.p, got, c.want)
		}
	}
}

func TestEvalPredicateRawAndNonJSON(t *testing.T) {
	raw := []byte("BINARYblob-not-json")
	decoded := decodeJSON(raw) // missing sentinel
	if !isMissing(decoded) {
		t.Fatal("non-JSON payload should decode to missing")
	}
	if !evalPredicate(pred("", "raw_size_gt", 5), decoded, raw) {
		t.Error("raw_size_gt should be true for a 19-byte payload > 5")
	}
	if evalPredicate(pred("", "raw_size_lt", 5), decoded, raw) {
		t.Error("raw_size_lt 5 should be false for a 19-byte payload")
	}
	if !evalPredicate(pred("", "raw_prefix", "BINARY"), decoded, raw) {
		t.Error("raw_prefix BINARY should match")
	}
	// a field predicate against a non-JSON payload is simply false
	if evalPredicate(pred("anything", "eq", 1), decoded, raw) {
		t.Error("field predicate on non-JSON payload should be false")
	}
}

func TestListIndexPath(t *testing.T) {
	payload := []byte(`{"items":[{"sku":"A"},{"sku":"B"}]}`)
	decoded := decodeJSON(payload)
	if !evalPredicate(pred("items.1.sku", "eq", "B"), decoded, payload) {
		t.Error("list index path items.1.sku should read B")
	}
	if evalPredicate(pred("items.5.sku", "exists", nil), decoded, payload) {
		t.Error("out-of-range list index should be missing")
	}
}

func TestRenderTemplate(t *testing.T) {
	decoded := decodeJSON([]byte(`{"region":"eu","tier":"gold"}`))
	cases := []struct {
		template, topic string
		want            string
	}{
		{"vip/{topic}", "orders/eu", "vip/orders/eu"},
		{"vip.{region}", "orders/eu", "vip.eu"},
		{"vip.{region}.{tier}", "orders/eu", "vip.eu.gold"},
		{"vip.{region}.{missing}", "orders/eu", "vip.eu"}, // trailing empty collapsed + trimmed
		{"{missing}.{region}", "orders/eu", "eu"},         // leading empty trimmed
		{"a.{missing}.b", "orders/eu", "a.b"},             // interior empty collapsed
	}
	for _, c := range cases {
		if got := renderTemplate(c.template, c.topic, decoded); got != c.want {
			t.Errorf("renderTemplate(%q,%q)=%q want %q", c.template, c.topic, got, c.want)
		}
	}
}

func TestEvaluateFirstMatchWinsAndDefault(t *testing.T) {
	rs := []Rule{
		{Name: "vip", When: Match{Topic: "orders/>", Payload: []Predicate{pred("priority", "eq", "high")}},
			Route: Route{Broker: "broker-vip", Key: "vip.{region}", Topic: "vip/{topic}"}},
		{Name: "any-order", When: Match{Topic: "orders/>"},
			Route: Route{Broker: "broker-order"}},
	}
	// first rule matches
	d, err := Evaluate("orders/eu/new", []byte(`{"priority":"high","region":"eu"}`), rs, "broker-bulk")
	if err != nil {
		t.Fatal(err)
	}
	if d.Broker != "broker-vip" || d.Address != "vip/orders/eu/new" || d.Key != "vip.eu" || !d.Matched || d.Rule != "vip" {
		t.Errorf("unexpected decision: %+v", d)
	}
	// second rule wins when first predicate fails
	d, _ = Evaluate("orders/eu/new", []byte(`{"priority":"low"}`), rs, "broker-bulk")
	if d.Broker != "broker-order" || d.Rule != "any-order" || d.Address != "orders/eu/new" || d.Key != "" {
		t.Errorf("expected any-order fallback: %+v", d)
	}
	// default when nothing matches
	d, _ = Evaluate("events/x", nil, rs, "broker-bulk")
	if d.Broker != "broker-bulk" || d.Matched || d.Rule != "default" {
		t.Errorf("expected default: %+v", d)
	}
}

func TestEvaluateNoRouteError(t *testing.T) {
	rs := []Rule{{Name: "r", When: Match{Topic: "orders/>"}, Route: Route{Broker: "b"}}}
	if _, err := Evaluate("events/x", nil, rs, ""); err == nil {
		t.Error("expected error when nothing matches and no default broker")
	}
}

func TestTargetBrokers(t *testing.T) {
	rs := []Rule{
		{Route: Route{Broker: "a"}},
		{Route: Route{Broker: "b"}},
		{Route: Route{Broker: "a"}}, // dup
	}
	got := TargetBrokers(rs, "c")
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}
