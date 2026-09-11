package rules

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// Operators usable in a payload predicate. raw_* operate on the undecoded bytes so a rule can match
// non-JSON payloads (e.g. large binary blobs) by size or byte prefix.
var operators = map[string]bool{
	"eq": true, "ne": true, "in": true, "nin": true,
	"gt": true, "gte": true, "lt": true, "lte": true,
	"exists": true, "missing": true, "prefix": true, "contains": true, "regex": true,
	"raw_size_gt": true, "raw_size_lt": true, "raw_prefix": true,
}

// missing is the sentinel for "path not present". A distinct pointer value so it can be compared by
// identity and never collides with a real decoded JSON value (which is never this type).
type missingT struct{}

var missing = missingT{}

func isMissing(v any) bool {
	_, ok := v.(missingT)
	return ok
}

// getPath reads a dotted path out of a decoded JSON value. "order.region" reads
// obj["order"]["region"]; a numeric segment indexes a list ("items.0.sku"). Returns the missing
// sentinel if any segment is absent, so callers can distinguish "present and null" from "absent".
func getPath(obj any, path string) any {
	cur := obj
	if path == "" {
		return cur
	}
	for _, seg := range strings.Split(path, ".") {
		switch c := cur.(type) {
		case map[string]any:
			next, ok := c[seg]
			if !ok {
				return missing
			}
			cur = next
		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(c) {
				return missing
			}
			cur = c[idx]
		default:
			return missing
		}
	}
	return cur
}

// decodeJSON decodes a payload to a JSON value, or returns the missing sentinel if it is not JSON.
// Never errors: a non-JSON payload is a legitimate case handled by the raw_* operators. Numbers
// decode to json.Number so integer comparisons stay exact.
func decodeJSON(payload []byte) any {
	if payload == nil {
		return missing
	}
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return missing
	}
	// Reject trailing content (matches Python json.loads, which parses a single value).
	if dec.More() {
		return missing
	}
	return v
}

// Predicate is one payload condition: <Path> <Op> <Value>. For raw_* operators Path is ignored and
// the operator acts on the raw payload bytes. exists/missing ignore Value.
type Predicate struct {
	Path  string
	Op    string
	Value any
}

// evalPredicate evaluates a single predicate. Pure. Missing fields are false for every op except
// "missing". A type-mismatched comparison (e.g. number vs string) is false, not an error.
func evalPredicate(p Predicate, decoded any, payload []byte) bool {
	switch p.Op {
	case "raw_size_gt":
		return len(payload) > toInt(p.Value)
	case "raw_size_lt":
		return len(payload) < toInt(p.Value)
	case "raw_prefix":
		return strings.HasPrefix(string(payload), toStr(p.Value))
	}

	actual := getPath(decoded, p.Path)

	switch p.Op {
	case "exists":
		return !isMissing(actual)
	case "missing":
		return isMissing(actual)
	}
	if isMissing(actual) {
		return false // any comparison against an absent field is false
	}

	switch p.Op {
	case "eq":
		return jsonEqual(actual, p.Value)
	case "ne":
		return !jsonEqual(actual, p.Value)
	case "in":
		return containsValue(p.Value, actual)
	case "nin":
		return !containsValue(p.Value, actual)
	case "gt", "gte", "lt", "lte":
		return compare(p.Op, actual, p.Value)
	case "prefix":
		s, ok := actual.(string)
		return ok && strings.HasPrefix(s, toStr(p.Value))
	case "contains":
		switch a := actual.(type) {
		case string:
			return strings.Contains(a, toStr(p.Value))
		case []any:
			return containsValue(a, p.Value)
		}
		return false
	case "regex":
		s, ok := actual.(string)
		if !ok {
			return false
		}
		re, err := regexp.Compile(toStr(p.Value))
		if err != nil {
			return false
		}
		return re.FindStringIndex(s) != nil
	}
	return false
}

// jsonEqual compares two decoded/spec JSON values for equality, treating numbers by value across the
// json.Number / float64 / int spelling differences between a decoded payload and a spec literal.
func jsonEqual(a, b any) bool {
	if an, aok := asFloat(a); aok {
		if bn, bok := asFloat(b); bok {
			return an == bn
		}
		return false
	}
	return reflect.DeepEqual(a, b)
}

// containsValue reports whether haystack (a list, or a scalar treated as a one-element list) contains
// needle, using jsonEqual for element comparison.
func containsValue(haystack, needle any) bool {
	switch h := haystack.(type) {
	case []any:
		for _, el := range h {
			if jsonEqual(el, needle) {
				return true
			}
		}
		return false
	default:
		return jsonEqual(haystack, needle)
	}
}

// compare evaluates gt/gte/lt/lte for numbers and strings. A cross-type comparison is false.
func compare(op string, a, b any) bool {
	if af, aok := asFloat(a); aok {
		if bf, bok := asFloat(b); bok {
			return cmpOrder(op, af > bf, af >= bf, af < bf, af <= bf)
		}
		return false
	}
	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			return cmpOrder(op, as > bs, as >= bs, as < bs, as <= bs)
		}
		return false
	}
	return false
}

func cmpOrder(op string, gt, gte, lt, lte bool) bool {
	switch op {
	case "gt":
		return gt
	case "gte":
		return gte
	case "lt":
		return lt
	default: // lte
		return lte
	}
}

// asFloat coerces a decoded JSON number (json.Number), a spec literal (float64/int), or a numeric
// bool-free scalar to float64. Returns ok=false for non-numeric values (including bool).
func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

func toInt(v any) int {
	if f, ok := asFloat(v); ok {
		return int(f)
	}
	if s, ok := v.(string); ok {
		if i, err := strconv.Atoi(s); err == nil {
			return i
		}
	}
	return 0
}

func toStr(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case json.Number:
		return s.String()
	case nil:
		return ""
	default:
		return ""
	}
}
