package rules

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	templateRe = regexp.MustCompile(`\{([^{}]+)\}`)
	dotRunRe   = regexp.MustCompile(`\.{2,}`)
)

// renderTemplate fills {topic} and {dotted.path} placeholders. Absent or null fields render as the
// empty string. Consecutive separators left by an empty field are collapsed, so "vip.{region}.{tier}"
// with a missing tier yields "vip.region" rather than "vip.region.".
func renderTemplate(template, topic string, decoded any) string {
	out := templateRe.ReplaceAllStringFunc(template, func(m string) string {
		ref := m[1 : len(m)-1] // strip the braces
		if ref == "topic" {
			return topic
		}
		val := getPath(decoded, ref)
		if isMissing(val) || val == nil {
			return ""
		}
		return scalarString(val)
	})
	out = dotRunRe.ReplaceAllString(out, ".")
	return strings.Trim(out, ".")
}

// scalarString renders a decoded JSON scalar the way Python's str() would for the values a template
// can reference (strings, numbers, booleans).
func scalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "True" // match Python str(True)
		}
		return "False"
	default:
		if s := toStr(v); s != "" {
			return s
		}
		if f, ok := asFloat(v); ok {
			// Should not be reached for json.Number (handled by toStr); guard for float64 literals.
			return strings.TrimRight(strings.TrimRight(formatFloat(f), "0"), ".")
		}
		return ""
	}
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
