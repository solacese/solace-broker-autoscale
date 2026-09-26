package qualification

import "testing"

func TestSupportedJavaVersion(t *testing.T) {
	for _, output := range []string{
		`java version "1.8.0_503"`,
		`openjdk version "17.0.12" 2024-07-16`,
		`openjdk version "25.0.2" 2026-01-20`,
	} {
		if !supportedJavaVersion(output) {
			t.Fatalf("supportedJavaVersion(%q) = false", output)
		}
	}
	for _, output := range []string{`java version "1.7.0_80"`, `garbled`, ``} {
		if supportedJavaVersion(output) {
			t.Fatalf("supportedJavaVersion(%q) = true", output)
		}
	}
}
