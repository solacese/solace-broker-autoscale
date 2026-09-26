package routing

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestCanonicalContractVectors(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/contract-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var testFile struct {
		Contract     string `json:"contract"`
		LengthPrefix string `json:"length_prefix"`
		Vectors      []struct {
			Name       string   `json:"name"`
			Components []string `json:"components"`
			EncodedHex string   `json:"canonical_hex"`
			DigestHex  string   `json:"sha256"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &testFile); err != nil {
		t.Fatal(err)
	}
	if testFile.Contract != "sha256-length-prefixed-utf8-v1" || testFile.LengthPrefix != "unsigned-32-bit-big-endian-byte-length" {
		t.Fatalf("unexpected vector contract metadata: %+v", testFile)
	}

	for _, test := range testFile.Vectors {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			encoded, err := CanonicalBytes(test.Components...)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(encoded); got != test.EncodedHex {
				t.Fatalf("CanonicalBytes() = %s\nwant %s", got, test.EncodedHex)
			}
			digest, err := SHA256(test.Components...)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(digest[:]); got != test.DigestHex {
				t.Fatalf("SHA256() = %s\nwant %s", got, test.DigestHex)
			}
		})
	}
}

func TestLengthPrefixesDisambiguateComponents(t *testing.T) {
	first, err := SHA256("ab", "c")
	if err != nil {
		t.Fatal(err)
	}
	second, err := SHA256("a", "bc")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("component boundaries did not affect digest")
	}
}

func TestCanonicalContractDoesNotNormalize(t *testing.T) {
	composed, err := SHA256("Café")
	if err != nil {
		t.Fatal(err)
	}
	decomposed, err := SHA256("Café")
	if err != nil {
		t.Fatal(err)
	}
	if composed == decomposed {
		t.Fatal("contract unexpectedly normalized Unicode")
	}
	upper, _ := SHA256("BA")
	lower, _ := SHA256("ba")
	if upper == lower {
		t.Fatal("contract unexpectedly folded case")
	}
}

func TestCanonicalContractRejectsInvalidUTF8(t *testing.T) {
	invalid := string([]byte{0xff})
	if utf8.ValidString(invalid) {
		t.Fatal("test setup produced valid UTF-8")
	}
	if _, err := CanonicalBytes(invalid); err == nil {
		t.Fatal("CanonicalBytes accepted invalid UTF-8")
	}
	if _, err := AppendUTF8(nil, invalid); err == nil {
		t.Fatal("AppendUTF8 accepted invalid UTF-8")
	}
}

func TestParseSHA256CanonicalHex(t *testing.T) {
	const value = "979decdda63987635de3ff054b3c914662d5e4330eef4ce0d3a31ab616aafa96"
	digest, err := ParseSHA256(value)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(digest[:]) != value {
		t.Fatalf("ParseSHA256() = %x", digest)
	}
	for _, invalid := range []string{"", value[:63], strings.ToUpper(value), strings.Repeat("z", 64)} {
		if _, err := ParseSHA256(invalid); err == nil {
			t.Fatalf("ParseSHA256(%q) succeeded", invalid)
		}
	}
}

func TestExampleHelpersMatchSharedVectors(t *testing.T) {
	flight, err := FlightOperationsHash("BA", "117", "2026-09-25", "LHR-JFK")
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(flight[:]); got != "979decdda63987635de3ff054b3c914662d5e4330eef4ce0d3a31ab616aafa96" {
		t.Fatalf("FlightOperationsHash() = %s", got)
	}
	bag, err := BaggageHash("BA", "0123456789")
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(bag[:]); got != "7e955126e2c5fface0697e4d75b986f54c4e83cadf1cde449bde040c8618ffc9" {
		t.Fatalf("BaggageHash() = %s", got)
	}
}

func TestExampleHelpersRequireCanonicalComponents(t *testing.T) {
	for name, call := range map[string]func() error{
		"missing flight carrier": func() error { _, err := FlightOperationsHash("", "117", "2026-09-25", "LHR-JFK"); return err },
		"missing leg":            func() error { _, err := FlightOperationsHash("BA", "117", "2026-09-25", ""); return err },
		"missing bag tag":        func() error { _, err := BaggageHash("BA", ""); return err },
		"NUL":                    func() error { _, err := BaggageHash("BA", "12\x003"); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); err == nil {
				t.Fatal("helper accepted invalid component")
			}
		})
	}
}
