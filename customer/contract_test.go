package customer

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
	data, err := os.ReadFile("../routing/testdata/contract-vectors.json")
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
			encoded, err := canonicalStrings(test.Components...)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(encoded); got != test.EncodedHex {
				t.Fatalf("canonicalStrings() = %s\nwant %s", got, test.EncodedHex)
			}
			digest, err := hashCanonicalStrings(test.Components...)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(digest[:]); got != test.DigestHex {
				t.Fatalf("hashCanonicalStrings() = %s\nwant %s", got, test.DigestHex)
			}
		})
	}
}

func TestLengthPrefixesDisambiguateComponents(t *testing.T) {
	first, err := hashCanonicalStrings("ab", "c")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hashCanonicalStrings("a", "bc")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("component boundaries did not affect digest")
	}
}

func TestCanonicalContractDoesNotNormalize(t *testing.T) {
	composed, err := hashCanonicalStrings("Café")
	if err != nil {
		t.Fatal(err)
	}
	decomposed, err := hashCanonicalStrings("Café")
	if err != nil {
		t.Fatal(err)
	}
	if composed == decomposed {
		t.Fatal("contract unexpectedly normalized Unicode")
	}
	upper, _ := hashCanonicalStrings("BA")
	lower, _ := hashCanonicalStrings("ba")
	if upper == lower {
		t.Fatal("contract unexpectedly folded case")
	}
}

func TestCanonicalContractRejectsInvalidUTF8(t *testing.T) {
	invalid := string([]byte{0xff})
	if utf8.ValidString(invalid) {
		t.Fatal("test setup produced valid UTF-8")
	}
	if _, err := canonicalStrings(invalid); err == nil {
		t.Fatal("CanonicalBytes accepted invalid UTF-8")
	}
}

func TestParseSHA256CanonicalHex(t *testing.T) {
	const value = "979decdda63987635de3ff054b3c914662d5e4330eef4ce0d3a31ab616aafa96"
	digest, err := ParseBusinessHash(value)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(digest[:]) != value {
		t.Fatalf("ParseBusinessHash() = %x", digest)
	}
	for _, invalid := range []string{"", value[:63], strings.ToUpper(value), strings.Repeat("z", 64)} {
		if _, err := ParseBusinessHash(invalid); err == nil {
			t.Fatalf("ParseBusinessHash(%q) succeeded", invalid)
		}
	}
}

func TestEntityHashVectors(t *testing.T) {
	for _, test := range []struct{ group, entity, want string }{{"events-a", "entity-001", "2b482aa21c7203aa5f3354d90353b5f015fc47724c3410ed8d5af1ccc26528bc"}, {"events-b", "entity-002", "b36a1a0e9145dddc2a5fb0be0abec0b0f38e3f71f10f9749925a5acfa3bd810a"}} {
		digest, err := EntityHash(test.group, test.entity)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(digest[:]); got != test.want {
			t.Fatalf("EntityHash(%q,%q)=%s", test.group, test.entity, got)
		}
	}
}
func TestEntityHashRequiresComponents(t *testing.T) {
	for _, values := range [][2]string{{"", "entity"}, {"events-a", ""}, {"events-a", "bad\x00id"}} {
		if _, err := EntityHash(values[0], values[1]); err == nil {
			t.Fatalf("accepted %#v", values)
		}
	}
}
