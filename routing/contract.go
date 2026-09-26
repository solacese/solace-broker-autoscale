// Package routing implements the language-neutral business-hash and broker
// selection contracts. Membership order is authoritative and must never be
// sorted or otherwise normalized by a client.
package routing

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

const (
	// LengthPrefixBytes is the width of every canonical component length.
	LengthPrefixBytes = 4
	// DigestBytes is the size of a SHA-256 business hash.
	DigestBytes = sha256.Size
)

// BusinessHash is an exact SHA-256 digest.
type BusinessHash = [DigestBytes]byte

// AppendUTF8 appends one canonical component: its unsigned 32-bit big-endian
// byte length followed by its unmodified UTF-8 bytes. No Unicode
// normalization, case folding, trimming, delimiter escaping, or sorting is
// performed.
func AppendUTF8(dst []byte, value string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return nil, errors.New("routing: canonical component is not valid UTF-8")
	}
	if uint64(len(value)) > math.MaxUint32 {
		return nil, errors.New("routing: canonical component exceeds uint32 length")
	}
	var prefix [LengthPrefixBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(value)))
	dst = append(dst, prefix[:]...)
	dst = append(dst, value...)
	return dst, nil
}

// CanonicalBytes encodes components in their supplied order using AppendUTF8.
func CanonicalBytes(components ...string) ([]byte, error) {
	total := 0
	for _, component := range components {
		if !utf8.ValidString(component) {
			return nil, errors.New("routing: canonical component is not valid UTF-8")
		}
		if uint64(len(component)) > math.MaxUint32 {
			return nil, errors.New("routing: canonical component exceeds uint32 length")
		}
		if len(component) > math.MaxInt-LengthPrefixBytes-total {
			return nil, errors.New("routing: canonical encoding is too large")
		}
		total += LengthPrefixBytes + len(component)
	}

	encoded := make([]byte, 0, total)
	for _, component := range components {
		// Values were checked above, so this cannot fail.
		encoded, _ = AppendUTF8(encoded, component)
	}
	return encoded, nil
}

// SHA256 hashes the canonical length-prefixed UTF-8 encoding of components.
func SHA256(components ...string) (BusinessHash, error) {
	encoded, err := CanonicalBytes(components...)
	if err != nil {
		return BusinessHash{}, err
	}
	return sha256.Sum256(encoded), nil
}

// ParseSHA256 parses the canonical lowercase hexadecimal form of a SHA-256
// digest. Uppercase and other equivalent spellings are deliberately rejected.
func ParseSHA256(value string) (BusinessHash, error) {
	var digest BusinessHash
	if len(value) != hex.EncodedLen(len(digest)) {
		return digest, fmt.Errorf("routing: SHA-256 hex must contain exactly %d lowercase digits", hex.EncodedLen(len(digest)))
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || hex.EncodeToString(decoded) != value {
		return digest, fmt.Errorf("routing: SHA-256 hex must contain exactly %d lowercase digits", hex.EncodedLen(len(digest)))
	}
	copy(digest[:], decoded)
	return digest, nil
}
