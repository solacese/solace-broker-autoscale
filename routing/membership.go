package routing

import "errors"

// BrokerIndex maps the entire unsigned 256-bit, big-endian digest to an index
// in the supplied ordered membership. It is equivalent to
// int(bigEndianUnsigned(digest) % len(membership)) without truncation.
func BrokerIndex(digest BusinessHash, membership []string) (int, error) {
	if len(membership) == 0 {
		return 0, errors.New("routing: broker membership is empty")
	}

	// Streaming modular reduction avoids allocating a big integer and, unlike
	// taking only the first eight bytes, preserves all 256 hash bits.
	modulus := uint64(len(membership))
	var remainder uint64
	for _, octet := range digest {
		// remainder < modulus and len(slice) <= max int, so remainder*256 can
		// overflow uint64 on 64-bit hosts. The identity below consumes one byte
		// through eight doubling steps without overflow.
		for bit := 7; bit >= 0; bit-- {
			remainder = addModulo(remainder, remainder, modulus)
			if octet&(1<<uint(bit)) != 0 {
				remainder = addModulo(remainder, 1, modulus)
			}
		}
	}
	return int(remainder), nil
}

// BrokerForHash returns the member selected by BrokerIndex. The order and
// spelling of membership are authoritative; this function never sorts,
// deduplicates, trims, or case-folds them.
func BrokerForHash(digest BusinessHash, membership []string) (string, error) {
	index, err := BrokerIndex(digest, membership)
	if err != nil {
		return "", err
	}
	return membership[index], nil
}

func addModulo(left, right, modulus uint64) uint64 {
	// left and right are already less than modulus. This computes their sum
	// modulo modulus without overflowing.
	if left >= modulus-right {
		return left - (modulus - right)
	}
	return left + right
}
