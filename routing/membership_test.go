package routing

import (
	"encoding/hex"
	"math/big"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestBrokerIndexKnownFullWidthVectors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		hex       string
		members   int
		wantIndex int
	}{
		{hex: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", members: 13, wantIndex: 2},
		{hex: "00000000000000000000000000000000000000000000000000000000000000ff", members: 13, wantIndex: 8},
		{hex: "8000000000000000000000000000000000000000000000000000000000000000", members: 5, wantIndex: 3},
		{hex: "979decdda63987635de3ff054b3c914662d5e4330eef4ce0d3a31ab616aafa96", members: 7, wantIndex: 6},
	}
	for _, test := range tests {
		bytes, err := hex.DecodeString(test.hex)
		if err != nil {
			t.Fatal(err)
		}
		var digest BusinessHash
		copy(digest[:], bytes)
		membership := make([]string, test.members)
		for i := range membership {
			membership[i] = string(rune('A' + i))
		}
		index, err := BrokerIndex(digest, membership)
		if err != nil {
			t.Fatal(err)
		}
		if index != test.wantIndex {
			t.Fatalf("BrokerIndex(%s, %d) = %d, want %d", test.hex, test.members, index, test.wantIndex)
		}
	}
}

func TestBrokerIndexMatchesBigIntModulo(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewPCG(0x52a7, 0xb190))
	for iteration := 0; iteration < 5_000; iteration++ {
		var digest BusinessHash
		for i := range digest {
			digest[i] = byte(random.Uint32())
		}
		count := 1 + random.IntN(10_000)
		membership := make([]string, count)
		got, err := BrokerIndex(digest, membership)
		if err != nil {
			t.Fatal(err)
		}
		integer := new(big.Int).SetBytes(digest[:])
		want := new(big.Int).Mod(integer, big.NewInt(int64(count))).Int64()
		if got != int(want) {
			t.Fatalf("iteration %d: BrokerIndex(%x, %d) = %d, want %d", iteration, digest, count, got, want)
		}
	}
}

func TestBrokerForHashPreservesAuthoritativeOrderAndInput(t *testing.T) {
	digest := BusinessHash{}
	digest[len(digest)-1] = 1
	membership := []string{"broker-z", "broker-a", "broker-m"}
	before := slices.Clone(membership)

	broker, err := BrokerForHash(digest, membership)
	if err != nil {
		t.Fatal(err)
	}
	if broker != "broker-a" {
		t.Fatalf("BrokerForHash() = %q", broker)
	}
	if !slices.Equal(membership, before) {
		t.Fatalf("membership mutated: %v", membership)
	}

	reordered := []string{"broker-a", "broker-z", "broker-m"}
	broker, err = BrokerForHash(digest, reordered)
	if err != nil {
		t.Fatal(err)
	}
	if broker != "broker-z" {
		t.Fatalf("reordered BrokerForHash() = %q", broker)
	}
}

func TestBrokerIndexUsesLowAndHighBits(t *testing.T) {
	membership := []string{"zero", "one", "two", "three", "four"}
	var low BusinessHash
	low[len(low)-1] = 1
	var high BusinessHash
	high[0] = 0x80
	lowIndex, _ := BrokerIndex(low, membership)
	highIndex, _ := BrokerIndex(high, membership)
	if lowIndex != 1 || highIndex != 3 {
		t.Fatalf("indices low/high = %d/%d, want 1/3", lowIndex, highIndex)
	}
}

func TestBrokerIndexRejectsEmptyMembership(t *testing.T) {
	if _, err := BrokerIndex(BusinessHash{}, nil); err == nil {
		t.Fatal("BrokerIndex accepted empty membership")
	}
	if _, err := BrokerForHash(BusinessHash{}, []string{}); err == nil {
		t.Fatal("BrokerForHash accepted empty membership")
	}
}
