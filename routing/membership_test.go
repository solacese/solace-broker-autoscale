package routing

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/solacese/solace-workload-balancer/customer"
)

type shaPolicy struct{}

func (shaPolicy) RoutingAlgorithm() string { return customer.RendezvousSHA256Contract }
func (shaPolicy) GetRendezvousScore(input customer.ScoreInput) (customer.RoutingScore, error) {
	return customer.SHA256RendezvousScore(input)
}

func TestRendezvousFixedVectors(t *testing.T) {
	digest := sha256.Sum256([]byte("business-key-1"))
	tests := []struct{ broker, score string }{
		{"broker-a", "f749849441949ad882ad555376408fe9edcc228df0180b3cbb24ef21b20dedbb"},
		{"broker-b", "27c0ce334c817ba570915283d23a5953c299de31fa0ccc21ceb9ce1884efb018"},
	}
	for _, test := range tests {
		score, err := shaPolicy{}.GetRendezvousScore(customer.ScoreInput{Algorithm: customer.RendezvousSHA256Contract, ScalingGroup: "events-a", BusinessHash: digest, BrokerID: test.broker})
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", score); got != test.score {
			t.Fatalf("score(%s) = %s", test.broker, got)
		}
	}
}

func TestRendezvousPlacementProperties(t *testing.T) {
	membership := []string{"broker-a", "broker-b", "broker-c"}
	reordered := []string{"broker-c", "broker-a", "broker-b"}
	before := slices.Clone(membership)
	for i := range 10000 {
		hash := sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
		first, err := RendezvousBroker(shaPolicy{}, "orders", hash, membership)
		if err != nil {
			t.Fatal(err)
		}
		second, err := RendezvousBroker(shaPolicy{}, "orders", hash, reordered)
		if err != nil {
			t.Fatal(err)
		}
		if first != second {
			t.Fatalf("membership reorder moved key %d: %s -> %s", i, first, second)
		}
		added, _ := RendezvousBroker(shaPolicy{}, "orders", hash, append(slices.Clone(membership), "broker-d"))
		if added != first && added != "broker-d" {
			t.Fatalf("adding broker moved key between old brokers: %s -> %s", first, added)
		}
		removed, _ := RendezvousBroker(shaPolicy{}, "orders", hash, membership[:2])
		if first != "broker-c" && removed != first {
			t.Fatalf("removing other broker moved key: %s -> %s", first, removed)
		}
	}
	if !slices.Equal(membership, before) {
		t.Fatal("membership mutated")
	}
}

func TestRendezvousDistributionAnd200To201Movement(t *testing.T) {
	membership := make([]string, 201)
	for i := range membership {
		membership[i] = fmt.Sprintf("broker-%03d", i)
	}
	const keys = 100000
	counts := make(map[string]int)
	moved := 0
	for i := range keys {
		hash := sha256.Sum256([]byte(fmt.Sprintf("synthetic-%d", i)))
		before, _ := RendezvousBroker(shaPolicy{}, "simulation", hash, membership[:200])
		after, _ := RendezvousBroker(shaPolicy{}, "simulation", hash, membership)
		counts[after]++
		if before != after {
			if after != membership[200] {
				t.Fatal("key moved to existing broker")
			}
			moved++
		}
	}
	expected := float64(keys) / 201
	if math.Abs(float64(moved)-expected) > 5*math.Sqrt(expected) {
		t.Fatalf("movement = %d, expected about %.1f", moved, expected)
	}
	mean := float64(keys) / 201
	for broker, count := range counts {
		if math.Abs(float64(count)-mean) > 6*math.Sqrt(mean) {
			t.Fatalf("%s count=%d outside tolerance", broker, count)
		}
	}
}

type alternativeHashPolicy struct{}

func (alternativeHashPolicy) RoutingAlgorithm() string { return "test-xor-rendezvous-v1" }
func (alternativeHashPolicy) GetRendezvousScore(input customer.ScoreInput) (customer.RoutingScore, error) {
	var score customer.RoutingScore
	for index, value := range []byte(input.ScalingGroup + "\x00" + input.BrokerID) {
		score[index%len(score)] ^= value
	}
	for index, value := range input.BusinessHash {
		score[index] ^= value
	}
	return score, nil
}

func TestRendezvousUsesInjectedCustomerHash(t *testing.T) {
	policy := alternativeHashPolicy{}
	hash := sha256.Sum256([]byte("customer-key"))
	membership := []string{"broker-a", "broker-b", "broker-c"}
	got, err := RendezvousBroker(policy, "events-a", hash, membership)
	if err != nil {
		t.Fatal(err)
	}
	want := ""
	var highest customer.RoutingScore
	for _, broker := range membership {
		score, scoreErr := policy.GetRendezvousScore(customer.ScoreInput{Algorithm: policy.RoutingAlgorithm(), ScalingGroup: "events-a", BusinessHash: hash, BrokerID: broker})
		if scoreErr != nil {
			t.Fatal(scoreErr)
		}
		if want == "" || bytes.Compare(score[:], highest[:]) > 0 || bytes.Equal(score[:], highest[:]) && broker < want {
			want, highest = broker, score
		}
	}
	if got != want {
		t.Fatalf("RendezvousBroker() = %q, want injected result %q", got, want)
	}
}

func TestRendezvousRejectsInvalidMembership(t *testing.T) {
	if _, err := RendezvousBroker(shaPolicy{}, "orders", BusinessHash{}, nil); err == nil {
		t.Fatal("accepted empty membership")
	}
	if _, err := RendezvousBroker(shaPolicy{}, "orders", BusinessHash{}, []string{"a", "a"}); err == nil {
		t.Fatal("accepted duplicate broker")
	}
}
