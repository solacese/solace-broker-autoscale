package routing

import (
	"testing"

	"github.com/solacese/solace-workload-balancer/customer"
)

func BenchmarkEntityHash(b *testing.B) {
	for b.Loop() {
		if _, err := customer.EntityHash("events-a", "entity-001"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBrokerForHash(b *testing.B) {
	digest, err := customer.EntityHash("events-a", "entity-001")
	if err != nil {
		b.Fatal(err)
	}
	membership := []string{"broker-a", "broker-b", "broker-c"}
	b.ResetTimer()
	for b.Loop() {
		if _, err := RendezvousBroker(shaPolicy{}, "benchmark", digest, membership); err != nil {
			b.Fatal(err)
		}
	}
}
