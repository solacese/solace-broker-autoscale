package routing

import "testing"

func BenchmarkFlightOperationsHash(b *testing.B) {
	for b.Loop() {
		_, err := FlightOperationsHash("UA", "123", "2026-09-25", "ORD-LAX")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBrokerForHash(b *testing.B) {
	digest, err := FlightOperationsHash("UA", "123", "2026-09-25", "ORD-LAX")
	if err != nil {
		b.Fatal(err)
	}
	membership := []string{"broker-a", "broker-b", "broker-c"}
	b.ResetTimer()
	for b.Loop() {
		if _, err := BrokerForHash(digest, membership); err != nil {
			b.Fatal(err)
		}
	}
}
