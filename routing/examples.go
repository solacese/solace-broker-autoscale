package routing

import (
	"fmt"
	"strings"
)

// Domain separators used by the example contracts. Versioning the domain lets
// an application intentionally introduce a new hash contract without silently
// changing placement for existing messages.
const (
	FlightOperationsDomain = "flight-operations-v1"
	BaggageDomain          = "baggage-journey-v1"
)

// FlightOperationsHash is an example contract that keeps all events for an
// operating flight leg together. Every value must already be in the canonical
// form defined by the customer schema (for example, an IATA carrier code and
// ISO 8601 scheduled departure date). The helper deliberately does not trim,
// case-fold, parse, or otherwise normalize caller data.
func FlightOperationsHash(carrier, flightNumber, scheduledDepartureDate, legID string) (BusinessHash, error) {
	if err := requireComponents("flight operations", carrier, flightNumber, scheduledDepartureDate, legID); err != nil {
		return BusinessHash{}, err
	}
	return SHA256(FlightOperationsDomain, carrier, flightNumber, scheduledDepartureDate, legID)
}

// BaggageHash is an example contract that keeps a bag's journey together. The
// issuing carrier and bag tag are separate, required, already-canonical
// components, making otherwise ambiguous concatenations unambiguous.
func BaggageHash(issuingCarrier, bagTag string) (BusinessHash, error) {
	if err := requireComponents("baggage journey", issuingCarrier, bagTag); err != nil {
		return BusinessHash{}, err
	}
	return SHA256(BaggageDomain, issuingCarrier, bagTag)
}

func requireComponents(contract string, components ...string) error {
	for index, component := range components {
		if component == "" {
			return fmt.Errorf("routing: %s component %d is required", contract, index)
		}
		if strings.IndexByte(component, 0) >= 0 {
			return fmt.Errorf("routing: %s component %d contains NUL", contract, index)
		}
	}
	return nil
}
