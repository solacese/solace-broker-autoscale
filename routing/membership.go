package routing

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/solacese/solace-workload-balancer/customer"
)

// RendezvousBroker orchestrates equal-weight candidate scoring and selects the
// highest full unsigned score. Hash construction belongs entirely to policy;
// routing only validates membership, asks for each score, compares opaque bytes,
// and applies the lexicographically smaller broker-ID collision tie break.
func RendezvousBroker(policy customer.RoutingHashPolicy, group string, businessHash customer.BusinessHash, membership []string) (string, error) {
	if policy == nil {
		return "", errors.New("routing: customer hash policy is required")
	}
	if len(membership) == 0 {
		return "", errors.New("routing: broker membership is empty")
	}
	algorithm := policy.RoutingAlgorithm()
	if algorithm == "" {
		return "", errors.New("routing: customer library returned an empty routing algorithm")
	}
	seen := make(map[string]struct{}, len(membership))
	var selected string
	var highest customer.RoutingScore
	for _, brokerID := range membership {
		if brokerID == "" {
			return "", errors.New("routing: broker ID is empty")
		}
		if _, duplicate := seen[brokerID]; duplicate {
			return "", fmt.Errorf("routing: duplicate broker ID %q", brokerID)
		}
		seen[brokerID] = struct{}{}
		score, err := policy.GetRendezvousScore(customer.ScoreInput{
			Algorithm: algorithm, ScalingGroup: group, BusinessHash: businessHash, BrokerID: brokerID,
		})
		if err != nil {
			return "", fmt.Errorf("routing: score broker %q: %w", brokerID, err)
		}
		comparison := bytes.Compare(score[:], highest[:])
		if selected == "" || comparison > 0 || comparison == 0 && brokerID < selected {
			selected, highest = brokerID, score
		}
	}
	return selected, nil
}
