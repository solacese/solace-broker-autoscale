// Package routing performs hash-neutral broker selection. Customer code owns
// business hashing, score hashing, and their version identifiers.
package routing

import "github.com/solacese/solace-workload-balancer/customer"

// BusinessHash is the fixed-width v1 wire container. Its contents are opaque to
// routing and need not be a SHA-256 digest.
type BusinessHash = customer.BusinessHash
