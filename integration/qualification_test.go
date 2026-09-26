//go:build integration

package integration_test

import (
	"testing"

	"github.com/solacese/solace-workload-balancer/qualification"
)

// TestLVQBrowserBridgeCapability verifies the local prerequisite for the
// real-broker browse scenario. The Cloud qualification itself proves that two
// independent JCSMP browser processes can retrieve the same retained snapshot
// and that a later browse still sees it.
func TestLVQBrowserBridgeCapability(t *testing.T) {
	options := qualification.Options{JarPath: "../tools/lvq-browser/target/lvq-browser-1.0.0-SNAPSHOT-all.jar"}
	if err := qualification.CheckDefaultCapabilities(options); err != nil {
		t.Fatal(err)
	}
}
