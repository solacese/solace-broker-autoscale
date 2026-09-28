// Command sdkperf-provision creates the two bounded, test-only SDKPerf queues on
// an existing broker. It is not built or invoked by the production runtime.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/solacese/solace-workload-balancer/semp"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	flags := flag.NewFlagSet("sdkperf-provision", flag.ContinueOnError)
	endpoint := flags.String("semp-endpoint", "", "SEMP HTTPS endpoint")
	vpn := flags.String("vpn", "", "message VPN")
	usernameEnv := flags.String("username-env", "", "SEMP username environment variable")
	passwordEnv := flags.String("password-env", "", "SEMP password environment variable")
	owner := flags.String("owner", "", "existing SDKPerf client username")
	ingressQueue := flags.String("ingress-queue", "", "test ingress queue")
	ingressTopic := flags.String("ingress-topic", "", "test ingress topic")
	resultQueue := flags.String("result-queue", "", "test result queue")
	resultTopic := flags.String("result-topic", "", "test result topic prefix")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *endpoint == "" || *vpn == "" || *usernameEnv == "" || *passwordEnv == "" || *owner == "" || *ingressQueue == "" || *ingressTopic == "" || *resultQueue == "" || *resultTopic == "" {
		return errors.New("sdkperf-provision: all flags are required")
	}
	username, usernameOK := os.LookupEnv(*usernameEnv)
	password, passwordOK := os.LookupEnv(*passwordEnv)
	if !usernameOK || !passwordOK || username == "" {
		return errors.New("sdkperf-provision: credential environment variables are unavailable")
	}
	client, err := semp.NewClient(semp.ClientOptions{BaseURL: *endpoint, Username: username, Password: password})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, item := range []struct{ queue, topic string }{{*ingressQueue, *ingressTopic}, {*resultQueue, *resultTopic + "/>"}} {
		spec := semp.QueueSpec{
			MessageVPN: *vpn, Name: item.queue, AccessType: "exclusive", Owner: *owner, Permission: "consume",
			IngressEnabled: true, EgressEnabled: true, MaxMsgSpoolUsage: 100, MaxRedeliveryCount: 3,
			ConsumerAckPropagationEnabled: true, MaxDeliveredUnackedMsgsPerFlow: 1000,
			RejectMsgToSenderOnDiscardBehavior: "always",
		}
		if err := client.EnsureQueue(ctx, spec); err != nil {
			return err
		}
		if err := client.CreateSubscription(ctx, *vpn, item.queue, item.topic); err != nil {
			return err
		}
	}
	fmt.Println("SDKPerf test queues are ready")
	return nil
}
