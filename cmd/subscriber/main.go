package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/solacese/solace-workload-balancer/cmd/internal/participantio"
	"github.com/solacese/solace-workload-balancer/config"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/customer"
	swlbruntime "github.com/solacese/solace-workload-balancer/runtime"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
)

type process interface {
	Run(context.Context) error
}

type assembler func(context.Context, config.Config, swlbruntime.Credentials, string, customer.CustomerLibrary, shimSubscriber.Handler) (process, error)

var assembleProduction assembler = func(ctx context.Context, cfg config.Config, credentials swlbruntime.Credentials, participant string, library customer.CustomerLibrary, handler shimSubscriber.Handler) (process, error) {
	return swlbruntime.AssembleSubscriberParticipant(ctx, cfg, credentials, participant, library, handler, swlbruntime.DefaultParticipantAssembler())
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.LookupEnv, assembleProduction); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, lookup swlbruntime.LookupEnv, assemble assembler) error {
	flags := flag.NewFlagSet("subscriber", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: subscriber -config FILE -participant ID [-validate-only]")
		fmt.Fprintln(stderr, "\nWrite one JSON delivery per line to stdout and read application responses from stdin.")
		fmt.Fprintln(stderr, `Delivery fields: delivery_id, event_id, topic, headers, payload_base64. Respond with {"delivery_id":"...","outcome":"ack|retry|reject|release"}; outcomes are exact and case-sensitive; maximum record size 1 MiB.`)
		fmt.Fprintln(stderr, "ack accepts the native message; retry re-presents the retained message with a new delivery_id; reject and release terminally reject it and unblock its ordered key.")
		fmt.Fprintln(stderr, "\nOptions:")
		flags.PrintDefaults()
	}
	configPath := flags.String("config", "", "YAML configuration file (required)")
	participant := flags.String("participant", "", "subscriber identity declared in the configuration (required)")
	validateOnly := flags.Bool("validate-only", false, "validate configuration and participant without resolving credentials or starting runtime")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *configPath == "" {
		return errors.New("subscriber: -config is required")
	}
	if *participant == "" {
		return errors.New("subscriber: -participant is required")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	groups, err := swlbruntime.ParticipantGroups(cfg, *participant, control.RoleSubscriber)
	if err != nil {
		return err
	}
	if *validateOnly {
		fmt.Fprintf(stdout, "configuration valid: subscriber %s, %d scaling groups\n", *participant, len(groups))
		return nil
	}
	credentials, err := swlbruntime.ResolveParticipantCredentials(cfg, *participant, false, lookup)
	if err != nil {
		return fmt.Errorf("resolve participant credentials: %w", err)
	}
	bridge, err := participantio.NewSubscriberBridge(stdin, stdout)
	if err != nil {
		return err
	}
	if assemble == nil {
		return errors.New("subscriber: production assembler is required")
	}
	process, err := assemble(ctx, cfg, credentials, *participant, swlbruntime.AirlineCustomerLibrary{}, bridge.Handler())
	if err != nil {
		return fmt.Errorf("assemble subscriber participant: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	logger.Info("subscriber participant starting", "participant", *participant, "groups", len(groups))
	if err := process.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("subscriber runtime stopped: %w", err)
	}
	return nil
}
