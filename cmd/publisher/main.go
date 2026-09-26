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
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
)

type process interface {
	Run(context.Context) error
	Accept(customer.MessageView) (shimPublisher.Receipt, error)
}

// assembler is intentionally injectable so validation and lifecycle behavior
// can be tested without opening network connections.
type assembler func(context.Context, config.Config, swlbruntime.Credentials, string, customer.CustomerLibrary) (process, error)

var assembleProduction assembler = func(ctx context.Context, cfg config.Config, credentials swlbruntime.Credentials, participant string, library customer.CustomerLibrary) (process, error) {
	return swlbruntime.AssemblePublisherParticipant(ctx, cfg, credentials, participant, library, swlbruntime.DefaultParticipantAssembler())
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
	flags := flag.NewFlagSet("publisher", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: publisher -config FILE -participant ID [-validate-only]")
		fmt.Fprintln(stderr, "\nRead one JSON object per line from stdin and write durable-acceptance receipts to stdout.")
		fmt.Fprintln(stderr, `Input fields: event_id, topic, headers, payload_base64 (base64 payload; maximum record size 1 MiB).`)
		fmt.Fprintln(stderr, "\nOptions:")
		flags.PrintDefaults()
	}
	configPath := flags.String("config", "", "YAML configuration file (required)")
	participant := flags.String("participant", "", "publisher identity declared in the configuration (required)")
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
		return errors.New("publisher: -config is required")
	}
	if *participant == "" {
		return errors.New("publisher: -participant is required")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	groups, err := swlbruntime.ParticipantGroups(cfg, *participant, control.RolePublisher)
	if err != nil {
		return err
	}
	if *validateOnly {
		fmt.Fprintf(stdout, "configuration valid: publisher %s, %d scaling groups\n", *participant, len(groups))
		return nil
	}
	credentials, err := swlbruntime.ResolveParticipantCredentials(cfg, *participant, false, lookup)
	if err != nil {
		return fmt.Errorf("resolve participant credentials: %w", err)
	}
	if assemble == nil {
		return errors.New("publisher: production assembler is required")
	}
	process, err := assemble(ctx, cfg, credentials, *participant, swlbruntime.AirlineCustomerLibrary{})
	if err != nil {
		return fmt.Errorf("assemble publisher participant: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	logger.Info("publisher participant starting", "participant", *participant, "groups", len(groups))
	processCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runtimeResult := make(chan error, 1)
	go func() { runtimeResult <- process.Run(processCtx) }()
	inputResult := make(chan error, 1)
	go func() { inputResult <- participantio.RunPublisher(processCtx, stdin, stdout, process) }()
	select {
	case err := <-runtimeResult:
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("publisher runtime stopped: %w", err)
		}
		return nil
	case err := <-inputResult:
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("publisher input stopped: %w", err)
		}
		runtimeErr := <-runtimeResult
		if runtimeErr != nil && !errors.Is(runtimeErr, context.Canceled) {
			return runtimeErr
		}
		return nil
	case <-ctx.Done():
		cancel()
		err := <-runtimeResult
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	}
}
