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

	"github.com/solacese/solace-workload-balancer/config"
	swlbruntime "github.com/solacese/solace-workload-balancer/runtime"
)

type process interface {
	Run(context.Context) error
}

type assembler func(context.Context, config.Config, swlbruntime.Credentials, *slog.Logger) (process, error)

var assembleProduction assembler = func(ctx context.Context, cfg config.Config, credentials swlbruntime.Credentials, logger *slog.Logger) (process, error) {
	return swlbruntime.AssembleController(ctx, cfg, credentials, logger, swlbruntime.DefaultControllerAssembler())
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.LookupEnv, assembleProduction); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, lookup swlbruntime.LookupEnv, assemble assembler) error {
	flags := flag.NewFlagSet("controller", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: controller -config FILE [-validate-only]")
		fmt.Fprintln(stderr, "\nRun the workload-balancer controller with an explicit YAML configuration.")
		fmt.Fprintln(stderr, "\nOptions:")
		flags.PrintDefaults()
	}
	configPath := flags.String("config", "", "YAML configuration file (required)")
	validateOnly := flags.Bool("validate-only", false, "validate configuration without resolving credentials or starting runtime")
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
		return errors.New("controller: -config is required")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	if *validateOnly {
		fmt.Fprintf(stdout, "configuration valid: %d data brokers, %d independent scaling groups\n", len(cfg.DataBrokers), len(cfg.Groups))
		return nil
	}

	credentials, err := swlbruntime.ResolveCredentials(cfg, false, lookup)
	if err != nil {
		return fmt.Errorf("resolve runtime credentials: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	if assemble == nil {
		return errors.New("controller: production assembler is required")
	}
	controllerProcess, err := assemble(ctx, cfg, credentials, logger)
	if err != nil {
		return fmt.Errorf("assemble production runtime: %w", err)
	}
	if controllerProcess == nil {
		return errors.New("assemble production runtime: assembler returned nil process")
	}
	logger.Info("controller runtime starting", "controller", cfg.Runtime.Controller, "groups", len(cfg.Groups))
	if err := controllerProcess.Run(ctx); err != nil {
		return fmt.Errorf("controller runtime stopped: %w", err)
	}
	logger.Info("controller runtime stopped")
	return nil
}
