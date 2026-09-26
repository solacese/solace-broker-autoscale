package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/solacese/solace-workload-balancer/qualification"
)

const (
	defaultRunTimeout     = 30 * time.Minute
	defaultCleanupTimeout = 10 * time.Minute
	maximumRunTimeout     = 2 * time.Hour
	maximumCleanupTimeout = 30 * time.Minute
)

var errQualificationWorkloadNotWired = errors.New("qualification workload not wired")

type cloudRunner interface {
	Preflight(context.Context, string, string, Plan) (Plan, error)
	Provision(context.Context, string, string, Plan, string) ([]ResourceRecord, error)
	Cleanup(context.Context, string, string, Journal, string) error
}

type qualificationHook func(context.Context, string, string, Plan, []ResourceRecord, time.Duration, time.Duration) (qualification.Result, error)

type dependencies struct {
	runner  cloudRunner
	qualify qualificationHook
	now     func() time.Time
	random  io.Reader
	stdout  io.Writer
	stderr  io.Writer
}

type options struct {
	mode           string
	baseURL        string
	envPath        string
	jwtVariable    string
	journalPath    string
	evidencePath   string
	runTimeout     time.Duration
	cleanupTimeout time.Duration
}

func parseOptions(args []string, output io.Writer) (options, error) {
	set := flag.NewFlagSet("qualify-cloud", flag.ContinueOnError)
	set.SetOutput(output)
	set.Usage = func() {
		fmt.Fprintln(output, "Usage: qualify-cloud (--preflight | --run | --cleanup) [options]")
		set.PrintDefaults()
	}
	preflight := set.Bool("preflight", false, "validate credentials, access, and plan without creating services")
	run := set.Bool("run", false, "create, qualify, and mandatorily clean up four services")
	cleanup := set.Bool("cleanup", false, "delete exact service IDs retained in the private journal")
	baseURL := set.String("base-url", cloudDefaultBaseURL, "Solace Cloud Mission Control API base URL")
	envPath := set.String("env", ".env", "dotenv file containing the raw cloud JWT")
	jwtVariable := set.String("jwt-var", defaultJWTVariable, "dotenv variable holding the raw cloud JWT")
	journalPath := set.String("journal", ".qualify-cloud/journal.json", "private qualification plan and cleanup journal")
	evidencePath := set.String("result", defaultEvidencePath, "shareable redacted qualification result JSON")
	runTimeout := set.Duration("timeout", defaultRunTimeout, "maximum preflight/run duration")
	cleanupTimeout := set.Duration("cleanup-timeout", defaultCleanupTimeout, "maximum mandatory cleanup duration")
	if err := set.Parse(args); err != nil {
		return options{}, err
	}
	if set.NArg() != 0 {
		return options{}, fmt.Errorf("positional arguments are not accepted")
	}
	selected := 0
	mode := ""
	for name, enabled := range map[string]bool{"preflight": *preflight, "run": *run, "cleanup": *cleanup} {
		if enabled {
			selected++
			mode = name
		}
	}
	if selected != 1 {
		return options{}, fmt.Errorf("select exactly one of --preflight, --run, or --cleanup")
	}
	if *baseURL == "" || *envPath == "" || *jwtVariable == "" || *journalPath == "" || *evidencePath == "" {
		return options{}, fmt.Errorf("base URL, credential, journal, and result paths, and JWT variable must be non-empty")
	}
	for _, privatePath := range []string{*envPath, *journalPath, cloudJournalPath(*journalPath), cloudJournalPath(*journalPath) + ".lock"} {
		if filepath.Clean(*evidencePath) == filepath.Clean(privatePath) {
			return options{}, fmt.Errorf("result path must not replace credentials or a private cleanup journal")
		}
	}
	if *runTimeout <= 0 || *runTimeout > maximumRunTimeout {
		return options{}, fmt.Errorf("timeout must be positive and no greater than %s", maximumRunTimeout)
	}
	if *cleanupTimeout <= 0 || *cleanupTimeout > maximumCleanupTimeout {
		return options{}, fmt.Errorf("cleanup-timeout must be positive and no greater than %s", maximumCleanupTimeout)
	}
	return options{
		mode:           mode,
		baseURL:        *baseURL,
		envPath:        *envPath,
		jwtVariable:    *jwtVariable,
		journalPath:    *journalPath,
		evidencePath:   *evidencePath,
		runTimeout:     *runTimeout,
		cleanupTimeout: *cleanupTimeout,
	}, nil
}

func execute(ctx context.Context, args []string, deps dependencies) int {
	if deps.stdout == nil {
		deps.stdout = io.Discard
	}
	if deps.stderr == nil {
		deps.stderr = io.Discard
	}
	if ctx == nil {
		ctx = context.Background()
	}
	qualificationWired := deps.qualify != nil
	if deps.qualify == nil {
		deps.qualify = realQualificationHook
	}
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.random == nil {
		deps.random = rand.Reader
	}

	options, err := parseOptions(args, deps.stdout)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(deps.stderr, "invalid invocation; select exactly one safe mode and valid bounded deadlines")
		return 2
	}
	if deps.runner == nil {
		fmt.Fprintln(deps.stderr, "qualification failed: cloud runner unavailable")
		return 1
	}
	jwt, err := loadRawJWT(options.envPath, options.jwtVariable)
	if err != nil {
		fmt.Fprintln(deps.stderr, "qualification failed: credential could not be loaded")
		return 1
	}
	store := fileJournal{path: options.journalPath}

	if options.mode == "cleanup" {
		journal, err := store.Load()
		if err != nil {
			fmt.Fprintln(deps.stderr, "cleanup failed: private journal could not be loaded")
			return 1
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), options.cleanupTimeout)
		err = deps.runner.Cleanup(cleanupCtx, jwt, options.baseURL, journal, cloudJournalPath(options.journalPath))
		cancel()
		if err == nil {
			err = removeJournals(store, cloudJournalPath(options.journalPath))
		}
		if err != nil {
			fmt.Fprintf(deps.stderr, "cleanup incomplete: %s; private journal retained for retry\n", safeFailureClass(err))
			return 1
		}
		fmt.Fprintln(deps.stdout, "cleanup complete: all journaled service IDs removed")
		return 0
	}

	plan, err := newPlan(deps.now(), deps.random)
	if err != nil {
		fmt.Fprintln(deps.stderr, "qualification failed: plan could not be generated")
		return 1
	}
	runCtx, cancel := context.WithTimeout(ctx, options.runTimeout)
	defer cancel()
	plan, err = deps.runner.Preflight(runCtx, jwt, options.baseURL, plan)
	if err != nil {
		fmt.Fprintln(deps.stderr, "preflight failed; no services were requested")
		return 1
	}
	if options.mode == "preflight" {
		fmt.Fprintln(deps.stdout, "preflight passed for the four-service qualification plan; no services were created")
		return 0
	}
	if !qualificationWired {
		if err := qualificationCapabilityCheck(); err != nil {
			fmt.Fprintln(deps.stderr, "qualification workload not wired; no services were requested")
			return 1
		}
	}

	journal := Journal{Version: journalVersion, Plan: plan}
	if err := store.Initialize(journal); err != nil {
		fmt.Fprintf(deps.stderr, "qualification failed: private journal could not be initialized: %v\n", err)
		return 1
	}
	cleaned := false
	defer func() {
		if !cleaned {
			// The process remains recoverable through --cleanup if this best-effort
			// finalizer cannot finish before its independent deadline.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), options.cleanupTimeout)
			_ = deps.runner.Cleanup(cleanupCtx, jwt, options.baseURL, journal, cloudJournalPath(options.journalPath))
			cleanupCancel()
		}
	}()

	resources, runErr := deps.runner.Provision(runCtx, jwt, options.baseURL, plan, cloudJournalPath(options.journalPath))
	if runErr != nil {
		runErr = newQualificationStageError("cloud-provisioning", "not-started", runErr)
	}
	if len(resources) != 0 {
		journal.Resources = append([]ResourceRecord(nil), resources...)
		if err := store.Save(journal); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}
	var result qualification.Result
	if runErr == nil {
		result, runErr = deps.qualify(runCtx, jwt, options.baseURL, plan, append([]ResourceRecord(nil), resources...), options.runTimeout, options.cleanupTimeout)
		if runErr != nil {
			var staged *qualificationStageError
			if !errors.As(runErr, &staged) {
				runErr = newQualificationStageError("workload-hook", "unknown", runErr)
			}
		}
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), options.cleanupTimeout)
	cleanupErr := deps.runner.Cleanup(cleanupCtx, jwt, options.baseURL, journal, cloudJournalPath(options.journalPath))
	cleanupCancel()
	if cleanupErr == nil {
		cleanupErr = removeJournals(store, cloudJournalPath(options.journalPath))
	}
	if cleanupErr != nil {
		cleanupCtx, cleanupCancel = context.WithTimeout(context.WithoutCancel(ctx), options.cleanupTimeout)
		cleanupErr = deps.runner.Cleanup(cleanupCtx, jwt, options.baseURL, journal, cloudJournalPath(options.journalPath))
		cleanupCancel()
		if cleanupErr == nil {
			cleanupErr = removeJournals(store, cloudJournalPath(options.journalPath))
		}
	}
	// Both the mandatory cleanup and its final best-effort retry are complete.
	// Persist evidence only now so its cleanup status cannot be made stale by the
	// deferred recovery attempt.
	cleaned = true
	cloudCleanup := "complete"
	if cleanupErr != nil {
		cloudCleanup = "incomplete"
	}
	if evidenceErr := writeQualificationEvidence(options.evidencePath, newQualificationEvidence(deps.now(), result, cloudCleanup, runErr)); evidenceErr != nil {
		fmt.Fprintln(deps.stderr, "qualification evidence could not be written")
		return 1
	}
	if cleanupErr != nil {
		fmt.Fprintln(deps.stderr, "qualification failed and cleanup is incomplete; private journal retained for --cleanup")
		return 1
	}
	if runErr != nil {
		if errors.Is(runErr, errQualificationWorkloadNotWired) {
			fmt.Fprintln(deps.stderr, "qualification workload not wired; mandatory cleanup completed")
		} else {
			var safe interface{ SafeMessage() string }
			if errors.As(runErr, &safe) {
				fmt.Fprintf(deps.stderr, "%s; mandatory service cleanup completed\n", safe.SafeMessage())
			} else {
				fmt.Fprintln(deps.stderr, "qualification failed; mandatory cleanup completed")
			}
		}
		return 1
	}
	fmt.Fprintln(deps.stdout, "qualification complete: four services exercised and mandatory cleanup completed")
	return 0
}

func cloudJournalPath(journalPath string) string {
	return journalPath + ".cloud"
}

func removeJournals(store journalStore, cloudPath string) error {
	if err := store.Remove(); err != nil {
		return err
	}
	for _, path := range []string{cloudPath, cloudPath + ".lock"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
