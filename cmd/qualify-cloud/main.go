package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code := execute(ctx, os.Args[1:], dependencies{
		runner: missionControlRunner{},
		stdout: os.Stdout,
		stderr: os.Stderr,
	})
	os.Exit(code)
}
