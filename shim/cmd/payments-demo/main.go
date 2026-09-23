// payments-demo is a small real application used by the manager demo and live tests.
// stdin/stdout are JSON lines; broker credentials come only from the environment.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/solacese/solace-broker-autoscale/shim/messaging"
	bolt "go.etcd.io/bbolt"
)

type credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func parseGroups(raw string) []string {
	groups := make([]string, 0)
	seen := map[string]bool{}
	for _, value := range strings.Split(raw, ",") {
		group := strings.TrimSpace(value)
		if group == "" || seen[group] {
			continue
		}
		seen[group] = true
		groups = append(groups, group)
	}
	return groups
}

func resolveCredential(
	credentials map[string]credential,
	broker string,
	getenv func(string) string,
) (string, string, error) {
	if value, ok := credentials[broker]; ok && value.Username != "" && value.Password != "" {
		return value.Username, value.Password, nil
	}
	username, password := getenv("SOLACE_USERNAME"), getenv("SOLACE_PASSWORD")
	if username == "" || password == "" {
		return "", "", fmt.Errorf("credentials unavailable for broker %s", broker)
	}
	return username, password, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	controller := flag.String("controller", "http://127.0.0.1:8099", "controller URL")
	state := flag.String("state", "state/payments-demo", "persistent local directory")
	subscribe := flag.Bool("subscribe", false, "run subscribers")
	groupsFlag := flag.String("groups", "ledger,audit", "comma-separated declared subscriber groups")
	handlerDelay := flag.Duration("handler-delay", 0, "delay each subscriber handler")
	flag.Parse()
	groups := parseGroups(*groupsFlag)
	if *subscribe && len(groups) == 0 {
		return fmt.Errorf("at least one subscriber group is required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	credentials := map[string]credential{}
	if raw := os.Getenv("SOLACE_CREDENTIALS_JSON"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &credentials); err != nil {
			return fmt.Errorf("invalid SOLACE_CREDENTIALS_JSON: %w", err)
		}
	}
	credentialFor := func(broker string) (string, string, error) {
		return resolveCredential(credentials, broker, os.Getenv)
	}
	c, err := messaging.Open(ctx, messaging.Options{ControllerURL: *controller, APIKey: os.Getenv("AUTOSCALE_API_KEY"), OutboxPath: filepath.Join(*state, "publisher.outbox.db"), PollInterval: 100 * time.Millisecond, Credentials: credentialFor})
	if err != nil {
		return err
	}
	defer c.Close()
	var output sync.Mutex
	emit := func(v any) { output.Lock(); defer output.Unlock(); _ = json.NewEncoder(os.Stdout).Encode(v) }
	if *subscribe {
		db, err := bolt.Open(filepath.Join(*state, "business.ledger.db"), 0600, &bolt.Options{Timeout: time.Second})
		if err != nil {
			return err
		}
		defer db.Close()
		defer c.Close() // Stop handlers before closing their business database.
		for _, group := range groups {
			err = c.Subscribe(ctx, group, func(ctx context.Context, m messaging.Message) error {
				if *handlerDelay > 0 {
					select {
					case <-time.After(*handlerDelay):
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				duplicate := false
				err := db.Update(func(tx *bolt.Tx) error {
					b, err := tx.CreateBucketIfNotExists([]byte(group))
					if err != nil {
						return err
					}
					if b.Get([]byte(m.EventID)) != nil {
						duplicate = true
						return nil
					}
					return b.Put([]byte(m.EventID), m.Payload)
				})
				if err == nil {
					emit(map[string]any{"kind": "processed", "group": group, "event_id": m.EventID, "duplicate": duplicate, "topic": m.Topic, "payload": m.Payload})
				}
				return err
			})
			if err != nil {
				return err
			}
		}
		emit(map[string]any{"kind": "subscribed"})
		<-ctx.Done()
		return nil
	}
	type input struct {
		data []byte
		err  error
	}
	lines := make(chan input)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 4096), 2<<20)
		for scanner.Scan() {
			data := append([]byte(nil), scanner.Bytes()...)
			select {
			case lines <- input{data: data}:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case lines <- input{err: err}:
			case <-ctx.Done():
			}
		}
	}()
	for {
		var line input
		select {
		case <-ctx.Done():
			return nil
		case item, ok := <-lines:
			if !ok {
				return nil
			}
			line = item
		}
		if line.err != nil {
			return line.err
		}
		var command struct {
			Op      string          `json:"op"`
			Topic   string          `json:"topic"`
			Payload json.RawMessage `json:"payload"`
			ID      string          `json:"event_id"`
		}
		if err := json.Unmarshal(line.data, &command); err != nil {
			return err
		}
		switch command.Op {
		case "status":
			emit(map[string]any{"kind": "status", "status": c.Status()})
		case "flush":
			wait, stop := context.WithTimeout(ctx, 30*time.Second)
			err := c.Flush(wait)
			stop()
			if err != nil {
				emit(map[string]any{"kind": "error", "error": err.Error()})
			} else {
				emit(map[string]any{"kind": "flushed"})
			}
		default:
			if err := c.Publish(command.Topic, command.Payload, command.ID); err != nil {
				emit(map[string]any{"kind": "rejected", "event_id": command.ID, "error": err.Error()})
			} else {
				emit(map[string]any{"kind": "accepted", "event_id": command.ID})
			}
		}
	}
}
