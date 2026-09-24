package messaging

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"unicode/utf8"
)

const (
	maxRoutingKeyBytes  = 1024
	maxHeaders          = 64
	maxHeaderNameBytes  = 128
	maxHeaderValueBytes = 4096
	maxHeadersBytes     = 32768
)

// Publication is borrowed by a local routing evaluator before JSON serialization.
// The caller retains ownership of Payload and Headers, must not mutate them concurrently,
// and evaluators must not mutate or retain them after Evaluate returns.
type Publication struct {
	Topic   string
	Payload any
	EventID string
	Headers map[string]string
}

// RoutingResult is intentionally sealed so a hex-looking BusinessKey can never be
// mistaken for a digest. Use BusinessKey or SHA256Digest explicitly.
type RoutingResult interface{ routingResult() }

type BusinessKey string

func (BusinessKey) routingResult() {}

type SHA256Digest [32]byte

func (SHA256Digest) routingResult() {}

// Evaluator is trusted customer code installed and registered by the application.
type Evaluator interface {
	Evaluate(*Publication) (RoutingResult, error)
}

// EvaluatorFunc adapts a function to Evaluator.
type EvaluatorFunc func(*Publication) (RoutingResult, error)

func (f EvaluatorFunc) Evaluate(m *Publication) (RoutingResult, error) { return f(m) }

type evaluatorIdentity struct{ name, version string }

// EvaluatorRegistry contains only explicitly registered, in-process implementations.
type EvaluatorRegistry struct {
	mu         sync.RWMutex
	evaluators map[evaluatorIdentity]Evaluator
}

func NewEvaluatorRegistry() *EvaluatorRegistry {
	return &EvaluatorRegistry{evaluators: map[evaluatorIdentity]Evaluator{}}
}

func (r *EvaluatorRegistry) Register(name, version string, evaluator Evaluator) error {
	id, err := validateEvaluatorIdentity(name, version)
	if err != nil {
		return err
	}
	if evaluator == nil {
		return errors.New("routing evaluator is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.evaluators == nil {
		r.evaluators = map[evaluatorIdentity]Evaluator{}
	}
	if _, exists := r.evaluators[id]; exists {
		return fmt.Errorf("routing evaluator %q version %q is already registered", name, version)
	}
	r.evaluators[id] = evaluator
	return nil
}

func (r *EvaluatorRegistry) require(name, version string) (Evaluator, error) {
	id, err := validateEvaluatorIdentity(name, version)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("required routing evaluator %q version %q is not registered", name, version)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	evaluator := r.evaluators[id]
	if evaluator == nil {
		return nil, fmt.Errorf("required routing evaluator %q version %q is not registered", name, version)
	}
	return evaluator, nil
}

func validateEvaluatorIdentity(name, version string) (evaluatorIdentity, error) {
	if !utf8.ValidString(name) || !utf8.ValidString(version) || len(name) < 1 || len(name) > 128 || len(version) < 1 || len(version) > 64 || containsNUL(name) || containsNUL(version) {
		return evaluatorIdentity{}, errors.New("routing evaluator name/version must be bounded nonempty UTF-8 strings without NUL")
	}
	return evaluatorIdentity{name, version}, nil
}

func containsNUL(value string) bool {
	for _, b := range []byte(value) {
		if b == 0 {
			return true
		}
	}
	return false
}

func validateHeaders(headers map[string]string) error {
	if len(headers) > maxHeaders {
		return fmt.Errorf("message headers exceed %d entries", maxHeaders)
	}
	total := 0
	for name, value := range headers {
		if !utf8.ValidString(name) || !utf8.ValidString(value) || len(name) < 1 || len(name) > maxHeaderNameBytes || len(value) > maxHeaderValueBytes || containsNUL(name) || containsNUL(value) {
			return errors.New("message header name/value is invalid, too long, or contains NUL")
		}
		total += len(name) + len(value)
	}
	if total > maxHeadersBytes {
		return fmt.Errorf("message headers exceed %d UTF-8 bytes", maxHeadersBytes)
	}
	return nil
}

func invokeEvaluator(e Evaluator, message *Publication) (result RoutingResult, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("routing evaluator panicked")
		}
	}()
	result, err = e.Evaluate(message)
	if err != nil {
		return nil, errors.New("routing evaluator failed")
	}
	return result, nil
}

func resolveEvaluator(e Evaluator, format string, message *Publication, shard string, partitions int) (kind, value string, partition int, err error) {
	result, err := invokeEvaluator(e, message)
	if err != nil {
		return "", "", 0, err
	}
	switch format {
	case "key":
		key, ok := result.(BusinessKey)
		if !ok {
			return "", "", 0, errors.New("routing evaluator must return BusinessKey for result key")
		}
		value = string(key)
		if !utf8.ValidString(value) || len(value) < 1 || len(value) > maxRoutingKeyBytes || containsNUL(value) {
			return "", "", 0, fmt.Errorf("routing key must be 1-%d UTF-8 bytes without NUL", maxRoutingKeyBytes)
		}
		return "key", value, partitionFor(shard, value, partitions), nil
	case "sha256":
		digest, ok := result.(SHA256Digest)
		if !ok {
			return "", "", 0, errors.New("routing evaluator must return SHA256Digest for result sha256")
		}
		value = fmt.Sprintf("%x", digest[:])
		partition = int(new(big.Int).Mod(new(big.Int).SetBytes(digest[:]), big.NewInt(int64(partitions))).Int64())
		return "sha256", value, partition, nil
	default:
		return "", "", 0, errors.New("unsupported routing evaluator result")
	}
}
