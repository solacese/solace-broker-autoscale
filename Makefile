.DEFAULT_GOAL := help

GO ?= go
MVN ?= mvn
BIN_DIR ?= bin

.PHONY: help build validate test test-race vet check ci java-helper integration qualify-preflight qualify-cloud cleanup-cloud clean

help: ## Show available targets
	@printf '%s\n' \
		'Usage: make <target>' \
		'' \
		'Targets:' \
		'  build              Build controller, publisher, and subscriber binaries' \
		'  validate           Validate config.example.yaml without loading credentials' \
		'  test               Run the Go test suite' \
		'  test-race          Run the Go test suite with the race detector' \
		'  vet                Run Go static analysis' \
		'  java-helper        Test and package the local JCSMP LVQ browser' \
		'  integration        Verify local native adapters and Java helper capability' \
		'  check              Run local customer-readiness checks' \
		'  qualify-preflight  Validate Cloud access and the qualification plan (read-only)' \
		'  qualify-cloud      Run the opt-in, resource-creating Cloud qualification' \
		'  cleanup-cloud      Remove journaled Cloud qualification resources' \
		'  clean              Remove generated binaries and Java build output'

build: ## Build customer-facing commands
	@mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -o $(BIN_DIR)/controller ./cmd/controller
	$(GO) build -trimpath -o $(BIN_DIR)/publisher ./cmd/publisher
	$(GO) build -trimpath -o $(BIN_DIR)/subscriber ./cmd/subscriber

validate: ## Validate the example configuration without credentials
	$(GO) run ./cmd/controller -config config.example.yaml -validate-only

test: ## Run unit tests
	$(GO) test ./...

test-race: ## Run unit tests with the race detector
	$(GO) test -race ./...

vet: ## Run Go static analysis
	$(GO) vet ./...

java-helper: ## Test and package the local JCSMP LVQ browser
	$(MVN) -B -ntp -f tools/lvq-browser/pom.xml clean verify

# This target verifies local wiring only; it does not connect to a broker or
# create broker or Cloud resources.
integration: java-helper ## Run local integration capability checks
	$(GO) test -tags=integration ./integration -count=1

check: test vet integration ## Run local customer-readiness checks

ci: test-race vet integration ## Run all CI checks

# Read-only Cloud/API validation. No service is created.
qualify-preflight: java-helper
	$(GO) run ./cmd/qualify-cloud --preflight

# Opt-in real qualification: creates exactly four journaled services and always
# attempts exact-ID cleanup. Requires explicit operator invocation.
qualify-cloud: java-helper
	$(GO) run ./cmd/qualify-cloud --run

cleanup-cloud:
	$(GO) run ./cmd/qualify-cloud --cleanup

clean: ## Remove generated local build outputs
	$(GO) clean
	$(MVN) -B -ntp -f tools/lvq-browser/pom.xml clean
	@find $(BIN_DIR) -type f -delete 2>/dev/null || true
	@rmdir $(BIN_DIR) 2>/dev/null || true
