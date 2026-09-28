.DEFAULT_GOAL := help

GO ?= go
BIN_DIR ?= bin

.PHONY: help build validate test test-python test-race vet integration check ci clean

help:
	@printf '%s\n' \
		'build        Build controller, publisher, and subscriber' \
		'validate     Validate config.example.yaml without credentials' \
		'test         Run all Go tests' \
		'test-python  Run dashboard and SDKPerf harness tests' \
		'test-race    Run all Go tests with the race detector' \
		'vet          Run go vet' \
		'integration  Run broker-free AMQP adapter integration tests' \
		'check        Run test, vet, build, and integration' \
		'ci           Run race, vet, build, and integration'

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -o $(BIN_DIR)/controller ./cmd/controller
	$(GO) build -trimpath -o $(BIN_DIR)/publisher ./cmd/publisher
	$(GO) build -trimpath -o $(BIN_DIR)/subscriber ./cmd/subscriber

validate:
	$(GO) run ./cmd/controller -config config.example.yaml -validate-only

test:
	$(GO) test ./...

test-python:
	cd tools && python3 -m unittest live_state_test.py sdkperf_harness_test.py live_dashboard_test.py

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

# Broker-free. The separate-process harness under integration/separate_process_test.go
# requires explicitly supplied existing broker endpoints and is not run by CI.
integration:
	$(GO) test -tags=integration ./integration -count=1

check: test test-python vet build integration
ci: test-race test-python vet build integration

clean:
	$(GO) clean
	@find $(BIN_DIR) -type f -delete 2>/dev/null || true
	@rmdir $(BIN_DIR) 2>/dev/null || true
