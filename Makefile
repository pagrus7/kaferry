GO ?= go
PYTHON ?= python3
export CGO_ENABLED := 0
KAFERRY_VERSION ?= dev
KAFERRY_REVISION ?= $(shell git rev-parse HEAD 2>/dev/null || printf unknown)$(if $(shell git status --porcelain --untracked-files=no 2>/dev/null),-dirty)
FUZZ_ITERATIONS ?= 100000
KAFKA_TEST_BROKERS ?= 127.0.0.1:39092
KAFKA_TEST_DEST_BROKERS ?= 127.0.0.1:40092
KAFERRY_TEST_OUTPUT ?= -v
SOAK_TEST_TIMEOUT ?= 1h
ifeq ($(KAFERRY_TEST_BINARY),)
TEST_BINARY_PREREQUISITE = build
# A leading "load " is a GNU Make plugin directive.
qualification load soak smoke fault: export KAFERRY_TEST_BINARY = $(CURDIR)/bin/kaferry
endif
export KAFERRY_TEST_OUTPUT

.PHONY: build test check fast smoke integration fault load soak qualification fuzz benchmark metrics broker broker-down
build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags='-s -w -buildid= -X github.com/pagrus7/kaferry/src/internal/app.version=$(KAFERRY_VERSION) -X github.com/pagrus7/kaferry/src/internal/app.revision=$(KAFERRY_REVISION)' -o bin/kaferry ./src/cmd/kaferry
test:
	$(GO) test ./...
# Only race-instrumented tests enable CGO.
check:
	$(GO) vet ./...
	CGO_ENABLED=1 $(GO) test -race ./...
fast: check
	test -z "$$(gofmt -l src)"
	$(PYTHON) -m unittest discover -s tools -t . -p 'test_*.py'
	$(PYTHON) -m tools.docs.docs
	$(GO) test -run '^$$' -tags=integration,load,fault,qualification,migration ./src/tests/system
	$(MAKE) build
	$(PYTHON) -m tools.release.notices bin/kaferry bin/THIRD_PARTY_NOTICES.txt
	$(PYTHON) -m tools.release.install_smoke
	$(MAKE) smoke KAFERRY_TEST_BINARY=$(CURDIR)/bin/kaferry
smoke: $(TEST_BINARY_PREREQUISITE)
	$(PYTHON) -m tools.ci.ci smoke $(GO) test -count=1 -json -tags=integration,load -run '^(TestArchiveTutorial(Cancellation)?|TestLoadObservability)$$/^round-0$$/^disabled$$' ./src/tests/system
	$(PYTHON) -m tools.ci.ci verify-tests smoke 'TestLoadObservability/round-0/disabled' 'TestArchiveTutorial' 'TestArchiveTutorialCancellation'
broker:
	docker compose -f src/tests/compose.yaml -p kaferry-compat --profile cross-cluster up -d --wait --wait-timeout 60
	# Listening sockets precede cold group-coordinator readiness.
	docker compose -f src/tests/compose.yaml -p kaferry-compat --profile cross-cluster exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group kaferry-compat-warmup --timeout 60000
	docker compose -f src/tests/compose.yaml -p kaferry-compat --profile cross-cluster exec -T kafka-dr /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group kaferry-compat-warmup --timeout 60000
broker-down:
	docker compose -f src/tests/compose.yaml -p kaferry-compat --profile cross-cluster down
integration:
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFKA_TEST_DEST_BROKERS=$(KAFKA_TEST_DEST_BROKERS) CGO_ENABLED=1 $(PYTHON) -m tools.ci.suites run integration --go=$(GO) --packages=./... --output=
fault: $(TEST_BINARY_PREREQUISITE)
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFKA_TEST_DEST_BROKERS=$(KAFKA_TEST_DEST_BROKERS) KAFKA_FAULT_CONTAINER=kaferry-compat-kafka-1 KAFKA_FAULT_DEST_CONTAINER=kaferry-compat-kafka-dr-1 $(PYTHON) -m tools.ci.suites run fault --go=$(GO) --output=$(KAFERRY_TEST_OUTPUT)
load: $(TEST_BINARY_PREREQUISITE)
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) $(PYTHON) -m tools.ci.suites run load --go=$(GO) --output=$(KAFERRY_TEST_OUTPUT)
soak: $(TEST_BINARY_PREREQUISITE)
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) $(PYTHON) -m tools.ci.suites run soak --go=$(GO) --output=$(KAFERRY_TEST_OUTPUT) --timeout=$(SOAK_TEST_TIMEOUT)
qualification: $(TEST_BINARY_PREREQUISITE)
	GO=$(GO) sh src/tests/qualification/run.sh
fuzz:
	$(GO) test -run '^$$' -fuzz FuzzProgressLifecycle -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./src/internal/copy
	$(GO) test -run '^$$' -fuzz FuzzParse -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./src/internal/sql
	$(GO) test -run '^$$' -fuzz FuzzContainer -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./src/internal/archive/avro
	$(GO) test -run '^$$' -fuzz FuzzState -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./src/internal/archive/capture
	$(GO) test -run '^$$' -fuzz FuzzCheckpoint -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./src/internal/archive/replay
benchmark:
	$(GO) test -run '^$$' -bench 'Benchmark(Encode|Decode)$$' -benchmem ./src/internal/archive/avro
	$(GO) test -run '^$$' -bench "Benchmark(ReplayOverlap|CheckpointEncoding|CheckpointSaving)" -benchmem ./src/internal/archive/replay
metrics:
	@rg --files src/cmd src/internal -g '*.go' -g '!**/*_test.go' | xargs wc -l
	$(GO) run github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0 -top 10 $$(rg --files src/cmd src/internal -g '*.go' -g '!**/*_test.go')

# Standalone qualification: Python 3.11+, Go, Docker and Compose; no agent required.
.PHONY: qualification-setup qualification-pr qualification-full model
qualification-setup: build
	$(PYTHON) -m tools.qualification.qualify setup
qualification-pr:
	$(PYTHON) -m tools.qualification.qualify matrix --tier pr
qualification-full:
	$(PYTHON) -m tools.qualification.qualify matrix --tier full
model:
	CGO_ENABLED=1 $(GO) test -race -count=1 -run '^TestProgressLifecycleModel$$' ./src/internal/copy
