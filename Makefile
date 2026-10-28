GO ?= go
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
qualification load soak smoke: export KAFERRY_TEST_BINARY = $(CURDIR)/bin/kaferry
endif
export KAFERRY_TEST_OUTPUT

.PHONY: build test check fast smoke integration fault load soak qualification fuzz benchmark metrics broker broker-down transactions
build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags='-s -w -buildid= -X github.com/pagrus7/kaferry/internal/app.version=$(KAFERRY_VERSION) -X github.com/pagrus7/kaferry/internal/app.revision=$(KAFERRY_REVISION)' -o bin/kaferry ./cmd/kaferry
test:
	$(GO) test ./...
# Only race-instrumented tests enable CGO.
check:
	$(GO) vet ./...
	CGO_ENABLED=1 $(GO) test -race ./...
fast: check
	test -z "$$(gofmt -l cmd internal tests)"
	python3 -m unittest discover -s scripts -p 'test_*.py'
	python3 scripts/docs.py
	$(GO) test -run '^$$' -tags=integration,load,fault,qualification,migration ./tests/compat
	$(MAKE) build
	python3 scripts/notices.py bin/kaferry bin/THIRD_PARTY_NOTICES.txt
	python3 scripts/install_smoke.py
	$(MAKE) smoke KAFERRY_TEST_BINARY=$(CURDIR)/bin/kaferry
smoke: $(TEST_BINARY_PREREQUISITE)
	python3 scripts/ci.py smoke $(GO) test -count=1 -json -tags=integration,load -run '^(TestArchiveTutorial(Cancellation)?|TestLoadObservability)$$/^round-0$$/^disabled$$' ./tests/compat
	python3 scripts/ci.py verify-tests smoke 'TestLoadObservability/round-0/disabled' 'TestArchiveTutorial' 'TestArchiveTutorialCancellation'
broker:
	docker compose -p kaferry-compat --profile cross-cluster up -d --wait --wait-timeout 60
	# Listening sockets precede cold group-coordinator readiness.
	docker compose -p kaferry-compat --profile cross-cluster exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group kaferry-compat-warmup --timeout 60000
	docker compose -p kaferry-compat --profile cross-cluster exec -T kafka-dr /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group kaferry-compat-warmup --timeout 60000
broker-down:
	docker compose -p kaferry-compat --profile cross-cluster down
integration:
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFKA_TEST_DEST_BROKERS=$(KAFKA_TEST_DEST_BROKERS) CGO_ENABLED=1 $(GO) test -count=1 -race -tags=integration ./...
fault:
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFKA_FAULT_CONTAINER=kaferry-compat-kafka-1 $(GO) test -count=1 -tags=integration,fault -run 'TestBlockingCommit|TestRebalanceChurn' $(KAFERRY_TEST_OUTPUT) ./tests/compat
load: $(TEST_BINARY_PREREQUISITE)
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) $(GO) test -count=1 -tags=integration,load -run '^TestLoad' $(KAFERRY_TEST_OUTPUT) ./tests/compat
soak: $(TEST_BINARY_PREREQUISITE)
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) $(GO) test -count=1 -timeout=$(SOAK_TEST_TIMEOUT) -tags=integration,load -run TestStreamingSoak $(KAFERRY_TEST_OUTPUT) ./tests/compat
qualification: $(TEST_BINARY_PREREQUISITE)
	GO=$(GO) sh tests/qualification/run.sh
fuzz:
	$(GO) test -run '^$$' -fuzz FuzzParse -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./internal/sql
	$(GO) test -run '^$$' -fuzz FuzzContainer -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./internal/archive/avro
	$(GO) test -run '^$$' -fuzz FuzzState -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./internal/archive/capture
	$(GO) test -run '^$$' -fuzz FuzzCheckpoint -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./internal/archive/replay
benchmark:
	$(GO) test -run '^$$' -bench 'Benchmark(Encode|Decode)$$' -benchmem ./internal/archive/avro
	$(GO) test -run '^$$' -bench "Benchmark(ReplayOverlap|CheckpointEncoding|CheckpointSaving)" -benchmem ./internal/archive/replay
metrics:
	@rg --files cmd internal -g '*.go' -g '!**/*_test.go' | xargs wc -l
	$(GO) run github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0 -top 10 $$(rg --files cmd internal -g '*.go' -g '!**/*_test.go')

transactions:
	KAFERRY_MARKER_FIXTURE=1 CGO_ENABLED=1 $(GO) test -count=1 -timeout=6m -race -run '^TestCoordinatorDecidedCaptureRecoveryWithDelayedMarkers$$' $(KAFERRY_TEST_OUTPUT) ./internal/app
