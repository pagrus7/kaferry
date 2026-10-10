GO ?= go
FUZZ_ITERATIONS ?= 100000
KAFKA_TEST_BROKERS ?= 127.0.0.1:39092
KAFKA_TEST_DEST_BROKERS ?= 127.0.0.1:40092

.PHONY: build test check integration fault load soak qualification fuzz benchmark metrics broker broker-down
build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags='-s -w -buildid=' -o bin/kaferry ./cmd/kaferry
test:
	$(GO) test ./...
check:
	$(GO) vet ./...
	$(GO) test -race ./...
broker:
	docker compose -p kaferry-compat --profile cross-cluster up -d --wait --wait-timeout 60
	# Initialize group coordinators in these disposable fixtures before native clients run.
	docker compose -p kaferry-compat --profile cross-cluster exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group kaferry-compat-warmup --timeout 60000
	docker compose -p kaferry-compat --profile cross-cluster exec -T kafka-dr /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group kaferry-compat-warmup --timeout 60000
broker-down:
	docker compose -p kaferry-compat --profile cross-cluster down
integration:
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFKA_TEST_DEST_BROKERS=$(KAFKA_TEST_DEST_BROKERS) $(GO) test -count=1 -race -tags=integration ./...
fault:
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFKA_FAULT_CONTAINER=kaferry-compat-kafka-1 $(GO) test -count=1 -tags=integration,fault -run 'TestBlockingCommit|TestAssignmentLost|TestRebalanceChurn' -v ./tests/compat
load: build
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFERRY_TEST_BINARY=$(CURDIR)/bin/kaferry $(GO) test -count=1 -tags=integration,load -run TestLoadBoundedKafkaCopy -v ./tests/compat
soak: build
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFERRY_TEST_BINARY=$(CURDIR)/bin/kaferry $(GO) test -count=1 -timeout=1h -tags=integration,load -run TestStreamingSoak -v ./tests/compat
qualification: build
	GO=$(GO) KAFERRY_TEST_BINARY=$(CURDIR)/bin/kaferry sh tests/qualification/run.sh
fuzz:
	$(GO) test -run '^$$' -fuzz FuzzParse -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./internal/sql
	$(GO) test -run '^$$' -fuzz FuzzFraming -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./internal/file
	$(GO) test -run '^$$' -fuzz FuzzRecordDecoder -fuzztime=$(FUZZ_ITERATIONS)x -parallel=2 ./internal/file
benchmark:
	$(GO) test -run '^$$' -bench BenchmarkRecordFraming -benchmem ./internal/file
metrics:
	@rg --files cmd internal -g '*.go' -g '!**/*_test.go' | xargs wc -l
	$(GO) run github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0 -top 10 $$(rg --files cmd internal -g '*.go' -g '!**/*_test.go')
