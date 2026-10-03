GO ?= go
KAFKA_TEST_BROKERS ?= 127.0.0.1:39092
KAFKA_TEST_DEST_BROKERS ?= 127.0.0.1:40092

.PHONY: build test check integration fault load benchmark metrics broker broker-down
build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags='-s -w -buildid=' -o bin/kafka-fwd2 ./cmd/kafka-fwd2
test:
	$(GO) test ./...
check:
	$(GO) vet ./...
	$(GO) test -race ./...
broker:
	docker compose -p kafka-fwd2-compat --profile cross-cluster up -d --wait --wait-timeout 60
broker-down:
	docker compose -p kafka-fwd2-compat --profile cross-cluster down
integration:
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFKA_TEST_DEST_BROKERS=$(KAFKA_TEST_DEST_BROKERS) $(GO) test -race -tags=integration ./...
fault:
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KAFKA_FAULT_CONTAINER=kafka-fwd2-compat-kafka-1 $(GO) test -tags=integration,fault -run 'TestBlockingCommit|TestAssignmentLost' -v ./tests/compat
load: build
	KAFKA_TEST_BROKERS=$(KAFKA_TEST_BROKERS) KFWD_TEST_BINARY=$(CURDIR)/bin/kafka-fwd2 $(GO) test -tags=integration,load -run TestLoadBoundedKafkaCopy -v ./tests/compat
benchmark:
	$(GO) test -run '^$$' -bench BenchmarkRecordFraming -benchmem ./internal/file
metrics:
	@rg --files cmd internal -g '*.go' -g '!**/*_test.go' | xargs wc -l
	$(GO) run github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0 -top 10 $$(rg --files cmd internal -g '*.go' -g '!**/*_test.go')
