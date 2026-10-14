package app

import (
	"fmt"
	"runtime"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Release builds set these through the linker; source builds identify as dev.
var version = "dev"
var revision = "unknown"

func versionString() string {
	_, native := kafka.LibraryVersion()
	return fmt.Sprintf("kaferry %s (commit %s; %s; librdkafka %s)", version, revision, runtime.Version(), native)
}
