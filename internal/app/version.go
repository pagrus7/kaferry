package app

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Release builds set these through the linker; source builds identify as dev.
var version = "dev"
var revision = "unknown"

func versionString() string {
	client := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dependency := range info.Deps {
			if dependency.Path == "github.com/twmb/franz-go" {
				client = dependency.Version
			}
		}
	}
	return fmt.Sprintf("kaferry %s (commit %s; %s; franz-go %s)", version, revision, runtime.Version(), client)
}
