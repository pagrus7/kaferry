package app

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Linker values override the metadata embedded by the Go toolchain.
var version string
var revision string

type buildInfo struct {
	version, revision, client string
}

func currentBuild() buildInfo {
	info, _ := debug.ReadBuildInfo()
	return resolveBuildInfo(info, version, revision)
}

func resolveBuildInfo(info *debug.BuildInfo, version, revision string) buildInfo {
	build := buildInfo{version: "dev", client: "unknown"}
	if info != nil {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			build.version = info.Main.Version
		}
		dirty := false
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				build.revision = setting.Value
			case "vcs.modified":
				dirty = setting.Value == "true"
			}
		}
		if dirty && build.revision != "" {
			build.revision += "-dirty"
		}
		for _, dependency := range info.Deps {
			if dependency.Path == "github.com/twmb/franz-go" {
				build.client = dependency.Version
			}
		}
	}
	if version != "" {
		build.version = version
	}
	if revision != "" {
		build.revision = revision
	}
	if build.revision == "" {
		build.revision = "unknown"
	}
	return build
}

func versionString() string {
	build := currentBuild()
	return fmt.Sprintf("kaferry %s (commit %s; %s; franz-go %s)", build.version, build.revision, runtime.Version(), build.client)
}
