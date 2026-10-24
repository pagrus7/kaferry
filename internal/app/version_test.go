package app

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildIdentity(t *testing.T) {
	for _, tc := range []struct {
		name              string
		info              *debug.BuildInfo
		version, revision string
		want              buildInfo
	}{
		{name: "missing metadata", want: buildInfo{"dev", "unknown", "unknown"}},
		{name: "empty metadata", info: &debug.BuildInfo{}, want: buildInfo{"dev", "unknown", "unknown"}},
		{name: "module install", info: &debug.BuildInfo{
			Main: debug.Module{Version: "v0.2.3"},
			Deps: []*debug.Module{{Path: "unrelated", Version: "v1.0.0"}, {Path: "github.com/twmb/franz-go", Version: "v1.22.2"}},
		}, want: buildInfo{"v0.2.3", "unknown", "v1.22.2"}},
		{name: "pseudo-version", info: &debug.BuildInfo{Main: debug.Module{Version: "v0.2.4-0.20261023123456-abcdef123456"}},
			want: buildInfo{"v0.2.4-0.20261023123456-abcdef123456", "unknown", "unknown"}},
		{name: "checkout", info: &debug.BuildInfo{
			Main:     debug.Module{Version: "(devel)"},
			Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "checkout-commit"}, {Key: "vcs.modified", Value: "false"}},
		}, want: buildInfo{"dev", "checkout-commit", "unknown"}},
		{name: "dirty checkout", info: &debug.BuildInfo{
			Main:     debug.Module{Version: "v0.2.3+dirty"},
			Settings: []debug.BuildSetting{{Key: "vcs.modified", Value: "true"}, {Key: "vcs.revision", Value: "checkout-commit"}},
		}, want: buildInfo{"v0.2.3+dirty", "checkout-commit-dirty", "unknown"}},
		{name: "dirty without revision", info: &debug.BuildInfo{
			Settings: []debug.BuildSetting{{Key: "vcs.modified", Value: "true"}},
		}, want: buildInfo{"dev", "unknown", "unknown"}},
		{name: "release overrides metadata", version: "v0.3.0", revision: "release-commit", info: &debug.BuildInfo{
			Main:     debug.Module{Version: "v0.2.3+dirty"},
			Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "checkout-commit"}, {Key: "vcs.modified", Value: "true"}},
		}, want: buildInfo{"v0.3.0", "release-commit", "unknown"}},
		{name: "make dev overrides module version", version: "dev", info: &debug.BuildInfo{
			Main:     debug.Module{Version: "v0.2.3"},
			Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "checkout-commit"}},
		}, want: buildInfo{"dev", "checkout-commit", "unknown"}},
		{name: "revision override only", revision: "explicit-commit", info: &debug.BuildInfo{Main: debug.Module{Version: "v0.2.3"}},
			want: buildInfo{"v0.2.3", "explicit-commit", "unknown"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given module/VCS metadata and optional linker values.
			// When resolving the build identity for CLI output and metrics.
			got := resolveBuildInfo(tc.info, tc.version, tc.revision)
			// Then preserve explicit values and report only available metadata.
			require.Equal(t, tc.want, got)
		})
	}
}
