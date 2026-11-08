//go:build integration && load

package compat

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Reports are optional build artifacts, never part of the archive or CLI API.
func TestLoadReportIdentifiesExecutedBinary(t *testing.T) {
	// Given a CLI binary and an override naming a missing binary.
	binary := testBinary(t)
	data, err := os.ReadFile(binary)
	require.NoError(t, err)
	info, err := buildinfo.ReadFile(binary)
	require.NoError(t, err)
	clientVersion := ""
	for _, dependency := range info.Deps {
		if dependency.Path == "github.com/twmb/franz-go" {
			clientVersion = dependency.Version
		}
	}
	require.NotEmpty(t, clientVersion)
	directory := t.TempDir()
	// When the process finishes and its test cleanup writes a report.
	t.Run("collect", func(t *testing.T) {
		t.Setenv("KAFERRY_REPORT_DIR", directory)
		t.Setenv("KAFERRY_TEST_BINARY", filepath.Join(directory, "unused-binary"))
		report := newLoadReport(t, "identity")
		report.cli = startCLI(t, t.Context(), binary, "", "--version")
		require.NoError(t, report.cli.wait())
		require.Contains(t, report.cli.logs(), "franz-go "+clientVersion+")")
	})
	// Then the report hashes the binary that ran despite the unrelated override.
	files, err := filepath.Glob(filepath.Join(directory, "*.metrics.json"))
	if err != nil || len(files) != 1 {
		t.Fatal("missing report", files, err)
	}
	encoded, err := os.ReadFile(files[0])
	require.NoError(t, err)
	var report loadReport
	require.NoError(t, json.Unmarshal(encoded, &report))
	require.Equal(t, "franz-go", report.FixtureClient)
	require.Equal(t, "cyclic-v1", report.Workload.Name)
	require.Equal(t, [2]int{1024, 5119}, report.Workload.ValueBytes)
	require.EqualValues(t, 42, report.Workload.Seed)
	require.Zero(t, report.SourcePartitions)
	require.Zero(t, report.DestinationPartitions)
	require.Equal(t, "github.com/twmb/franz-go "+clientVersion, report.Client)
	if !report.Passed || report.Skipped || report.BinarySHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) || report.Client == "" {
		t.Fatal("report does not identify the tested binary", report)
	}
	log, err := os.ReadFile(strings.TrimSuffix(files[0], ".metrics.json") + ".cli.log")
	require.NoError(t, err)
	require.Contains(t, string(log), "franz-go "+clientVersion+")")
}

func TestLoadReportPreservesFullCLILogBeforeTemporaryCleanup(t *testing.T) {
	payload := "stdout\n" + strings.Repeat("recorded output\n", 8192) + "stderr\n"
	mode := os.Getenv("KAFERRY_LOG_RETENTION_CHILD")
	if mode == "writer" {
		_, _ = fmt.Fprint(os.Stdout, strings.TrimSuffix(payload, "stderr\n"))
		_, _ = fmt.Fprint(os.Stderr, "stderr\n")
		code, err := strconv.Atoi(os.Getenv("KAFERRY_LOG_RETENTION_EXIT"))
		if err != nil {
			os.Exit(9)
		}
		os.Exit(code)
	}
	if mode == "collector" {
		// Given a report created before the process's first temporary directory.
		report := newLoadReport(t, "retention")
		binary, err := os.Executable()
		require.NoError(t, err)
		t.Setenv("KAFERRY_LOG_RETENTION_CHILD", "writer")
		report.cli = startCLI(t, t.Context(), binary, "", "-test.run=^TestLoadReportPreservesFullCLILogBeforeTemporaryCleanup$")
		// When the process exits, its nonzero result also fails this child test.
		require.NoError(t, report.cli.wait())
		return
	}
	binary, err := os.Executable()
	require.NoError(t, err)
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprintf("exit-%d", code), func(t *testing.T) {
			directory := t.TempDir()
			command := exec.CommandContext(t.Context(), binary, "-test.run=^TestLoadReportPreservesFullCLILogBeforeTemporaryCleanup$")
			command.Env = append(os.Environ(), "KAFERRY_REPORT_DIR="+directory,
				"KAFERRY_LOG_RETENTION_CHILD=collector", "KAFERRY_LOG_RETENTION_EXIT="+strconv.Itoa(code))
			output, err := command.CombinedOutput()
			if code == 0 {
				require.NoError(t, err, "%s", output)
			} else {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, "%s", output)
				require.Equal(t, 1, exit.ExitCode(), "the child test must actually fail")
			}
			// Then success and failure retain every byte after all child cleanup.
			logs, err := filepath.Glob(filepath.Join(directory, "*.cli.log"))
			require.NoError(t, err)
			require.Len(t, logs, 1)
			data, err := os.ReadFile(logs[0])
			require.NoError(t, err)
			require.Equal(t, payload, string(data))
			metrics, err := os.ReadFile(strings.TrimSuffix(logs[0], ".cli.log") + ".metrics.json")
			require.NoError(t, err)
			var report loadReport
			require.NoError(t, json.Unmarshal(metrics, &report))
			require.Equal(t, code == 0, report.Passed)
		})
	}
}
