//go:build integration && load

package compat

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
}
