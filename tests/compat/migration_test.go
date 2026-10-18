//go:build integration && migration

package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Supply binaries built from the previous and candidate revisions.
func TestPreviousBinaryProgressAndArchiveCompatibility(t *testing.T) {
	previous := os.Getenv("KAFERRY_PREVIOUS_BINARY")
	require.NotEqual(t, "", previous, "set KAFERRY_PREVIOUS_BINARY for migration checks")
	candidate := testBinary(t)
	// Compatibility is declared from wire identifiers, independently of candidate decoding.
	const candidateFormat = "kaferry:2"
	const candidateCheckpointVersion = 3
	previousFormat := ""
	previousCheckpointVersion := 0
	for _, direction := range []struct{ name, first, second string }{
		{"previous to candidate", previous, candidate}, {"candidate to previous", candidate, previous},
	} {
		t.Run(direction.name, func(t *testing.T) {
			// Given an archive and committed Kafka progress created by one binary.
			admin := admin(t)
			source := topic(t, admin, 1)
			producer := producer(t)
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			publish(t, producer, &kgo.Record{Topic: source, Partition: 0, Key: []byte{0, 255}, Value: []byte{}, Headers: []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, Timestamp: stamp})
			archive := filepath.Join(t.TempDir(), "archive.cbor")
			clone := filepath.Join(t.TempDir(), "clone.cbor")
			config := configuration(t)
			sql := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s', CUTOFF=INTERVAL '0' SECOND)", archive, source, stamp.Add(time.Second).Format(time.RFC3339Nano), source+"-migration")
			runMigrationBinary(t, direction.first, sql, config)
			before := archiveRecords(t, archive)
			if len(before) != 1 || !bytes.Equal(before[0].Key, []byte{0, 255}) || before[0].Value == nil {
				t.Fatal("invalid archive fixture", before)
			}
			progress := committedPartitions(t, admin, source+"-migration", source, 1)[0]
			require.Equal(t, int64(1), progress.At, "first binary did not save progress")
			require.NotEmpty(t, progress.Metadata, "missing metadata")
			format := strings.SplitN(progress.Metadata, ";", 2)[0]
			if direction.first == previous {
				previousFormat = format
			} else {
				require.Equal(t, candidateFormat, format)
			}
			want := 0
			if previousFormat != candidateFormat {
				want = 2
			}
			// When the other binary resumes, then equal metadata versions must resume successfully.
			diagnostic := ""
			if want == 2 && direction.second == candidate {
				diagnostic = "unsupported version of commit metadata"
			}
			runMigrationExpect(t, direction.second, sql, config, want, diagnostic)
			if got := archiveRecords(t, archive); !reflect.DeepEqual(got, before) {
				t.Fatal("Kafka progress/archive resume changed records", got)
			}
			cloneSQL := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='file-migration')", clone, archive)
			runMigrationBinary(t, direction.first, cloneSQL, config)
			checkpointVersion := migrationCheckpointVersion(t, config)
			if direction.first == previous {
				previousCheckpointVersion = checkpointVersion
			} else {
				require.Equal(t, candidateCheckpointVersion, checkpointVersion)
			}
			fileWant := 0
			if previousCheckpointVersion != candidateCheckpointVersion {
				fileWant = 2
			}
			diagnostic = ""
			if fileWant == 2 && direction.second == candidate {
				diagnostic = "invalid checkpoint"
			}
			runMigrationExpect(t, direction.second, cloneSQL, config, fileWant, diagnostic)
			// Then matching formats resume, incompatible formats fail closed, and neither duplicates output.
			if got := archiveRecords(t, clone); !reflect.DeepEqual(got, before) {
				t.Fatal("file checkpoint/archive compatibility failed", got)
			}
			if direction.first == candidate {
				// Given independently known unsupported Kafka metadata versions with otherwise valid fields.
				supportedMetadata := progress.Metadata
				for _, unsupported := range []string{"kaferry:1", "kaferry:3"} {
					text := strings.Replace(supportedMetadata, candidateFormat+";", unsupported+";", 1)
					progress.Metadata = text
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					result, err := admin.CommitOffsets(ctx, source+"-migration", kadm.Offsets{source: {0: progress}})
					cancel()
					require.NoError(t, err)
					require.Len(t, result[source], 1)
					require.NoError(t, result.Error())
					// When the candidate resumes, then it rejects the version specifically before output changes.
					runMigrationExpect(t, candidate, sql, config, 2, "unsupported version of commit metadata")
					require.Equal(t, before, archiveRecords(t, archive))
				}
			}
			// When using a fresh group/output, then the other binary can still read the unchanged archive format.
			freshClone := filepath.Join(t.TempDir(), "read.cbor")
			freshSQL := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='archive-read')", freshClone, archive)
			runMigrationBinary(t, direction.second, freshSQL, config)
			if got := archiveRecords(t, freshClone); !reflect.DeepEqual(got, before) {
				t.Fatal("archive format changed", got)
			}
		})
	}
}
func runMigrationBinary(t *testing.T, binary, statement, config string) {
	t.Helper()
	runMigrationExpect(t, binary, statement, config, 0)
}
func runMigrationExpect(t *testing.T, binary, statement, config string, want int, diagnostic ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	process := startCLI(t, ctx, binary, "", "run", "-e", statement, "-c", config)
	err := process.wait()
	code := 0
	if err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			code = e.ExitCode()
		} else {
			t.Fatal(err)
		}
	}
	require.Equal(t, want, code, "migration: %s", process.logs())
	for _, text := range diagnostic {
		if text != "" {
			require.Contains(t, process.logs(), text)
		}
	}
}

func migrationCheckpointVersion(t *testing.T, configuration string) int {
	t.Helper()
	data, err := os.ReadFile(configuration)
	require.NoError(t, err)
	var config struct {
		Worker struct {
			CheckpointDir string `toml:"checkpoint_dir"`
		}
	}
	require.NoError(t, toml.Unmarshal(data, &config))
	paths, err := filepath.Glob(filepath.Join(config.Worker.CheckpointDir, "*.json"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	data, err = os.ReadFile(paths[0])
	require.NoError(t, err)
	var checkpoint struct {
		Version int `json:"version"`
	}
	require.NoError(t, json.Unmarshal(data, &checkpoint))
	require.Positive(t, checkpoint.Version)
	return checkpoint.Version
}
