//go:build integration && migration

package compat

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Supply binaries built from the previous and candidate revisions.
func TestPreviousBinaryProgressAndArchiveCompatibility(t *testing.T) {
	previous := os.Getenv("KAFERRY_PREVIOUS_BINARY")
	require.NotEmpty(t, previous, "set KAFERRY_PREVIOUS_BINARY for migration checks")
	candidate := testBinary(t)
	for _, direction := range []struct{ name, first, second string }{
		{"previous to candidate", previous, candidate}, {"candidate to previous", candidate, previous},
	} {
		t.Run(direction.name, func(t *testing.T) {
			// Given two records and binaries that share the Kafka and Avro progress contracts.
			admin := admin(t)
			source := topic(t, admin, 1)
			producer := producer(t)
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			for i := range 2 {
				publish(t, producer, &kgo.Record{Topic: source, Partition: 0, Key: []byte{0, byte(i)}, Value: []byte{}, Headers: []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, Timestamp: stamp})
			}
			archive := filepath.Join(t.TempDir(), "archive")
			clone := filepath.Join(t.TempDir(), "clone")
			config := configuration(t, "max_frame_bytes=8192")
			group := source + "-migration"
			sql := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', CUTOFF=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", archive, source, group, stamp.Add(time.Second).Format(time.RFC3339Nano))
			limited := func(query string) string {
				return query + " OPTIONS(WORKER_STOP_AFTER_RECORDS=1)"
			}
			runMigrationBinary(t, direction.first, limited(sql), config)
			first := archiveRecords(t, archive)
			require.Len(t, first, 1)
			require.Equal(t, []byte{0, 0}, first[0].Key)
			require.NotNil(t, first[0].Value, "empty value must remain distinct from null")
			progress := committedPartitions(t, admin, group, source, 1)[0]
			require.Equal(t, int64(1), progress.At)
			require.True(t, strings.HasPrefix(progress.Metadata, "kaferry:2;"))

			// When the other binary resumes, then it copies only the remaining record.
			runMigrationBinary(t, direction.second, sql, config)
			complete := archiveRecords(t, archive)
			require.Len(t, complete, 2)
			require.Equal(t, first[0], complete[0])
			require.Equal(t, []byte{0, 1}, complete[1].Key)
			require.Equal(t, int64(2), committedPartitions(t, admin, group, source, 1)[0].At)

			// Given a partially replayed archive with a saved record cursor.
			cloneSQL := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH (GROUP='file-migration') ", clone, archive)
			runMigrationBinary(t, direction.first, limited(cloneSQL), config)
			require.Equal(t, complete[:1], archiveRecords(t, clone))
			state := migrationCheckpoint(t, config, "file-migration")
			require.Equal(t, 1, state.Version)
			require.Equal(t, int64(1), state.Position)
			require.False(t, state.Complete)

			// When the other binary resumes, then it keeps the selection and produces no duplicates.
			runMigrationBinary(t, direction.second, cloneSQL, config)
			require.Equal(t, complete, archiveRecords(t, clone))
			state = migrationCheckpoint(t, config, "file-migration")
			require.Equal(t, int64(2), state.Position)
			require.True(t, state.Complete)

			// Given independently known unsupported commit prefixes, reject them before changing output.
			supported := progress.Metadata
			for _, unsupported := range []string{"kaferry:1", "kaferry:3"} {
				progress.Metadata = strings.Replace(supported, "kaferry:2;", unsupported+";", 1)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				result, err := admin.CommitOffsets(ctx, group, kadm.Offsets{source: {0: progress}})
				cancel()
				require.NoError(t, err)
				require.NoError(t, result.Error())
				require.Len(t, result[source], 1)
				runMigrationExpect(t, candidate, sql, config, 2, "unsupported version of commit metadata")
				require.Equal(t, complete, archiveRecords(t, archive))
			}

			// When a fresh group reads the archive, then it sees the same full records.
			fresh := filepath.Join(t.TempDir(), "read")
			query := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH (GROUP='archive-read') ", fresh, archive)
			runMigrationBinary(t, direction.second, query, config)
			require.Equal(t, complete, archiveRecords(t, fresh))
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

func migrationCheckpoint(t *testing.T, configuration, group string) *replay.State {
	t.Helper()
	data, err := os.ReadFile(configuration)
	require.NoError(t, err)
	var config struct {
		Worker struct {
			CheckpointDir string `toml:"checkpoint_dir"`
		}
	}
	require.NoError(t, toml.Unmarshal(data, &config))
	state, err := replay.ReadCheckpoint(config.Worker.CheckpointDir, group, replay.NativeGroupFiles{})
	require.NoError(t, err)
	require.NotNil(t, state)
	return state
}
