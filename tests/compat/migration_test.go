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
			sql := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s') ", archive, source, group)
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
			require.True(t, strings.HasPrefix(progress.Metadata, "kaferry:2;") || strings.HasPrefix(progress.Metadata, "kaferry:4;"))

			// When the other binary encounters incompatible delivery authority, then it refuses before changing output.
			runMigrationExpect(t, direction.second, limited(sql), config, 2, "unsupported version of commit metadata")
			complete := archiveRecords(t, archive)
			require.Equal(t, first, complete)

			// Given a partially replayed archive with a saved record cursor.
			cloneSQL := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH (GROUP='file-migration') ", clone, archive)
			runMigrationBinary(t, direction.first, limited(cloneSQL), config)
			require.Equal(t, complete[:1], archiveRecords(t, clone))
			state := migrationCheckpoint(t, config, "file-migration")
			require.Equal(t, 1, state.Version)
			require.Equal(t, int64(1), state.Position)
			require.False(t, state.Complete)

			// When another implementation resumes this checkpoint, then its incompatible output state is rejected.
			runMigrationReject(t, direction.second, cloneSQL, config)
			require.Equal(t, complete[:1], archiveRecords(t, clone))

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
	for _, pattern := range diagnostic {
		if pattern != "" {
			require.Regexp(t, pattern, process.logs())
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

func TestPreviousBinaryRejectsChangedCapturedEndQuery(t *testing.T) {
	// Given a candidate bounded copy with a saved captured end.
	previous := os.Getenv("KAFERRY_PREVIOUS_BINARY")
	require.NotEmpty(t, previous)
	a := admin(t)
	source := topic(t, a, 1)
	p := producer(t)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	publish(t, p, &kgo.Record{Topic: source, Partition: 0, Value: []byte("bounded"), Timestamp: stamp})
	out := filepath.Join(t.TempDir(), "archive")
	group := source + "-bounded"
	config := configuration(t)
	statement := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH(GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s'", out, source, group, stamp.Add(time.Second).Format(time.RFC3339Nano))
	runMigrationBinary(t, testBinary(t), statement, config)
	progress := committedPartitions(t, a, group, source, 1)[0]
	require.True(t, strings.HasPrefix(progress.Metadata, "kaferry:4;"))
	before := archiveRecords(t, out)

	// When the other binary reads those commits with a changed query, then it rejects the format or query identity before writing.
	oldSQL := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH(GROUP='%s')", out, source, group)
	runMigrationExpect(t, previous, oldSQL, config, 2, "unsupported version of commit metadata|query fingerprint mismatch")
	require.Equal(t, before, archiveRecords(t, out))
}

func runMigrationReject(t *testing.T, binary, statement, config string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	process := startCLI(t, ctx, binary, "", "run", "-e", statement, "-c", config)
	err := process.wait()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, "%s", process.logs())
	require.Contains(t, []int{1, 2}, exit.ExitCode())
	require.Regexp(t, "checkpoint|state|identity|version|field", process.logs())
}
