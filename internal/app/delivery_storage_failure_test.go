package app

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

type failingDataCheckpoint struct {
	checkpoint.DurableWriter
	mirror  string
	failed  atomic.Bool
	failure error
}

func (w *failingDataCheckpoint) Replace(path string, data []byte) error {
	dataBearing := false
	if w.mirror != "" && path == w.mirror {
		state, err := checkpoint.Decode[replay.State](data, 64<<20)
		if err != nil {
			return err
		}
		dataBearing = state.Position > 0
	}
	if w.mirror == "" && filepath.Base(path) == "state.json" {
		envelope, err := checkpoint.Decode[archiveEnvelope](data, 96<<20)
		if err != nil {
			return err
		}
		dataBearing = envelope.Frame != nil && envelope.Frame.Cursor.Position > 0 && len(envelope.Capture.Files) > 0
	}
	if dataBearing && w.failed.CompareAndSwap(false, true) {
		return w.failure
	}
	return w.DurableWriter.Replace(path, data)
}
func TestOutputAndCursorSurviveCheckpointReplacementFailures(t *testing.T) {
	for _, c := range []struct {
		name, src, dst string
		mirror         bool
	}{
		{"local authority", "ARCHIVE", "ARCHIVE", false},
		{"local mirror after Kafka authority", "TOPIC", "ARCHIVE", false},
		{"replay mirror after local authority", "ARCHIVE", "ARCHIVE", true},
		{"replay mirror after Kafka output", "ARCHIVE", "TOPIC", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			// Given a failing data-bearing replacement through the actual archive adapter's writer port.
			source, destination := clusters(t, 2)
			input, target := "TOPIC 'input@src'", "TOPIC 'output@dst'"
			expected := map[string]int{}
			if c.src == "ARCHIVE" {
				root := t.TempDir()
				expected = archiveInput(t, root)
				input = "ARCHIVE '" + root + "'"
			} else {
				seed(t, source, 1, 4)
				for n := range 4 {
					expected[fmt.Sprintf("0:%d", n)] = 1
				}
			}
			outputRoot := ""
			if c.dst == "ARCHIVE" {
				outputRoot = t.TempDir()
				target = "ARCHIVE '" + outputRoot + "'"
			}
			ast, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH(GROUP='storage-failure') WHERE RECORD_OFFSET<LATEST", target, input))
			require.NoError(t, err)
			p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source, ",")}, "dst": {"bootstrap.servers": strings.Join(destination, ",")}})
			require.NoError(t, err)
			cfg := config.Defaults("")
			directory := t.TempDir()
			cfg.Worker.CheckpointDir = &directory
			cfg.Worker.RecordCap = 1
			cfg.Worker.PartitionRecordCap = 1
			failure := errors.New("injected durable replacement failure")
			writer := &failingDataCheckpoint{failure: failure}
			if c.mirror {
				resolved, err := filepath.EvalSymlinks(directory)
				require.NoError(t, err)
				writer.mirror = checkpoint.Path(resolved, p.Group)
			}
			// When saving fails before or after the authoritative commit, then failure propagates without publishing unapproved bytes.
			err = Run(testContext(t), p, cfg, Options{ProgressTopic: "progress", Writer: writer})
			require.ErrorIs(t, err, failure)
			require.True(t, writer.failed.Load())
			if c.dst == "ARCHIVE" {
				records, _, err := scanArchiveRecords(outputRoot)
				require.NoError(t, err)
				require.Empty(t, records, "a failed save must not publish private files")
			}
			// When the same job restarts, then ownership was released and authority determines the exact resume cursor.
			require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
			if c.dst == "ARCHIVE" {
				require.Equal(t, expected, archiveValues(t, outputRoot))
			} else {
				require.Equal(t, expected, output(t, destination, 2))
			}
			if c.src == "ARCHIVE" {
				state, err := replay.ReadCheckpoint(directory, p.Group, replay.NativeGroupFiles{})
				require.NoError(t, err)
				require.NotNil(t, state)
				require.True(t, state.Complete)
			}
		})
	}
}
