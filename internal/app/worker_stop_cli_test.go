package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/stretchr/testify/require"
)

func TestFileWorkerStopResumeAndSavedStatus(t *testing.T) {
	type step struct {
		stopAfter, records int
		reason             string
	}
	resume := []step{{2, 2, "worker_record_cap"}, {1, 3, "worker_record_cap"}, {10, 5, "eof"}}
	for _, scenario := range []struct {
		name, group string
		steps       []step
	}{
		{"named group", "'preview'", resume},
		{"one-off resumed by logged group", "ONE_OFF", resume},
		{"one-off reaches EOF before cap", "ONE_OFF", []step{{10, 5, "eof"}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given five archived records and an empty destination.
			fixture := filePreview(t)
			source := filepath.Join(fixture.directory, "source.avro")
			var records []core.Record
			for i := range 5 {
				records = append(records, core.Record{Value: []byte{byte(i)}, TimestampType: "unknown"})
			}
			writeArchiveRecords(t, source, records)
			query := strings.Replace(fixture.query, "'preview'", scenario.group, 1)
			for _, step := range scenario.steps {
				limited := strings.Replace(query, "OPTIONS(", fmt.Sprintf("OPTIONS(WORKER_STOP_AFTER_RECORDS=%d,", step.stopAfter), 1)
				var diagnostics bytes.Buffer
				// When running or resuming with a fresh cap, possibly a different one.
				code := Execute(t.Context(), []string{"run", "-e", limited, "-c", fixture.config}, nil, io.Discard, &diagnostics)
				// Then keep exactly the expected prefix and a resumable checkpoint.
				require.Zero(t, code, diagnostics.String())
				output := archiveRecords(t, fixture.destination)
				require.Len(t, output, step.records)
				for i, record := range output {
					require.Equal(t, []byte{byte(i)}, record.Value)
				}
				checkpoints, err := filepath.Glob(filepath.Join(fixture.directory, "*.json"))
				require.NoError(t, err)
				require.Len(t, checkpoints, 1, "limited ONE_OFF progress must remain available")
				data, err := os.ReadFile(checkpoints[0])
				require.NoError(t, err)
				state, err := checkpoint.Decode[replay.State](data, 64<<20)
				require.NoError(t, err)
				query = strings.Replace(fixture.query, "'preview'", "'"+state.Group+"'", 1)
				var stdout bytes.Buffer
				diagnostics.Reset()
				code = Execute(t.Context(), []string{"status", "-e", query, "-c", fixture.config, "--json"}, nil, &stdout, &diagnostics)
				require.Equal(t, 0, code, diagnostics.String())
				var report inspect.StatusReport
				require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
				require.Len(t, report.Partitions, 1)
				require.Equal(t, "stopped", report.Verdict)
				require.NotNil(t, report.Partitions[0].Stop)
				require.Equal(t, step.reason, report.Partitions[0].Stop.Reason)
				require.Equal(t, inspect.StopDescription(step.reason), report.Reason)
			}
		})
	}
}
