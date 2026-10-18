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

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/stretchr/testify/require"
)

func TestFileLimitResumeAndSavedStatus(t *testing.T) {
	type step struct{ limit, records, status int }
	resume := []step{{2, 2, 4}, {1, 3, 4}, {10, 5, 0}}
	for _, scenario := range []struct {
		name, group string
		steps       []step
	}{
		{"named group", "'preview'", resume},
		{"one-off resumed by logged group", "ONE_OFF", resume},
		{"one-off reaches EOF before cap", "ONE_OFF", []step{{10, 5, 0}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given five complete record frames and an empty destination.
			fixture := filePreview(t)
			codec, err := file.NewCodec(1 << 20)
			require.NoError(t, err)
			source := filepath.Join(fixture.directory, "source.cbor")
			data, err := os.ReadFile(source)
			require.NoError(t, err)
			var frames [][]byte
			for i := range 5 {
				frame, err := codec.EncodeRecord(core.Record{Value: []byte{byte(i)}, TimestampType: "unknown"})
				require.NoError(t, err)
				frames = append(frames, frame)
				data = append(data, frame...)
			}
			require.NoError(t, os.WriteFile(source, data, 0600))
			query := strings.Replace(fixture.query, "'preview'", scenario.group, 1)
			for _, step := range scenario.steps {
				limited := strings.Replace(query, "OPTIONS", fmt.Sprintf("LIMIT %d OPTIONS", step.limit), 1)
				var diagnostics bytes.Buffer
				// When running or resuming with a fresh cap, possibly a different one.
				code := Execute(t.Context(), []string{"run", "-e", limited, "-c", fixture.config}, nil, io.Discard, &diagnostics)
				// Then keep exactly the expected prefix and a resumable checkpoint.
				require.Zero(t, code, diagnostics.String())
				output, err := os.ReadFile(fixture.destination)
				require.NoError(t, err)
				header, start, err := codec.Header(bytes.NewReader(output), int64(len(output)))
				require.NoError(t, err)
				require.Equal(t, bytes.Join(frames[:step.records], nil), output[start:])
				checkpoints, err := filepath.Glob(filepath.Join(fixture.directory, "*.json"))
				require.NoError(t, err)
				require.Len(t, checkpoints, 1, "limited ONE_OFF progress must remain available")
				query = strings.Replace(fixture.query, "'preview'", "'"+header.Group+"'", 1)
				var stdout bytes.Buffer
				diagnostics.Reset()
				code = Execute(t.Context(), []string{"status", "-e", query, "-c", fixture.config, "--json"}, nil, &stdout, &diagnostics)
				require.Equal(t, step.status, code, diagnostics.String())
				var report inspect.StatusReport
				require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
				require.Len(t, report.Partitions, 1)
				if step.status == 4 {
					require.Equal(t, "incomplete", report.Verdict)
					require.Nil(t, report.Partitions[0].Stop, "a cap is not evidence of source exhaustion")
				} else {
					require.Equal(t, "stopped", report.Verdict)
					require.NotNil(t, report.Partitions[0].Stop)
					require.Equal(t, "eof", report.Partitions[0].Stop.Reason)
				}
			}
		})
	}
}
