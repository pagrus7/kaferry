package app

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/stretchr/testify/require"
)

func TestArchiveCriteriaCLIUsesOriginalStreamsAndResumesCompletion(t *testing.T) {
	for _, mode := range []string{"full snapshot", "disorder", "offset end"} {
		t.Run(mode, func(t *testing.T) {
			// Given an immutable archive with decreasing timestamps in one original partition.
			root := t.TempDir()
			input, output := filepath.Join(root, "input"), filepath.Join(root, "output")
			source := avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}
			var values []avro.Record
			for i, stamp := range []int64{10, 100, 20} {
				values = append(values, avro.Record{Source: source, Offset: 100 + int64(i), TimestampMS: new(stamp), TimestampKind: "create_time", Value: []byte{byte(i)}})
			}
			archiveFixture(t, input, 1, values)
			cfg := filepath.Join(root, "worker.toml")
			require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\n", root)), 0600))
			settings, criteria := "", "RECORD_TIMESTAMP < TIMESTAMP '1970-01-01T00:00:00.050Z'"
			want := []int64{100, 102}
			if mode == "disorder" {
				settings = ",MAX_OUT_OF_ORDER=INTERVAL '0' SECOND"
				want = []int64{100}
			}
			if mode == "offset end" {
				criteria = "RECORD_OFFSET>=101 AND RECORD_OFFSET<102"
				want = []int64{101}
			}
			query := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH(GROUP='criteria'%s) WHERE %s", output, input, settings, criteria)
			// When run and then resumed, then copy only matching original records and do not reopen completed streams.
			for range 2 {
				var logs bytes.Buffer
				code := Execute(t.Context(), []string{"run", "-e", query, "-c", cfg}, nil, &logs, &logs)
				require.Zero(t, code, logs.String())
			}
			got := archiveRecords(t, output)
			var offsets []int64
			for _, record := range got {
				offsets = append(offsets, record.Offset)
			}
			require.Equal(t, want, offsets)
		})
	}
}
