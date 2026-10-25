package app

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
)

func TestMissingTimestampSkipAndResumeWithDifferentPolicy(t *testing.T) {
	// Given a file with a missing timestamp between two valid records.
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "source.avro"), filepath.Join(dir, "copy.avro")
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	ms := stamp.UnixMilli()
	var records []core.Record
	for i, ts := range []*int64{&ms, nil, &ms} {
		kind := "create_time"
		if ts == nil {
			kind = "unknown"
		}
		records = append(records, core.Record{Value: []byte(fmt.Sprint(i)), TimestampMS: ts, TimestampType: kind})
	}
	writeArchiveRecords(t, src, records)
	cfg := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir = %q\n", dir)), 0600))
	prefix := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH(GROUP='missing') WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' ", dst, src, stamp.Add(-time.Second).Format(time.RFC3339Nano))
	var output bytes.Buffer

	// When copying with the default policy, then the missing timestamp fails the run.
	require.EqualValues(t, 1, Execute(t.Context(), []string{"run", "-e", prefix, "-c", cfg}, nil, io.Discard, &output))
	output.Reset()
	// When resuming the same group with SKIP, then copying succeeds.
	require.Zero(t, Execute(t.Context(), []string{"run", "-e", prefix + " OPTIONS(ON_MISSING_TIMESTAMP=SKIP)", "-c", cfg}, nil, io.Discard, &output))

	// Then both valid records remain in order, without loss or duplication.
	var values []string
	for _, record := range archiveRecords(t, dst) {
		values = append(values, string(record.Value))
	}
	require.Equal(t, []string{"0", "2"}, values)
}
