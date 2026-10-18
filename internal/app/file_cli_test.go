package app

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/stretchr/testify/require"
)

func TestMissingTimestampSkipAndResumeWithDifferentPolicy(t *testing.T) {
	// Given a file with a missing timestamp between two valid records.
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "source.cbor"), filepath.Join(dir, "copy.cbor")
	codec, err := file.NewCodec(64 << 20)
	require.NoError(t, err)
	writer, err := file.NewOutput(file.NewPublisher(), src, "fixture", "fixture", "", 0, codec, 10, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(writer.Close)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	ms := stamp.UnixMilli()
	for i, ts := range []*int64{&ms, nil, &ms} {
		kind := "create_time"
		if ts == nil {
			kind = "unknown"
		}
		require.NoError(t, writer.Submit(core.Record{Value: []byte(fmt.Sprint(i)), TimestampMS: ts, TimestampType: kind}, core.Token{}))
	}
	require.NoError(t, writer.Flush())
	writer.Close()
	cfg := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir = %q\n", dir)), 0600))
	prefix := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' ", dst, src, stamp.Add(-time.Second).Format(time.RFC3339Nano))
	var output bytes.Buffer

	// When copying with the default policy, then the missing timestamp fails the run.
	require.EqualValues(t, 1, Execute(t.Context(), []string{"run", "-e", prefix + "OPTIONS(GROUP='missing')", "-c", cfg}, nil, io.Discard, &output))
	output.Reset()
	// When resuming the same group with SKIP, then copying succeeds.
	require.Zero(t, Execute(t.Context(), []string{"run", "-e", prefix + "OPTIONS(GROUP='missing',ON_MISSING_TIMESTAMP=SKIP)", "-c", cfg}, nil, io.Discard, &output))

	// Then both valid records remain in order, without loss or duplication.
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	_, pos, err := codec.Header(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	var values []string
	for pos < int64(len(data)) {
		require.GreaterOrEqual(t, int64(len(data))-pos, int64(8))
		length := int64(binary.BigEndian.Uint32(data[pos:]))
		require.LessOrEqual(t, length, int64(len(data))-pos-8)
		record, err := codec.DecodeRecord(data[pos+4 : pos+4+length])
		require.NoError(t, err)
		values = append(values, string(record.Value))
		pos += length + 8
	}
	require.Equal(t, []string{"0", "2"}, values)
}
