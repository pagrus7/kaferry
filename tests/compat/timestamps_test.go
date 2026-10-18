//go:build integration

package compat

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLogAppendTimeDestinationRestampsUnpreservableValues(t *testing.T) {
	// Given
	a := admin(t)
	dst := topic(t, a, 1)
	alterTopicConfig(t, a, dst, map[string]string{"message.timestamp.type": "LogAppendTime"})

	path := filepath.Join(t.TempDir(), "source.cbor")
	codec, _ := file.NewCodec(1 << 20)
	o, err := file.NewOutput(file.NewPublisher(), path, "fixture", "fixture", "", 0, codec, 10, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	for i, stamp := range []int64{0, -1, math.MaxInt64} {
		require.NoError(t, o.Submit(core.Record{Value: []byte(fmt.Sprint(i)), TimestampMS: &stamp, TimestampType: "create_time"}, core.Token{}))
	}
	require.NoError(t, o.Flush())
	o.Close()
	before := time.Now().Add(-time.Second)

	// When
	execute(t, fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='%s-restore')", dst, path, dst), configuration(t))
	c := consumer(t)

	// Then
	assign(t, c, dst, 1)
	for i := range 3 {
		r := read(t, c)
		if string(r.Value) != fmt.Sprint(i) || r.Attrs.TimestampType() != 1 || r.Timestamp.Before(before) || r.Timestamp.After(time.Now().Add(time.Second)) {
			t.Fatal("LogAppendTime did not restamp while preserving data")
		}
	}
}

func TestTimestampLookupAcrossSegmentsAndPreEpochBound(t *testing.T) {
	// Given
	a := admin(t)
	src := topic(t, a, 1)
	alterTopicConfig(t, a, src, map[string]string{"segment.bytes": "4096", "index.interval.bytes": "1024"})

	p := producer(t)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for i := range 40 {
		ts := stamp
		if i%2 == 0 {
			ts = ts.Add(-time.Millisecond)
		}
		value := make([]byte, 1024)
		value[0] = byte(i)
		publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: value, Timestamp: ts})
	}
	cfg := configuration(t)
	for _, test := range []struct {
		name, lower string
		count       int
	}{{"equal", stamp.Format(time.RFC3339Nano), 20}, {"preepoch", "1969-12-31T23:59:59.999Z", 40}} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "out.cbor")
			statement := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s-%s',CUTOFF=INTERVAL '0' SECOND)", path, src, test.lower, stamp.Add(time.Second).Format(time.RFC3339Nano), src, test.name)

			// When
			execute(t, statement, cfg)
			records := archiveRecords(t, path)

			// Then
			require.Len(t, records, test.count, "timestamp lookup omitted matching records: got %d want %d", len(records), test.count)
			for i, r := range records {
				want := i
				if test.name == "equal" {
					want = 2*i + 1
				}
				if len(r.Value) != 1024 || int(r.Value[0]) != want {
					t.Fatal("segment lookup skipped earlier matching timestamps")
				}
			}
		})
	}
}
