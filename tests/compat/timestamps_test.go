//go:build integration

package compat

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLogAppendTimeDestinationRestampsUnpreservableValues(t *testing.T) {
	// Given
	a := admin(t)
	dst := topic(t, a, 1)
	alterTopicConfig(t, a, dst, map[string]string{"message.timestamp.type": "LogAppendTime"})

	path := filepath.Join(t.TempDir(), "source.avro")
	var records []core.Record
	for i, stamp := range []int64{0, -1, math.MaxInt64} {
		records = append(records, core.Record{Value: []byte(fmt.Sprint(i)), TimestampMS: &stamp, TimestampType: "create_time"})
	}
	writeArchiveRecords(t, path, records)
	before := time.Now().Add(-time.Second)

	// When
	execute(t, fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM FILE avro('%s') WITH (GROUP='%s-restore') ", dst, path, dst), configuration(t))
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
			path := filepath.Join(t.TempDir(), "out.avro")
			statement := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM TOPIC '%s' WITH (GROUP='%s-%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' ", path, src, src, test.name, test.lower, stamp.Add(time.Second).Format(time.RFC3339Nano))

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
