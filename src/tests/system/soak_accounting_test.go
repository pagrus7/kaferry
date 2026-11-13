//go:build integration && load

package compat

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func streamFixtureRecord(id int, stamp time.Time) *kgo.Record {
	record := archiveLoadRecord(id)
	headers := make([]kgo.RecordHeader, len(record.Headers))
	for i, h := range record.Headers {
		headers[i] = kgo.RecordHeader{Key: h.Name, Value: h.Value}
	}
	return &kgo.Record{Key: record.Key, Value: record.Value, Headers: headers, Timestamp: stamp, Partition: int32(id % 4)}
}
