package avro

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func benchmarkRecords(dataset string) []Record {
	random := rand.New(rand.NewPCG(1, 2))
	result := make([]Record, 1000)
	for i := range result {
		record := Record{Source: identity().Source, Offset: 42 + int64(i), TimestampMS: new(int64(1700000000000 + i)), TimestampKind: "create_time", Key: []byte("account-17"), Headers: []Header{{Key: []byte("region"), Value: []byte("us-east")}}}
		switch dataset {
		case "tiny":
			record.Value = []byte("order created")
		case "json1k":
			record.Value = append([]byte(`{"status":"created","text":"`), bytes.Repeat([]byte("order payload "), 73)...)
			record.Value = append(record.Value, []byte(`"}`)...)
		case "random1k":
			record.Value = make([]byte, 1024)
			for j := range record.Value {
				record.Value[j] = byte(random.Uint32())
			}
		}
		result[i] = record
	}
	return result
}

func BenchmarkEncode(b *testing.B) {
	for _, dataset := range []string{"tiny", "json1k", "random1k"} {
		b.Run(dataset, func(b *testing.B) {
			c, values := codec(b, 64<<20), benchmarkRecords(dataset)
			b.ReportAllocs()
			b.ResetTimer()
			var size int64
			for range b.N {
				// Reset offsets through a fresh container, as capture does at rotation.
				w, err := c.NewWriter(io.Discard, identity())
				require.NoError(b, err)
				for _, record := range values {
					if err := w.Append(record); err != nil {
						b.Fatal(err)
					}
				}
				require.NoError(b, w.Flush())
				size = w.Size()
			}
			b.ReportMetric(float64(size)/float64(len(values)), "container-B/record")
			b.ReportMetric(float64(len(values))*float64(b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, dataset := range []string{"tiny", "json1k", "random1k"} {
		b.Run(dataset, func(b *testing.B) {
			c, values := codec(b, 64<<20), benchmarkRecords(dataset)
			data := write(b, c, values, len(values))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r, err := c.Open(bytes.NewReader(data), int64(len(data)))
				require.NoError(b, err)
				stats, err := r.Scan()
				require.NoError(b, err)
				require.EqualValues(b, len(values), stats.Records)
			}
			b.ReportMetric(float64(len(values))*float64(b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
