package file

import (
	"bytes"
	"fmt"
	"testing"

	core "kafka-fwd2/internal/copy"
)

func BenchmarkRecordFraming(b *testing.B) {
	for _, size := range []int{1024, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			codec, err := NewCodec(64 << 20)
			if err != nil {
				b.Fatal(err)
			}
			stamp := int64(1791040000000)
			r := core.Record{Key: []byte("key"), Value: make([]byte, size), TimestampMS: &stamp, TimestampType: "create_time", Headers: []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				frame, err := codec.EncodeRecord(r)
				if err != nil {
					b.Fatal(err)
				}
				payload, _, _, err := readFrame(bytes.NewReader(frame), 0, int64(len(frame)), codec.limit)
				if err != nil {
					b.Fatal(err)
				}
				decoded, err := codec.DecodeRecord(payload)
				if err != nil || len(decoded.Value) != size {
					b.Fatal("round trip failed", err)
				}
			}
		})
	}
}
