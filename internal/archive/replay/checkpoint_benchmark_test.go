package replay

import (
	"fmt"
	"testing"

	"github.com/pagrus7/kaferry/internal/checkpoint"
)

func BenchmarkCheckpointEncoding(b *testing.B) {
	for _, files := range []int{1, 1000, 10000, 100000} {
		b.Run(fmt.Sprintf("files=%d", files), func(b *testing.B) {
			// Given a representative pinned selection with a cursor for every file.
			state := State{Version: 1, Group: "group", Fingerprint: "fingerprint", Files: make([]Progress, files)}
			for i := range state.Files {
				f := descriptor(i+1, records(42))
				state.Files[i] = Progress{File: f, Next: initial(f)}
			}
			data, err := checkpoint.Encode(state, maxCheckpointBytes)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			// When saving, measure JSON serialization and checksum together, without disk latency.
			for b.Loop() {
				if _, err := checkpoint.Encode(state, maxCheckpointBytes); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
