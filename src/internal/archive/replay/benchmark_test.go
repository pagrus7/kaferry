package replay

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// Exercise cache eviction with overlapping source offsets, not just sequential files.
func BenchmarkReplayOverlap(b *testing.B) {
	for _, cached := range []int{2, 32} {
		b.Run(fmt.Sprintf("cached-%d", cached), func(b *testing.B) {
			root := b.TempDir()
			for id := range 16 {
				values := records()
				for offset := range 1000 {
					values = append(values, record(42+int64(offset)))
				}
				writeFile(b, root, id+1, values, 0)
			}
			catalog := NewCatalog(root, codec(b), NativeFiles{})
			selection, err := catalog.Select(nil)
			require.NoError(b, err)
			progress := make([]Progress, len(selection.Files))
			for i, f := range selection.Files {
				progress[i] = Progress{File: f, Next: initial(f)}
			}
			b.ReportAllocs()
			b.ResetTimer()
			var loads int
			for range b.N {
				meter := &measuredOpener{Opener: catalog}
				pool, err := NewPool(meter, Limits{OpenFiles: 32, Blocks: cached})
				require.NoError(b, err)
				r, err := NewReader(progress, pool)
				require.NoError(b, err)
				count := 0
				for {
					_, err := r.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					count++
				}
				require.Equal(b, 16000, count)
				require.NoError(b, r.Close())
				loads += meter.blocks
			}
			b.ReportMetric(float64(loads)/float64(b.N), "block-loads/op")
			b.ReportMetric(float64(16000*b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
