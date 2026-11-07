//go:build integration && load

package compat

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLoadSamplesMemoryBeforeReleasingInput(t *testing.T) {
	// Given a CLI blocked on SQL input, which may finish before the next sample.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	process := launchCLI(t, ctx, testBinary(t), "run", "-f", "-")

	// When sampling finishes before SQL is released.
	finish := watchRSS(t, process)
	memory := finish()

	// Then memory was measured while the CLI was alive and waiting for input.
	require.Positive(t, memory.samples, "initial memory sample")
	require.Positive(t, memory.peak, "resident memory")
	require.False(t, process.finished(), "CLI must wait for SQL")
	require.Equal(t, memory, finish(), "sampling cleanup is safe to repeat")
	require.NoError(t, process.send("SELECT"))
	var exit *exec.ExitError
	require.ErrorAs(t, process.wait(), &exit, "invalid SQL must fail")
	require.Equal(t, 2, exit.ExitCode(), "invalid SQL exit code")
	require.NoError(t, ctx.Err(), "CLI must exit before the timeout")
}

func TestLoadStreamingVerifierBoundsHistory(t *testing.T) {
	// Given many epochs arriving out of order within a bounded verification window.
	stamp := time.Now().Truncate(time.Millisecond)
	stream := newStreamRecords("fixture", stamp)
	stream.published = 4096
	for first := 0; first < stream.published; first += 16 {
		for partition := 3; partition >= 0; partition-- {
			for id := first + partition; id < first+16; id += 4 {
				input := archiveLoadRecord(id)
				headers := make([]kgo.RecordHeader, len(input.Headers))
				for i, h := range input.Headers {
					headers[i] = kgo.RecordHeader{Key: h.Name, Value: h.Value}
				}
				// When consumed records are verified, then completed history is released.
				stream.verify(t, &kgo.Record{Key: input.Key, Value: input.Value, Headers: headers, Timestamp: stamp, Partition: int32(id % 4)})
				require.LessOrEqual(t, len(stream.seen), 16)
			}
		}
	}
	require.Equal(t, stream.published, stream.verified)
	require.Equal(t, stream.published, stream.contiguous)
	require.Empty(t, stream.seen)
}
