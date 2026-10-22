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
			state := benchmarkCheckpointState(files)
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

func benchmarkCheckpointState(files int) State {
	state := State{Version: 1, Group: "group", Fingerprint: "fingerprint", Files: make([]Progress, files)}
	for i := range state.Files {
		f := descriptor(i+1, records(42))
		state.Files[i] = Progress{File: f, Next: initial(f)}
	}
	return state
}

type discardCheckpoint struct{}

func (discardCheckpoint) Replace(string, []byte) error { return nil }

func BenchmarkCheckpointSaving(b *testing.B) {
	for _, files := range []int{1, 1000, 10000, 100000} {
		b.Run(fmt.Sprintf("files=%d", files), func(b *testing.B) {
			// Given the real checkpoint validator/encoder and an atomic writer without disk latency.
			state := benchmarkCheckpointState(files)
			journal, err := NewJournal(b.TempDir(), state.Group, NativeGroupFiles{}, discardCheckpoint{})
			if err != nil {
				b.Fatal(err)
			}
			defer journal.Close()
			if err = journal.Save(state); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			// When saving a batch, measure all journal work; filesystem sync is measured by the CLI loads.
			for b.Loop() {
				if err := journal.Save(state); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
