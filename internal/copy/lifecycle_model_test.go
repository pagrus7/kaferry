package copy

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

type modelDelivery struct {
	token                 Token
	position, next, bytes int64
	done, submitted       bool
}
type modelPartition struct {
	saved, consumed int64
	work            []modelDelivery
}

// The model computes the first unfinished input from its complete event history;
// it does not use the tracker's generation, pending map or prefix implementation.
func checkLifecycle(t *testing.T, actions []byte) {
	t.Helper()
	tracker := NewTracker()
	parts := [4]modelPartition{}
	stale := make([]Token, 0, 32)
	for p := range parts {
		tracker.Assign(Cursor{Partition: int32(p)})
	}
	for step, action := range actions {
		p := int(action >> 6)
		part := &parts[p]
		cursor := Cursor{Partition: int32(p), Position: part.consumed}
		confirmed := part.consumed
		for _, work := range part.work {
			if !work.done {
				confirmed = min(confirmed, work.position)
			}
		}
		switch action % 8 {
		case 0, 1:
			if len(part.work) < 64 {
				token, err := tracker.Observe(cursor, cursor.Position+1, int64(action)+1)
				require.NoError(t, err)
				part.work = append(part.work, modelDelivery{token: token, position: cursor.Position, next: cursor.Position + 1, bytes: int64(action) + 1})
				part.consumed++
			}
		case 2:
			if len(part.work) != 0 {
				index := step % len(part.work)
				work := &part.work[index]
				if work.bytes > 0 {
					tracker.Submitted(work.token)
					work.submitted = true
				}
			}
		case 3, 4:
			if len(part.work) != 0 {
				work := &part.work[step%len(part.work)]
				if work.bytes == 0 {
					break
				}
				success := action%8 == 3
				require.True(t, tracker.Settle(work.token, success))
				work.done = work.done || success
				work.submitted = false
			}
		case 5:
			tracker.Committed(Cursor{Partition: int32(p), Position: confirmed})
			kept := part.work[:0]
			for _, work := range part.work {
				if work.next > confirmed {
					kept = append(kept, work)
				} else {
					stale = append(stale, work.token)
				}
			}
			part.work, part.saved = kept, confirmed
		case 6:
			for _, work := range part.work {
				stale = append(stale, work.token)
			}
			tracker.Revoke(int32(p))
			tracker.Assign(Cursor{Partition: int32(p), Position: part.saved})
			part.work, part.consumed = nil, part.saved
		case 7:
			if len(part.work) < 64 {
				cursor.Position++
				require.NoError(t, tracker.Advance(cursor))
				// Advance creates resolved work without a delivery callback.
				part.work = append(part.work, modelDelivery{position: cursor.Position, next: cursor.Position, done: true})
				part.consumed++
			}
		}
		if len(stale) > 32 {
			stale = stale[len(stale)-32:]
		}
		for _, token := range stale {
			require.False(t, tracker.Settle(token, true), "stale outcome at step %d", step)
		}
		totalRecords, totalBytes, inFlight := 0, int64(0), false
		for id, state := range parts {
			want, bytes, resolved := state.consumed, int64(0), true
			for _, work := range state.work {
				bytes += work.bytes
				if !work.done {
					want = min(want, work.position)
					resolved = false
				}
				inFlight = inFlight || work.submitted && !work.done
			}
			require.Equal(t, Cursor{Partition: int32(id), Position: want}, tracker.Confirmed(int32(id)), "step %d", step)
			require.Equal(t, state.saved, tracker.Saved(int32(id)))
			records, actualBytes := tracker.Work(int32(id))
			require.Equal(t, len(state.work), records)
			require.Equal(t, bytes, actualBytes)
			require.Equal(t, resolved, tracker.Resolved(int32(id)))
			totalRecords += records
			totalBytes += bytes
		}
		records, bytes := tracker.Total()
		require.Equal(t, totalRecords, records)
		require.Equal(t, totalBytes, bytes)
		require.Equal(t, inFlight, tracker.InFlight())
	}
}

func TestProgressLifecycleModel(t *testing.T) {
	// Given reproducible interleavings of four owners and delayed outcomes.
	for seed := range uint64(32) {
		t.Run(string(rune('A'+seed)), func(t *testing.T) {
			random := rand.New(rand.NewPCG(seed, 42))
			actions := make([]byte, 1024)
			for i := range actions {
				actions[i] = byte(random.Uint32())
			}
			// When ownership, submissions, deliveries, gaps and saves interleave,
			// then confirmed progress and resource accounting match the model.
			checkLifecycle(t, actions)
		})
	}
}

func FuzzProgressLifecycle(f *testing.F) {
	f.Add([]byte{0, 1, 3, 5, 6, 3, 64, 67, 69})
	f.Add([]byte{7, 0, 2, 4, 6, 3, 5})
	f.Fuzz(func(t *testing.T, actions []byte) {
		if len(actions) > 2048 {
			t.Skip()
		}
		checkLifecycle(t, actions)
	})
}
