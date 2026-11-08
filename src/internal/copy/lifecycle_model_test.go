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
type lifecycleModel struct {
	tracker *Tracker
	parts   [4]modelPartition
	stale   []Token
}

func checkLifecycle(t *testing.T, actions []byte) {
	t.Helper()
	model := lifecycleModel{tracker: NewTracker(), stale: make([]Token, 0, 32)}
	for id := range model.parts {
		model.tracker.Assign(Cursor{Partition: int32(id)})
	}
	for step, action := range actions {
		model.apply(t, step, action)
		model.check(t, step)
	}
}
func (m *lifecycleModel) apply(t *testing.T, step int, action byte) {
	t.Helper()
	id := int(action >> 6)
	part := &m.parts[id]
	cursor := Cursor{Partition: int32(id), Position: part.consumed}
	switch action % 8 {
	case 0, 1:
		if len(part.work) < 64 {
			token, err := m.tracker.Observe(cursor, cursor.Position+1, int64(action)+1)
			require.NoError(t, err)
			part.work = append(part.work, modelDelivery{token: token, position: cursor.Position, next: cursor.Position + 1, bytes: int64(action) + 1})
			part.consumed++
		}
	case 2:
		part.submit(m.tracker, step)
	case 3, 4:
		part.settle(t, m.tracker, step, action%8 == 3)
	case 5:
		m.commit(id)
	case 6:
		m.revoke(id)
	case 7:
		if len(part.work) < 64 {
			cursor.Position++
			require.NoError(t, m.tracker.Advance(cursor))
			// Advance creates resolved work without a delivery callback.
			part.work = append(part.work, modelDelivery{position: cursor.Position, next: cursor.Position, done: true})
			part.consumed++
		}
	}
}
func (p *modelPartition) submit(tracker *Tracker, step int) {
	if len(p.work) == 0 {
		return
	}
	work := &p.work[step%len(p.work)]
	if work.bytes > 0 {
		tracker.Submitted(work.token)
		work.submitted = true
	}
}
func (p *modelPartition) settle(t *testing.T, tracker *Tracker, step int, success bool) {
	t.Helper()
	if len(p.work) == 0 {
		return
	}
	work := &p.work[step%len(p.work)]
	if work.bytes == 0 {
		return
	}
	require.True(t, tracker.Settle(work.token, success))
	work.done = work.done || success
	work.submitted = false
}
func (m *lifecycleModel) commit(id int) {
	part := &m.parts[id]
	confirmed, _, _, _ := part.expected()
	m.tracker.Committed(Cursor{Partition: int32(id), Position: confirmed})
	kept := part.work[:0]
	for _, work := range part.work {
		if work.next > confirmed {
			kept = append(kept, work)
		} else {
			m.stale = append(m.stale, work.token)
		}
	}
	part.work, part.saved = kept, confirmed
}
func (m *lifecycleModel) revoke(id int) {
	part := &m.parts[id]
	for _, work := range part.work {
		m.stale = append(m.stale, work.token)
	}
	m.tracker.Revoke(int32(id))
	m.tracker.Assign(Cursor{Partition: int32(id), Position: part.saved})
	part.work, part.consumed = nil, part.saved
}
func (p modelPartition) expected() (confirmed, bytes int64, resolved, inFlight bool) {
	confirmed, resolved = p.consumed, true
	for _, work := range p.work {
		bytes += work.bytes
		if !work.done {
			confirmed = min(confirmed, work.position)
			resolved = false
		}
		inFlight = inFlight || work.submitted && !work.done
	}
	return
}
func (m *lifecycleModel) check(t *testing.T, step int) {
	t.Helper()
	if len(m.stale) > 32 {
		m.stale = m.stale[len(m.stale)-32:]
	}
	for _, token := range m.stale {
		require.False(t, m.tracker.Settle(token, true), "stale outcome at step %d", step)
	}
	totalRecords, totalBytes, inFlight := 0, int64(0), false
	for id, state := range m.parts {
		want, bytes, resolved, submitted := state.expected()
		inFlight = inFlight || submitted
		require.Equal(t, Cursor{Partition: int32(id), Position: want}, m.tracker.Confirmed(int32(id)), "step %d", step)
		require.Equal(t, state.saved, m.tracker.Saved(int32(id)))
		records, actualBytes := m.tracker.Work(int32(id))
		require.Equal(t, len(state.work), records)
		require.Equal(t, bytes, actualBytes)
		require.Equal(t, resolved, m.tracker.Resolved(int32(id)))
		totalRecords += records
		totalBytes += bytes
	}
	records, bytes := m.tracker.Total()
	require.Equal(t, totalRecords, records)
	require.Equal(t, totalBytes, bytes)
	require.Equal(t, inFlight, m.tracker.InFlight())
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
