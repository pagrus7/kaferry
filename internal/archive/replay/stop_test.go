package replay

import (
	"github.com/pagrus7/kaferry/internal/archive/avro"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStopSavesAtUnchangedPositionWithoutClaimingEOF(t *testing.T) {
	// Given two inputs and only one confirmed record.
	values := records(42, 43)
	f := descriptor(1, values)
	store := &memoryCheckpoints{}
	s := session(t, store, &memorySnapshots{files: []File{f}}, &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values}}, 10)
	_, err := s.Next()
	require.NoError(t, err)
	stop := &core.Stop{Reason: "worker_record_cap", At: 1}
	// When saving a worker cap, then the selection remains incomplete and owns its stop data.
	require.NoError(t, s.CheckpointStop(1, stop))
	stop.At = 99
	require.EqualValues(t, 1, s.Saved().Stop.At)
	require.False(t, s.Saved().Complete)
	require.ErrorContains(t, s.CheckpointStop(1, &core.Stop{Reason: "eof", At: 1}), "invalid replay stop")
	require.NoError(t, s.CheckpointStop(1, nil))
	require.Nil(t, s.Saved().Stop)
	// When EOF is confirmed after a periodic save, then the unchanged position still saves its reason.
	_, err = s.Next()
	require.NoError(t, err)
	require.NoError(t, s.Checkpoint(2))
	require.True(t, s.Saved().Complete)
	before := len(store.writes)
	require.NoError(t, s.CheckpointStop(2, &core.Stop{Reason: "eof", At: 2}))
	require.Len(t, store.writes, before+1)
	require.Equal(t, "eof", s.Saved().Stop.Reason)
}
