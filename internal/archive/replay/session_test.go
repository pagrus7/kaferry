package replay

import (
	"io"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestSelectionIsSavedBeforeAnyRecordAndOnlyConfirmedCursorsAdvance(t *testing.T) {
	// Given a new group, one file and a destination that confirms only the first input.
	values := records(42, 42, 43)
	f := descriptor(1, values)
	store := &memoryCheckpoints{}
	blocks := &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values}}
	inputs := &memorySnapshots{files: []File{f}, private: 2}
	s := session(t, store, inputs, blocks, 10)
	require.Len(t, store.writes, 1, "pin the selection before producing")
	require.Zero(t, blocks.reads)
	require.Equal(t, 2, s.PrivateFiles)
	// When all three records are read but only the first prefix is saved.
	for range 3 {
		_, err := s.Next()
		require.NoError(t, err)
	}
	require.NoError(t, s.Checkpoint(1))
	// Then persisted progress includes one physical record, retaining its equal-offset successor.
	require.EqualValues(t, 1, store.saved.Position)
	require.EqualValues(t, 1, store.saved.Files[0].Next.Record)
	require.False(t, store.saved.Complete)
	require.Len(t, s.pending, 2)
	require.NoError(t, s.Checkpoint(1))
	require.Len(t, store.writes, 2, "quiet progress performs no write")
	require.Error(t, s.Checkpoint(0))
	require.Error(t, s.Checkpoint(4))
	require.Len(t, store.writes, 2)
}

func TestPendingCapResumesOnlyAfterConfirmedProgress(t *testing.T) {
	// Given a two-record pending budget and three available records.
	values := records(42, 43, 44)
	f := descriptor(1, values)
	store := &memoryCheckpoints{}
	blocks := &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values}}
	s := session(t, store, &memorySnapshots{files: []File{f}}, blocks, 2)
	for range 2 {
		_, err := s.Next()
		require.NoError(t, err)
	}
	before := blocks.reads
	// When full, then another read performs no I/O; confirming one prefix admits the next record.
	_, err := s.Next()
	require.ErrorIs(t, err, ErrPendingFull)
	require.Equal(t, before, blocks.reads)
	require.NoError(t, s.Checkpoint(1))
	item, err := s.Next()
	require.NoError(t, err)
	require.EqualValues(t, 44, item.Record.Offset)
	require.NoError(t, s.Checkpoint(3))
	require.True(t, store.saved.Complete)
}

func TestSaveFailureStopsIntakeWithoutAdvancingMemoryOrRetryingWrites(t *testing.T) {
	// Given one delivered input and a checkpoint replacement failure.
	values := records(42, 43)
	f := descriptor(1, values)
	store := &memoryCheckpoints{}
	blocks := &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values}}
	s := session(t, store, &memorySnapshots{files: []File{f}}, blocks, 10)
	_, err := s.Next()
	require.NoError(t, err)
	store.saveErr = errDisk
	// When saving fails, then no new reads or saves can hide the failure.
	require.ErrorIs(t, s.Checkpoint(1), errDisk)
	before := blocks.reads
	_, err = s.Next()
	require.ErrorIs(t, err, errDisk)
	require.Equal(t, before, blocks.reads)
	require.ErrorIs(t, s.Checkpoint(1), errDisk)
	require.Len(t, store.writes, 2)
	require.Zero(t, s.Saved().Position)
}

func TestReadFailureStillAllowsSavingAnEarlierConfirmedPrefix(t *testing.T) {
	// Given one delivered record followed by a source read failure.
	values := records(42, 43)
	f := descriptor(1, values)
	store := &memoryCheckpoints{}
	blocks := &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values}, fail: func(_ File, p Position) error {
		if p.Record == 1 {
			return errDisk
		}
		return nil
	}}
	s := session(t, store, &memorySnapshots{files: []File{f}}, blocks, 10)
	_, err := s.Next()
	require.NoError(t, err)
	// When the next read fails, then confirmed earlier output can still be saved.
	_, err = s.Next()
	require.ErrorIs(t, err, errDisk)
	before := blocks.reads
	require.NoError(t, s.Checkpoint(1))
	require.EqualValues(t, 1, store.saved.Position)
	_, err = s.Next()
	require.ErrorIs(t, err, errDisk)
	require.Equal(t, before, blocks.reads)
}

func TestSavedSelectionIgnoresNewFilesAndACompletedGroupStaysCompleted(t *testing.T) {
	// Given a group with one saved record and one remaining record.
	values := records(42, 43)
	f := descriptor(1, values)
	store := &memoryCheckpoints{}
	inputs := &memorySnapshots{files: []File{f}}
	s := session(t, store, inputs, &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values}}, 10)
	item, err := s.Next()
	require.NoError(t, err)
	require.NoError(t, s.Checkpoint(item.Next))
	require.NoError(t, s.Close())
	other := descriptor(2, records(44))
	inputs.files = append(inputs.files, other)
	// When the same group resumes, then its selection remains fixed.
	resumed := session(t, store, inputs, &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values}}, 10)
	require.Equal(t, records(43), drain(t, resumed, true))
	require.Equal(t, 1, inputs.selects)
	require.Equal(t, 1, inputs.validates)
	require.True(t, store.saved.Complete)
	require.NoError(t, resumed.Close())
	// Then completion requires no archive inspection, even after its inputs become unavailable.
	inputs.validateErr = errDisk
	inputs.selectErr = errDisk
	before := len(store.writes)
	done := session(t, store, inputs, &memoryBlocks{}, 10)
	_, err = done.Next()
	require.ErrorIs(t, err, io.EOF)
	require.Len(t, store.writes, before)
	require.Equal(t, 1, inputs.validates)
	// A new group starts from all files currently present.
	q := query()
	q.Group = "fresh-group"
	fresh, err := NewSession(q, clock{}, &memoryCheckpoints{}, &memorySnapshots{files: []File{f, other}}, &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values, other.Identity.FileID: records(44)}}, 10)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fresh.Close()) })
	require.Equal(t, records(42, 43, 44), drain(t, fresh, true))
}

func TestResolvedWindowSurvivesRestartAndOutOfOrderTimestamps(t *testing.T) {
	// Given NOW-based bounds and records whose timestamps are not in source-offset order.
	q := query()
	q.Window = plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -1000}, Upper: &plan.Expression{Now: true}}
	values := records(42, 43, 44)
	values[0].TimestampMS = new(int64(11000))
	values[1].TimestampMS = new(int64(9500))
	values[2].TimestampMS = nil
	f := descriptor(1, values)
	store := &memoryCheckpoints{}
	inputs := &memorySnapshots{files: []File{f}}
	blocks := &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values}}
	s, err := NewSession(q, clock{time.UnixMilli(10000)}, store, inputs, blocks, 10)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	// When restarting much later, then all records remain visible and the original bounds survive.
	blocks = &memoryBlocks{values: blocks.values}
	s, err = NewSession(q, clock{time.UnixMilli(20000)}, store, inputs, blocks, 10)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.EqualValues(t, 9000, *s.Saved().Window.Lower)
	require.EqualValues(t, 10000, *s.Saved().Window.Upper)
	got := drain(t, s, true)
	require.Equal(t, values, got)
	require.True(t, s.Saved().Window.Contains(*got[1].TimestampMS), "a later in-range timestamp is not lost after an out-of-range record")
}

func TestStartupFailureNeverProducesOrChangesSavedProgress(t *testing.T) {
	for _, stage := range []string{"load", "selection", "initial save", "resume validation", "query identity", "window", "resume read"} {
		t.Run(stage, func(t *testing.T) {
			// Given a failure at one startup boundary.
			values := records(42, 43)
			f := descriptor(1, values)
			store := &memoryCheckpoints{}
			inputs := &memorySnapshots{files: []File{f}}
			blocks := &memoryBlocks{values: map[string][]avro.Record{f.Identity.FileID: values}}
			q := query()
			if stage == "resume validation" || stage == "query identity" || stage == "window" || stage == "resume read" {
				prior := session(t, store, inputs, blocks, 10)
				if stage == "resume read" {
					_, err := prior.Next()
					require.NoError(t, err)
					require.NoError(t, prior.Checkpoint(1))
				}
				require.NoError(t, prior.Close())
				blocks = &memoryBlocks{values: blocks.values}
			}
			switch stage {
			case "load":
				store.loadErr = errDisk
			case "selection":
				inputs.selectErr = errDisk
			case "initial save":
				store.saveErr = errDisk
			case "resume validation":
				inputs.validateErr = errDisk
			case "query identity":
				q.Fingerprint = "different"
			case "window":
				q.Window = plan.WindowSpec{Lower: &plan.Expression{LiteralMS: 1}}
			case "resume read":
				blocks.fail = func(File, Position) error { return errDisk }
			}
			before := store.saved
			reads, closes := blocks.reads, store.closes
			// When opening fails, then saved progress and caller-owned resources remain unchanged.
			_, err := NewSession(q, clock{}, store, inputs, blocks, 10)
			require.Error(t, err)
			if stage == "resume read" {
				require.Equal(t, reads+1, blocks.reads)
			} else {
				require.Equal(t, reads, blocks.reads)
			}
			require.Equal(t, before, store.saved)
			require.Zero(t, blocks.closes, "failed construction leaves input cleanup to its caller")
			require.Equal(t, closes, store.closes, "failed construction leaves checkpoint cleanup to its caller")
		})
	}
}

func TestEmptySelectionAndClosedSession(t *testing.T) {
	// Given a group whose fixed selection is empty.
	store := &memoryCheckpoints{}
	blocks := &memoryBlocks{}
	s := session(t, store, &memorySnapshots{}, blocks, 10)
	require.True(t, s.Saved().Complete)
	_, err := s.Next()
	require.ErrorIs(t, err, io.EOF)
	// When closed, then its resources close exactly once and later operations fail.
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
	require.Equal(t, 1, blocks.closes)
	require.Equal(t, 1, store.closes)
	_, err = s.Next()
	require.Error(t, err)
	require.Error(t, s.Checkpoint(0))
	require.Len(t, store.writes, 1)
	_, err = NewSession(query(), clock{}, &memoryCheckpoints{}, &memorySnapshots{}, &memoryBlocks{}, -1)
	require.Error(t, err)
}

func TestPrematureEOFCannotFinishOrDropAnUnfinishedFile(t *testing.T) {
	// Given two unfinished inputs and an unexpected EOF after the first record of one input.
	a, b := descriptor(1, records(42, 43)), descriptor(2, records(100))
	blocks := &memoryBlocks{values: map[string][]avro.Record{a.Identity.FileID: records(42, 43), b.Identity.FileID: records(100)}, fail: func(f File, p Position) error {
		if f == a && p.Record == 1 {
			return io.EOF
		}
		return nil
	}}
	store := &memoryCheckpoints{}
	s := session(t, store, &memorySnapshots{files: []File{a, b}}, blocks, 10)
	_, err := s.Next()
	require.NoError(t, err)
	// When another record is expected, then EOF is a sticky source failure, never snapshot completion.
	_, err = s.Next()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NotErrorIs(t, err, io.EOF)
	reads := blocks.reads
	_, again := s.Next()
	require.Equal(t, err, again)
	require.Equal(t, reads, blocks.reads)
	// Earlier confirmed output can still be saved, retaining the unfinished selection.
	require.NoError(t, s.Checkpoint(1))
	require.False(t, store.saved.Complete)
	require.EqualValues(t, 1, store.saved.Position)
}
