package replay

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestFileDisorderFinishesOnlyItsOriginalStreamAndResumes(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "default full snapshot", true: "explicit disorder"}[enabled], func(t *testing.T) {
			// Given overlapping files, a later in-range timestamp, and another original partition.
			a := records(1, 2, 3)
			a[1].TimestampMS = new(int64(100))
			a[2].TimestampMS = new(int64(20))
			b := records(2, 4)
			b[0].TimestampMS = new(int64(100))
			b[1].TimestampMS = new(int64(30))
			c := records(1, 2)
			for i := range c {
				c[i].Source.Partition = 1
			}
			fa, fb, fc := descriptor(1, a), descriptor(2, b), descriptor(3, c)
			inputs := &memorySnapshots{files: []File{fa, fb, fc}}
			blocks := func() *memoryBlocks {
				return &memoryBlocks{values: map[string][]avro.Record{fa.Identity.FileID: a, fb.Identity.FileID: b, fc.Identity.FileID: c}}
			}
			q := query()
			q.Window.Upper = &plan.Expression{LiteralMS: 50}
			if enabled {
				q.MaxOutOfOrderMS = new(int64(0))
			}
			store := &memoryCheckpoints{}
			s, err := NewSession(q, clock{}, store, inputs, blocks(), 20)
			require.NoError(t, err)
			// When a stop is observed but only the earlier input is confirmed, then do not save the skip prematurely.
			first, err := s.Next()
			require.NoError(t, err)
			_, err = s.Next()
			require.NoError(t, err)
			require.NoError(t, s.Checkpoint(first.Next))
			require.False(t, store.saved.Complete)
			require.False(t, store.saved.Files[1].ended())
			require.NoError(t, s.Close())
			// When restarted from confirmed physical cursors, then the other partition remains eligible.
			s, err = NewSession(q, clock{}, store, inputs, blocks(), 20)
			require.NoError(t, err)
			got := drain(t, s, true)
			require.NoError(t, s.Close())
			require.True(t, store.saved.Complete)
			if enabled {
				require.Len(t, got, 3)
				require.EqualValues(t, 0, got[0].Source.Partition)
				require.EqualValues(t, 1, got[1].Source.Partition)
				require.EqualValues(t, 1, got[2].Source.Partition)
			} else {
				require.Len(t, got, 6)
			}
		})
	}
}

func TestFileOffsetEndSkipsAcrossFilesWithoutDroppingOtherPartitions(t *testing.T) {
	// Given two original partitions whose source offsets differ from physical replay cursors.
	a := records(42, 43, 44, 45)
	b := records(42, 43, 44)
	for i := range b {
		b[i].Source.Partition = 2
	}
	fa, fb := descriptor(1, a), descriptor(2, b)
	q := query()
	q.PartitionOffsets = []plan.PartitionOffsetRange{{Partition: 0, Range: plan.OffsetRange{Upper: new(int64(44))}}, {Partition: 2, Range: plan.OffsetRange{Latest: true}}}
	store := &memoryCheckpoints{}
	blocks := &memoryBlocks{values: map[string][]avro.Record{fa.Identity.FileID: a, fb.Identity.FileID: b}}
	s, err := NewSession(q, clock{}, store, &memorySnapshots{files: []File{fa, fb}}, blocks, 20)
	require.NoError(t, err)
	// When the first original stream reaches its end, then finish only that stream and persist its EOF through confirmed progress.
	got := drain(t, s, true)
	require.NoError(t, s.Close())
	require.Len(t, got, 6)
	require.True(t, store.saved.Complete)
	require.EqualValues(t, 44, got[2].Offset)
	require.EqualValues(t, 2, got[3].Source.Partition)
}
