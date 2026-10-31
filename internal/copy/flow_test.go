package copy

import (
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/stretchr/testify/require"
)

type workCount struct {
	records int
	bytes   int64
}

type mockWorkCounts struct {
	total      workCount
	partitions map[int32]workCount
}

func (m mockWorkCounts) Total() (int, int64) { return m.total.records, m.total.bytes }
func (m mockWorkCounts) Work(id int32) (int, int64) {
	p := m.partitions[id]
	return p.records, p.bytes
}

func TestFlowAdmissionHonorsEveryCapAndAllowsOversizedAlone(t *testing.T) {
	input := incoming(3, 0, 16)
	input.Record.Key = []byte("k")
	input.Record.Headers = []Header{{Name: "name", Value: []byte("header")}}
	input.Record.Source = &Provenance{Topic: "orders", Partition: 3, Offset: 0}
	// The estimate includes the envelope, key/value, header and source topic.
	const size int64 = 64 + 1 + 16 + 24 + 4 + 6 + 40 + 6
	require.Equal(t, size, input.Record.Size())
	for _, tc := range []struct {
		name       string
		total      workCount
		partition  workCount
		inputBytes int
		want       bool
	}{
		{"fits exactly", workCount{1, 3 * size}, workCount{1, size}, 16, true},
		{"worker records", workCount{4, size}, workCount{1, size}, 16, false},
		{"partition records", workCount{2, size}, workCount{2, size}, 16, false},
		{"worker bytes", workCount{1, 3*size + 1}, workCount{1, size}, 16, false},
		{"partition bytes", workCount{1, size + 1}, workCount{1, size + 1}, 16, false},
		{"oversized alone", workCount{}, workCount{}, 10000, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given independent worker/partition counts provided through the narrow port.
			limits := config.Worker{RecordCap: 4, ByteCap: 4 * size, PartitionRecordCap: 2, PartitionByteCap: 2 * size}
			work := mockWorkCounts{total: tc.total, partitions: map[int32]workCount{3: tc.partition}}
			f := flow{}

			// When admitting the next record, including its key/value/header accounting.
			next := input
			next.Record.Value = make([]byte, tc.inputBytes)
			got := f.admits(next, work, limits)

			// Then every cap applies and the lone oversized-record exception is retained.
			require.Equal(t, tc.want, got)
		})
	}
}

func TestFlowCommitCadenceAndPauseReasons(t *testing.T) {
	for _, tc := range []struct {
		name                string
		total, partition    workCount
		elapsed             time.Duration
		deferred            bool
		wantDue, wantPaused bool
	}{
		{name: "none", total: workCount{1, 100}, partition: workCount{1, 100}},
		{name: "periodic", total: workCount{1, 100}, partition: workCount{1, 100}, elapsed: time.Second, wantDue: true},
		{name: "below worker records", total: workCount{9, 100}, partition: workCount{1, 100}},
		{name: "below worker bytes", total: workCount{1, 999}, partition: workCount{1, 100}},
		{name: "worker full records", total: workCount{10, 100}, partition: workCount{1, 100}, wantDue: true, wantPaused: true},
		{name: "worker full bytes", total: workCount{1, 1000}, partition: workCount{1, 100}, wantDue: true, wantPaused: true},
		{name: "below partition records", total: workCount{7, 100}, partition: workCount{7, 100}},
		{name: "partition records", total: workCount{8, 100}, partition: workCount{8, 100}, wantDue: true},
		{name: "below partition bytes", total: workCount{1, 799}, partition: workCount{1, 799}},
		{name: "partition bytes", total: workCount{1, 800}, partition: workCount{1, 800}, wantDue: true},
		{name: "deferred", total: workCount{1, 100}, partition: workCount{1, 100}, deferred: true, wantDue: true, wantPaused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given independent work counts, held input and elapsed time.
			now := time.Unix(10, 0)
			f := flow{lastCommit: now}
			if tc.deferred {
				f.deferred = &deferredInput{}
			}
			limits := config.Worker{RecordCap: 10, ByteCap: 1000, PartitionRecordCap: 8, PartitionByteCap: 800}
			work := mockWorkCounts{total: tc.total, partitions: map[int32]workCount{3: tc.partition}}

			// When evaluating commit cadence and worker-wide pause independently.
			due := f.commitDue(now.Add(tc.elapsed), work, map[int32]*partition{3: {}}, limits)
			paused := f.full(work, limits)

			// Then full budgets save and deferred input pauses intake.
			require.Equal(t, tc.wantDue, due, "commit due")
			require.Equal(t, tc.wantPaused, paused, "worker paused")
		})
	}
}

func TestPartitionCapsPauseOnlyTheFullPartitionUntilSaved(t *testing.T) {
	for _, tc := range []struct {
		name    string
		records int
		bytes   int64
	}{
		{"records", 2, 1000000},
		{"bytes", 100, 2 * (64 + 10)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given two partitions below the worker caps, with only partition zero full.
			r, source, sink, store, _ := fixture(t)
			r.limits.RecordCap, r.limits.ByteCap = 100, 1000000
			r.limits.PartitionRecordCap, r.limits.PartitionByteCap = tc.records, tc.bytes
			require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0}}, {Cursor: Cursor{Partition: 1}}}))
			require.NoError(t, r.input(incoming(0, 0, 10)))
			require.NoError(t, r.input(incoming(0, 1, 10)))
			require.NoError(t, r.input(incoming(1, 0, 10)))

			// When applying pauses, then only the partition at its cap pauses.
			require.NoError(t, r.pause())
			require.Equal(t, map[int32]bool{0: true}, source.paused)
			require.False(t, r.partitions[1].paused)

			// When both writes for partition zero are confirmed, they still use its budget.
			sink.reports <- Delivery{Token: sink.writes[0]}
			sink.reports <- Delivery{Token: sink.writes[1]}
			require.NoError(t, r.deliveries())
			require.NoError(t, r.pause())
			require.True(t, source.paused[0])

			// When confirmed progress is saved, then that partition resumes independently.
			require.NoError(t, r.commit(r.ids()))
			require.NoError(t, r.pause())
			require.Equal(t, [][]Cursor{{{Partition: 0, Position: 2}}}, store.saves)
			require.Equal(t, map[int32]bool{0: false}, source.paused)
			require.False(t, r.partitions[0].paused)
			require.False(t, r.partitions[1].paused)
		})
	}
}
