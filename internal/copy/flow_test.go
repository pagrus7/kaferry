package copy

import (
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
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
	size := input.Record.Size()
	for _, tc := range []struct {
		name       string
		total      workCount
		partition  workCount
		inputBytes int
		want       bool
	}{
		{"fits exactly", workCount{1, size}, workCount{1, size}, 16, true},
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
			got := f.admits(incoming(3, 0, tc.inputBytes), work, limits)

			// Then every cap applies and the lone oversized-record exception is retained.
			if got != tc.want {
				t.Fatal(got, tc.want)
			}
		})
	}
}

func TestFlowCommitCadenceAndPauseReasons(t *testing.T) {
	for _, reason := range []string{"none", "periodic", "worker records", "worker bytes", "worker full records", "worker full bytes", "partition records", "partition bytes", "deferred", "pending"} {
		t.Run(reason, func(t *testing.T) {
			// Given work below every half-cap and a periodic save not yet due.
			now := time.Unix(10, 0)
			f := flow{lastCommit: now}
			limits := config.Worker{RecordCap: 10, ByteCap: 1000, PartitionRecordCap: 8, PartitionByteCap: 800}
			work := mockWorkCounts{total: workCount{1, 100}, partitions: map[int32]workCount{3: {1, 100}}}
			switch reason {
			case "periodic":
				now = now.Add(time.Second)
			case "worker records":
				work.total.records = 5
			case "worker bytes":
				work.total.bytes = 500
			case "worker full records":
				work.total.records = 10
			case "worker full bytes":
				work.total.bytes = 1000
			case "partition records":
				work.total.records = 4
				work.partitions[3] = workCount{4, 100}
			case "partition bytes":
				work.total.bytes = 400
				work.partitions[3] = workCount{1, 400}
			case "deferred":
				f.deferred = &Input{}
			case "pending":
				f.pending = &submission{}
			}

			// When evaluating commit cadence and worker-wide pause independently.
			due := f.commitDue(now, work, map[int32]*partition{3: {}}, limits)
			paused := f.full(work, limits)

			// Then half-caps save; held input pauses; pending alone does not force saves.
			wantDue := reason != "none" && reason != "pending"
			wantPaused := reason == "pending" || reason == "deferred" || reason == "worker full records" || reason == "worker full bytes"
			if due != wantDue || paused != wantPaused {
				t.Fatal(reason, due, paused)
			}
		})
	}
}
