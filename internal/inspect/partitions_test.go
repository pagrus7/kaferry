package inspect

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestInspectionUsesSelectedPartitionsForPositioningRoutingAndStatus(t *testing.T) {
	// Given only partition zero selected, with partition one absent on the destination.
	s, p, f, _ := fixture()
	p.Partitions = &plan.PartitionFilter{Exclude: true, IDs: []int32{1}}
	p.Destination.Partitioning = plan.Preserve
	f.connections["output"].info.Partitions = []int32{0}
	source := f.connections["input"]
	source.saved[0] = kafkaport.SavedProgress{Position: 5, Progress: kafkaport.Progress{Stop: &core.Stop{Reason: "tip", At: 5}}}
	// When inspecting, then excluded partitions need no destination ID, lookup or saved stop.
	r := preview(t, s, p)
	require.Equal(t, []string{"describe", "saved", "describe"}, source.observations)
	source.observations = nil
	require.Len(t, r.Partitions, 1)
	require.EqualValues(t, 0, r.Partitions[0].ID)
	require.Equal(t, p.Partitions, r.Source.PartitionFilter)
	status, err := s.Status(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
	require.NoError(t, err)
	require.Equal(t, "complete_within_staleness", status.Verdict)
	require.Equal(t, []string{"describe", "members", "saved", "members", "describe"}, source.observations)
	require.Len(t, status.Partitions, 1)
	// When a new partition appears, then NOT IN makes its missing progress visible.
	source.info.Partitions = append(source.info.Partitions, 2)
	status, err = s.Status(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
	require.NoError(t, err)
	require.Equal(t, "incomplete", status.Verdict)
	require.Len(t, status.Partitions, 2)
}

func TestInspectionRejectsMissingPartitionIDsBeforeReadingProgress(t *testing.T) {
	for _, exclude := range []bool{false, true} {
		// Given a predicate naming a nonexistent source ID.
		s, p, f, _ := fixture()
		p.Partitions = &plan.PartitionFilter{Exclude: exclude, IDs: []int32{9}}
		// When explaining or checking status, then fail without reading group progress.
		_, err := s.Explain(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
		require.ErrorContains(t, err, "absent from source metadata")
		_, err = s.Status(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
		require.ErrorContains(t, err, "absent from source metadata")
		require.Zero(t, f.connections["input"].groupReads)
		require.Equal(t, []string{"describe", "describe"}, f.connections["input"].observations)
		require.Equal(t, 2, f.connections["input"].closed)
	}
}

func TestInspectionRevalidatesSelectedPartitionsAfterReadingProgress(t *testing.T) {
	for _, mode := range []string{"explain", "status"} {
		t.Run(mode, func(t *testing.T) {
			// Given a valid initial selection that disappears during the progress read.
			s, p, f, _ := fixture()
			p.Partitions = &plan.PartitionFilter{IDs: []int32{1}}
			source := f.connections["input"]
			source.afterSaved = func() { source.info.Partitions = []int32{0} }
			// When inspecting, then the final metadata observation rejects the stale selection.
			if mode == "explain" {
				_, err := s.Explain(t.Context(), p, config.Worker{}, core.NoopStopObserver{})
				require.ErrorContains(t, err, "absent from source metadata")
			} else {
				r, err := s.Status(t.Context(), p, config.Worker{}, core.NoopStopObserver{})
				require.NoError(t, err)
				require.Equal(t, "unknown", r.Verdict)
			}
			require.Equal(t, 1, source.groupReads)
			require.Equal(t, 1, source.closed)
		})
	}
}
