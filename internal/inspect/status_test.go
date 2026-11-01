package inspect

import (
	"errors"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestStatusCompletionScopeAndPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reasons   []string
		offsetEnd bool
		disorder  *int64
		want      string
		code      int
	}{
		{"offset end", []string{"tip"}, true, nil, "complete", 0},
		{"captured end", []string{"tip"}, false, nil, "complete_within_staleness", 0},
		{"timestamp stop", []string{"timestamp"}, false, new(int64(0)), "complete_within_staleness", 0},
		{"mixed timestamp and end", []string{"timestamp", "tip"}, false, new(int64(0)), "complete_within_staleness", 0},
		{"archive eof", []string{"eof"}, false, nil, "complete", 0},
		{"archive disorder off", []string{"eof"}, false, new(int64(-1)), "complete", 0},
		{"archive disorder enabled", []string{"eof"}, false, new(int64(0)), "complete_within_staleness", 0},
		{"archive offset end", []string{"eof"}, true, nil, "complete", 0},
		{"worker cap", []string{"worker_record_cap"}, false, nil, "limit_reached", 7},
		{"cap before conditional completion", []string{"tip", "worker_record_cap"}, false, nil, "limit_reached", 7},
		{"cap before strict completion", []string{"tip", "worker_record_cap"}, true, nil, "limit_reached", 7},
		{"unfinished before cap", []string{"worker_record_cap", ""}, false, nil, "incomplete", 4},
		{"no partitions", nil, false, nil, "unknown", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given validated saved stops and the query that defines their source scope.
			p := plan.Plan{MaxOutOfOrderMS: tc.disorder}
			if tc.offsetEnd {
				p.Offsets = &plan.OffsetRange{Upper: new(int64(10))}
			}
			r := StatusReport{}
			for id, reason := range tc.reasons {
				row := SavedPartition{ID: int32(id), Position: new(int64(10))}
				if reason != "" {
					row.Stop = &core.Stop{Reason: reason, At: 10}
				}
				r.Partitions = append(r.Partitions, row)
			}
			// When aggregating the saved status.
			r.classify(p)
			// Then the least complete outcome determines the verdict and shell result.
			require.Equal(t, tc.want, r.Verdict)
			require.Equal(t, tc.code, r.ExitCode())
		})
	}
}

func TestKafkaStatusVerdictsAndReadOrder(t *testing.T) {
	for _, tc := range []struct {
		name, verdict string
		code          int
	}{
		{"no progress", "unknown", 5}, {"partial progress", "incomplete", 4}, {"no stop", "incomplete", 4}, {"stopped", "complete_within_staleness", 0},
		{"worker threshold", "limit_reached", 7}, {"mixed stops", "limit_reached", 7}, {"partial worker stops", "incomplete", 4}, {"active after worker stop", "running", 6},
		{"active", "running", 6}, {"changed empty group state", "running", 6}, {"rebalance without members", "running", 6},
		{"foreign member", "unknown", 5}, {"identity conflict while active", "unknown", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given two current partitions and independently observed membership/progress.
			s, p, f, _ := fixture()
			source := f.connections["input"]
			for _, id := range []int32{0, 1} {
				source.positions[id] = int64(id)
				source.metadata[id] = kafkaport.Progress{Stop: &core.Stop{Reason: "tip", At: int64(id)}, End: new(int64(id))}
			}
			switch tc.name {
			case "worker threshold", "active after worker stop", "partial worker stops", "mixed stops":
				source.metadata[0].Stop.Reason = "worker_record_cap"
				if tc.name == "mixed stops" {
					source.metadata[1].Stop.Reason = "timestamp"
				} else if tc.name == "partial worker stops" {
					source.metadata[1] = kafkaport.Progress{}
				} else {
					source.metadata[1].Stop.Reason = "worker_record_cap"
				}
				if tc.name == "active after worker stop" {
					source.memberships = []kafkaport.Membership{{Idle: false}}
				}
			case "no progress":
				clear(source.positions)
				clear(source.metadata)
			case "partial progress":
				delete(source.positions, 1)
				delete(source.metadata, 1)
			case "no stop":
				source.metadata[1] = kafkaport.Progress{}
			case "active":
				source.memberships = []kafkaport.Membership{{Idle: true}, {Idle: false}}
			case "changed empty group state":
				source.memberships = []kafkaport.Membership{{Idle: true, State: "Empty"}, {Idle: true, State: "Dead"}}
			case "rebalance without members":
				source.memberships = []kafkaport.Membership{{Idle: false}}
			case "foreign member":
				source.memberships = []kafkaport.Membership{{Foreign: true, Idle: false}}
			case "identity conflict while active":
				source.fail = "saved"
				source.failure = &kafkaport.Conflict{Err: errors.New("different query")}
				source.memberships = []kafkaport.Membership{{Idle: false}}
			}
			// When observing without joining, consuming, positioning or committing.
			r, err := s.Status(t.Context(), p, config.Defaults("home").Worker, core.NoopStopObserver{})
			// Then identity conflicts win, activity prevents completion, and worker caps remain distinct.
			require.NoError(t, err)
			require.Equal(t, tc.verdict, r.Verdict)
			require.Equal(t, tc.code, r.ExitCode())
			if tc.name == "worker threshold" {
				require.Equal(t, "worker record threshold reached", r.Reason)
			} else if tc.name == "mixed stops" {
				require.Equal(t, "timestamp disorder threshold reached; worker record threshold reached", r.Reason)
			}
			require.EqualValues(t, 1, source.closed)
			require.EqualValues(t, 1, f.connections["output"].closed)
			require.Empty(t, source.queries)
			if tc.name != "identity conflict while active" {
				require.Equal(t, []string{"describe", "members", "saved", "members", "describe"}, source.observations)
			}
			if tc.name == "stopped" {
				require.Len(t, r.Partitions, 2)
				require.Zero(t, r.Partitions[0].ID)
				require.NotNil(t, r.Partitions[1].Position)
				require.Equal(t, int64(1), *r.Partitions[1].Position)
				require.Equal(t, new(int64(1)), r.Partitions[1].End)
			}
		})
	}
}
func TestStatusPreservesOperationalFailuresAndRejectsOneOff(t *testing.T) {
	for _, stage := range []string{"members", "saved", "close", "ONE_OFF"} {
		t.Run(stage, func(t *testing.T) {
			// Given a failed read/cleanup or an explicitly anonymous group.
			s, p, f, _ := fixture()
			source := f.connections["input"]
			source.fail = stage
			if stage == "ONE_OFF" {
				p.OneOff = true
			}
			// When checking status.
			_, err := s.Status(t.Context(), p, config.Worker{}, core.NoopStopObserver{})
			// Then unavailable state is not reported as successful absence, and ONE_OFF opens nothing.
			if stage == "ONE_OFF" {
				var code interface{ ExitCode() int }
				require.ErrorAs(t, err, &code)
				require.EqualValues(t, 2, code.ExitCode())
				require.Zero(t, f.opened)
			} else {
				require.ErrorIs(t, err, source.failure)
			}
		})
	}
}
func TestStatusRejectsStaleStopsAfterFinalObservationFailure(t *testing.T) {
	for _, failure := range []string{"second membership", "final description", "changed topic identity"} {
		t.Run(failure, func(t *testing.T) {
			// Given stopped progress that must still be checked against current membership and topic identity.
			service, p, factory, _ := fixture()
			source, destination := factory.connections["input"], factory.connections["output"]
			for _, id := range source.info.Partitions {
				source.positions[id] = int64(id)
				source.metadata[id] = kafkaport.Progress{Stop: &core.Stop{Reason: "tip", At: int64(id)}}
			}
			source.afterSaved = func() {
				switch failure {
				case "second membership":
					source.fail = "members"
				case "final description":
					source.fail = "describe"
				case "changed topic identity":
					source.info.TopicID = "replacement-topic"
				}
			}
			// When the observation after reading saved stops fails.
			report, err := service.Status(t.Context(), p, config.Worker{}, core.NoopStopObserver{})
			// Then stale progress never yields stopped, causes survive, and both connections close once.
			if failure == "changed topic identity" {
				require.NoError(t, err)
				require.Equal(t, "unknown", report.Verdict)
				require.Equal(t, "saved progress does not match this query or source", report.Reason)
				require.Equal(t, 5, report.ExitCode())
			} else {
				require.ErrorIs(t, err, source.failure)
				require.Empty(t, report.Verdict)
			}
			require.Empty(t, report.Partitions)
			want := []string{"describe", "members", "saved", "members"}
			if failure != "second membership" {
				want = append(want, "describe")
			}
			require.Equal(t, want, source.observations)
			require.Equal(t, 1, source.closed)
			require.Equal(t, 1, destination.closed)
		})
	}
}
