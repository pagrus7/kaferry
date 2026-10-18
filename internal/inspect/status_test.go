package inspect

import (
	"errors"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/stretchr/testify/require"
)

func TestKafkaStatusVerdictsAndReadOrder(t *testing.T) {
	for _, tc := range []struct {
		name, verdict string
		code          int
	}{
		{"no progress", "unknown", 5}, {"partial progress", "incomplete", 4}, {"no stop", "incomplete", 4}, {"stopped", "stopped", 0},
		{"worker threshold", "stopped", 0}, {"mixed stops", "stopped", 0}, {"partial worker stops", "incomplete", 4}, {"active after worker stop", "running", 6},
		{"active", "running", 6}, {"changed empty group state", "running", 6}, {"rebalance without members", "running", 6},
		{"foreign member", "unknown", 5}, {"identity conflict while active", "unknown", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given two current partitions and independently observed membership/progress.
			s, p, f, _ := fixture()
			source := f.connections["input"]
			for _, id := range []int32{0, 1} {
				source.positions[id] = int64(id)
				source.metadata[id] = kafkaport.Progress{Stop: &core.Stop{Reason: "tip", At: int64(id)}}
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
			// Then identity conflicts win, activity prevents stopped, and only every durable stop yields 0.
			require.NoError(t, err)
			require.Equal(t, tc.verdict, r.Verdict)
			require.Equal(t, tc.code, r.ExitCode())
			if tc.name == "worker threshold" {
				require.Equal(t, "worker record threshold reached", r.Reason)
			} else if tc.name == "mixed stops" {
				require.Equal(t, "timestamp cutoff reached; worker record threshold reached", r.Reason)
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
func TestFileStatusUsesOpeningSizeNotEOFPosition(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		size         int64
		verdict      string
	}{
		{"preexisting partial final frame", "eof", 150, "stopped"}, {"appended bytes", "eof", 151, "incomplete"},
		{"appended after worker threshold", "worker_record_cap", 151, "stopped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a saved EOF before a partial final frame that was already present at open.
			s, p, _, files := fixture()
			p.Source.Kind = "FILE"
			p.Source.Name = "input.cbor"
			opening := int64(150)
			files.source = file.SourceInspection{ArchiveInspection: file.ArchiveInspection{Exists: true, ID: "source", Size: tc.size}, Saved: true, Position: 100, OpeningSize: &opening, Stop: &core.Stop{Reason: tc.reason, At: 100}}
			// When checking the saved stop against the current file size.
			r, err := s.Status(t.Context(), p, config.Worker{}, core.NoopStopObserver{})
			// Then only growth after opening invalidates that saved stop.
			require.NoError(t, err)
			require.Equal(t, tc.verdict, r.Verdict)
			require.Equal(t, "not checked for files", r.Activity)
			require.Equal(t, tc.size, r.Source.File.SizeBytes)
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
