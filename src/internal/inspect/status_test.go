package inspect

import (
	"context"
	"errors"
	"testing"

	"github.com/pagrus7/kaferry/src/internal/archive/io"
	"github.com/pagrus7/kaferry/src/internal/config"
	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/kafka"
	"github.com/pagrus7/kaferry/src/internal/plan"
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
		{"captured end", []string{"tip"}, false, nil, "complete", 0},
		{"timestamp stop", []string{"timestamp"}, false, new(int64(0)), "complete_within_disorder", 0},
		{"mixed timestamp and end", []string{"timestamp", "tip"}, false, new(int64(0)), "complete_within_disorder", 0},
		{"archive eof", []string{"eof"}, false, nil, "complete", 0},
		{"archive disorder off", []string{"eof"}, false, new(int64(-1)), "complete", 0},
		{"archive offset end", []string{"eof"}, true, nil, "complete", 0},
		{"worker cap", []string{"worker_record_cap"}, false, nil, "limit_reached", 7},
		{"cap before conditional completion", []string{"tip", "worker_record_cap"}, false, nil, "limit_reached", 7},
		{"cap before strict completion", []string{"tip", "worker_record_cap"}, true, nil, "limit_reached", 7},
		{"unfinished before cap", []string{"worker_record_cap", ""}, false, nil, "incomplete", 4},
		{"no partitions", nil, false, nil, "unknown", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given validated saved stops and the query that defines their source scope.
			r := StatusReport{}
			for id, reason := range tc.reasons {
				row := SavedPartition{ID: int32(id), Position: new(int64(10))}
				if reason != "" {
					row.Stop = &core.Stop{Reason: reason, At: 10}
				}
				r.Partitions = append(r.Partitions, row)
			}
			// When aggregating the saved status.
			r.classify()
			// Then the least complete outcome determines the verdict and shell result.
			require.Equal(t, tc.want, r.Verdict)
			require.Equal(t, tc.code, r.ExitCode())
		})
	}
}

//kaferry:guarantee inspect
func TestKafkaStatusVerdictsAndReadOrder(t *testing.T) {
	for _, tc := range []struct {
		name, verdict string
		code          int
	}{
		{"no progress", "unknown", 5}, {"partial progress", "incomplete", 4}, {"no stop", "incomplete", 4}, {"stopped", "complete", 0}, {"offset end", "complete", 0},
		{"worker threshold", "limit_reached", 7}, {"mixed stops", "limit_reached", 7}, {"partial worker stops", "incomplete", 4}, {"active after worker stop", "running", 6},
		{"active", "running", 6}, {"changed empty group state", "running", 6}, {"rebalance without members", "running", 6},
		{"foreign member", "unknown", 5}, {"identity conflict while active", "unknown", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kafkaStatusVerdict(t, tc.name, tc.verdict, tc.code)
		})
	}
}
func TestIdleFollowStatusExplainsIncompleteProgress(t *testing.T) {
	// Given an idle FOLLOW group with a saved position and no source end.
	s, p, factory, _ := fixture()
	p.Follow = true
	p.Window.Upper, p.MaxOutOfOrderMS = nil, nil
	factory.connections["input"].saved[0] = kafka.SavedProgress{Position: 4}

	// When inspecting the group, then an incomplete verdict explains that following can continue.
	r, err := s.Status(t.Context(), p, config.Worker{}, core.NoopStopObserver{})
	require.NoError(t, err)
	require.Equal(t, "incomplete", r.Verdict)
	require.Equal(t, 4, r.ExitCode())
	require.Contains(t, r.Notes, "FOLLOW = ON has no source end. An incomplete verdict is expected when following stops.")
	require.Equal(t, "To keep following, rerun with the same query, group, and configuration.", r.Next)
}

func TestStatusExposesIncompatibleProgressAndActivity(t *testing.T) {
	for _, activity := range []string{"idle", "active", "foreign"} {
		t.Run(activity, func(t *testing.T) {
			// Given source-safe, stopped progress with different fingerprints, including an excluded partition.
			service, p, factory, _ := fixture()
			p.Partitions = &plan.PartitionFilter{IDs: []int32{0}}
			source := factory.connections["input"]
			source.saved = map[int32]kafka.SavedProgress{
				0: {Position: 3, Progress: kafka.Progress{Fingerprint: "old-a", End: new(int64(3)), Stop: &core.Stop{Reason: "tip", At: 3}}},
				1: {Position: 7, Progress: kafka.Progress{Fingerprint: "old-b", End: new(int64(7)), Stop: &core.Stop{Reason: "tip", At: 7}}},
			}
			source.fail, source.failure = "saved", &kafka.Conflict{Err: plan.ErrFingerprintMismatch}
			if activity != "idle" {
				source.memberships = []kafka.Membership{{Foreign: activity == "foreign"}}
			}
			// When status observes incompatible progress without joining or committing.
			report, err := service.Status(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
			// Then all saved rows remain visible, identity wins over completion, and activity remains observable.
			require.NoError(t, err)
			require.Equal(t, "unknown", report.Verdict)
			require.Equal(t, 5, report.ExitCode())
			require.Len(t, report.Partitions, 2)
			require.Equal(t, int32(0), report.Partitions[0].ID)
			require.Equal(t, new(int64(7)), report.Partitions[1].Position)
			require.Equal(t, "old-a", report.Partitions[0].Fingerprint)
			require.Equal(t, "old-b", report.Partitions[1].Fingerprint)
			require.Equal(t, source.saved[1].Stop, report.Partitions[1].Stop)
			require.Equal(t, 2, source.membershipReads)
			require.Contains(t, report.Reason, "saved positions are for inspection")
			switch activity {
			case "foreign":
				require.Contains(t, report.Reason, "incompatible active member")
			case "active":
				require.Contains(t, report.Reason, "group activity")
			default:
				require.Contains(t, report.Activity, "idle twice")
			}
		})
	}
}

func TestStatusExposesIncompatibleArchiveCursor(t *testing.T) {
	// Given source-validated archive progress whose fingerprint differs from the inspected query.
	service, p, _, archives := fixture()
	p.Source.Kind, p.Source.Format = "ARCHIVE", "avro"
	archives.source = archiveio.SourceInspection{Saved: true, Position: 7, Fingerprint: "old", Stop: &core.Stop{Reason: "worker_record_cap", At: 7}}
	archives.failSource = &archiveio.Conflict{Err: plan.ErrFingerprintMismatch}
	// When checking status, then the cursor remains inspectable without claiming completion or compatibility.
	report, err := service.Status(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
	require.NoError(t, err)
	require.Len(t, report.Partitions, 1)
	require.Equal(t, "unknown", report.Verdict)
	require.Equal(t, 5, report.ExitCode())
	require.Equal(t, new(int64(7)), report.Partitions[0].Position)
	require.Equal(t, "old", report.Partitions[0].Fingerprint)
	require.Equal(t, archives.source.Stop, report.Partitions[0].Stop)
	requireStatusGuidance(t, report, true)
}

func TestArchiveStatusGuidance(t *testing.T) {
	for _, tc := range []struct {
		name, verdict, stop string
		saved, corrupt      bool
		disorder            *int64
	}{
		{name: "fresh selection", verdict: "unknown"},
		{name: "corrupt checkpoint", verdict: "unknown", corrupt: true},
		{name: "unfinished selection", verdict: "incomplete", saved: true},
		{name: "record cap", verdict: "limit_reached", saved: true, stop: "worker_record_cap"},
		{name: "completed selection", verdict: "complete", saved: true, stop: "eof"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an archive inspection with or without a validated saved selection.
			service, p, _, archives := fixture()
			p.Source.Kind, p.Source.Format = "ARCHIVE", "avro"
			archives.source.Saved = tc.saved
			if tc.stop != "" {
				archives.source.Stop = &core.Stop{Reason: tc.stop}
			}
			if tc.corrupt {
				archives.failSource = &archiveio.Conflict{Err: errors.New("corrupt checkpoint")}
			}

			// When checking saved archive status.
			report, err := service.Status(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})

			// Then guidance reflects the verdict and any validated saved selection.
			require.NoError(t, err)
			require.Equal(t, tc.verdict, report.Verdict)
			requireStatusGuidance(t, report, tc.saved)
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

func TestStatusCancelsAfterReadingSavedProgress(t *testing.T) {
	// Given stopped Kafka progress and cancellation during the saved-progress read.
	service, p, factory, _ := fixture()
	source, destination := factory.connections["input"], factory.connections["output"]
	for _, id := range source.info.Partitions {
		source.saved[id] = kafka.SavedProgress{Position: 10, Progress: kafka.Progress{Stop: &core.Stop{Reason: "tip", At: 10}}}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	source.afterSaved = cancel

	// When the inspection finishes processing the saved stops.
	report, err := service.Status(ctx, p, config.Worker{}, core.NoopStopObserver{})

	// Then cancellation suppresses guidance and releases both connections.
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, report.Meaning)
	require.Empty(t, report.Notes)
	require.Empty(t, report.Next)
	require.Equal(t, 1, source.closed)
	require.Equal(t, 1, destination.closed)
}

func TestStatusRejectsStaleStopsAfterFinalObservationFailure(t *testing.T) {
	for _, failure := range []string{"second membership", "final description", "changed topic identity"} {
		t.Run(failure, func(t *testing.T) {
			// Given stopped progress that must still be checked against current membership and topic identity.
			service, p, factory, _ := fixture()
			source, destination := factory.connections["input"], factory.connections["output"]
			for _, id := range source.info.Partitions {
				source.saved[id] = kafka.SavedProgress{Position: int64(id), Progress: kafka.Progress{Stop: &core.Stop{Reason: "tip", At: int64(id)}}}
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
				require.Equal(t, "saved progress does not match this query or endpoints", report.Reason)
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

func kafkaStatusVerdict(t *testing.T, name, verdict string, code int) {
	// Given two current partitions and independently observed membership/progress.
	s, p, f, _ := fixture()
	source := f.connections["input"]
	for _, id := range []int32{0, 1} {
		source.saved[id] = kafka.SavedProgress{Position: int64(id), Progress: kafka.Progress{Stop: &core.Stop{Reason: "tip", At: int64(id)}, End: new(int64(id))}}
	}
	switch name {
	case "worker threshold", "active after worker stop", "partial worker stops", "mixed stops":
		source.saved[0].Stop.Reason = "worker_record_cap"
		switch name {
		case "mixed stops":
			source.saved[1].Stop.Reason = "timestamp"
		case "partial worker stops":
			source.saved[1] = kafka.SavedProgress{Position: 1}
		default:
			source.saved[1].Stop.Reason = "worker_record_cap"
		}
		if name == "active after worker stop" {
			source.memberships = []kafka.Membership{{Idle: false}}
		}
	case "no progress":
		clear(source.saved)
	case "partial progress":
		delete(source.saved, 1)
	case "no stop":
		source.saved[1] = kafka.SavedProgress{Position: 1}
	case "active":
		source.memberships = []kafka.Membership{{Idle: true}, {Idle: false}}
	case "changed empty group state":
		source.memberships = []kafka.Membership{{Idle: true, State: "Empty"}, {Idle: true, State: "Dead"}}
	case "rebalance without members":
		source.memberships = []kafka.Membership{{Idle: false}}
	case "foreign member":
		source.memberships = []kafka.Membership{{Foreign: true, Idle: false}}
	case "identity conflict while active":
		source.fail = "saved"
		source.failure = &kafka.Conflict{Err: errors.New("different query")}
		source.memberships = []kafka.Membership{{Idle: false}}
	case "offset end":
		p.Window = plan.WindowSpec{}
		p.Offsets = &plan.OffsetRange{Upper: new(int64(1))}
	}
	// When observing without joining, consuming, positioning or committing.
	r, err := s.Status(t.Context(), p, config.Defaults("home").Worker, core.NoopStopObserver{})
	// Then identity conflicts win, activity prevents completion, and worker caps remain distinct.
	require.NoError(t, err)
	require.Equal(t, verdict, r.Verdict)
	require.Equal(t, code, r.ExitCode())
	requireStatusGuidance(t, r, false)
	switch name {
	case "worker threshold":
		require.Equal(t, "worker record threshold reached", r.Reason)
	case "mixed stops":
		require.Equal(t, "timestamp disorder threshold reached; worker record threshold reached", r.Reason)
	}
	require.EqualValues(t, 1, source.closed)
	require.EqualValues(t, 1, f.connections["output"].closed)
	require.Empty(t, source.queries)
	if name != "identity conflict while active" {
		require.Equal(t, []string{"describe", "members", "saved", "members", "describe"}, source.observations)
	}
	if name == "stopped" {
		require.Len(t, r.Partitions, 2)
		require.Zero(t, r.Partitions[0].ID)
		require.NotNil(t, r.Partitions[1].Position)
		require.Equal(t, int64(1), *r.Partitions[1].Position)
		require.Equal(t, new(int64(1)), r.Partitions[1].End)
	}
}

func requireStatusGuidance(t *testing.T, report StatusReport, savedArchiveSelection bool) {
	t.Helper()
	const completionNote = "Completion permits filtered or skipped records, does not guarantee exactly-once writes and does not verify destination data."
	want := map[string]struct {
		meaning, next string
		notes         []string
	}{
		"unknown": {
			meaning: "Saved progress is absent, invalid or incompatible, or the group has an incompatible active member.",
			next:    "Check the reason, then verify the group, query, and endpoints.",
		},
		"running": {
			meaning: "Kafka group activity, unsettled state or a membership change was observed.",
			next:    "Wait briefly, then check status again.",
		},
		"incomplete": {
			meaning: "Part of the selection has no saved stop.",
			next:    "To resume, rerun with the same query, group, and configuration.",
		},
		"limit_reached": {
			meaning: "All selected partitions have saved stops and at least one stopped at a worker record cap.",
			next:    "Rerun the same job for another allowance.",
		},
		"complete_within_disorder": {
			meaning: "All selected partitions finished and at least one stopped early under MAX_OUT_OF_ORDER.",
			notes: []string{
				"Later records in the time window can be missed if timestamps move backwards by more than the allowance.",
				completionNote,
			},
		},
		"complete": {
			meaning: "Source processing finished for the selected finite scope: saved offset ends or the saved archive selection.",
			notes:   []string{completionNote},
		},
	}[report.Verdict]
	if savedArchiveSelection {
		want.notes = append(want.notes, "This group's saved archive checkpoint fixes the file selection.")
	}
	require.Equal(t, 3, report.Version)
	require.Equal(t, want.meaning, report.Meaning)
	require.Equal(t, want.next, report.Next)
	require.Equal(t, want.notes, report.Notes)
}
