//go:build integration && fault && load && harness

package compat

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

func TestOwnershipGenerationsReuseOneAuditSnapshot(t *testing.T) {
	for missing := range 4 {
		t.Run(fmt.Sprintf("missing-worker-%d", missing), func(t *testing.T) {
			// Given three selected members with disjoint assignments over all 24
			// partitions, each requires its own current-generation saved proof.
			members := ownershipGenerationMembers()
			s := &ownershipCase{t: t, group: "group", audit: &replicatedAudit{targets: readinessTargets()}, lab: &faults.Lab{}}
			for generation := int32(2); generation <= 3; generation++ {
				query, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				proof := ownershipGenerationRows(members, missing, generation)
				run, calls := ownershipGenerationReader(query, proof)
				s.lab.Docker = run
				// When the full cohort is checked, then only one read per broker
				// is needed, and a new poll must read its new generation again.
				observed := s.acceptedGenerations(query, members, make([]int64, replicatedPartitions), generation-1)
				cancel()
				require.Equal(t, map[string]int{"one": 1, "two": 1, "three": 1}, calls)
				if missing != 0 {
					require.Nil(t, observed, "one member's missing partition must reject the entire cohort")
				} else {
					require.Equal(t, map[string]int32{"worker1": generation, "worker2": generation, "worker3": generation}, observed)
				}
			}
		})
	}
	t.Run("expired-read-is-unavailable", func(t *testing.T) {
		// Given a complete cohort whose bounded read consumes this poll's budget.
		query, cancel := context.WithCancel(t.Context())
		defer cancel()
		members := ownershipGenerationMembers()
		run, calls := ownershipGenerationReader(query, ownershipGenerationRows(members, 0, 3))
		s := &ownershipCase{t: t, group: "group", audit: &replicatedAudit{targets: readinessTargets()}, lab: &faults.Lab{}}
		s.lab.Docker = func(ctx context.Context, args ...string) ([]byte, error) {
			data, err := run(ctx, args...)
			cancel()
			return data, err
		}
		// When the read joins, then this attempt is unavailable without failing
		// the case or accepting proof after the observation context expired.
		require.Nil(t, s.acceptedGenerations(query, members, make([]int64, replicatedPartitions), 2))
		require.ErrorIs(t, query.Err(), context.Canceled)
		require.Equal(t, map[string]int{"one": 1, "two": 1, "three": 1}, calls)
	})
}

func TestOwnershipCaseRejectsUnprovedMembership(t *testing.T) {
	// Given a complete stable assignment, when a member or partition changes,
	// then a matching count cannot substitute for the selected worker identities.
	parts := make([]int32, replicatedPartitions)
	for index := range parts {
		parts[index] = int32(index)
	}
	state := ownershipCaseObservation{State: "Stable", Members: []ownershipMember{{ClientID: "kaferry-worker1", MemberID: "member1", Partitions: parts}}}
	_, valid := ownershipCaseMembers(state, map[int]string{1: "worker1"})
	require.True(t, valid)
	for _, change := range []func(*ownershipCaseObservation){
		func(s *ownershipCaseObservation) { s.State = "PreparingRebalance" },
		func(s *ownershipCaseObservation) { s.Members[0].MemberID = "" },
		func(s *ownershipCaseObservation) { s.Members[0].ClientID = "other" },
		func(s *ownershipCaseObservation) { s.Members[0].Partitions[23] = 22 },
		func(s *ownershipCaseObservation) { s.Members[0].Partitions = s.Members[0].Partitions[:23] },
	} {
		candidate := state
		candidate.Members = slices.Clone(state.Members)
		candidate.Members[0].Partitions = slices.Clone(parts)
		change(&candidate)
		_, candidateValid := ownershipCaseMembers(candidate, map[int]string{1: "worker1"})
		require.False(t, candidateValid, "%+v", candidate)
	}
	// When two selected workers divide the same 24 partitions, then only
	// disjoint complete ownership passes; counts cannot hide overlap or loss.
	state.Members[0].Partitions = slices.Clone(parts[:12])
	state.Members = append(state.Members, ownershipMember{ClientID: "kaferry-worker2", MemberID: "member2", Partitions: slices.Clone(parts[12:])})
	_, valid = ownershipCaseMembers(state, map[int]string{1: "worker1", 2: "worker2"})
	require.True(t, valid)
	state.Members[1].Partitions[0] = 0
	_, valid = ownershipCaseMembers(state, map[int]string{1: "worker1", 2: "worker2"})
	require.False(t, valid)
}

func TestOwnershipKilledWorkerKeepsEarlierPeak(t *testing.T) {
	// Given live progress before an abrupt kill, when later progress is lower,
	// then sampled maxima stay separate from unavailable exact lifetime peaks.
	logs := `{"msg":"progress","submitted":2000,"confirmed":1995,"pending_entries":5,"pending_estimated_bytes":100}` + "\n" +
		`{"msg":"progress","submitted":2000,"confirmed":1998,"pending_entries":2,"pending_estimated_bytes":10}` + "\n"
	work, ready, err := ownershipCaseWork(logs)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 5, work.SampledPending)
	require.EqualValues(t, 100, work.SampledPendingBytes)
	require.Equal(t, 2, work.ProgressSamples)
	require.Nil(t, work.Summary)
	evidence, err := ownershipWorkMeasurement(work, true)
	require.NoError(t, err)
	require.Equal(t, false, evidence["lifetime_highwater_available"])
	require.Nil(t, evidence["peak_uncommitted_records"])
	require.Nil(t, evidence["peak_uncommitted_bytes"])
	require.Equal(t, ownershipKilledWorkScope, evidence["cap_measurement_scope"])
	_, err = ownershipWorkMeasurement(work, false)
	require.Error(t, err, "a normal exit still requires exact lifetime highwater")
	work, ready, err = ownershipCaseWork(logs + `{"msg":"copy summary","submitted":2000,"confirmed":2000,"peak_uncommitted_records":8,"peak_uncommitted_bytes":200}` + "\n")
	require.NoError(t, err)
	require.True(t, ready)
	evidence, err = ownershipWorkMeasurement(work, false)
	require.NoError(t, err)
	require.Equal(t, 8, evidence["peak_uncommitted_records"])
	require.EqualValues(t, 200, evidence["peak_uncommitted_bytes"])
	_, err = ownershipWorkMeasurement(work, true)
	require.Error(t, err, "SIGKILL must not conceal an already completed copy loop")
	// When a later complete row loses required cap evidence, then earlier
	// valid observations cannot conceal the missing or malformed measurement.
	for _, row := range []string{`{"msg":"progress","submitted":2,"confirmed":1}`, `{"msg":"progress","peak_uncommitted_records":-1}`, `{"msg":"progress"`, `{"msg":"progress","submitted":2000,"confirmed":975,"pending_entries":1025,"pending_estimated_bytes":100}`} {
		_, _, err := ownershipCaseWork(logs + row + "\n")
		require.Error(t, err, row)
	}
}
