//go:build integration && fault && load

package compat

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type brokerHealthCheck struct {
	At         time.Time `json:"at"`
	Containers []string  `json:"containers"`
	Output     string    `json:"output"`
	Error      string    `json:"error,omitempty"`
	Passed     bool      `json:"passed"`
}

type brokerPoll struct {
	Label      string              `json:"condition"`
	DurationMS int64               `json:"query_milliseconds"`
	Complete   bool                `json:"observation_complete"`
	QueryError string              `json:"query_error,omitempty"`
	State      brokerObservation   `json:"state"`
	Health     []brokerHealthCheck `json:"health_checks,omitempty"`
	Conditions []string            `json:"condition_queries,omitempty"`
}

func brokerDiagnosticText(value string) string {
	if len(value) > 2048 {
		return value[:2048] + " [truncated]"
	}
	return value
}

func (s *brokerCopy) recordPoll(poll brokerPoll) {
	if len(s.polls) == 128 {
		copy(s.polls, s.polls[1:])
		s.polls[len(s.polls)-1] = poll
		s.omittedPolls++
		return
	}
	s.polls = append(s.polls, poll)
}

func (s *brokerCopy) retainPolls() {
	data, err := json.MarshalIndent(map[string]any{"retained_poll_limit": 128, "omitted_polls": s.omittedPolls, "polls": s.polls}, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(s.directory, "broker-polls.json"), append(data, '\n'), 0600)
	}
	if err != nil {
		s.t.Error("retain broker query and health diagnostics: ", err)
	}
}

func TestBrokerPollJournalRetainsFailedQueryAndHealthTail(t *testing.T) {
	// Given steady polls before a failure, when the bounded journal rolls over,
	// then the failed query and actual unhealthy Docker output survive on disk.
	s := &brokerCopy{t: t, directory: t.TempDir()}
	for range 129 {
		s.recordPoll(brokerPoll{Label: "healthy prefix", Complete: true})
	}
	s.recordPoll(brokerPoll{Label: "restored broker health", QueryError: "context deadline exceeded",
		State:  brokerObservation{QueryErrors: []string{"source saved offsets: coordinator unavailable"}},
		Health: []brokerHealthCheck{{Containers: []string{"owned-container"}, Output: "true starting", Passed: false}}})
	s.retainPolls()
	data, err := os.ReadFile(filepath.Join(s.directory, "broker-polls.json"))
	require.NoError(t, err)
	var journal struct {
		Omitted int          `json:"omitted_polls"`
		Polls   []brokerPoll `json:"polls"`
	}
	require.NoError(t, json.Unmarshal(data, &journal))
	require.Equal(t, 2, journal.Omitted)
	require.Len(t, journal.Polls, 128)
	last := journal.Polls[127]
	require.False(t, last.Complete)
	require.Equal(t, "context deadline exceeded", last.QueryError)
	require.Equal(t, []string{"source saved offsets: coordinator unavailable"}, last.State.QueryErrors)
	require.Equal(t, "true starting", last.Health[0].Output)
	require.False(t, last.Health[0].Passed)
}
