package plan

import (
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestWorkerStopValidationAndFingerprint(t *testing.T) {
	const base = "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' OPTIONS(GROUP='g',START=EARLIEST)"
	original := build(t, base)
	want, err := original.Fingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	for _, limit := range []struct {
		text string
		want uint64
	}{{"1", 1}, {"005", 5}, {"9007199254740992", 1 << 53}} {
		// Given the same query with a per-invocation limit.
		query := strings.Replace(base, "OPTIONS(", "OPTIONS(WORKER_STOP_AFTER_RECORDS="+limit.text+",", 1)
		// When planning and identifying its saved progress.
		p := build(t, query)
		got, err := p.Fingerprint("source-cluster", "destination-cluster")
		// Then changing or removing the cap preserves the resumable query identity.
		require.NoError(t, err)
		require.Equal(t, limit.want, p.WorkerStopAfterRecords)
		require.Equal(t, want, got)
	}
	require.Zero(t, original.WorkerStopAfterRecords, "omitting WORKER_STOP_AFTER_RECORDS leaves the query uncapped")
}

func TestInvalidWorkerStopRangeDoesNotEchoInput(t *testing.T) {
	for _, value := range []string{"0", "9007199254740993", "18446744073709551616"} {
		// Given a syntactically valid integer outside the supported range.
		query := "INSERT INTO FILE avro('out') SELECT * FROM FILE avro('in')\nOPTIONS(WORKER_STOP_AFTER_RECORDS=" + value + ",GROUP='g')"
		statement, err := sql.NewParser().Parse(query)
		require.NoError(t, err)
		// When planning it.
		_, err = NewPlanner(nil).Build(statement, nil)
		// Then explain the fixed range at the value's location, without echoing it.
		var diagnostic *sql.Diagnostic
		require.ErrorAs(t, err, &diagnostic)
		require.Equal(t, sql.Position{Line: 2, Column: 35}, diagnostic.Position)
		require.Equal(t, "WORKER_STOP_AFTER_RECORDS must be an integer from 1 to 9007199254740992", diagnostic.Err.Error())
	}
}

func TestWorkerStopDoesNotReplaceSourceStartOrGroup(t *testing.T) {
	for _, query := range []string{
		"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' OPTIONS(GROUP='g',WORKER_STOP_AFTER_RECORDS=5)",
		"INSERT INTO FILE avro('out') SELECT * FROM FILE avro('in') OPTIONS(ON_ERROR=FAIL,WORKER_STOP_AFTER_RECORDS=5)",
	} {
		// Given a cap without an otherwise required start or group.
		statement, err := sql.NewParser().Parse(query)
		require.NoError(t, err)
		// When planning, then retain the existing source/group requirements.
		_, err = NewPlanner(nil).Build(statement, Profiles{"default": {"bootstrap.servers": "broker"}})
		require.Error(t, err)
	}
}

func TestWorkerStopOptionCannotRepeat(t *testing.T) {
	// Given two worker stop values in one query.
	statement, err := sql.NewParser().Parse("INSERT INTO FILE avro('out') SELECT * FROM FILE avro('in') OPTIONS(GROUP='g',WORKER_STOP_AFTER_RECORDS=1,WORKER_STOP_AFTER_RECORDS=2)")
	require.NoError(t, err)
	// When planning, then reject the second declaration rather than silently overriding it.
	_, err = NewPlanner(nil).Build(statement, nil)
	require.ErrorContains(t, err, "option WORKER_STOP_AFTER_RECORDS repeats")
}
