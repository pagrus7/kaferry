package plan

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestWorkerStopValidationAndFingerprint(t *testing.T) {
	const base = "INSERT INTO TOPIC 'b' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC 'a' WITH (GROUP='g', START=EARLIEST) "
	original := build(t, base)
	want, err := original.Fingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	for _, limit := range []struct {
		text string
		want uint64
	}{{"1", 1}, {"005", 5}, {"9007199254740992", 1 << 53}} {
		// Given the same query with a per-invocation limit.
		query := base + " OPTIONS(WORKER_STOP_AFTER_RECORDS=" + limit.text + ")"
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
		query := "INSERT INTO FILE avro('out') SELECT * FROM FILE avro('in') WITH(GROUP='g')\nOPTIONS(WORKER_STOP_AFTER_RECORDS=" + value + ")"
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

func TestWorkerStopKeepsSourceDefaults(t *testing.T) {
	for _, tc := range []struct{ settings, group string }{
		{"WITH (GROUP='g')", "g"},
		{"WITH (START=EARLIEST)", "ONE_OFF"},
		{"", "ONE_OFF"},
	} {
		// Given a cap and optional source settings.
		query := "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' " + tc.settings + " OPTIONS(WORKER_STOP_AFTER_RECORDS=5)"
		// When planning, then the cap leaves explicit groups and default starts intact.
		p := build(t, query)
		require.Equal(t, tc.group, p.Group)
		require.Equal(t, tc.group == "ONE_OFF", p.OneOff)
		require.Equal(t, "EARLIEST", p.Start)
		require.EqualValues(t, 5, p.WorkerStopAfterRecords)
	}
}

func TestWorkerStopOptionCannotRepeat(t *testing.T) {
	// Given two worker stop values in one query.
	statement, err := sql.NewParser().Parse("INSERT INTO FILE avro('out') SELECT * FROM FILE avro('in') WITH (GROUP='g') OPTIONS (WORKER_STOP_AFTER_RECORDS=1, WORKER_STOP_AFTER_RECORDS=2)")
	require.NoError(t, err)
	// When planning, then reject the second declaration rather than silently overriding it.
	_, err = NewPlanner(nil).Build(statement, nil)
	require.ErrorContains(t, err, "option WORKER_STOP_AFTER_RECORDS repeats")
}
