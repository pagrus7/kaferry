package plan

import (
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestLimitValidationAndFingerprint(t *testing.T) {
	const base = "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' OPTIONS(GROUP='g',START=EARLIEST)"
	original := build(t, base)
	want, err := original.Fingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	for _, limit := range []struct {
		text string
		want uint64
	}{{"1", 1}, {"005", 5}, {"9007199254740992", 1 << 53}} {
		// Given the same query with a per-invocation limit.
		query := strings.Replace(base, "OPTIONS", "LIMIT "+limit.text+" OPTIONS", 1)
		// When planning and identifying its saved progress.
		p := build(t, query)
		got, err := p.Fingerprint("source-cluster", "destination-cluster")
		// Then changing or removing the cap preserves the resumable query identity.
		require.NoError(t, err)
		require.Equal(t, limit.want, p.Limit)
		require.Equal(t, want, got)
	}
	require.Zero(t, original.Limit, "omitting LIMIT leaves the query uncapped")
}

func TestInvalidLimitRangeDoesNotEchoInput(t *testing.T) {
	for _, value := range []string{"0", "9007199254740993", "18446744073709551616"} {
		// Given a syntactically valid integer outside the supported range.
		query := "INSERT INTO FILE cbor('out') SELECT * FROM FILE cbor('in')\nLIMIT " + value + " OPTIONS(GROUP='g')"
		statement, err := sql.NewParser().Parse(query)
		require.NoError(t, err)
		// When planning it.
		_, err = NewPlanner(nil).Build(statement, nil)
		// Then explain the fixed range at the value's location, without echoing it.
		var diagnostic *sql.Diagnostic
		require.ErrorAs(t, err, &diagnostic)
		require.Equal(t, sql.Position{Line: 2, Column: 7}, diagnostic.Position)
		require.Equal(t, "LIMIT must be an integer from 1 to 9007199254740992", diagnostic.Err.Error())
	}
}

func TestLimitDoesNotReplaceSourceStartOrGroup(t *testing.T) {
	for _, query := range []string{
		"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' LIMIT 5 OPTIONS(GROUP='g')",
		"INSERT INTO FILE cbor('out') SELECT * FROM FILE cbor('in') LIMIT 5 OPTIONS(ON_ERROR=FAIL)",
	} {
		// Given a cap without an otherwise required start or group.
		statement, err := sql.NewParser().Parse(query)
		require.NoError(t, err)
		// When planning, then retain the existing source/group requirements.
		_, err = NewPlanner(nil).Build(statement, Profiles{"default": {"bootstrap.servers": "broker"}})
		require.Error(t, err)
	}
}
