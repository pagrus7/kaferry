package plan

import (
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestCriteriaDefaultsAndIndependentAllowances(t *testing.T) {
	for _, tc := range []struct {
		kind, options string
		s, d          int64
	}{
		{"TOPIC 'a'", "", 300000, 300000},
		{"TOPIC 'a'", "MAX_STALENESS=INTERVAL '10' MINUTE", 600000, 600000},
		{"TOPIC 'a'", "MAX_OUT_OF_ORDER=INTERVAL '10' SECOND,MAX_STALENESS=INTERVAL '10' MINUTE", 600000, 10000},
		{"TOPIC 'a'", "MAX_STALENESS=OFF", -1, -1},
		{"TOPIC 'a'", "MAX_STALENESS=OFF,MAX_OUT_OF_ORDER=INTERVAL '0' SECOND", -1, 0},
		{"TOPIC 'a'", "MAX_OUT_OF_ORDER=OFF", 300000, -1},
		{"ARCHIVE 'a'", "", 0, -1},
		{"ARCHIVE 'a'", "MAX_OUT_OF_ORDER=INTERVAL '0' SECOND", 0, 0},
		{"ARCHIVE 'a'", "MAX_OUT_OF_ORDER=OFF", 0, -1},
	} {
		t.Run(tc.kind+tc.options, func(t *testing.T) {
			// Given an upper timestamp bound and source-specific settings in any order.
			q := "INSERT INTO TOPIC 'b' SELECT * FROM " + tc.kind
			if tc.options != "" {
				q += " WITH(" + tc.options + ")"
			}
			// When planning, then resolve the inherited allowance without changing the filter.
			p := build(t, q+" WHERE RECORD_TIMESTAMP < TIMESTAMP '2026-10-22T10:00:00Z'")
			require.Equal(t, tc.s, p.MaxStalenessMS)
			require.Equal(t, tc.d, p.DisorderMS())
			require.Equal(t, tc.kind == "ARCHIVE 'a'" || tc.s >= 0 || tc.d >= 0, p.Finite())
		})
	}
}

func TestOffsetRangesAndPartitionBranches(t *testing.T) {
	// Given disjoint ranges and a shared timestamp filter.
	q := "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE (PARTITION IN(2) AND RECORD_OFFSET>=900 AND RECORD_OFFSET<LATEST) OR (PARTITION IN(0,1) AND RECORD_OFFSET>99 AND RECORD_OFFSET<=499) AND RECORD_TIMESTAMP < NOW()"
	// When planning, then each partition has one normalized half-open range.
	p := build(t, q)
	require.Equal(t, []int32{0, 1, 2}, p.Partitions.IDs)
	require.EqualValues(t, 100, *p.OffsetRange(0).Lower)
	require.EqualValues(t, 500, *p.OffsetRange(1).Upper)
	require.True(t, p.OffsetRange(2).Latest)
	require.Nil(t, p.OffsetRange(3))
	require.True(t, p.Finite())
	require.EqualValues(t, -1, p.DisorderMS())
	// Reordering branches and partition IDs does not change query identity.
	reordered := strings.Replace(q, "(PARTITION IN(2) AND RECORD_OFFSET>=900 AND RECORD_OFFSET<LATEST) OR (PARTITION IN(0,1) AND RECORD_OFFSET>99 AND RECORD_OFFSET<=499)", "(PARTITION IN(1,0) AND RECORD_OFFSET>99 AND RECORD_OFFSET<=499) OR (PARTITION IN(2) AND RECORD_OFFSET>=900 AND RECORD_OFFSET<LATEST)", 1)
	a, e := p.Fingerprint("source", "destination")
	require.NoError(t, e)
	b, e := build(t, reordered).Fingerprint("source", "destination")
	require.NoError(t, e)
	require.Equal(t, a, b)
	for _, fragment := range []string{"RECORD_OFFSET<LATEST", "RECORD_OFFSET>=10", "RECORD_OFFSET<20", "RECORD_OFFSET>=10 AND RECORD_OFFSET<20"} {
		r := build(t, "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE "+fragment)
		require.NotNil(t, r.Offsets)
	}
}

func TestCriteriaDiagnosticsAreLocatedAndRedacted(t *testing.T) {
	const base = "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' "
	for _, tc := range []struct{ tail, marker, hint string }{
		{"WITH(MAX_OUT_OF_ORDER=OFF)", "MAX_OUT_OF_ORDER", "requires an upper timestamp"},
		{"WITH(MAX_STALENESS=OFF) WHERE RECORD_OFFSET<LATEST AND RECORD_TIMESTAMP<NOW()", "MAX_STALENESS", "explicit offset end"},
		{"WITH(MAX_OUT_OF_ORDER=OFF) WHERE RECORD_OFFSET<LATEST AND RECORD_TIMESTAMP<NOW()", "MAX_OUT_OF_ORDER", "explicit offset end"},
		{"WHERE RECORD_OFFSET>=LATEST", ">=", "LATEST requires"},
		{"WHERE RECORD_OFFSET>=5 AND RECORD_OFFSET<5", "<5", "range is empty"},
		{"WHERE RECORD_OFFSET>=1 AND RECORD_OFFSET>2", ">2", "lower bound repeats"},
		{"WHERE RECORD_OFFSET<5 AND RECORD_OFFSET<=6", "<=6", "upper bound repeats"},
		{"WHERE RECORD_OFFSET<LATEST AND RECORD_OFFSET<LATEST", "<LATEST AND", "upper bound repeats"},
		{"WHERE RECORD_OFFSET<9223372036854775808", "9223372036854775808", "non-negative signed 64-bit"},
		{"WHERE RECORD_OFFSET<=9223372036854775807", "<=", "overflows"},
		{"WHERE (PARTITION IN(0) AND RECORD_OFFSET<10) OR (PARTITION IN(0) AND RECORD_OFFSET<LATEST)", "PARTITION IN(0) AND RECORD_OFFSET<LATEST", "branches overlap"},
		{"WHERE (PARTITION NOT IN(0) AND RECORD_OFFSET<10)", "(", "requires PARTITION IN"},
		{"WHERE (RECORD_OFFSET<10)", "(", "requires PARTITION IN"},
		{"WHERE (PARTITION IN(0) AND RECORD_OFFSET>=10)", "(", "requires an upper offset"},
		{"WHERE (PARTITION IN(0) AND RECORD_TIMESTAMP<NOW())", "<NOW()", "branches accept"},
		{"WHERE (PARTITION IN(0) AND RECORD_OFFSET<10) AND RECORD_OFFSET>=1", "WHERE", "shared partition or offset"},
	} {
		t.Run(tc.tail, func(t *testing.T) {
			// Given a malformed criterion, when parsing/planning, then report a position and fixed hint.
			q := base + tc.tail
			statement, err := sql.NewParser().Parse(q)
			if err == nil {
				_, err = NewPlanner(nil).Build(statement, Profiles{"default": {"bootstrap.servers": "secret_SENTINEL"}})
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.hint)
			require.NotContains(t, err.Error(), "secret_SENTINEL")
			var diagnostic *sql.Diagnostic
			require.ErrorAs(t, err, &diagnostic)
			// The upper-repeat case points to the second declaration.
			index := strings.Index(q, tc.marker)
			if strings.Count(tc.tail, "<LATEST") == 2 {
				index = strings.LastIndex(q, "<LATEST")
			}
			require.Equal(t, sql.Position{Line: 1, Column: index + 1}, diagnostic.Position)
		})
	}
}

func TestCriteriaIdentityIncludesBehaviorAndExcludesPositions(t *testing.T) {
	const base = "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' "
	for _, suffix := range []string{"WHERE RECORD_TIMESTAMP<NOW()", "WHERE RECORD_OFFSET<LATEST", "WHERE RECORD_OFFSET>=10 AND RECORD_OFFSET<20", "WITH(MAX_STALENESS=OFF,MAX_OUT_OF_ORDER=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP<NOW()"} {
		// Given equivalent whitespace and source positions, when fingerprinting, then preserve identity.
		a := build(t, base+suffix)
		b := build(t, "-- shifted positions\n"+base+suffix)
		x, err := a.Fingerprint("s", "d")
		require.NoError(t, err)
		y, err := b.Fingerprint("s", "d")
		require.NoError(t, err)
		require.Equal(t, x, y)
	}
	a := build(t, base+"WHERE RECORD_TIMESTAMP<NOW()")
	b := build(t, base+"WITH(MAX_OUT_OF_ORDER=INTERVAL '5' MINUTE) WHERE RECORD_TIMESTAMP<NOW()")
	x, _ := a.Fingerprint("s", "d")
	y, _ := b.Fingerprint("s", "d")
	require.Equal(t, x, y)
	b = build(t, base+"WITH(MAX_OUT_OF_ORDER=OFF) WHERE RECORD_TIMESTAMP<NOW()")
	y, _ = b.Fingerprint("s", "d")
	require.NotEqual(t, x, y)
}
