package plan

import (
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/src/internal/sql"
	"github.com/stretchr/testify/require"
)

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
	a, e := p.testFingerprint("source", "destination")
	require.NoError(t, e)
	b, e := build(t, reordered).testFingerprint("source", "destination")
	require.NoError(t, e)
	require.Equal(t, a, b)
	for _, fragment := range []string{"RECORD_OFFSET<LATEST", "RECORD_OFFSET>=10", "RECORD_OFFSET<20", "RECORD_OFFSET>=10 AND RECORD_OFFSET<20"} {
		r := build(t, "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE "+fragment)
		require.NotNil(t, r.Offsets)
	}
}

func TestKafkaStopRules(t *testing.T) {
	const base = "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' "
	for _, tc := range []struct {
		tail                   string
		follow, finite, latest bool
		numeric                *int64
		disorder               int64
	}{
		{"", false, true, true, nil, -1},
		{"WITH(FOLLOW=OFF)", false, true, true, nil, -1},
		{"WHERE RECORD_OFFSET<LATEST", false, true, true, nil, -1},
		{"WHERE RECORD_OFFSET<8", false, true, false, new(int64(8)), -1},
		{"WHERE PARTITION IN(2)", false, true, true, nil, -1},
		{"WHERE RECORD_OFFSET>=2", false, true, true, nil, -1},
		{"WHERE RECORD_TIMESTAMP<NOW()", false, true, true, nil, -1},
		{"WITH(MAX_OUT_OF_ORDER=OFF) WHERE RECORD_TIMESTAMP<NOW()", false, true, true, nil, -1},
		{"WITH(MAX_OUT_OF_ORDER=INTERVAL '2' SECOND) WHERE RECORD_TIMESTAMP<NOW() AND RECORD_OFFSET<LATEST", false, true, true, nil, 2000},
		{"WITH(FOLLOW=ON)", true, false, false, nil, -1},
		{"WITH(FOLLOW=ON, START=LATEST)", true, false, false, nil, -1},
		{"WITH(FOLLOW=ON) WHERE PARTITION IN(2) AND RECORD_OFFSET>=4 AND RECORD_TIMESTAMP>=NOW()", true, false, false, nil, -1},
		{"WITH(FOLLOW=ON) OPTIONS(WORKER_STOP_AFTER_RECORDS=1)", true, true, false, nil, -1},
	} {
		t.Run(tc.tail, func(t *testing.T) {
			// Given a Kafka source, when planned, then its end and signal behavior agree.
			p := build(t, base+tc.tail)
			require.Equal(t, tc.follow, p.Follow)
			require.Equal(t, tc.finite, p.Finite())
			require.Equal(t, tc.disorder, p.DisorderMS())
			if p.Offsets == nil {
				require.True(t, tc.follow)
				return
			}
			require.Equal(t, tc.latest, p.Offsets.Latest)
			require.Equal(t, tc.numeric, p.Offsets.Upper)
		})
	}
}

func TestCriteriaDiagnosticsAreLocatedAndRedacted(t *testing.T) {
	const base = "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' "
	for _, tc := range []struct{ tail, marker, hint string }{
		{"WITH(FOLLOW=ON) WHERE RECORD_OFFSET<LATEST", "FOLLOW", "remove FOLLOW or the upper bound"},
		{"WITH(FOLLOW=ON) WHERE RECORD_OFFSET<55", "FOLLOW", "remove FOLLOW or the upper bound"},
		{"WITH(FOLLOW=ON) WHERE RECORD_TIMESTAMP<NOW()", "FOLLOW", "remove FOLLOW or the upper bound"},
		{"WITH(FOLLOW=ON) WHERE (PARTITION IN(0) AND RECORD_OFFSET<4) OR (PARTITION IN(1) AND RECORD_OFFSET<LATEST)", "FOLLOW", "remove FOLLOW or the upper bound"},
		{"WITH(FOLLOW=ON, MAX_OUT_OF_ORDER=INTERVAL '1' SECOND) WHERE RECORD_TIMESTAMP<NOW()", "FOLLOW", "remove FOLLOW or the upper bound"},
		{"WITH(START=LATEST) WHERE RECORD_OFFSET<LATEST", "START", "remove START"},
		{"WITH(START=EARLIEST)", "START", "remove START"},
		{"WITH(MAX_OUT_OF_ORDER=OFF)", "MAX_OUT_OF_ORDER", "requires an upper timestamp"},
		{"WITH(MAX_OUT_OF_ORDER=INTERVAL '1' SECOND) WHERE (PARTITION IN(0) AND RECORD_OFFSET<LATEST) OR (PARTITION IN(1) AND RECORD_OFFSET<55) AND RECORD_TIMESTAMP<NOW()", "MAX_OUT_OF_ORDER", "numeric offset end"},
		{"WITH(MAX_OUT_OF_ORDER=OFF) WHERE RECORD_OFFSET<123 AND RECORD_TIMESTAMP<NOW()", "MAX_OUT_OF_ORDER", "numeric offset end"},
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
		{"WHERE (PARTITION IN(0) AND RECORD_TIMESTAMP<NOW())", "<NOW()", "cannot contain timestamp bounds"},
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
	for _, suffix := range []string{"WHERE RECORD_TIMESTAMP<NOW()", "WHERE RECORD_OFFSET<LATEST", "WHERE RECORD_OFFSET>=10 AND RECORD_OFFSET<20", "WITH(MAX_OUT_OF_ORDER=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP<NOW()"} {
		// Given equivalent whitespace and source positions, when fingerprinting, then preserve identity.
		a := build(t, base+suffix)
		b := build(t, "-- shifted positions\n"+base+suffix)
		x, err := a.testFingerprint("s", "d")
		require.NoError(t, err)
		y, err := b.testFingerprint("s", "d")
		require.NoError(t, err)
		require.Equal(t, x, y)
	}
	a := build(t, base+"WHERE RECORD_TIMESTAMP<NOW()")
	b := build(t, base+"WITH(MAX_OUT_OF_ORDER=INTERVAL '5' MINUTE) WHERE RECORD_TIMESTAMP<NOW()")
	x, _ := a.testFingerprint("s", "d")
	y, _ := b.testFingerprint("s", "d")
	require.NotEqual(t, x, y)
	b = build(t, base+"WITH(MAX_OUT_OF_ORDER=OFF) WHERE RECORD_TIMESTAMP<NOW()")
	y, _ = b.testFingerprint("s", "d")
	require.Equal(t, x, y)
}
