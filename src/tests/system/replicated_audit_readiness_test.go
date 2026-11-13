//go:build integration && fault

package compat

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

type readinessReadResult struct {
	data []byte
	err  error
}

func readinessTargets() []faults.Target {
	return []faults.Target{{Broker: 1, Container: "one"}, {Broker: 2, Container: "two"}, {Broker: 3, Container: "three"}}
}

func readinessRows() [][]byte {
	rows := [][]byte{[]byte(`Completed request:{"requestHeader":{"clientId":"worker","requestApiKeyName":"JOIN_GROUP"},"request":{"groupId":"group"},"response":{"errorCode":0,"memberId":"member","generationId":2}}` + "\n")}
	for partition := range 2 {
		rows = append(rows, fmt.Appendf(nil, `Completed request:{"requestHeader":{"clientId":"worker","requestApiKeyName":"OFFSET_COMMIT"},"request":{"groupId":"group","memberId":"member","generationIdOrMemberEpoch":2,"topics":[{"partitions":[{"partitionIndex":%d,"committedOffset":6}]}]},"response":{"topics":[{"partitions":[{"partitionIndex":%d,"errorCode":0}]}]}}`+"\n", partition, partition))
	}
	return rows
}

func awaitReadinessStarts(ctx context.Context, t *testing.T, started <-chan string) {
	t.Helper()
	seen := make(map[string]bool)
	for range readinessTargets() {
		select {
		case container := <-started:
			require.False(t, seen[container], "one read per broker, without retries")
			seen[container] = true
		case <-ctx.Done():
			t.Fatal("all broker reads must start before any is released: ", ctx.Err())
		}
	}
	require.Len(t, seen, 3)
}

func awaitReadinessResult(ctx context.Context, t *testing.T, result <-chan readinessReadResult) readinessReadResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-ctx.Done():
		t.Fatal("broker reads did not join within the polling bound: ", ctx.Err())
		return readinessReadResult{}
	}
}

func timedReadinessRow(row []byte, completed time.Time, total string) []byte {
	row = bytes.Replace(row, []byte(`{"requestHeader":`), []byte(`{"totalTimeMs":`+total+`,"requestHeader":`), 1)
	return fmt.Appendf(nil, "[%s] DEBUG %s", completed.Format("2006-01-02 15:04:05,000"), row)
}

func replicatedAuditRequiresPostTransferReceipt(t *testing.T) {
	// Given accepted peer commits received before transfer but completed after it.
	barrier := time.Date(2026, 11, 9, 16, 30, 31, 704037733, time.UTC)
	completed := time.Date(2026, 11, 9, 16, 30, 31, 736000000, time.UTC)
	rows := readinessRows()
	before := bytes.Join([][]byte{rows[0], timedReadinessRow(rows[1], completed, "67.31"),
		timedReadinessRow(rows[2], completed, "67.31")}, nil)
	check := func(t *testing.T, data []byte, want bool, invalid bool) {
		t.Helper()
		generation, ready, err := currentGenerationSaved(data, "worker", "group", "member", []int32{0, 1},
			map[int32]int64{0: 2, 1: 2}, barrier)
		if invalid {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.EqualValues(t, 2, generation)
		}
		require.Equal(t, want, ready)
	}
	// When readiness checks the receipt barrier, then these old requests must
	// not permit healing, although the pre-fault zero-barrier gate accepts them.
	_, ready, err := currentGenerationSaved(before, "worker", "group", "member", []int32{0, 1}, nil, time.Time{})
	require.NoError(t, err)
	require.True(t, ready)
	check(t, before, false, false)

	// Then every required partition must have a newly received, accepted save;
	// the exact lower boundary is inclusive, as in the independent final audit.
	part0 := timedReadinessRow(rows[1], completed, "31.962267")
	part1 := timedReadinessRow(rows[2], completed, "31.962267")
	withPart0 := append(bytes.Clone(before), part0...)
	check(t, withPart0, false, false)
	for _, test := range []struct {
		name, old, replacement string
		invalid                bool
	}{
		{name: "one nanosecond too early", old: `"totalTimeMs":31.962267`, replacement: `"totalTimeMs":31.962268`},
		{name: "other member", old: `"memberId":"member"`, replacement: `"memberId":"other"`},
		{name: "old generation", old: `"generationIdOrMemberEpoch":2`, replacement: `"generationIdOrMemberEpoch":1`},
		{name: "other client", old: `"clientId":"worker"`, replacement: `"clientId":"other"`},
		{name: "other group", old: `"groupId":"group"`, replacement: `"groupId":"other"`},
		{name: "rejected save", old: `"errorCode":0`, replacement: `"errorCode":25`},
		{name: "unadvanced offset", old: `"committedOffset":6`, replacement: `"committedOffset":2`},
		{name: "missing request time", old: `"totalTimeMs":31.962267,`, invalid: true},
		{name: "negative request time", old: `"totalTimeMs":31.962267`, replacement: `"totalTimeMs":-1`, invalid: true},
		{name: "negative fractional request time", old: `"totalTimeMs":31.962267`, replacement: `"totalTimeMs":-0.0000001`, invalid: true},
		{name: "overflow request time", old: `"totalTimeMs":31.962267`, replacement: `"totalTimeMs":1e100`, invalid: true},
		{name: "unknown completion", old: "] DEBUG ", replacement: "] INFO ", invalid: true},
		{name: "quoted request time", old: `"totalTimeMs":31.962267`, replacement: `"totalTimeMs":"31.962267"`, invalid: true},
		{name: "null request time", old: `"totalTimeMs":31.962267`, replacement: `"totalTimeMs":null`, invalid: true},
		{name: "malformed request time", old: `"totalTimeMs":31.962267`, replacement: `"totalTimeMs":false`, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := bytes.Replace(part1, []byte(test.old), []byte(test.replacement), 1)
			require.NotEqual(t, part1, changed)
			check(t, append(bytes.Clone(withPart0), changed...), false, test.invalid)
		})
	}
	check(t, append(bytes.Clone(withPart0), part1...), true, false)
	exponent := bytes.Replace(part1, []byte(`"totalTimeMs":31.962267`), []byte(`"totalTimeMs":3.1962267e1`), 1)
	check(t, append(bytes.Clone(withPart0), exponent...), true, false)
	// An expired observation cannot supply the post-transfer readiness proof.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, ready, err = (&replicatedAudit{}).ready(ctx, "worker", "group", "member", []int32{0, 1}, nil, barrier)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ready)
}
