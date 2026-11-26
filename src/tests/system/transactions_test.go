//go:build integration

package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/internal/kafka/commitmeta"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

//kaferry:guarantee completion.offset-end
func TestSnapshotRejectsUnreadableNumericEndAndExcludesOpenTransaction(t *testing.T) {
	// Given one record in a transaction that has not committed.
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	p := producer(t, kgo.TransactionalID(src+"-open"))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, p.BeginTransaction())
	publish(t, p, &kgo.Record{Topic: src, Value: []byte("pending")})
	query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH(GROUP='%s')", dst, src, src+"-snapshot")
	cfg := configuration(t)
	// When requesting a numeric end beyond the readable end, fail before copying.
	result := binaryWorker(ctx, t, query+" WHERE RECORD_OFFSET<1", cfg)
	require.Equal(t, 2, result.code, result.detail)
	require.Contains(t, result.detail, "exceeds the readable end")
	// Then a default snapshot finishes now and keeps the empty end even after commit.
	result = binaryWorker(ctx, t, query, cfg)
	require.Zero(t, result.code, result.detail)
	require.NoError(t, p.EndTransaction(ctx, kgo.TryCommit))
	result = binaryWorker(ctx, t, query, cfg)
	require.Zero(t, result.code, result.detail)
	require.Zero(t, topicRecordCount(t, a, dst, 1))
}

//kaferry:guarantee completion.timestamp
func TestDisorderStopsOnlyAtVisibleTrigger(t *testing.T) {
	// Given an aborted trigger, an in-range committed record, a visible trigger and a late match.
	a := admin(t)
	src := topic(t, a, 1)
	p := producer(t, kgo.TransactionalID(src+"-aborted"))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	hi := time.Now().Truncate(time.Millisecond)
	publishAt := func(value string, stamp time.Time) {
		publish(t, p, &kgo.Record{Topic: src, Value: []byte(value), Timestamp: stamp})
	}
	require.NoError(t, p.BeginTransaction())
	publishAt("aborted", hi.Add(time.Second))
	require.NoError(t, p.EndTransaction(ctx, kgo.TryAbort))
	require.NoError(t, p.BeginTransaction())
	publishAt("match", hi.Add(-time.Second))
	publishAt("trigger", hi.Add(time.Second))
	publishAt("late", hi.Add(-time.Second))
	require.NoError(t, p.EndTransaction(ctx, kgo.TryCommit))
	for _, tc := range []struct {
		name, option, reason string
		values               []string
	}{
		{"bounded", "", "tip", []string{"match", "late"}},
		{"disorder", ",MAX_OUT_OF_ORDER=INTERVAL '0' SECOND", "timestamp", []string{"match"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// When each group scans the same committed records twice.
			dst, group := topic(t, a, 1), src+"-"+tc.name
			query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH(GROUP='%s'%s) WHERE RECORD_TIMESTAMP<TIMESTAMP '%s'", dst, src, group, tc.option, hi.Format(time.RFC3339Nano))
			cfg := configuration(t)
			for range 2 {
				result := binaryWorker(ctx, t, query, cfg)
				require.Zero(t, result.code, result.detail)
			}
			// Then only the opt-in stop omits the late match, and resume never repeats it.
			require.EqualValues(t, len(tc.values), topicRecordCount(t, a, dst, 1))
			c := consumer(t)
			assign(t, c, dst, 1)
			for _, value := range tc.values {
				require.Equal(t, value, string(read(t, c).Value))
			}
			raw := committedPartitions(t, a, group, src, 1)[0]
			progress, err := commitmeta.NewCodec().Decode(raw.Metadata)
			require.NoError(t, err)
			require.NotNil(t, progress.Stop)
			require.Equal(t, tc.reason, progress.Stop.Reason)
		})
	}
}
