//go:build integration

package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	src, dst := topic(t, a, 1), topic(t, a, 1)
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
	// When scanning with zero disorder, the committed trigger ends the scan.
	query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH(GROUP='%s',MAX_OUT_OF_ORDER=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP<TIMESTAMP '%s'", dst, src, src+"-disorder", hi.Format(time.RFC3339Nano))
	cfg := configuration(t)
	for range 2 {
		result := binaryWorker(ctx, t, query, cfg)
		require.Zero(t, result.code, result.detail)
	}
	// Then resume preserves the early stop and only the first visible match was published.
	require.EqualValues(t, 1, topicRecordCount(t, a, dst, 1))
	c := consumer(t)
	assign(t, c, dst, 1)
	require.Equal(t, "match", string(read(t, c).Value))
}
