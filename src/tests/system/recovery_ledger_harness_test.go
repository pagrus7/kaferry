//go:build harness

package compat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRecoveryLedgerChecksEveryCopy(t *testing.T) {
	// Given two expected records and a valid replay duplicate.
	stamp := time.Unix(100, 0)
	a := &kgo.Record{Key: []byte("a"), Value: []byte{0, 255}, Timestamp: stamp,
		Headers: []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}}
	b := &kgo.Record{Key: []byte("b"), Value: []byte{}, Timestamp: stamp, Partition: 1}
	l := recoveryLedger{expected: map[string]*kgo.Record{"a": a, "b": b}, seen: map[string]bool{}}
	// When arrival order changes and an ID repeats, then the ledger retains completeness.
	require.NoError(t, l.observe(b))
	require.NoError(t, l.observe(a))
	require.NoError(t, l.observe(a))
	require.Len(t, l.seen, 2)
	require.Equal(t, 1, l.duplicates)
	for _, mutation := range []func(*kgo.Record){
		func(r *kgo.Record) { r.Value = []byte("changed") },
		func(r *kgo.Record) { r.Headers = nil },
		func(r *kgo.Record) { r.Timestamp = stamp.Add(time.Millisecond) },
		func(r *kgo.Record) { r.Partition = 1 },
		func(r *kgo.Record) { r.Key = []byte("unknown") },
	} {
		// Then corruption in a duplicate is rejected too.
		bad := *a
		mutation(&bad)
		require.Error(t, l.observe(&bad))
	}
}
