//go:build integration && load && harness

package compat

import (
	"fmt"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLoadEndurancePartitionedFidelityAndHistory(t *testing.T) {
	// Given all 24 source partitions and high entropy records, independent of brokers.
	t.Setenv("KAFERRY_ARCHIVE_PAYLOAD", "random")
	const partitions, count = 24, 2400
	for _, archive := range []bool{false, true} {
		stream := newPartitionedStreamRecords("fixture", time.Now().Truncate(time.Millisecond), partitions)
		stream.published, stream.acknowledged = count, count
		for id := range count {
			r := archiveLoadRecordPartitions(id, partitions)
			require.Equal(t, int32(id%partitions), r.Source.Partition)
			require.Equal(t, int64(id/partitions), r.Source.Offset)
			stream.receipts.submit(id, &kgo.Record{Key: r.Key, Value: r.Value, Headers: stream.headers})
		}
		// When partitions arrive in reverse order, per-partition history must still be released.
		for partition := partitions - 1; partition >= 0; partition-- {
			for id := partition; id < count; id += partitions {
				r := archiveLoadRecordPartitions(id, partitions)
				stamp := stream.stamp.UnixMilli()
				r.TimestampMS = &stamp
				if archive {
					stream.verifyArchive(t, r, r.Source)
				} else {
					stream.verify(t, &kgo.Record{Key: r.Key, Value: r.Value, Timestamp: stream.stamp, Headers: stream.headers, Partition: int32(id % 32)})
				}
			}
			require.Empty(t, stream.receipts.pending)
		}
		// Then every record reconciles; a late duplicate is still detected after history was released.
		delivery := stream.delivery(true)
		require.Equal(t, count, delivery.Verified)
		require.Zero(t, *delivery.Missing)
		require.Zero(t, delivery.Reordered)
		require.True(t, *delivery.Integrity.Matched)
		stream.verify(t, streamFixtureRecord(23, stream.stamp))
		require.Equal(t, 1, stream.duplicates)
	}
}

func TestLoadEnduranceRejectsWrongKeyRouting(t *testing.T) {
	// Given an intact keyed record whose source placement is independent of its key route.
	for _, wrong := range []bool{false, true} {
		stream := newPartitionedStreamRecords("fixture", time.Now().Truncate(time.Millisecond), 24)
		stream.keyRouting = kgo.StickyKeyPartitioner(nil).ForTopic("destination")
		stream.destinationPartitions = 32
		stream.published = 2
		record := streamFixtureRecord(1, stream.stamp)
		record.Partition = int32(stream.keyRouting.Partition(record, 32))
		if wrong {
			record.Partition = (record.Partition + 1) % 32
		}
		// When only the route changes, then byte/hash fidelity cannot hide the failure.
		stream.verify(t, record)
		if wrong {
			require.Equal(t, 1, stream.corrupted)
			require.Zero(t, stream.verified)
		} else {
			require.Zero(t, stream.corrupted)
			require.Equal(t, 1, stream.verified)
		}
	}
}

func TestLoadIntegrityFingerprintPreservesFieldBoundariesAndNullBytes(t *testing.T) {
	// Given records that naive concatenation or header maps would conflate.
	pairs := [][2]*kgo.Record{
		{{Key: []byte("ab"), Value: []byte("c")}, {Key: []byte("a"), Value: []byte("bc")}},
		{{Key: nil}, {Key: []byte{}}},
		{{Value: nil}, {Value: []byte{}}},
		{{Headers: []kgo.RecordHeader{{Key: "h", Value: nil}}}, {Headers: []kgo.RecordHeader{{Key: "h", Value: []byte{}}}}},
		{{Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("ab")}}}, {Headers: []kgo.RecordHeader{{Key: "ha", Value: []byte("b")}}}},
		{{Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("a")}, {Key: "h", Value: []byte("b")}}},
			{Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("b")}, {Key: "h", Value: []byte("a")}}}},
		{{Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("a")}, {Key: "h", Value: []byte("a")}}},
			{Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("a")}}}},
	}
	for _, pair := range pairs {
		// When hashed, then distinct logical bytes have distinct fingerprints.
		a, b := pair[0], pair[1]
		require.NotEqual(t, recordFingerprint(a.Key, a.Value, a.Headers), recordFingerprint(b.Key, b.Value, b.Headers))
	}
}

func TestLoadIntegrityReconcilesDifferentPartitionArrivalOrdersAndReleasesHistory(t *testing.T) {
	// Given submitted records and receipts that cross partitions in a different order.
	integrity := newReceiptLedger(4)
	const count = 65536
	stamp := time.Now()
	for id := range count {
		integrity.submit(id, streamFixtureRecord(id, stamp))
	}
	for _, partition := range []int{3, 0, 2, 1} {
		for id := partition; id < count; id += 8 {
			// When neighboring IDs swap, a bounded digest gap must still reconcile.
			for _, received := range []int{id + 4, id} {
				record := streamFixtureRecord(received, stamp)
				integrity.receive(t, received, record.Key, record.Value, record.Headers)
			}
			require.Empty(t, integrity.pending, "completed partition history is released")
		}
	}

	// Then every actual submission and receipt contributes to matching final evidence.
	require.Nil(t, integrity.snapshot(false, count, count).Matched, "partial evidence cannot claim a match")
	result := integrity.snapshot(true, count, count)
	require.True(t, *result.Matched)
	require.Equal(t, count, result.SubmittedRecords)
	require.Equal(t, count, result.ReceivedRecords)
	require.Len(t, result.SubmittedSHA256, 64)
	require.Equal(t, result.SubmittedSHA256, result.ReceivedSHA256)
}

func TestLoadReceiptLedgerKeepsFirstDigestAcrossGapsAndLateDuplicates(t *testing.T) {
	// Given two submitted records in one partition, received in reverse order.
	ledger := newReceiptLedger(1)
	first := &kgo.Record{Value: []byte("first")}
	second := &kgo.Record{Value: []byte("second")}
	ledger.submit(0, first)
	ledger.submit(1, second)
	require.True(t, ledger.receive(t, 1, nil, second.Value, nil))
	require.Zero(t, ledger.contiguous())

	// When duplicates carry different bytes before and after the gap closes.
	require.False(t, ledger.receive(t, 1, nil, []byte("changed"), nil))
	require.True(t, ledger.receive(t, 0, nil, first.Value, nil))
	require.False(t, ledger.receive(t, 1, nil, []byte("changed again"), nil))

	// Then each first receipt is counted and hashed once, with no retained history.
	result := ledger.snapshot(true, 2, 2)
	require.Equal(t, 2, ledger.contiguous())
	require.Equal(t, 2, result.ReceivedRecords)
	require.Zero(t, result.PendingRecords)
	require.True(t, *result.Matched)
}

func TestLoadIntegrityDetectsKafkaAndArchiveMutationsAlongsideByteChecks(t *testing.T) {
	// Given original submitted bytes and individually damaged destination fields.
	mutations := map[string]func(*kgo.Record){
		"payload-bit":       func(r *kgo.Record) { r.Value[len(r.Value)-1] ^= 1 },
		"key-bit":           func(r *kgo.Record) { r.Key[0] ^= 1 },
		"header-bit":        func(r *kgo.Record) { r.Headers[0].Value[0] ^= 1 },
		"header-name":       func(r *kgo.Record) { r.Headers[0].Key = "different" },
		"header-order":      func(r *kgo.Record) { r.Headers[1], r.Headers[2] = r.Headers[2], r.Headers[1] },
		"header-null-empty": func(r *kgo.Record) { r.Headers[1].Value = []byte{} },
		"header-dropped":    func(r *kgo.Record) { r.Headers = r.Headers[:2] },
	}
	for name, mutate := range mutations {
		for _, mode := range []string{"kafka", "archive"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				stream := newStreamRecords("fixture", time.Now().Truncate(time.Millisecond))
				stream.published, stream.acknowledged = 4, 4
				for id := range 4 {
					record := streamFixtureRecord(id, stream.stamp)
					stream.receipts.submit(id, record)
					if id == 1 {
						mutate(record)
					}
					// When received through either verifier, then corruption reaches both checks.
					if mode == "archive" {
						stored := archiveLoadRecord(id)
						stamp := stream.stamp.UnixMilli()
						stored.TimestampMS, stored.Key, stored.Value = &stamp, record.Key, record.Value
						stored.Headers = make([]core.Header, len(record.Headers))
						for i, h := range record.Headers {
							stored.Headers[i] = core.Header{Name: h.Key, Value: h.Value}
						}
						stream.verifyArchive(t, stored, stored.Source)
					} else {
						stream.verify(t, record)
					}
				}
				result := stream.delivery(true)
				require.Equal(t, 1, result.Corrupted)
				require.Equal(t, 3, result.Verified)
				require.False(t, *result.Integrity.Matched)
				require.NotEqual(t, result.Integrity.SubmittedSHA256, result.Integrity.ReceivedSHA256)
			})
		}
	}
}

func TestLoadIntegrityKeepsTimestampAndArchiveProvenanceChecks(t *testing.T) {
	// Given intact payload/header bytes with damaged metadata outside the digest scope.
	for _, field := range []string{"kafka-timestamp", "archive-timestamp", "archive-offset", "archive-cluster"} {
		t.Run(field, func(t *testing.T) {
			stream := newStreamRecords("fixture", time.Now().Truncate(time.Millisecond))
			stream.published, stream.acknowledged = 1, 1
			record := streamFixtureRecord(0, stream.stamp)
			stream.receipts.submit(0, record)
			if field == "kafka-timestamp" {
				record.Timestamp = record.Timestamp.Add(time.Millisecond)
				stream.verify(t, record)
			} else {
				stored := archiveLoadRecord(0)
				identity := *stored.Source
				stamp := stream.stamp.UnixMilli()
				stored.TimestampMS = &stamp
				switch field {
				case "archive-timestamp":
					stamp++
				case "archive-offset":
					stored.Source.Offset++
				case "archive-cluster":
					stored.Source.Cluster = "damaged"
				}
				stream.verifyArchive(t, stored, &identity)
			}

			// When fingerprints match, then the original metadata comparison still rejects it.
			result := stream.delivery(true)
			require.True(t, *result.Integrity.Matched)
			require.Equal(t, 1, result.Corrupted)
			require.Zero(t, result.Verified)
		})
	}
}

func TestLoadFiniteVerifierAllowsDestinationPartitionSkew(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			// Given a finite key-routed copy whose destination partitions can arrive independently.
			const count = 20000
			stream := newFiniteStreamRecords("fixture", time.Now().Truncate(time.Millisecond), count)
			stream.published, stream.acknowledged = count, count
			stream.keyRouting = kgo.StickyKeyPartitioner(nil).ForTopic("destination")
			stream.destinationPartitions = 5
			var routes [5][]int
			for id := range count {
				r := streamFixtureRecord(id, stream.stamp)
				partition := id % 5
				if r.Key != nil {
					partition = stream.keyRouting.Partition(r, 5)
				}
				routes[partition] = append(routes[partition], id)
				stream.receipts.submit(id, r)
			}
			// When each destination partition is exhausted before reading the next.
			peak := 0
			for partition, ids := range routes {
				for _, id := range ids {
					if missing && id == 1 {
						continue
					}
					r := streamFixtureRecord(id, stream.stamp)
					r.Partition = int32(partition)
					stream.verify(t, r)
					peak = max(peak, len(stream.receipts.pending))
				}
			}
			// Then legal skew reconciles, while a missing record and late duplicate remain visible.
			require.Greater(t, peak, 8192)
			result := stream.delivery(true)
			require.Zero(t, result.Corrupted)
			require.Zero(t, result.Reordered)
			if missing {
				require.Equal(t, 1, *result.Missing)
				require.Equal(t, count-1, result.Verified)
				require.False(t, *result.Integrity.Matched)
			} else {
				require.Zero(t, *result.Missing)
				require.Equal(t, count, result.Verified)
				require.True(t, *result.Integrity.Matched)
				require.Empty(t, stream.receipts.pending)
			}
			stream.verify(t, streamFixtureRecord(0, stream.stamp))
			require.Equal(t, 1, stream.duplicates)
		})
	}
}
