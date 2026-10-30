package franz

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaRecordSizeMatchesActualWireEncoding(t *testing.T) {
	for _, record := range []core.Record{{}, {Key: []byte{}, Value: []byte{}}, {Key: bytes.Repeat([]byte{1}, 128), Value: bytes.Repeat([]byte{2}, 16384)}, {Headers: []core.Header{{Name: "a\x00b", Value: nil}, {Name: "a\x00b", Value: []byte{}}}}, {Value: make([]byte, 930), Source: &core.Provenance{Topic: "not serialized into destination"}}} {
		// Given a logical record with nil/empty or variable-length fields.
		r := kmsg.Record{Key: record.Key, Value: record.Value}
		for _, h := range record.Headers {
			r.Headers = append(r.Headers, kmsg.Header{Key: h.Name, Value: h.Value})
		}
		r.Length = int32(len(r.AppendTo(nil)) - 1)
		batch := kmsg.RecordBatch{Magic: 2, NumRecords: 1, Records: r.AppendTo(nil)}
		// When measuring without copying payload bytes, then the result equals the protocol encoder.
		if got, want := kafkaRecordBytes(record), int64(len(batch.AppendTo(nil))); got != want {
			t.Fatal("wire size differs", got, want)
		}
	}
}

func TestProducerBoundaryRefusesOversizeBeforeAsyncSubmission(t *testing.T) {
	// Given the actual production sink with a 1000-byte client batch ceiling.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
	defer cluster.Close()
	p := testPlan(t, map[string]string{"bootstrap.servers": strings.Join(cluster.ListenAddrs(), ","), "message.max.bytes": "1000"})
	factory := componentFactory()
	connection, err := factory.Open(t.Context(), p.Destination)
	require.NoError(t, err)
	defer connection.Close(context.Background())
	if _, err = connection.ClusterID(t.Context()); err != nil {
		t.Fatal(err)
	}
	sink, err := connection.Sink(t.Context(), config.Defaults("").Worker)
	require.NoError(t, err)
	defer sink.Close()
	for _, tc := range []struct {
		length   int
		rejected bool
	}{{926, false}, {927, true}, {930, true}} {
		record := core.Record{Value: make([]byte, tc.length)}
		// When a record touches the ceiling, including the enclosing bytes prefix.
		err := sink.Submit(record, core.Token{})
		// Then any oversize result is synchronously skippable; accepted records deliver successfully.
		if tc.rejected {
			var rejected *core.RecordError
			require.ErrorAs(t, err, &rejected)
			require.Equal(t, "validation", rejected.Class)
		} else {
			require.NoError(t, err)
			select {
			case result := <-sink.Deliveries():
				require.NoError(t, result.Err, "preflight allowed an async size failure")
			case <-time.After(3 * time.Second):
				t.Fatal("boundary record did not deliver")
			}
		}
	}
}
