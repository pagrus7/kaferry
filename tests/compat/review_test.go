//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	adapter "github.com/pagrus7/kaferry/internal/franz"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCopyDestinationAcceptableRecordAboveProducerDefault(t *testing.T) {
	// Given a source record above the default producer limit but within Kafka's topic limit.
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	p := producer(t, kgo.ProducerBatchMaxBytes(2000000))
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	payload := bytes.Repeat([]byte{0, 255}, 505000)
	headers := []kgo.RecordHeader{{Key: "duplicate", Value: nil}, {Key: "duplicate", Value: []byte{}}}
	publish(t, p, &kgo.Record{Topic: src, Partition: 0, Key: []byte("large"), Value: payload, Headers: headers, Timestamp: stamp})
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s-large', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", dst, src, src, stamp.Add(time.Second).Format(time.RFC3339Nano))

	// When copying with no explicit producer size setting.
	execute(t, statement, configuration(t))

	// Then the whole logical record reaches the destination unchanged.
	c := consumer(t)
	assign(t, c, dst, 1)
	got := read(t, c)
	if !bytes.Equal(got.Value, payload) || string(got.Key) != "large" || !got.Timestamp.Equal(stamp) || !reflect.DeepEqual(got.Headers, headers) {
		t.Fatal("large record was rejected or changed")
	}
}

func TestLegacyTimestampLimitSkipsOnlyRejectedRecord(t *testing.T) {
	// Given Kafka 3.9 directional defaults and only the legacy acceptance limit set.
	a := admin(t)
	dst := topic(t, a, 1)
	alterTopicConfig(t, a, dst, map[string]string{"message.timestamp.difference.max.ms": "3600000"})
	path := filepath.Join(t.TempDir(), "legacy.avro")
	stamp := time.Now().Truncate(time.Millisecond)
	var records []core.Record
	for i, age := range []time.Duration{0, -24 * time.Hour, 0} {
		ts := stamp.Add(age).UnixMilli()
		records = append(records, core.Record{Key: []byte("same"), Value: []byte(fmt.Sprint(i)), TimestampMS: &ts, TimestampType: "create_time"})
	}
	writeArchiveRecords(t, path, records)
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM FILE avro('%s') WITH (GROUP='%s-legacy') OPTIONS (ON_REJECTED_TIMESTAMP=SKIP)", dst, path, dst)

	// When restoring mixed valid and rejected timestamps into a producer batch.
	execute(t, statement, configuration(t))

	// Then only the too-old record is skipped and both innocent records are delivered.
	reader := consumer(t)
	assign(t, reader, dst, 1)
	for _, want := range []string{"0", "2"} {
		got := read(t, reader)
		if string(got.Value) != want || !got.Timestamp.Equal(stamp) {
			t.Fatal("legacy preflight lost an innocent record", got)
		}
	}
}

type probeOwnership struct {
	owned   map[int32]bool
	revoked int32
}

func (p *probeOwnership) Assign(assignments []core.Assignment) error {
	for _, assignment := range assignments {
		p.owned[assignment.Cursor.Partition] = true
	}
	return nil
}
func (p *probeOwnership) Revoke(ids []int32, _ bool) error {
	for _, id := range ids {
		delete(p.owned, id)
		p.revoked = id
	}
	return nil
}

func probeSource(t *testing.T, name, group string) *adapter.Source {
	t.Helper()
	source, _ := probeSourceProgress(t, name, group)
	return source
}
func probeSourceProgress(t *testing.T, name, group string) (*adapter.Source, core.ProgressStore) {
	t.Helper()
	statement, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO TOPIC 'unused' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", name, group))
	require.NoError(t, err)
	p, err := plan.NewPlanner(nil).Build(statement, plan.Profiles{"default": {"bootstrap.servers": broker(t)}})
	require.NoError(t, err)
	clients := adapter.NewClients()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	lookups := kafkaport.NewOffsetLookupFactory()
	factory := adapter.NewFactory(commitmeta.NewCodec(), clients, kafkaport.NewPositionerFactory(core.RealClock{}, logger, lookups), core.RealClock{}, logger, lookups)
	connection, err := factory.Open(context.Background(), p.Source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	if _, err = connection.ClusterID(context.Background()); err != nil {
		t.Fatal(err)
	}
	input, store, err := connection.Source(context.Background(), p, "pause-probe", config.Defaults(t.TempDir()).Worker, nil)
	require.NoError(t, err)
	source := input.(*adapter.Source)
	t.Cleanup(func() { _ = source.Close() })
	return source, store
}

func pollOwnership(t *testing.T, sources []*adapter.Source, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !ready() && time.Now().Before(deadline) {
		for _, source := range sources {
			if _, err := source.Poll(20 * time.Millisecond); err != nil {
				t.Fatal("ownership probe poll", err)
			}
		}
	}
	if !ready() {
		t.Fatal("ownership probe did not converge")
	}
}

func TestCooperativeReassignmentResumesOnlyNewPartitions(t *testing.T) {
	// Given worker A with both partitions paused before worker B joins.
	a := admin(t)
	name := topic(t, a, 2)
	group := name + "-pause"
	first := probeSource(t, name, group)
	ownershipA := &probeOwnership{owned: make(map[int32]bool), revoked: -1}
	require.NoError(t, first.Start(ownershipA, nil))
	pollOwnership(t, []*adapter.Source{first}, func() bool { return len(ownershipA.owned) == 2 })
	for id := range ownershipA.owned {
		require.NoError(t, first.Pause(id, true))
	}
	second := probeSource(t, name, group)
	ownershipB := &probeOwnership{owned: make(map[int32]bool)}
	require.NoError(t, second.Start(ownershipB, nil))
	pollOwnership(t, []*adapter.Source{first, second}, func() bool { return len(ownershipA.owned) == 1 && len(ownershipB.owned) == 1 })
	reassigned := ownershipA.revoked
	if reassigned < 0 {
		t.Fatal("no cooperative partition revocation observed")
	}

	// When B exits and A reacquires its previously application-paused partition.
	require.NoError(t, second.Close())
	pollOwnership(t, []*adapter.Source{first}, func() bool { return len(ownershipA.owned) == 2 })
	p := producer(t)
	for id := range 2 {
		publish(t, p, &kgo.Record{Topic: name, Partition: int32(id), Value: []byte("resume"), Timestamp: time.Now()})
	}

	// Then the reacquired partition fetches while the retained partition stays paused.
	deadline := time.Now().Add(5 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		event, err := first.Poll(20 * time.Millisecond)
		require.NoError(t, err)
		if event.Input != nil {
			require.Equal(t, reassigned, event.Input.Cursor.Partition, "resumed a retained paused partition")
			if event.Input.Cursor.Position != 0 || string(event.Input.Record.Value) != "resume" {
				t.Fatal("resume changed assigned position", event.Input)
			}
			seen = true
		}
		if seen && event.EOF != nil && event.EOF.Partition == reassigned {
			return
		}
	}
	t.Fatal("application pause survived reassignment or EOF was not restored", seen)
}
