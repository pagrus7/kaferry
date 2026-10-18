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

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	adapter "github.com/pagrus7/kaferry/internal/franz"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
)

func TestCopyDestinationAcceptableRecordAboveProducerDefault(t *testing.T) {
	// Given a source record above the default producer limit but within Kafka's topic limit.
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	p := producer(t, k.ConfigMap{"message.max.bytes": 2000000})
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	payload := bytes.Repeat([]byte{0, 255}, 505000)
	headers := []k.Header{{Key: "duplicate", Value: nil}, {Key: "duplicate", Value: []byte{}}}
	publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Key: []byte("large"), Value: payload, Headers: headers, Timestamp: stamp})
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s-large',CUTOFF=INTERVAL '0' SECOND)", dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), src)

	// When copying with no explicit producer size setting.
	execute(t, statement, configuration(t))

	// Then the whole logical record reaches the destination unchanged.
	c := consumer(t, dst+"-verify")
	if err := c.Assign([]k.TopicPartition{{Topic: &dst, Partition: 0, Offset: 0}}); err != nil {
		t.Fatal(err)
	}
	got := read(t, c)
	if !bytes.Equal(got.Value, payload) || string(got.Key) != "large" || !got.Timestamp.Equal(stamp) || !reflect.DeepEqual(got.Headers, headers) {
		t.Fatal("large record was rejected or changed")
	}
}

func TestLegacyTimestampLimitSkipsOnlyRejectedRecord(t *testing.T) {
	// Given Kafka 3.9 directional defaults and only the legacy acceptance limit set.
	a := admin(t)
	dst := topic(t, a, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := a.IncrementalAlterConfigs(ctx, []k.ConfigResource{{Type: k.ResourceTopic, Name: dst, Config: []k.ConfigEntry{{Name: "message.timestamp.difference.max.ms", Value: "3600000", IncrementalOperation: k.AlterConfigOpTypeSet}}}})
	if err != nil || len(results) != 1 || results[0].Error.Code() != k.ErrNoError {
		t.Fatal("configure legacy timestamp limit", results, err)
	}
	waitTopicConfig(t, a, dst, "message.timestamp.difference.max.ms", "3600000")
	path := filepath.Join(t.TempDir(), "legacy.cbor")
	c, err := file.NewCodec(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	out, err := file.NewOutput(file.NewPublisher(), path, "fixture", "fixture", "", 0, c, 10, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().Truncate(time.Millisecond)
	for i, age := range []time.Duration{0, -24 * time.Hour, 0} {
		ts := stamp.Add(age).UnixMilli()
		if err := out.Submit(core.Record{Key: []byte("same"), Value: []byte(fmt.Sprint(i)), TimestampMS: &ts, TimestampType: "create_time"}, core.Token{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := out.Flush(); err != nil {
		t.Fatal(err)
	}
	out.Close()
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='%s-legacy',ON_REJECTED_TIMESTAMP=SKIP)", dst, path, dst)

	// When restoring mixed valid and rejected timestamps into a producer batch.
	execute(t, statement, configuration(t))

	// Then only the too-old record is skipped and both innocent records are delivered.
	reader := consumer(t, dst+"-verify")
	if err := reader.Assign([]k.TopicPartition{{Topic: &dst, Partition: 0, Offset: 0}}); err != nil {
		t.Fatal(err)
	}
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
func probeSourceProgress(t *testing.T, name, group string) (*adapter.Source, kafkaport.SourceProgress) {
	t.Helper()
	statement, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO TOPIC 'unused' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", name, group))
	if err != nil {
		t.Fatal(err)
	}
	p, err := plan.NewPlanner(nil).Build(statement, plan.Profiles{"default": {"bootstrap.servers": broker(t)}})
	if err != nil {
		t.Fatal(err)
	}
	clients := adapter.NewClients()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	factory := adapter.NewFactory(commitmeta.NewCodec(), clients, kafkaport.NewPositionerFactory(core.RealClock{}, logger), core.RealClock{}, logger)
	connection, err := factory.Open(context.Background(), p.Source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	if _, err = connection.ClusterID(context.Background()); err != nil {
		t.Fatal(err)
	}
	input, store, err := connection.Source(context.Background(), p, "pause-probe", config.Defaults(t.TempDir()).Worker)
	if err != nil {
		t.Fatal(err)
	}
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
	if err := first.Start(ownershipA, nil); err != nil {
		t.Fatal(err)
	}
	pollOwnership(t, []*adapter.Source{first}, func() bool { return len(ownershipA.owned) == 2 })
	for id := range ownershipA.owned {
		if err := first.Pause(id, true); err != nil {
			t.Fatal(err)
		}
	}
	second := probeSource(t, name, group)
	ownershipB := &probeOwnership{owned: make(map[int32]bool)}
	if err := second.Start(ownershipB, nil); err != nil {
		t.Fatal(err)
	}
	pollOwnership(t, []*adapter.Source{first, second}, func() bool { return len(ownershipA.owned) == 1 && len(ownershipB.owned) == 1 })
	reassigned := ownershipA.revoked
	if reassigned < 0 {
		t.Fatal("no cooperative partition revocation observed")
	}

	// When B exits and A reacquires its previously application-paused partition.
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	pollOwnership(t, []*adapter.Source{first}, func() bool { return len(ownershipA.owned) == 2 })
	p := producer(t, nil)
	for id := range 2 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: int32(id)}, Value: []byte("resume"), Timestamp: time.Now()})
	}

	// Then the reacquired partition fetches while the retained partition stays paused.
	deadline := time.Now().Add(5 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		event, err := first.Poll(20 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if event.Input != nil {
			if event.Input.Cursor.Partition != reassigned {
				t.Fatal("resumed a retained paused partition", event.Input.Cursor)
			}
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
