//go:build integration && franzspike

package franzspike

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	native "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	core "github.com/pagrus7/kaferry/internal/copy"
	archive "github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestSpikeArchiveRoundTrip(t *testing.T) {
	// Given Kafka records with full raw headers and negative/zero timestamps.
	src, dst := topic(t, 1), topic(t, 1)
	p := client(t)
	want := []core.Record{
		{Key: nil, Value: nil, Headers: []core.Header{{Name: "a\x00b", Value: []byte{}}, {Name: string([]byte{255}), Value: nil}}, TimestampMS: int64ptr(0), TimestampType: "create_time"},
		{Key: []byte{}, Value: []byte{}, Headers: []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}, TimestampMS: int64ptr(-2), TimestampType: "create_time"},
	}
	for _, r := range want {
		var h []kgo.RecordHeader
		for _, v := range r.Headers {
			h = append(h, kgo.RecordHeader{Key: v.Name, Value: v.Value})
		}
		if err := p.ProduceSync(ctx(t), &kgo.Record{Topic: src, Key: r.Key, Value: r.Value, Headers: h, Timestamp: time.UnixMilli(*r.TimestampMS)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	c := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{src: {0: kgo.NewOffset().AtStart()}}))
	codec, err := archive.NewCodec(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	// When archiving Kafka records into the actual framed CBOR file format.
	header, err := codec.EncodeHeader(archive.Header{Version: 1, ID: "spike-file", Group: "spike", Fingerprint: "spike"})
	if err != nil {
		t.Fatal(err)
	}
	data := header
	for _, r := range readN(t, c, len(want)) {
		ms := r.Timestamp.UnixMilli()
		record := core.Record{Key: r.Key, Value: r.Value, TimestampMS: &ms, TimestampType: "create_time"}
		for _, h := range r.Headers {
			record.Headers = append(record.Headers, core.Header{Name: h.Key, Value: h.Value})
		}
		encoded, err := codec.EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, encoded...)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "archive.cbor")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	checkpoints, err := archive.NewCheckpoints(directory, "spike", archive.DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoints.Close()
	source, err := archive.NewSource(path, "spike", plan.WindowSpec{}, codec, checkpoints, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.Start(ignoreLifecycle{}); err != nil {
		t.Fatal(err)
	}
	for range want {
		event, err := source.Poll(0)
		if err != nil {
			t.Fatal(err)
		}
		if event.Input == nil {
			t.Fatal("archive ended early")
		}
		decoded := event.Input.Record
		var h []kgo.RecordHeader
		for _, v := range decoded.Headers {
			h = append(h, kgo.RecordHeader{Key: v.Name, Value: v.Value})
		}
		if err := p.ProduceSync(ctx(t), &kgo.Record{Topic: dst, Key: decoded.Key, Value: decoded.Value, Headers: h, Timestamp: time.UnixMilli(*decoded.TimestampMS)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	restored := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{dst: {0: kgo.NewOffset().AtStart()}}))
	// Then topic→CBOR record→topic is byte faithful.
	for i, r := range readN(t, restored, len(want)) {
		var h []core.Header
		for _, v := range r.Headers {
			h = append(h, core.Header{Name: v.Key, Value: v.Value})
		}
		if !reflect.DeepEqual(r.Key, want[i].Key) || !reflect.DeepEqual(r.Value, want[i].Value) || !reflect.DeepEqual(h, want[i].Headers) || r.Timestamp.UnixMilli() != *want[i].TimestampMS {
			t.Fatal("archive changed record", i, r)
		}
	}
}
func int64ptr(n int64) *int64 { return &n }

func TestSpikeNativeIdentityAndPartitionParity(t *testing.T) {
	// Given one topic reached by the current client and franz-go.
	src := topic(t, 7)
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:39092"
	}
	a, err := native.NewAdminClient(&native.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	description, err := a.DescribeTopics(ctx(t), native.NewTopicCollectionOfTopicNames([]string{src}))
	if err != nil {
		t.Fatal(err)
	}
	nc, err := a.ClusterID(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	k := client(t)
	metadata, err := kadm.NewClient(k).Metadata(ctx(t), src)
	if err != nil {
		t.Fatal(err)
	}
	id := metadata.Topics[src].ID
	canonical := base64.RawStdEncoding.EncodeToString(id[:])
	// When comparing identifiers used by saved fingerprints and metadata.
	t.Logf("native_topic_id=%s kadm_topic_id=%s canonical=%s", description.TopicDescriptions[0].TopicID.String(), id.String(), canonical)
	// Then cluster IDs match, and topic IDs use an explicit legacy-compatible encoder.
	if metadata.Cluster != nc || canonical != description.TopicDescriptions[0].TopicID.String() {
		t.Fatal("identity mismatch", metadata.Cluster, nc, canonical)
	}
	np, err := native.NewProducer(&native.ConfigMap{"bootstrap.servers": brokers, "partitioner": "murmur2_random"})
	if err != nil {
		t.Fatal(err)
	}
	defer np.Close()
	keys := [][]byte{[]byte{}, []byte("a"), []byte("b"), []byte("kaferry"), {0, 255, 128}}
	fp := client(t, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	for _, key := range keys {
		reports := make(chan native.Event, 1)
		if err := np.Produce(&native.Message{TopicPartition: native.TopicPartition{Topic: &src, Partition: native.PartitionAny}, Key: key, Value: []byte("native")}, reports); err != nil {
			t.Fatal(err)
		}
		var part int32
		select {
		case e := <-reports:
			r := e.(*native.Message)
			if r.TopicPartition.Error != nil {
				t.Fatal(r.TopicPartition.Error)
			}
			part = r.TopicPartition.Partition
		case <-ctx(t).Done():
			t.Fatal("native delivery timeout")
		}
		r := &kgo.Record{Topic: src, Key: key, Value: []byte("franz")}
		if err := fp.ProduceSync(ctx(t), r).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if r.Partition != part {
			t.Fatalf("routing differs for %x: %d vs %d", key, r.Partition, part)
		}
	}
}

func TestSpikeStaleMemberCommitIsFenced(t *testing.T) {
	// Given a joined member, then its explicit departure from the group.
	src := topic(t, 1)
	group := src + "-group"
	p := client(t)
	if err := p.ProduceSync(ctx(t), &kgo.Record{Topic: src, Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	c := client(t, kgo.ConsumerGroup(group), kgo.ConsumeTopics(src), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NoResetOffset().AtStart()), kgo.Balancers(kgo.CooperativeStickyBalancer()))
	readN(t, c, 1)
	member, generation := c.GroupMetadata()
	if member == "" {
		t.Fatal("member did not join")
	}
	if err := c.LeaveGroupContext(ctx(t)); err != nil {
		t.Fatal(err)
	}
	// When the old member attempts an offset commit with its obsolete generation.
	request := kmsg.NewPtrOffsetCommitRequest()
	request.Group = group
	request.MemberID = member
	request.Generation = generation
	topicRequest := kmsg.NewOffsetCommitRequestTopic()
	topicRequest.Topic = src
	part := kmsg.NewOffsetCommitRequestTopicPartition()
	part.Partition = 0
	part.Offset = 1
	topicRequest.Partitions = []kmsg.OffsetCommitRequestTopicPartition{part}
	request.Topics = []kmsg.OffsetCommitRequestTopic{topicRequest}
	response, err := request.RequestWith(ctx(t), p)
	if err != nil {
		t.Fatal(err)
	}
	// Then the broker rejects the stale ownership rather than advancing progress.
	failure := kerr.ErrorForCode(response.Topics[0].Partitions[0].ErrorCode)
	if failure != kerr.IllegalGeneration && failure != kerr.UnknownMemberID {
		t.Fatal("stale member was not fenced", failure)
	}
	t.Log(fmt.Sprintf("stale_generation=%d rejection=%v", generation, failure))
}

type ignoreLifecycle struct{}

func (ignoreLifecycle) Assign([]core.Assignment) error { return nil }
func (ignoreLifecycle) Revoke([]int32, bool) error     { return nil }
