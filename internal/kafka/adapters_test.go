package kafka

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	core "kafka-fwd2/internal/copy"
)

func TestRetentionWarningOnFirstRetainedRecord(t *testing.T) {
	for _, newer := range []bool{false, true} {
		var output bytes.Buffer
		s := &Source{retentionChecks: map[int32]retentionCheck{0: {low: 5, lower: 100}}, log: slog.New(slog.NewTextHandler(&output, nil))}
		stamp := int64(99)
		if newer {
			stamp = 101
		}
		s.checkRetention(0, &stamp)
		s.checkRetention(0, &stamp)
		want := 0
		if newer {
			want = 1
		}
		if got := strings.Count(output.String(), "time window may begin before retained data"); got != want {
			t.Fatalf("retention warnings=%d want=%d", got, want)
		}
	}
}

func TestEmptyAssignmentDoesNotCommitNativeOffsets(t *testing.T) {
	store := &Committer{}
	results, err := store.Save(context.Background(), nil)
	if err != nil || len(results) != 0 {
		t.Fatal("empty assignment needs no native commit", results, err)
	}
}

func TestOffsetsMatchTopicIdentityNotPointerAddress(t *testing.T) {
	requested, returned := "orders", "orders"
	result := k.ListOffsetsResult{ResultInfos: map[k.TopicPartition]k.ListOffsetsResultInfo{{Topic: &returned, Partition: 3}: {Offset: 42}}}
	info, ok := offsetInfo(result, requested, 3)
	if !ok || info.Offset != 42 {
		t.Fatal("returned topic pointer prevented matching Kafka partition")
	}
}

func TestInvalidProgressNeverBecomesAStartFallback(t *testing.T) {
	valid := Progress{Fingerprint: "fingerprint", TopicID: "topic"}.Encode()
	if _, err := decodeProgress(&valid, "fingerprint", "topic", false); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"", "foreign", valid + "{}", `kfwd2:1;{"fp":"fingerprint","tid":"topic","unknown":1}`, Progress{Fingerprint: "other", TopicID: "topic"}.Encode(), Progress{Fingerprint: "fingerprint", TopicID: "recreated"}.Encode(), Progress{Fingerprint: "fingerprint", TopicID: "topic", FileID: "file", Extent: -1}.Encode()} {
		if _, err := decodeProgress(&text, "fingerprint", "topic", false); err == nil {
			t.Fatalf("accepted invalid progress: %s", text)
		}
	}
	if _, err := decodeProgress(&valid, "fingerprint", "topic", true); err == nil {
		t.Fatal("relative query accepted missing window")
	}
}
func TestNativeQueueBoundsAndGoPropertyTypes(t *testing.T) {
	for _, key := range []string{"queue.buffering.max.messages", "queue.buffering.max.kbytes", "go.events.channel.size"} {
		for _, value := range []string{"0", "-1", "101", "oops"} {
			p := k.ConfigMap{key: value}
			if err := boundQueue(p, key, 100); err == nil {
				t.Fatal("accepted invalid queue bound", key, value)
			}
		}
		p := k.ConfigMap{key: "30"}
		if err := boundQueue(p, key, 100); err != nil {
			t.Fatal(err)
		}
		if key == "go.events.channel.size" {
			if _, ok := p[key].(int); !ok {
				t.Fatal("Go client requires an integer event capacity")
			}
		}
	}
}
func TestTimestampAndHeaderPreflightAreRecordAttributable(t *testing.T) {
	s := &Sink{clock: core.RealClock{}, configuration: map[string]string{"message.timestamp.type": "CreateTime", "message.timestamp.before.max.ms": "1000", "message.timestamp.after.max.ms": "1000"}}
	now := time.Now().UnixMilli()
	for _, stamp := range []int64{0, -1, math.MaxInt64, now - 10000, now + 10000} {
		if err := s.validate(core.Record{TimestampMS: &stamp}); err == nil {
			t.Fatal("unpreservable timestamp accepted", stamp)
		}
	}
	if err := s.validate(core.Record{TimestampMS: &now}); err != nil {
		t.Fatal(err)
	}
	if err := s.validate(core.Record{Headers: []core.Header{{Name: "nul\x00suffix"}}}); err == nil {
		t.Fatal("silently truncated header accepted")
	}
	s.configuration = map[string]string{"message.timestamp.type": "LogAppendTime"}
	old := int64(1000)
	if err := s.validate(core.Record{TimestampMS: &old}); err != nil {
		t.Fatal("CreateTime checks applied to LogAppendTime")
	}
	for _, stamp := range []int64{0, -1, math.MaxInt64} {
		if err := s.validate(core.Record{TimestampMS: &stamp}); err != nil {
			t.Fatal("restamped timestamp rejected before LogAppendTime destination", err)
		}
	}
}
func TestDeterministicErrorsNeverRetry(t *testing.T) {
	for _, code := range []k.ErrorCode{k.ErrAuthentication, k.ErrTopicAuthorizationFailed, k.ErrGroupAuthorizationFailed, k.ErrAutoOffsetReset, k.ErrMsgSizeTooLarge} {
		if transient(k.NewError(code, "failure", false)) {
			t.Fatal("retrying permanent error", code)
		}
	}
	if !transient(k.NewError(k.ErrTransport, "temporary", false)) {
		t.Fatal("transport error not classified")
	}
}
