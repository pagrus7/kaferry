package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
)

func TestRetentionWarningOnFirstRetainedRecord(t *testing.T) {
	for _, newer := range []bool{false, true} {
		var output bytes.Buffer
		s := &Source{retentionChecks: map[int32]RetentionCheck{0: {Low: 5, Lower: 100}}, log: slog.New(slog.NewTextHandler(&output, nil))}
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
	info, ok := partitionInfos(result, requested)[3]
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

type scriptedPoller struct{ events []k.Event }

func (p *scriptedPoller) Poll(int) k.Event {
	if len(p.events) == 0 {
		return nil
	}
	event := p.events[0]
	p.events = p.events[1:]
	return event
}

// A quiet partition gives no recovery signal after a reconnect, so transient
// outages must not fail the worker however long the quiet lasts.
func TestTransientSourceErrorsHaveNoDeadline(t *testing.T) {
	quiet := 30000 // Ten minutes of 20 ms polls, well past the 60 s delivery timeout.
	events := []k.Event{k.NewError(k.ErrTransport, "disconnected", false), k.NewError(k.ErrAllBrokersDown, "down", false)}
	events = append(events, make([]k.Event, quiet)...)
	s := &Source{events: &scriptedPoller{events: events}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for n := 0; n < quiet+2; n++ {
		if event, err := s.Poll(20 * time.Millisecond); err != nil || event.Input != nil || event.EOF != nil {
			t.Fatalf("poll %d: event=%+v err=%v", n, event, err)
		}
	}
	for _, terminal := range []k.Error{k.NewError(k.ErrTopicAuthorizationFailed, "denied", false), k.NewError(k.ErrTransport, "fatal", true)} {
		s.events = &scriptedPoller{events: []k.Event{terminal}}
		if _, err := s.Poll(20 * time.Millisecond); err == nil {
			t.Fatalf("%v must fail the worker", terminal)
		}
	}
}

func TestCommitClassification(t *testing.T) {
	var recoverable *core.Recoverable
	for _, code := range []k.ErrorCode{k.ErrRebalanceInProgress, k.ErrIllegalGeneration, k.ErrUnknownMemberID, k.ErrCoordinatorNotAvailable, k.ErrNotCoordinator, k.ErrCoordinatorLoadInProgress, k.ErrRequestTimedOut, k.ErrTransport, k.ErrTimedOut, k.ErrWaitCoord, k.ErrAllBrokersDown} {
		if err := classifyCommit(k.NewError(code, "", false)); !errors.As(err, &recoverable) {
			t.Fatalf("%v should be recoverable on revocation", code)
		}
	}
	for _, err := range []error{
		k.NewError(k.ErrGroupAuthorizationFailed, "", false), k.NewError(k.ErrTopicAuthorizationFailed, "", false),
		k.NewError(k.ErrOffsetMetadataTooLarge, "", false), k.NewError(k.ErrFencedInstanceID, "", false),
		k.NewError(k.ErrTransport, "", true), k.NewError(k.ErrUnknown, "", false), errors.New("not a client error"),
	} {
		if classified := classifyCommit(err); errors.As(classified, &recoverable) {
			t.Fatalf("%v must stay terminal", err)
		}
	}
	if classifyCommit(nil) != nil {
		t.Fatal("success must stay nil")
	}
}

type capturedCommits struct{ parts []k.TopicPartition }

func (c *capturedCommits) CommitOffsets(parts []k.TopicPartition) ([]k.TopicPartition, error) {
	c.parts = append(c.parts, parts...)
	return parts, nil
}

type durableExtent struct{ extent int64 }

func (d *durableExtent) DurableOutput() (string, int64) { return "archive-id", d.extent }

func TestKafkaFileCommitUsesDurableExtent(t *testing.T) {
	native := &capturedCommits{}
	output := &durableExtent{extent: 256}
	store := &Committer{consumer: native, topic: "orders", output: output, metadata: map[int32]Progress{0: {Fingerprint: "fp", TopicID: "topic-id"}}}
	for _, extent := range []int64{256, 1024, 1024} {
		// Failed I/O cannot move DurableOutput; a later commit keeps the last E.
		output.extent = extent
		results, err := store.Save(context.Background(), []core.Cursor{{Partition: 0, Position: 5}})
		if err != nil || results[0] != nil {
			t.Fatalf("commit: results=%v err=%v", results, err)
		}
		part := native.parts[len(native.parts)-1]
		progress, err := decodeProgress(part.Metadata, "fp", "topic-id", false)
		if err != nil || progress.FileID != "archive-id" || progress.Extent != extent || part.Offset != 5 {
			t.Fatalf("durable output identity/extent changed: part=%v progress=%+v err=%v", part, progress, err)
		}
	}
}

type countingOffsets struct {
	calls     []string
	low, high int64
	stable    int64
}

func (o *countingOffsets) Offsets(partitions []int32, spec k.OffsetSpec, readCommitted bool) (map[int32]int64, error) {
	o.calls = append(o.calls, fmt.Sprint(spec, readCommitted))
	value := o.high
	switch {
	case spec == k.EarliestOffsetSpec:
		value = o.low
	case readCommitted:
		value = o.stable
	}
	out := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		out[partition] = value
	}
	return out, nil
}

type countingTimes struct {
	calls int
	topic string
}

func (c *countingTimes) OffsetsForTimes(parts []k.TopicPartition, _ int) ([]k.TopicPartition, error) {
	c.calls++
	out := make([]k.TopicPartition, len(parts))
	for n, part := range parts {
		out[n] = k.TopicPartition{Topic: &c.topic, Partition: part.Partition, Offset: 500}
		if part.Partition%3 == 0 {
			out[n].Offset = k.OffsetEnd // No match: start at the read-committed end.
		}
	}
	return out, nil
}

func TestPositioningBatchesBrokerRequests(t *testing.T) {
	topic := "orders"
	hour := int64(3600000)
	spec := plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -hour}}
	clock := &fixedClock{now: time.UnixMilli(10 * hour)}
	offsets := &countingOffsets{low: 100, high: 1000, stable: 900}
	times := &countingTimes{topic: topic}
	positioner := NewPositionerFactory(clock, quietLogger()).New(offsets, times, plan.Plan{Source: plan.Endpoint{Name: topic}, Window: spec}, "fp", "topic-id")
	savedLower := 5 * hour
	saved := map[int32]k.TopicPartition{}
	metadata := map[int32]Progress{}
	parts := make([]k.TopicPartition, 200)
	for n := range parts {
		id := int32(n)
		parts[n] = k.TopicPartition{Topic: &topic, Partition: id}
		if n >= 150 {
			stored := int64(50) // Expired: below the log start.
			if n >= 175 {
				stored = 700
			}
			saved[id] = k.TopicPartition{Topic: &topic, Partition: id, Offset: k.Offset(stored)}
			metadata[id] = Progress{Window: &plan.Window{Lower: &savedLower}}
		}
	}
	positioned, err := positioner.Locate(parts, saved, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets.calls) != 3 || times.calls != 1 {
		t.Fatalf("broker requests: offsets=%v timestamp lookups=%d", offsets.calls, times.calls)
	}
	for _, a := range positioned.Assignments {
		id := a.Cursor.Partition
		want := int64(500)
		switch {
		case id >= 175:
			want = 700
		case id >= 150:
			want = 100
		case id%3 == 0:
			want = 900
		}
		if a.Cursor.Position != want {
			t.Fatalf("partition %d positioned at %d, want %d", id, a.Cursor.Position, want)
		}
		if *a.Window.Lower != savedLower {
			t.Fatalf("partition %d window %d did not adopt the lowest saved window", id, *a.Window.Lower)
		}
	}
}

func TestEmptyAssignmentNeedsNoOffsetRequests(t *testing.T) {
	offsets := &countingOffsets{}
	s := &Source{offsets: offsets}
	assignments, err := s.locate(nil, nil, nil)
	if err != nil || len(assignments) != 0 || len(offsets.calls) != 0 {
		t.Fatalf("empty assignment: assignments=%v requests=%v err=%v", assignments, offsets.calls, err)
	}
}

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time    { return c.now }
func (*fixedClock) Sleep(time.Duration) {}
