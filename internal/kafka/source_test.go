package kafka

import (
	"bytes"
	"context"
	"errors"
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	core "github.com/pagrus7/kaferry/internal/copy"
	"io"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

type watermarkOffsets struct {
	result    map[int32]int64
	err       error
	requested []int32
	spec      k.OffsetSpec
	committed bool
}

func TestEmptySourceAssignmentNeedsNoBrokerQueries(t *testing.T) {
	// Given a worker assigned no partitions, with no usable broker ports.
	source := &Source{}
	// When processing its empty assignment.
	assignments, err := source.position(nil)
	// Then no positioning or progress lookup is needed.
	if err != nil || len(assignments) != 0 {
		t.Fatal(assignments, err)
	}
}

func (m *watermarkOffsets) Offsets(ids []int32, spec k.OffsetSpec, committed bool) (map[int32]int64, error) {
	m.requested, m.spec, m.committed = ids, spec, committed
	return m.result, m.err
}

func TestWatermarkErrorsAndDNSRespectRuntimeRetryClassification(t *testing.T) {
	for _, scenario := range []struct {
		name                    string
		code                    k.ErrorCode
		fatal, retry, pollRetry bool
	}{
		{"timeout", k.ErrTimedOut, false, true, true}, {"DNS", k.ErrResolve, false, true, true},
		{"fatal DNS", k.ErrResolve, true, false, false}, {"authorization", k.ErrTopicAuthorizationFailed, false, false, false},
		{"not leader", k.ErrNotLeaderForPartition, false, true, false},
		{"leader unavailable", k.ErrLeaderNotAvailable, false, true, false},
		{"replica unavailable", k.ErrReplicaNotAvailable, false, true, false},
		{"storage", k.ErrKafkaStorageError, false, true, false},
		{"offset unavailable", k.ErrOffsetNotAvailable, false, true, false},
		{"fenced epoch", k.ErrFencedLeaderEpoch, false, true, false},
		{"unknown epoch", k.ErrUnknownLeaderEpoch, false, true, false},
		{"unknown topic", k.ErrUnknownTopicOrPart, false, false, false},
		{"fatal leader", k.ErrNotLeaderForPartition, true, false, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given an independently mocked watermark query failure.
			native := k.NewError(scenario.code, "query failure", scenario.fatal)
			source := &Source{offsets: &watermarkOffsets{err: native}}

			// When sampling a partition's high watermark.
			_, err := source.HighWatermark(0)

			// Then only nonfatal transient failures are marked recoverable.
			var recoverable *core.Recoverable
			if errors.As(err, &recoverable) != scenario.retry || transient(native) != scenario.pollRetry {
				t.Fatal("wrong classification", err)
			}
			if !errors.Is(err, native) {
				t.Fatal("native cause lost", err)
			}
		})
	}
	// Given an uncommitted log end distinct from the consumer's readable tip.
	offsets := &watermarkOffsets{result: map[int32]int64{3: math.MaxInt32}}
	source := &Source{offsets: offsets}

	// When reading the high watermark.
	high, err := source.HighWatermark(3)

	// Then its exact offset is returned without a retry marker.
	if err != nil || high != math.MaxInt32 || !reflect.DeepEqual(offsets.requested, []int32{3}) || offsets.spec != k.LatestOffsetSpec || offsets.committed {
		t.Fatal(high, err)
	}
}

func TestWatermarkRequiresValidResultAndRetriesAdminDeadline(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result map[int32]int64
		err    error
		retry  bool
	}{
		{"deadline", nil, context.DeadlineExceeded, true},
		{"cancelled", nil, context.Canceled, false},
		{"missing", map[int32]int64{1: 42}, nil, false},
		{"negative", map[int32]int64{0: -1}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an expired admin request or an unusable offset response.
			source := &Source{offsets: &watermarkOffsets{result: tc.result, err: tc.err}}
			// When sampling the log end.
			_, err := source.HighWatermark(0)
			// Then only a timeout is retried; invalid results cannot complete input.
			var recoverable *core.Recoverable
			if err == nil || errors.As(err, &recoverable) != tc.retry || tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatal("invalid watermark classification", err)
			}
		})
	}
}

func TestRetentionWarningOnFirstRetainedRecord(t *testing.T) {
	// Given
	for _, newer := range []bool{false, true} {
		var output bytes.Buffer
		s := &Source{retentionChecks: map[int32]RetentionCheck{0: {Low: 5, Lower: 100}}, log: slog.New(slog.NewTextHandler(&output, nil))}
		stamp := int64(99)
		if newer {
			stamp = 101
		}

		// When
		s.checkRetention(0, &stamp)
		s.checkRetention(0, &stamp)
		want := 0
		if newer {
			want = 1
		}

		// Then
		if got := strings.Count(output.String(), "time window may begin before retained data"); got != want {
			t.Fatalf("retention warnings=%d want=%d", got, want)
		}
	}
}

func TestDeterministicErrorsNeverRetry(t *testing.T) {
	// Given deterministic authentication, authorization, position and size errors.
	for _, code := range []k.ErrorCode{k.ErrAuthentication, k.ErrTopicAuthorizationFailed, k.ErrGroupAuthorizationFailed, k.ErrAutoOffsetReset, k.ErrMsgSizeTooLarge} {

		// When classifying a source error for runtime retry.
		retry := transient(k.NewError(code, "failure", false))

		// Then permanent failures are never retried.
		if retry {
			t.Fatal("retrying permanent error", code)
		}
	}
	if !transient(k.NewError(k.ErrTransport, "temporary", false)) {
		t.Fatal("transport error not classified")
	}
}

// A quiet partition gives no recovery signal after a reconnect, so transient
// outages must not fail the worker however long the quiet lasts.
func TestTransientSourceErrorsHaveNoDeadline(t *testing.T) {
	// Given
	quiet := 30000 // Ten minutes of 20 ms polls, well past the 60 s delivery timeout.
	events := []k.Event{k.NewError(k.ErrTransport, "disconnected", false), k.NewError(k.ErrAllBrokersDown, "down", false)}
	events = append(events, make([]k.Event, quiet)...)
	s := &Source{events: &scriptedPoller{events: events}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for n := 0; n < quiet+2; n++ {

		// When
		pollEvent, pollErr := s.Poll(20 * time.Millisecond)

		// Then
		if pollErr != nil || pollEvent.Input != nil || pollEvent.EOF != nil {
			t.Fatalf("poll %d: event=%+v err=%v", n, pollEvent, pollErr)
		}
	}
	for _, terminal := range []k.Error{k.NewError(k.ErrTopicAuthorizationFailed, "denied", false), k.NewError(k.ErrTransport, "fatal", true)} {
		s.events = &scriptedPoller{events: []k.Event{terminal}}
		if _, err := s.Poll(20 * time.Millisecond); err == nil {
			t.Fatalf("%v must fail the worker", terminal)
		}
	}
}

func TestSourceCommitsInitialIdentityBeforeAssigningAndStopsOnFailure(t *testing.T) {
	// Given
	failure := errors.New("failure")
	for _, fail := range []string{"", "progress", "position", "commit", "assign", "resume"} {
		t.Run("failure-"+fail, func(t *testing.T) {
			var actions []string
			consumer := &consumerMock{actions: &actions}
			metadata := &sourceMetadataMock{}
			positioning := &positioningMock{result: Positioning{Assignments: []core.Assignment{{Cursor: core.Cursor{Partition: 0, Position: 7}}}, Progress: map[int32]Progress{0: {Fingerprint: "fp", TopicID: "topic-id"}}}}
			switch fail {
			case "progress":
				metadata.err = failure
			case "position":
				positioning.err = failure
			case "commit":
				consumer.commitErr = failure
			case "assign":
				consumer.assignErr = failure
			case "resume":
				consumer.resumeErr = failure
			}
			source, clients := sourceWithMocks(t, consumer, metadata, positioning)
			if clients.cfg["enable.auto.commit"] != false || clients.cfg["enable.auto.offset.store"] != false || clients.cfg["isolation.level"] != "read_committed" {
				t.Fatal("unsafe consumer configuration", clients.cfg)
			}
			if err := source.Start(&lifecycleMock{actions: &actions}); err != nil {
				t.Fatal(err)
			}

			// When
			err := consumer.callback(nil, k.AssignedPartitions{Partitions: []k.TopicPartition{{Topic: &source.plan.Source.Name, Partition: 0}}})

			// Then
			if fail != "" && !errors.Is(err, failure) {
				t.Fatal("failure lost", err)
			}
			if fail == "" {
				if err != nil || !reflect.DeepEqual(actions, []string{"subscribe", "commit", "assign", "resume", "assigned"}) {
					t.Fatal("ownership acquired before initial progress", actions, err)
				}
				if len(consumer.resumed) != 1 || consumer.resumed[0].Partition != 0 {
					t.Fatal("assignment did not clear only the newly acquired pause", consumer.resumed)
				}
				saved, err := decodeProgress(consumer.commits[0].Metadata, "fp", "topic-id", false)
				if err != nil || saved.TopicID != "topic-id" || consumer.commits[0].Offset != 7 {
					t.Fatal("initial identity not committed", saved, err)
				}
			} else {
				if strings.Contains(strings.Join(actions, ","), "assigned") {
					t.Fatal("failed startup notified ownership", actions)
				}
				if _, err := source.Poll(0); !errors.Is(err, failure) {
					t.Fatal("failed startup resumed polling", err)
				}
			}
		})
	}
}

func TestSourceReleasesLostOwnershipThroughInjectedConsumer(t *testing.T) {
	// Given
	actions := []string{}
	consumer := &consumerMock{actions: &actions, lost: true}
	source, _ := sourceWithMocks(t, consumer, &sourceMetadataMock{}, nil)
	lifecycle := &lifecycleMock{actions: &actions}
	if err := source.Start(lifecycle); err != nil {
		t.Fatal(err)
	}

	// When
	callbackErr := consumer.callback(nil, k.RevokedPartitions{Partitions: []k.TopicPartition{{Partition: 2}}})

	// Then
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}
	if !lifecycle.lost || !reflect.DeepEqual(actions, []string{"subscribe", "revoked", "unassign"}) || len(consumer.commits) != 0 {
		t.Fatal("lost ownership was committed or retained", actions)
	}
}

func TestSourceAppliesPositioningAndRequiresEveryInitialCommitResult(t *testing.T) {
	// Given
	failure := errors.New("commit failure")
	for _, stage := range []string{"success", "request-failure", "partition-failure", "missing-result"} {
		t.Run(stage, func(t *testing.T) {
			store := &assignmentStoreMock{metadata: make(map[int32]Progress), results: map[int32]error{0: nil}}
			switch stage {
			case "request-failure":
				store.err = failure
			case "partition-failure":
				store.results[0] = failure
			case "missing-result":
				delete(store.results, 0)
			}
			progress := Progress{Fingerprint: "fp", TopicID: "topic-id"}
			check := RetentionCheck{Low: 7, Lower: 100}
			cursor := core.Cursor{Partition: 0, Position: 7}
			positioning := &positioningMock{result: Positioning{
				Assignments: []core.Assignment{{Cursor: cursor}},
				Progress:    map[int32]Progress{0: progress},
				Retention:   map[int32]RetentionCheck{0: check},
			}}
			source := &Source{consumer: &consumerMock{}, inspector: &sourceMetadataMock{}, positioning: positioning, committer: store, retentionChecks: make(map[int32]RetentionCheck)}
			parts := []k.TopicPartition{{Partition: 0, Offset: k.OffsetInvalid}}

			// When
			assigned, err := source.position(parts)

			// Then
			if !reflect.DeepEqual(store.cursors, []core.Cursor{cursor}) || !reflect.DeepEqual(store.metadata[0], progress) || parts[0].Offset != 7 || source.retentionChecks[0] != check {
				t.Fatal("resolved position, metadata or retention check lost")
			}
			if stage == "success" {
				if err != nil || len(assigned) != 1 || assigned[0].Cursor != cursor {
					t.Fatal("confirmed assignment missing", assigned, err)
				}
			} else if err == nil || assigned != nil || stage != "missing-result" && !errors.Is(err, failure) {
				t.Fatal("unconfirmed assignment returned", assigned, err)
			}
		})
	}
}

func TestSourceDecodesFullRecordWithoutNativeClient(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind k.TimestampType
		want string
	}{
		{"broker timestamp", k.TimestampLogAppendTime, "log_append_time"},
		{"producer timestamp", k.TimestampCreateTime, "create_time"},
		{"unavailable timestamp", k.TimestampNotAvailable, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a Kafka record with nulls, duplicate headers and a timestamp type.
			topic := "orders"
			message := &k.Message{TopicPartition: k.TopicPartition{Topic: &topic, Partition: 3, Offset: 42}, Key: nil, Value: []byte{}, Timestamp: time.UnixMilli(12345), TimestampType: tc.kind, Headers: []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}}
			source, _ := sourceWithMocks(t, &consumerMock{events: []k.Event{message}}, &sourceMetadataMock{}, nil)

			// When polling through the source interface.
			event, err := source.Poll(0)

			// Then all logical fields and the next offset are preserved.
			if err != nil || event.Input == nil {
				t.Fatal("record missing", err)
			}
			record := event.Input.Record
			if record.Key != nil || record.Value == nil || record.TimestampType != tc.want || !reflect.DeepEqual(record.Headers, []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}) || record.Source.Topic != topic || record.Source.Partition != 3 || record.Source.Offset != 42 || event.Input.Next != 43 {
				t.Fatal("logical record changed", record)
			}
			if tc.kind == k.TimestampNotAvailable {
				if record.TimestampMS != nil {
					t.Fatal("absent timestamp invented", record.TimestampMS)
				}
			} else if record.TimestampMS == nil || *record.TimestampMS != 12345 {
				t.Fatal("timestamp changed", record.TimestampMS)
			}
		})
	}
}
