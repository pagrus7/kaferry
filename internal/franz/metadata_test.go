package franz

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type metadataMock struct {
	request func(context.Context, kmsg.Request) (kmsg.Response, error)
	closed  bool
}

func (m *metadataMock) Request(ctx context.Context, r kmsg.Request) (kmsg.Response, error) {
	return m.request(ctx, r)
}
func (m *metadataMock) RequestTo(ctx context.Context, _ int32, r kmsg.Request) (kmsg.Response, error) {
	return m.Request(ctx, r)
}
func (m *metadataMock) Close() { m.closed = true }
func metadataResponse(id [16]byte) *kmsg.MetadataResponse {
	name, cluster := "input", "cluster"
	return &kmsg.MetadataResponse{ClusterID: &cluster, Topics: []kmsg.MetadataResponseTopic{{Topic: &name, TopicID: id, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0}, {Partition: 2}}}}}
}

func TestMetadataRefreshChecksIdentityAndSharesPartitionChanges(t *testing.T) {
	// Given an inspector with no cached identity and a broker returning two partitions.
	id := [16]byte{1}
	response := metadataResponse(id)
	m := &metadataMock{request: func(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
		if req.(*kmsg.MetadataRequest).AllowAutoTopicCreation {
			t.Fatal("metadata permits topic creation")
		}
		return response, nil
	}}
	i := &Inspector{codec: commitmeta.NewCodec(), client: m, topic: "input", log: quietLogger()}
	if err := i.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	// When the broker adds a partition, then the inspector updates its shared state.
	response.Topics[0].Partitions = append(response.Topics[0].Partitions, kmsg.MetadataResponseTopicPartition{Partition: 3})
	if err := i.Refresh(t.Context()); err != nil || !reflect.DeepEqual(i.parts, []int32{0, 2, 3}) {
		t.Fatal("partition change lost", i.parts, err)
	}
	// When the topic is replaced under the same name, then the old progress identity cannot be reused.
	response.Topics[0].TopicID = [16]byte{2}
	if err := i.Refresh(t.Context()); err == nil || i.topicID != id {
		t.Fatal("recreated topic accepted", err)
	}
}

func TestMetadataRequestsRespectCallerDeadline(t *testing.T) {
	// Given a broker transport that never answers.
	m := &metadataMock{request: func(ctx context.Context, _ kmsg.Request) (kmsg.Response, error) { <-ctx.Done(); return nil, ctx.Err() }}
	i := &Inspector{codec: commitmeta.NewCodec(), client: m, topic: "input", log: quietLogger()}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	// When its caller expires before the fixed request budget, then the request returns that deadline.
	if err := i.Refresh(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("caller deadline ignored", err)
	}
}

func TestOffsetLookupRejectsIncompleteAndFailedPartitions(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "foreign topic", "partition error", "request error"} {
		t.Run(mode, func(t *testing.T) {
			// Given batched timestamp queries with a match and a no-match result.
			failure := errors.New("transport failed")
			m := &metadataMock{request: func(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
				req := r.(*kmsg.ListOffsetsRequest)
				if req.IsolationLevel != 1 || len(req.Topics[0].Partitions) != 2 {
					t.Fatal("lookup not batched/read-committed")
				}
				p := []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, Offset: 42}, {Partition: 2, Offset: -1}}
				if mode == "request error" {
					return nil, failure
				}
				if mode == "missing" {
					p = p[:1]
				}
				if mode == "partition error" {
					p[1].ErrorCode = kerr.TopicAuthorizationFailed.Code
				}
				topic := "input"
				if mode == "foreign topic" {
					topic = "other"
				}
				return &kmsg.ListOffsetsResponse{Topics: []kmsg.ListOffsetsResponseTopic{{Topic: topic, Partitions: p}}}, nil
			}}
			i := &Inspector{codec: commitmeta.NewCodec(), client: m, topic: "input"}
			// When looking up both starts, then failures never become offset zero.
			result, err := i.OffsetsForTimes(t.Context(), map[int32]int64{0: 100, 2: 100})
			if mode == "valid" {
				if err != nil || !reflect.DeepEqual(result, map[int32]int64{0: 42, 2: -1}) {
					t.Fatal(result, err)
				}
			} else if err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}

func TestSavedProgressRequiresEveryPartitionAndExactMetadata(t *testing.T) {
	for _, mode := range []string{"valid", "absent", "missing result", "foreign metadata", "partition error"} {
		t.Run(mode, func(t *testing.T) {
			// Given current source partitions and progress encoded with the shared released format.
			id := [16]byte{1}
			metadata := encodeMetadata(kafkaport.Progress{Fingerprint: "fp", TopicID: topicID(id)})
			parts := map[int32]kmsg.OffsetFetchResponseTopicPartition{0: {Partition: 0, Offset: 42, Metadata: &metadata}, 2: {Partition: 2, Offset: -1}}
			switch mode {
			case "absent":
				parts[0] = kmsg.OffsetFetchResponseTopicPartition{Offset: -1}
			case "missing result":
				delete(parts, 2)
			case "foreign metadata":
				metadata = "foreign"
			case "partition error":
				p := parts[2]
				p.ErrorCode = kerr.TopicAuthorizationFailed.Code
				parts[2] = p
			}
			i := &Inspector{codec: commitmeta.NewCodec(), topicID: id, parts: []int32{0, 2}}
			// When decoding the assignment's saved starts, then only absent commits permit a fresh start.
			positions, progress, err := i.decodeSaved(parts, "fp", plan.WindowSpec{}, 0)
			valid := mode == "valid" || mode == "absent"
			if (err == nil) != valid {
				t.Fatal("invalid saved-position decision", err)
			}
			if mode == "valid" && (positions[0] != 42 || encodeMetadata(progress[0]) != metadata) {
				t.Fatal("progress changed", positions, progress)
			}
		})
	}
}

func TestSavedWindowsAndFileExtentsNeverBecomeAStartFallback(t *testing.T) {
	for _, scenario := range []string{"valid", "fingerprint", "topic identity", "file extent", "missing relative window", "stop beyond commit", "window shape", "literal mismatch", "cutoff overflow"} {
		t.Run(scenario, func(t *testing.T) {
			// Given committed progress whose query, window or output may be incompatible.
			id := [16]byte{1}
			progress := kafkaport.Progress{Fingerprint: "fp", TopicID: topicID(id)}
			var spec plan.WindowSpec
			var cutoff int64
			lower := int64(10)
			switch scenario {
			case "fingerprint":
				progress.Fingerprint = "other"
			case "topic identity":
				progress.TopicID = "other"
			case "file extent":
				progress.FileID, progress.Extent = "archive", 1
			case "stop beyond commit":
				progress.Stop = &core.Stop{Reason: "tip", At: 43}
			case "missing relative window":
				spec.Lower = &plan.Expression{Now: true}
			case "window shape":
				progress.Window = &plan.Window{Lower: &lower}
			case "literal mismatch":
				progress.Window = &plan.Window{Lower: &lower}
				spec.Lower = &plan.Expression{LiteralMS: lower + 1}
			case "cutoff overflow":
				upper := int64(math.MaxInt64)
				progress.Window = &plan.Window{Upper: &upper}
				spec.Upper = &plan.Expression{LiteralMS: upper}
				cutoff = 1
			}
			text := encodeMetadata(progress)
			if scenario == "file extent" {
				text = strings.Replace(text, `"extent":1`, `"extent":-1`, 1)
			}
			inspector := &Inspector{codec: commitmeta.NewCodec(), topicID: id, parts: []int32{0}}
			// When startup or assignment validates saved progress through their common path.
			positions, metadata, err := inspector.decodeSaved(map[int32]kmsg.OffsetFetchResponseTopicPartition{0: {Offset: 42, Metadata: &text}}, "fp", spec, cutoff)
			// Then only compatible progress can reach positioning.
			if scenario == "valid" {
				if err != nil || positions[0] != 42 || len(metadata) != 1 {
					t.Fatal(positions, metadata, err)
				}
			} else if err == nil || len(positions) > 0 || len(metadata) > 0 {
				t.Fatal("invalid progress became usable", positions, metadata, err)
			}
		})
	}
}
