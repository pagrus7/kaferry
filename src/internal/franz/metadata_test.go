package franz

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/internal/commitmeta"
	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/kafkaport"
	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, i.Refresh(t.Context()))
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
	// Given a caller deadline earlier than the inspector's request budget.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	entered := make(chan time.Time, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	m := &metadataMock{request: func(request context.Context, _ kmsg.Request) (kmsg.Response, error) {
		got, _ := request.Deadline()
		entered <- got
		select {
		case <-request.Done():
			return nil, request.Err()
		case <-release:
			return nil, context.Canceled
		}
	}}
	i := &Inspector{codec: commitmeta.NewCodec(), client: m, topic: "input", log: quietLogger()}
	result := make(chan error, 1)
	// When refreshing metadata against a transport that never answers.
	go func() { result <- i.Refresh(ctx) }()
	// Then the request inherits the caller's exact deadline and returns promptly.
	select {
	case got := <-entered:
		require.Equal(t, deadline, got)
	case <-time.After(time.Second):
		t.Fatal("metadata request did not start")
	}
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("caller deadline did not release metadata request")
	}
}

func TestOffsetLookupRejectsIncompleteAndFailedPartitions(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "foreign topic", "partition error", "request error"} {
		t.Run(mode, func(t *testing.T) {
			invalidOffsetLookup(t, mode)
		})
	}
}

func TestSavedProgressRequiresEveryPartitionAndExactMetadata(t *testing.T) {
	for _, mode := range []string{"valid", "absent", "missing result", "foreign metadata", "nil metadata", "partition error"} {
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
			case "nil metadata":
				parts[0] = kmsg.OffsetFetchResponseTopicPartition{Partition: 0, Offset: 42}
			case "partition error":
				p := parts[2]
				p.ErrorCode = kerr.TopicAuthorizationFailed.Code
				parts[2] = p
			}
			i := &Inspector{codec: commitmeta.NewCodec(), topicID: id, parts: []int32{0, 2}}
			// When decoding the assignment's saved starts, then only absent commits permit a fresh start.
			saved, err := i.decodeSaved(parts, "fp", plan.WindowSpec{}, 0)
			valid := mode == "valid" || mode == "absent"
			require.Equal(t, valid, (err == nil), "invalid saved-position decision")
			if mode == "nil metadata" {
				var conflict *kafkaport.Conflict
				require.ErrorAs(t, err, &conflict)
				require.Nil(t, saved)
			}
			if mode == "valid" && (saved[0].Position != 42 || encodeMetadata(saved[0].Progress) != metadata) {
				t.Fatal("progress changed", saved)
			}
		})
	}
}

func TestSavedWindowsNeverBecomeAStartFallback(t *testing.T) {
	for _, scenario := range []string{"valid", "fingerprint", "topic identity", "missing relative window", "stop beyond commit", "window shape", "literal mismatch", "staleness overflow", "offset beyond captured end", "captured end without upper bound"} {
		t.Run(scenario, func(t *testing.T) {
			// Given committed progress whose query, window or output may be incompatible.
			id := [16]byte{1}
			progress := kafkaport.Progress{Fingerprint: "fp", TopicID: topicID(id)}
			var spec plan.WindowSpec
			var staleness int64
			lower := int64(10)
			switch scenario {
			case "offset beyond captured end":
				progress.End = new(int64(41))
				spec.Upper = &plan.Expression{LiteralMS: 1000}
			case "captured end without upper bound":
				progress.End = new(int64(42))
			case "fingerprint":
				progress.Fingerprint = "other"
			case "topic identity":
				progress.TopicID = "other"
			case "stop beyond commit":
				progress.Stop = &core.Stop{Reason: "tip", At: 43}
			case "missing relative window":
				spec.Lower = &plan.Expression{Now: true}
			case "window shape":
				progress.Window = &plan.Window{Lower: &lower}
			case "literal mismatch":
				progress.Window = &plan.Window{Lower: &lower}
				spec.Lower = &plan.Expression{LiteralMS: lower + 1}
			case "staleness overflow":
				upper := int64(math.MaxInt64)
				progress.Window = &plan.Window{Upper: &upper}
				spec.Upper = &plan.Expression{LiteralMS: upper}
				staleness = 1
			}
			text := encodeMetadata(progress)
			inspector := &Inspector{codec: commitmeta.NewCodec(), topicID: id, parts: []int32{0}}
			// When startup or assignment validates saved progress through their common path.
			saved, err := inspector.decodeSaved(map[int32]kmsg.OffsetFetchResponseTopicPartition{0: {Offset: 42, Metadata: &text}}, "fp", spec, staleness)
			// Then only compatible progress can reach positioning.
			if scenario == "valid" || scenario == "captured end without upper bound" {
				require.NoError(t, err)
				require.Len(t, saved, 1)
				require.EqualValues(t, 42, saved[0].Position)

			} else if scenario == "fingerprint" {
				require.ErrorIs(t, err, plan.ErrFingerprintMismatch)
				require.Equal(t, int64(42), saved[0].Position)
				require.Equal(t, progress, saved[0].Progress)
			} else if err == nil || len(saved) > 0 {
				t.Fatal("invalid progress became usable", saved, err)
			}
		})
	}
}

func TestFingerprintConflictCannotHideUnsafeSourceProgress(t *testing.T) {
	for _, scenario := range []string{"mixed identities", "changed source", "corrupt metadata", "stop beyond position", "position beyond end", "matching query missing window"} {
		t.Run(scenario, func(t *testing.T) {
			// Given incompatible progress on one partition and another partition that must still be validated.
			id := [16]byte{1}
			first := encodeMetadata(kafkaport.Progress{Fingerprint: "old", TopicID: topicID(id)})
			last := kafkaport.Progress{Fingerprint: "current", TopicID: topicID(id)}
			spec := plan.WindowSpec{}
			switch scenario {
			case "changed source":
				last.TopicID = "another-source"
			case "stop beyond position":
				last.Stop = &core.Stop{Reason: "worker_record_cap", At: 8}
			case "position beyond end":
				last.End = new(int64(6))
			case "matching query missing window":
				spec.Lower = &plan.Expression{Now: true}
			}
			encoded := encodeMetadata(last)
			if scenario == "corrupt metadata" {
				encoded = "foreign"
			}
			parts := map[int32]kmsg.OffsetFetchResponseTopicPartition{0: {Offset: 3, Metadata: &first}, 2: {Offset: 7, Metadata: &encoded}}
			inspector := &Inspector{codec: commitmeta.NewCodec(), topicID: id, parts: []int32{0, 2}}
			// When validating all source partitions, not merely the first incompatible one.
			saved, err := inspector.decodeSaved(parts, "current", spec, 0)
			// Then only source-safe fingerprint conflicts expose diagnostic positions.
			require.Error(t, err)
			if scenario == "mixed identities" {
				require.ErrorIs(t, err, plan.ErrFingerprintMismatch)
				require.Len(t, saved, 2)
				require.EqualValues(t, 3, saved[0].Position)
				require.EqualValues(t, 7, saved[2].Position)
				require.Equal(t, "old", saved[0].Fingerprint)
				require.Equal(t, "current", saved[2].Fingerprint)
			} else {
				require.Nil(t, saved)
			}
		})
	}
}

func TestRoutingMetadataIsFreshCompleteAndDense(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ids     []int32
		failure error
		want    string
	}{
		{"sorted", []int32{1, 0}, nil, ""},
		{"no partitions", nil, nil, "no partitions"},
		{"gap", []int32{0, 2}, nil, "invalid partition IDs"},
		{"duplicate", []int32{0, 0}, nil, "invalid partition IDs"},
		{"unavailable", nil, errors.New("metadata unavailable"), "metadata unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given broker metadata that may be unavailable or malformed.
			response := metadataResponse([16]byte{1})
			response.Topics[0].Partitions = nil
			for _, id := range tc.ids {
				response.Topics[0].Partitions = append(response.Topics[0].Partitions, kmsg.MetadataResponseTopicPartition{Partition: id})
			}
			calls := 0
			client := &metadataMock{request: func(context.Context, kmsg.Request) (kmsg.Response, error) { calls++; return response, tc.failure }}
			c := &connection{inspector: &Inspector{client: client, topic: "input", log: quietLogger()}}
			// When retrieving routing IDs, then reject incomplete layouts and never use stale cached metadata.
			parts, err := c.Partitions(t.Context())
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				require.ErrorContains(t, c.ValidatePartitions(t.Context(), []int32{0}), tc.want)
			} else {
				require.NoError(t, err)
				require.Equal(t, []int32{0, 1}, parts)
				require.NoError(t, c.ValidatePartitions(t.Context(), []int32{1}))
				require.Equal(t, 2, calls)
			}
		})
	}
}

func TestDestinationLayoutAcceptsRecreationAndGrowth(t *testing.T) {
	// Given a destination whose replacement topic has more usable partitions.
	response := metadataResponse([16]byte{1})
	response.Topics[0].Partitions = []kmsg.MetadataResponseTopicPartition{{Partition: 0}, {Partition: 1}}
	client := &metadataMock{request: func(context.Context, kmsg.Request) (kmsg.Response, error) { return response, nil }}
	c := &connection{inspector: &Inspector{client: client, topic: "input", log: quietLogger()}}
	require.NoError(t, c.ValidatePartitions(t.Context(), []int32{1}))
	response.Topics[0].TopicID = [16]byte{2}
	response.Topics[0].Partitions = append(response.Topics[0].Partitions, kmsg.MetadataResponseTopicPartition{Partition: 2})
	// When rechecking assignments, then validate the current layout without applying source identity checks.
	require.NoError(t, c.ValidatePartitions(t.Context(), []int32{1, 2}))
	require.Equal(t, [16]byte{2}, c.inspector.topicID)
	require.ErrorContains(t, c.ValidatePartitions(t.Context(), []int32{3}), "original partition 3")
}

func invalidOffsetLookup(t *testing.T, mode string) {
	// Given batched timestamp queries with a match and a no-match result.
	failure := errors.New("transport failed")
	m := &metadataMock{request: func(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
		req := r.(*kmsg.ListOffsetsRequest)
		require.EqualValues(t, 1, req.IsolationLevel)
		require.Len(t, req.Topics[0].Partitions, 2)

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
		require.NoError(t, err)
		require.Equal(t, map[int32]int64{0: 42, 2: -1}, result)

	} else if err == nil {
		t.Fatal("invalid response accepted")
	}
}
