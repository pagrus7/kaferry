package inspect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archiveio"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time    { return c.now }
func (fixedClock) Sleep(time.Duration) {}
func number(n int64) *int64            { return &n }

type topicMock struct {
	afterSaved      func()
	memberships     []kafkaport.Membership
	observations    []string
	membershipReads int
	info            kafkaport.TopicDescription
	positions       map[int32]int64
	metadata        map[int32]kafkaport.Progress
	low, high       map[int32]int64
	stable          map[int32]int64
	times           func(map[int32]int64) map[int32]int64
	fail            string
	failure         error
	closed          int
	groupReads      int
	memberChecks    int
	queries         []map[int32]int64
}

func (m *topicMock) err(stage string) error {
	if m.fail == stage {
		return m.failure
	}
	return nil
}
func (m *topicMock) Describe(context.Context, bool) (kafkaport.TopicDescription, error) {
	m.observations = append(m.observations, "describe")
	info := m.info
	info.Partitions = slices.Clone(info.Partitions)
	return info, m.err("describe")
}
func (m *topicMock) CheckMembers(context.Context, string) error {
	m.memberChecks++
	return m.err("members")
}
func (m *topicMock) Membership(context.Context, string) (kafkaport.Membership, error) {
	m.observations = append(m.observations, "members")
	observation := kafkaport.Membership{Idle: true}
	if len(m.memberships) > 0 {
		observation = m.memberships[min(m.membershipReads, len(m.memberships)-1)]
	}
	m.membershipReads++
	return observation, m.err("members")
}
func (m *topicMock) Saved(context.Context, string, string, plan.WindowSpec, int64) (map[int32]int64, map[int32]kafkaport.Progress, error) {
	m.groupReads++
	m.observations = append(m.observations, "saved")
	if m.afterSaved != nil {
		m.afterSaved()
	}
	return m.positions, m.metadata, m.err("saved")
}
func (m *topicMock) Offsets(_ context.Context, _ []int32, kind kafkaport.OffsetKind, committed bool) (map[int32]int64, error) {
	if err := m.err("offsets"); err != nil {
		return nil, err
	}
	if kind == kafkaport.EarliestOffset {
		return m.low, nil
	}
	if committed {
		return m.stable, nil
	}
	return m.high, nil
}
func (m *topicMock) OffsetsForTimes(_ context.Context, queries map[int32]int64) (map[int32]int64, error) {
	m.queries = append(m.queries, queries)
	return m.times(queries), m.err("times")
}
func (m *topicMock) Close(context.Context) error { m.closed++; return m.err("close") }

type factoryMock struct {
	connections map[string]*topicMock
	fail        string
	failure     error
	opened      int
}

func (f *factoryMock) OpenInspection(_ context.Context, endpoint plan.Endpoint) (kafkaport.InspectionConnection, error) {
	f.opened++
	if f.fail == endpoint.Name {
		return nil, f.failure
	}
	return f.connections[endpoint.Name], nil
}

type filesMock struct {
	source      archiveio.SourceInspection
	destination archiveio.ArchiveInspection
	failSource  error
	failOutput  error
}

func (f *filesMock) Source(plan.Plan, string, config.Worker) (archiveio.SourceInspection, error) {
	return f.source, f.failSource
}
func (f *filesMock) Destination(string) (archiveio.ArchiveInspection, error) {
	return f.destination, f.failOutput
}

func fixture() (*Service, plan.Plan, *factoryMock, *filesMock) {
	p := plan.Plan{Source: plan.Endpoint{Kind: "TOPIC", Name: "input"}, Destination: plan.Endpoint{Kind: "TOPIC", Name: "output", Partitioning: plan.Key}, Group: "g", Policies: plan.Policies{MissingTimestamp: "FAIL", RejectedTimestamp: "FAIL", OtherRecord: "FAIL"}, Window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: 100}, Upper: &plan.Expression{LiteralMS: 1000}}, CutoffMS: 20}
	topic := func(name string) *topicMock {
		return &topicMock{info: kafkaport.TopicDescription{ClusterID: "cluster", TopicID: name, Partitions: []int32{1, 0}, TimestampType: "CreateTime"}, positions: map[int32]int64{}, metadata: map[int32]kafkaport.Progress{}, low: map[int32]int64{0: 0, 1: 0}, high: map[int32]int64{0: 10, 1: 11}, stable: map[int32]int64{0: 9, 1: 10}, failure: errors.New("injected"), times: func(queries map[int32]int64) map[int32]int64 {
			found := make(map[int32]int64)
			for id := range queries {
				found[id] = int64(id) + 2
			}
			return found
		}}
	}
	factory := &factoryMock{connections: map[string]*topicMock{"input": topic("input-id"), "output": topic("output-id")}, failure: errors.New("open failed")}
	files := &filesMock{}
	positioning := kafkaport.NewPositionerFactory(fixedClock{time.UnixMilli(5000)}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return NewExplainer(factory, files, positioning), p, factory, files
}

func preview(t *testing.T, service *Service, p plan.Plan) Report {
	t.Helper()
	r, err := service.Explain(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
	require.NoError(t, err)
	return r
}

func TestTopicPreviewUsesSavedPositionsAndOnlyRequiredStartLookups(t *testing.T) {
	// Given two partitions, one saved position, and different destination routing.
	service, p, factory, _ := fixture()
	source := factory.connections["input"]
	source.positions[0], source.metadata[0] = 4, kafkaport.Progress{}
	factory.connections["output"].info.Partitions = []int32{0, 1, 2}
	factory.connections["output"].info.TimestampType = "LogAppendTime"
	// When previewing through the real shared positioner with mocked metadata ports.
	report := preview(t, service, p)
	// Then starts, bounds and error policies are explicit without estimating work or changing progress.
	require.EqualValues(t, 1, report.Version)
	require.NotEmpty(t, report.Fingerprint)
	require.Equal(t, "g", report.Group)
	require.Len(t, report.Partitions, 2)
	window := plan.Window{Lower: number(100), Upper: number(1000)}
	want := []Partition{
		{ID: 0, Start: 4, Saved: true, LogEnd: 10, Window: window, WindowOrigin: "literal"},
		{ID: 1, Start: 3, LogEnd: 11, Window: window, WindowOrigin: "literal"},
	}
	require.Equal(t, want, report.Partitions)
	require.Len(t, source.queries, 1)
	require.Len(t, source.queries[0], 1)
	require.EqualValues(t, 100, source.queries[0][1])
	require.EqualValues(t, 1, source.memberChecks)
	require.EqualValues(t, 1, source.groupReads)
	require.Equal(t, map[int32]int64{0: 4}, source.positions)
	require.EqualValues(t, 1, source.closed)
	require.EqualValues(t, 1, factory.connections["output"].closed)
	require.Contains(t, strings.Join(report.Warnings, " "), "LogAppendTime")
	require.Len(t, report.Warnings, 1)
}

func TestLatestAndUnmatchedTimestampStartAtCommittedEnd(t *testing.T) {
	for _, start := range []string{"LATEST", "unmatched time"} {
		t.Run(start, func(t *testing.T) {
			// Given a read-uncommitted end ahead of the stable end.
			service, p, factory, _ := fixture()
			p.Window, p.Start = plan.WindowSpec{}, "LATEST"
			if start == "unmatched time" {
				p.Start, p.Window.Lower = "", &plan.Expression{LiteralMS: 100}
				factory.connections["input"].times = func(map[int32]int64) map[int32]int64 { return map[int32]int64{0: -1, 1: -1} }
			}
			// When inspecting an unbounded statement.
			report := preview(t, service, p)
			// Then the run starts at the stable end and no stopping lookup is invented.
			require.EqualValues(t, 9, report.Partitions[0].Start)
			require.EqualValues(t, 10, report.Partitions[0].LogEnd)
		})
	}
}

func TestRelativeWindowsPreserveSavedAndAdoptedBounds(t *testing.T) {
	// Given saved NOW windows that differ and a newly added partition.
	service, p, factory, _ := fixture()
	p.Window = plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -1000}, Upper: &plan.Expression{Now: true}}
	source := factory.connections["input"]
	source.info.Partitions = []int32{2, 1, 0}
	source.low[2], source.high[2] = 0, 12
	source.positions = map[int32]int64{0: 4, 1: 5}
	source.metadata = map[int32]kafkaport.Progress{0: {Window: &plan.Window{Lower: number(100), Upper: number(1000)}}, 1: {Window: &plan.Window{Lower: number(200), Upper: number(2000)}}}
	// When inspecting, then old partitions keep their windows and the new one adopts the lowest partition's window.
	report := preview(t, service, p)
	require.EqualValues(t, 2000, *report.Partitions[1].Window.Upper)
	require.Equal(t, "saved", report.Partitions[1].WindowOrigin)
	require.EqualValues(t, 1000, *report.Partitions[2].Window.Upper)
	require.Equal(t, "adopted", report.Partitions[2].WindowOrigin)
	// Given fresh progress instead, when inspecting, then NOW is preview-only.
	service, _, _, _ = fixture()
	report = preview(t, service, p)
	require.EqualValues(t, 5000, *report.Partitions[0].Window.Upper)
	require.Equal(t, "resolved", report.Partitions[0].WindowOrigin)
}

func TestOneOffDoesNotReadOrGenerateAConsumerGroup(t *testing.T) {
	// Given an explicitly one-off statement.
	service, p, factory, _ := fixture()
	p.OneOff = true
	// When inspecting, then no group is generated or queried.
	report := preview(t, service, p)
	require.Equal(t, "ONE_OFF", report.Group)
	require.True(t, report.OneOff)
	require.Zero(t, factory.connections["input"].groupReads)
	require.Zero(t, factory.connections["input"].memberChecks)
}

func TestArchiveInspectionCarriesSnapshotAndReplayPosition(t *testing.T) {
	// Given a saved archive selection and its destination directory.
	service, p, factory, files := fixture()
	p.Source.Kind, p.Destination.Kind = "FILE", "FILE"
	p.Source.Format, p.Destination.Format = "avro", "avro"
	files.source = archiveio.SourceInspection{ArchiveInspection: archiveio.ArchiveInspection{Exists: true, Size: 250, FileCount: 2}, Position: 200, Saved: true, CheckpointPath: "checkpoint.json"}
	files.destination = archiveio.ArchiveInspection{Exists: true}
	// When inspecting both endpoints.
	report := preview(t, service, p)
	// Then positions count records, sizes describe the selection, and Kafka is untouched.
	require.Zero(t, factory.opened)
	require.EqualValues(t, 200, *report.Source.File.PositionRecords)
	require.Equal(t, 2, report.Source.File.FileCount)
	require.EqualValues(t, 250, report.Source.File.SizeBytes)
	require.True(t, *report.Source.File.CheckpointExists)
	require.True(t, report.Destination.File.Exists)
}

func TestInspectionFailuresCloseOnlyAcquiredConnections(t *testing.T) {
	for _, failure := range []string{"source open", "source describe", "destination open", "destination describe", "source refresh", "members", "saved", "offsets", "times", "file output", "close"} {
		t.Run(failure, func(t *testing.T) {
			// Given a failure at one acquisition, positioning or cleanup boundary.
			service, p, factory, files := fixture()
			source, destination := factory.connections["input"], factory.connections["output"]
			wantSource, wantDestination := 1, 1
			injected := source.failure
			switch failure {
			case "source open":
				factory.fail, factory.failure, wantSource, wantDestination = "input", injected, 0, 0
			case "source describe":
				source.fail, wantDestination = "describe", 0
			case "destination open":
				factory.fail, factory.failure, wantDestination = "output", injected, 0
			case "destination describe":
				destination.fail, destination.failure = "describe", injected
			case "source refresh":
				source.afterSaved = func() { source.fail = "describe" }
			case "file output":
				p.Destination.Kind, files.failOutput, wantDestination = "FILE", injected, 0
			default:
				source.fail = failure
			}
			// When inspecting, then preserve the failure and close each acquired connection once.
			_, err := service.Explain(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
			require.ErrorIs(t, err, injected)
			require.Equal(t, wantSource, source.closed)
			require.Equal(t, wantDestination, destination.closed)
		})
	}
}

func TestInvalidSavedPositionsAndWindowOverflowFailBeforeLookup(t *testing.T) {
	for _, invalid := range []string{"past log end", "overflow", "missing lookup"} {
		t.Run(invalid, func(t *testing.T) {
			// Given invalid progress or an incomplete metadata response.
			service, p, factory, _ := fixture()
			source := factory.connections["input"]
			switch invalid {
			case "past log end":
				source.positions[0], source.metadata[0] = 100, kafkaport.Progress{}
			case "overflow":
				p.Window.Upper.LiteralMS = math.MaxInt64
			case "missing lookup":
				source.times = func(map[int32]int64) map[int32]int64 { return map[int32]int64{} }
			}
			// When inspecting, then reject the invalid evidence and release both connections.
			_, err := service.Explain(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
			require.Error(t, err)
			require.EqualValues(t, 1, source.closed)
			require.EqualValues(t, 1, factory.connections["output"].closed)
		})
	}
}

func TestUpperBoundOnlyDoesNotRequestTimestampLookups(t *testing.T) {
	// Given an upper-bound-only filter with a pre-epoch stopping threshold.
	service, p, factory, _ := fixture()
	p.Window = plan.WindowSpec{Upper: &plan.Expression{LiteralMS: -21}}
	p.CutoffMS = 20
	// When previewing, then start at the earliest offset without looking up an end timestamp.
	report := preview(t, service, p)
	require.Len(t, factory.connections["input"].queries, 0)
	require.Zero(t, report.Partitions[0].Start)
}

func TestExpiredPositionStartsAtLogStartWithOneWarning(t *testing.T) {
	// Given saved progress that predates retention.
	service, p, factory, _ := fixture()
	p.Window, p.Start = plan.WindowSpec{}, "EARLIEST"
	source := factory.connections["input"]
	source.low[0], source.positions[0], source.metadata[0] = 3, 1, kafkaport.Progress{}
	// When previewing, then show the actual resumed start and the skipped offset range.
	report := preview(t, service, p)
	require.EqualValues(t, 3, report.Partitions[0].Start)
	require.Contains(t, strings.Join(report.Warnings, " "), "saved offset 1 expired")
}

func TestCancelledInspectionDoesNotAcquireConnections(t *testing.T) {
	// Given a cancelled request.
	service, p, factory, _ := fixture()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// When inspecting, then return cancellation before acquiring resources.
	_, err := service.Explain(ctx, p, config.Worker{}, core.NoopStopObserver{})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, factory.opened)
}

func TestPreviewReportsNewPartitionFallbackAndEffectiveRecordPolicies(t *testing.T) {
	// Given START=LATEST, existing group progress and a missing timestamp SKIP policy.
	service, p, factory, _ := fixture()
	p.Window, p.Start = plan.WindowSpec{}, "LATEST"
	p.Policies.MissingTimestamp = "SKIP"
	source := factory.connections["input"]
	source.positions[0], source.metadata[0] = 4, kafkaport.Progress{}
	// When inspecting, then new partitions start earliest and the actual policy is exposed.
	report := preview(t, service, p)
	require.Equal(t, "EARLIEST", report.StartRule)
	require.Zero(t, report.Partitions[1].Start)
	require.Equal(t, (ErrorPolicies{MissingTimestamp: "SKIP", RejectedTimestamp: "FAIL", OtherRecord: "FAIL"}), report.RecordErrors)
}

func TestFilePreviewPreservesFreshAndSavedRelativeWindows(t *testing.T) {
	for _, saved := range []bool{false, true} {
		t.Run(fmt.Sprint(saved), func(t *testing.T) {
			// Given a file inspector's resolved NOW bounds, record position and checkpoint state.
			service, p, factory, files := fixture()
			p.Source.Kind = "FILE"
			p.Window = plan.WindowSpec{Upper: &plan.Expression{Now: true}}
			upper, origin := int64(5000), "resolved"
			if saved {
				upper, origin = 1000, "saved"
			}
			files.source = archiveio.SourceInspection{ArchiveInspection: archiveio.ArchiveInspection{Exists: true, Size: 300}, Position: 200, Saved: saved, Window: plan.Window{Upper: &upper}, CheckpointPath: "checkpoint.json"}
			// When composing the preview, then retain the inspector's bounds and describe their origin accurately.
			report := preview(t, service, p)
			info := report.Source.File
			require.Equal(t, origin, info.WindowOrigin)
			require.Equal(t, files.source.Window, *info.Window)
			require.EqualValues(t, 200, *info.PositionRecords)
			require.Equal(t, saved, *info.CheckpointExists)
			require.Equal(t, "checkpoint.json", info.Checkpoint)
			require.EqualValues(t, 1, factory.opened)
			require.EqualValues(t, 1, factory.connections["output"].closed)
		})
	}
}

func TestFreshTimeWindowWarnsWhenEarlierOffsetsAreNotRetained(t *testing.T) {
	// Given no saved progress and a time lookup that selects a positive log start.
	service, p, factory, _ := fixture()
	factory.connections["input"].low[0] = 2
	// When inspecting, then report the selected offset and possible missing history, without claiming proven loss.
	report := preview(t, service, p)
	require.EqualValues(t, 2, report.Partitions[0].Start)
	require.False(t, report.Partitions[0].Saved)
	require.Len(t, report.Warnings, 1)
	require.Contains(t, report.Warnings[0], "start of the requested window may be unavailable")
}

func TestExplainChecksEverySourcePartitionForPreservedRouting(t *testing.T) {
	// Given a time-filtered topic with partition 1 absent from the destination.
	service, p, factory, _ := fixture()
	p.Destination.Partitioning = plan.Preserve
	factory.connections["output"].info.Partitions = []int32{0}
	// When explaining, then reject missing IDs even though the filter could exclude their records.
	_, err := service.Explain(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
	require.ErrorContains(t, err, "original partition 1")
	require.EqualValues(t, 1, factory.connections["input"].closed)
	require.EqualValues(t, 1, factory.connections["output"].closed)
}

func TestExplainUsesArchiveProvenanceAndReportsDestinationRouting(t *testing.T) {
	// Given a fixed archive selection originally captured from partition 5.
	service, p, factory, files := fixture()
	p.Source.Kind = "FILE"
	p.Source.Format = "avro"
	p.Destination.Partitioning = plan.Preserve
	files.source.Partitions = []int32{0, 5}
	_, err := service.Explain(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
	require.ErrorContains(t, err, "original partition 5")
	// When expanded, then preview succeeds and identifies preserved routing only on the sink.
	factory.connections["output"].info.Partitions = []int32{0, 1, 2, 3, 4, 5}
	report := preview(t, service, p)
	require.Equal(t, plan.Preserve, report.Destination.Partitioning)
	require.Empty(t, report.Source.Partitioning)
}
