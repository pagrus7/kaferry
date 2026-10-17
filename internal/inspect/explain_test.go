package inspect

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time    { return c.now }
func (fixedClock) Sleep(time.Duration) {}
func number(n int64) *int64            { return &n }

type topicMock struct {
	info         kafkaport.TopicDescription
	positions    map[int32]int64
	metadata     map[int32]kafkaport.Progress
	low, high    map[int32]int64
	stable       map[int32]int64
	times        func(map[int32]int64) map[int32]int64
	fail         string
	failure      error
	closed       int
	groupReads   int
	memberChecks int
	queries      []map[int32]int64
}

func (m *topicMock) err(stage string) error {
	if m.fail == stage {
		return m.failure
	}
	return nil
}
func (m *topicMock) Describe(context.Context, bool) (kafkaport.TopicDescription, error) {
	info := m.info
	info.Partitions = slices.Clone(info.Partitions)
	return info, m.err("describe")
}
func (m *topicMock) CheckMembers(context.Context, string) error {
	m.memberChecks++
	return m.err("members")
}
func (m *topicMock) Saved(context.Context, string, string, plan.WindowSpec, int64) (map[int32]int64, map[int32]kafkaport.Progress, error) {
	m.groupReads++
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
	if m.fail == "end times" && len(m.queries) > 1 {
		return nil, m.failure
	}
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
	source      file.SourceInspection
	destination file.ArchiveInspection
	failSource  error
	failOutput  error
	output      file.DestinationProgress
}

func (f *filesMock) Source(plan.Plan, string, config.Worker) (file.SourceInspection, error) {
	return f.source, f.failSource
}
func (f *filesMock) Destination(_ plan.Plan, _ string, progress file.DestinationProgress, _ config.Worker) (file.ArchiveInspection, error) {
	f.output = progress
	return f.destination, f.failOutput
}

func fixture() (*Service, plan.Plan, *factoryMock, *filesMock) {
	p := plan.Plan{Source: plan.Endpoint{Kind: "TOPIC", Name: "input"}, Destination: plan.Endpoint{Kind: "TOPIC", Name: "output"}, Group: "g", Window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: 100}, Upper: &plan.Expression{LiteralMS: 1000}}, CutoffMS: 20}
	topic := func(name string) *topicMock {
		return &topicMock{info: kafkaport.TopicDescription{ClusterID: "cluster", TopicID: name, Partitions: []int32{1, 0}, TimestampType: "CreateTime"}, positions: map[int32]int64{}, metadata: map[int32]kafkaport.Progress{}, low: map[int32]int64{0: 0, 1: 0}, high: map[int32]int64{0: 10, 1: 11}, stable: map[int32]int64{0: 9, 1: 10}, failure: errors.New("injected"), times: func(queries map[int32]int64) map[int32]int64 {
			found := make(map[int32]int64)
			for id, stamp := range queries {
				found[id] = int64(id) + 2
				if stamp > 1000 {
					found[id] = int64(id) + 8
				}
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
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestTopicPreviewUsesSavedPositionsAndBatchedLookups(t *testing.T) {
	// Given two partitions, one saved position, and different destination routing.
	service, p, factory, _ := fixture()
	source := factory.connections["input"]
	source.positions[0], source.metadata[0] = 4, kafkaport.Progress{}
	factory.connections["output"].info.Partitions = []int32{0, 1, 2}
	factory.connections["output"].info.TimestampType = "LogAppendTime"
	// When previewing through the real shared positioner with mocked metadata ports.
	report := preview(t, service, p)
	// Then starts, spans and both warnings are explicit without changing progress.
	if report.Version != 1 || report.Fingerprint == "" || report.Group != "g" || len(report.Partitions) != 2 {
		t.Fatal(report)
	}
	window := plan.Window{Lower: number(100), Upper: number(1000)}
	want := []Partition{
		{ID: 0, Start: 4, Saved: true, LookupEnd: number(8), Span: number(4), LogEnd: 10, Window: window, WindowOrigin: "literal"},
		{ID: 1, Start: 3, LookupEnd: number(9), Span: number(6), LogEnd: 11, Window: window, WindowOrigin: "literal"},
	}
	if !reflect.DeepEqual(report.Partitions, want) {
		t.Fatal("wrong positions, bounds or spans", report.Partitions)
	}
	if len(source.queries) != 2 || len(source.queries[1]) != 2 || source.queries[1][0] != 1020 || source.memberChecks != 1 || source.groupReads != 1 || !reflect.DeepEqual(source.positions, map[int32]int64{0: 4}) {
		t.Fatal("positioning or progress changed", source)
	}
	if source.closed != 1 || factory.connections["output"].closed != 1 || !strings.Contains(strings.Join(report.Warnings, " "), "LogAppendTime") || !strings.Contains(strings.Join(report.Warnings, " "), "Partition counts differ") {
		t.Fatal("cleanup or warnings lost", report)
	}
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
			if report.Partitions[0].Start != 9 || report.Partitions[0].LogEnd != 10 || report.Partitions[0].LookupEnd != nil || report.Partitions[0].Span != nil {
				t.Fatal(report.Partitions)
			}
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
	if *report.Partitions[1].Window.Upper != 2000 || report.Partitions[1].WindowOrigin != "saved" || *report.Partitions[2].Window.Upper != 1000 || report.Partitions[2].WindowOrigin != "adopted" {
		t.Fatal(report.Partitions)
	}
	// Given fresh progress instead, when inspecting, then NOW is preview-only.
	service, _, _, _ = fixture()
	report = preview(t, service, p)
	if *report.Partitions[0].Window.Upper != 5000 || report.Partitions[0].WindowOrigin != "resolved" || !strings.Contains(strings.Join(report.Warnings, " "), "resolves it again") {
		t.Fatal(report)
	}
}

func TestMissingLookupAndSavedPositionPastLookupAreNotCompletionProof(t *testing.T) {
	for _, match := range []bool{true, false} {
		t.Run(map[bool]string{true: "saved past lookup", false: "no match"}[match], func(t *testing.T) {
			// Given a saved position after the matching timestamp, or no timestamp match.
			service, p, factory, _ := fixture()
			source := factory.connections["input"]
			source.positions[0], source.metadata[0] = 9, kafkaport.Progress{}
			if !match {
				source.times = func(map[int32]int64) map[int32]int64 { return map[int32]int64{0: -1, 1: -1} }
			}
			// When previewing, then report a zero span or no lookup, never a completion verdict.
			report := preview(t, service, p)
			if match && (report.Partitions[0].Span == nil || *report.Partitions[0].Span != 0) || !match && (report.Partitions[0].LookupEnd != nil || report.Partitions[0].Span != nil) {
				t.Fatal(report.Partitions)
			}
		})
	}
}

func TestOneOffDoesNotReadOrGenerateAConsumerGroup(t *testing.T) {
	// Given an explicitly one-off statement.
	service, p, factory, _ := fixture()
	p.OneOff = true
	// When inspecting, then no group is generated or queried.
	report := preview(t, service, p)
	if report.Group != "ONE_OFF" || !report.OneOff || factory.connections["input"].groupReads != 0 || factory.connections["input"].memberChecks != 0 {
		t.Fatal(report, factory.connections["input"])
	}
}

func TestFileInspectionCarriesPositionsAndAcknowledgedOutputExtent(t *testing.T) {
	// Given file-source checkpoint data and an existing destination.
	service, p, factory, files := fixture()
	p.Source.Kind, p.Destination.Kind = "FILE", "FILE"
	files.source = file.SourceInspection{ArchiveInspection: file.ArchiveInspection{Exists: true, ID: "source", Size: 250}, Position: 200, Saved: true, CheckpointPath: "checkpoint.json", Output: file.DestinationProgress{Exists: true, ID: "destination", Extent: 300}}
	files.destination = file.ArchiveInspection{Exists: true, ID: "destination", Size: 310}
	// When inspecting both file endpoints.
	report := preview(t, service, p)
	// Then the saved byte position and output extent are reported without Kafka access.
	if factory.opened != 0 || *report.Source.File.StartBytes != 200 || !*report.Source.File.CheckpointExists || *report.Destination.File.AcknowledgedExtent != 300 || files.output.ID != "destination" {
		t.Fatal(report)
	}
	// Given a Kafka source with per-partition extents, then the greatest acknowledged extent is used.
	service, p, factory, files = fixture()
	p.Destination.Kind = "FILE"
	for id := range 2 {
		factory.connections["input"].positions[int32(id)] = 4
		factory.connections["input"].metadata[int32(id)] = kafkaport.Progress{FileID: "archive", Extent: int64(200 + id*100)}
	}
	report = preview(t, service, p)
	if files.output.ID != "archive" || files.output.Extent != 300 || *report.Destination.File.AcknowledgedExtent != 300 {
		t.Fatal(report, files.output)
	}
}

func TestInspectionFailuresCloseOnlyAcquiredConnections(t *testing.T) {
	for _, failure := range []string{"source open", "source describe", "destination open", "destination describe", "members", "saved", "offsets", "times", "end times", "close"} {
		t.Run(failure, func(t *testing.T) {
			// Given a failure at one acquisition, positioning or cleanup boundary.
			service, p, factory, _ := fixture()
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
			default:
				source.fail = failure
			}
			// When inspecting, then preserve the failure and close each acquired connection once.
			_, err := service.Explain(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
			if !errors.Is(err, injected) || source.closed != wantSource || destination.closed != wantDestination {
				t.Fatal(err, source.closed, destination.closed)
			}
		})
	}
}

func TestInvalidSavedPositionsAndWindowOverflowFailBeforeLookup(t *testing.T) {
	for _, invalid := range []string{"past log end", "overflow", "missing lookup", "file identity"} {
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
				source.times = func(queries map[int32]int64) map[int32]int64 {
					found := make(map[int32]int64)
					for id, stamp := range queries {
						if stamp == 100 {
							found[id] = 2
						}
					}
					return found
				}
			case "file identity":
				source.metadata = map[int32]kafkaport.Progress{0: {FileID: "a", Extent: 100}, 1: {FileID: "b", Extent: 100}}
			}
			// When inspecting, then reject the invalid evidence and release both connections.
			_, err := service.Explain(t.Context(), p, config.Defaults("").Worker, core.NoopStopObserver{})
			if err == nil || source.closed != 1 || factory.connections["output"].closed != 1 {
				t.Fatal(err)
			}
		})
	}
}

func TestNegativeCutoffBoundaryNeverBecomesABrokerOffsetSpecifier(t *testing.T) {
	// Given a cutoff boundary that Kafka would treat as a reserved offset specifier.
	service, p, factory, _ := fixture()
	p.Window = plan.WindowSpec{Upper: &plan.Expression{LiteralMS: -21}}
	p.CutoffMS = 20
	// When previewing, then use no timestamp lookup and explain why it is unavailable.
	report := preview(t, service, p)
	if len(factory.connections["input"].queries) != 0 || report.Partitions[0].LookupEnd != nil || !strings.Contains(strings.Join(report.Warnings, " "), "negative cutoff") {
		t.Fatal(report)
	}
}

func TestExpiredPositionStartsAtLogStartWithOneWarning(t *testing.T) {
	// Given saved progress that predates retention.
	service, p, factory, _ := fixture()
	p.Window, p.Start = plan.WindowSpec{}, "EARLIEST"
	source := factory.connections["input"]
	source.low[0], source.positions[0], source.metadata[0] = 3, 1, kafkaport.Progress{}
	// When previewing, then show the actual resumed start and the skipped offset range.
	report := preview(t, service, p)
	if report.Partitions[0].Start != 3 || !strings.Contains(strings.Join(report.Warnings, " "), "saved offset 1 expired") {
		t.Fatal(report)
	}
}

func TestCancelledInspectionDoesNotAcquireConnections(t *testing.T) {
	// Given a cancelled request.
	service, p, factory, _ := fixture()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// When inspecting, then return cancellation before acquiring resources.
	_, err := service.Explain(ctx, p, config.Worker{}, core.NoopStopObserver{})
	if !errors.Is(err, context.Canceled) || factory.opened != 0 {
		t.Fatal(err, factory.opened)
	}
}
