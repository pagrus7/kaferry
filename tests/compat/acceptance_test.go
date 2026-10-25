//go:build integration

package compat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/app"
	"github.com/pagrus7/kaferry/internal/commitmeta"
	core "github.com/pagrus7/kaferry/internal/copy"
	adapter "github.com/pagrus7/kaferry/internal/franz"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/fx"
)

type recordObserver struct {
	slog.Handler
	observers []func(slog.Record)
}

func (h recordObserver) Handle(ctx context.Context, record slog.Record) error {
	for _, observe := range h.observers {
		observe(record)
	}
	return h.Handler.Handle(ctx, record)
}
func (h recordObserver) WithAttrs(attrs []slog.Attr) slog.Handler {
	return recordObserver{h.Handler.WithAttrs(attrs), h.observers}
}
func (h recordObserver) WithGroup(name string) slog.Handler {
	return recordObserver{h.Handler.WithGroup(name), h.observers}
}

func loggedWorker(ctx context.Context, statement, cfg string, observers ...func(slog.Record)) workerResult {
	var output bytes.Buffer
	var command app.Command
	graph := fx.New(fx.NopLogger, app.RuntimeModules, fx.Decorate(func(*slog.Logger) *slog.Logger {
		return slog.New(recordObserver{slog.NewJSONHandler(&output, nil), observers})
	}), fx.Populate(&command))
	if err := graph.Err(); err != nil {
		return workerResult{2, err.Error()}
	}
	code := 0
	if err := command.Run(ctx, statement, cfg, testShutdown{}); err != nil {
		code = 1
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) {
			code = coded.ExitCode()
		}
		fmt.Fprintln(&output, err)
	}
	return workerResult{code, output.String()}
}

func committedPartitions(t *testing.T, a *kadm.Client, group, topic string, count int) []kadm.Offset {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		result, err := a.FetchOffsets(ctx, group)
		if err == nil && result.Error() == nil && len(result[topic]) == count {
			valid := true
			for _, p := range result[topic] {
				valid = valid && p.At >= 0 && p.Metadata != ""
			}
			if valid {
				return kadm.OffsetResponses{topic: result[topic]}.Offsets().Sorted()
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("initial progress not committed: %v %+v", err, result)
		case <-tick.C:
		}
	}
}

func TestRecreatedSourceFailsBeforeJoiningEvenWithValidOffsets(t *testing.T) {
	// Given
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	p := producer(t)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: []byte("original"), Timestamp: stamp})
	group := src + "-recreated"
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", broker(t), dst, src, group, stamp.Add(time.Second).Format(time.RFC3339Nano))
	execute(t, statement, "")
	old := committedPartitions(t, a, group, src, 1)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	deleted, err := a.DeleteTopics(ctx, src)
	if err != nil || len(deleted) != 1 || deleted[src].Err != nil {
		t.Fatal("delete fixture topic", deleted, err)
	}
	// Kafka may remove offsets on deletion. Restore the old identity metadata so
	// the guard is exercised, with an offset inside the replacement topic's log.
	for {
		created, err := a.CreateTopic(ctx, 1, 1, nil, src)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("replacement topic not created", created, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	waitTopicReady(t, a, src, 1)
	replacement := producer(t)
	for range 2 {
		publish(t, replacement, &kgo.Record{Topic: src, Partition: 0, Value: []byte("replacement"), Timestamp: time.Now()})
	}
	result, err := a.CommitOffsets(ctx, group, kadm.Offsets{src: {0: old}})
	if err != nil || result.Error() != nil {
		t.Fatal("restore old progress", result, err)
	}
	var output bytes.Buffer

	// When
	executeCode := app.Execute(ctx, []string{"run", "-e", statement}, nil, io.Discard, &output)

	// Then
	if executeCode != 2 || !strings.Contains(output.String(), "source topic identity mismatch") {
		t.Fatalf("recreation was not rejected before intake: exit=%d %s", executeCode, output.String())
	}
	description, err := a.DescribeGroups(ctx, group)
	if err != nil || len(description) != 1 || description.Error() != nil || len(description[group].Members) != 0 {
		t.Fatal("recreated source joined the group", description, err)
	}
	end, err := a.ListEndOffsets(ctx, dst)
	info, ok := end.Lookup(dst, 0)
	if err != nil || !ok || info.Offset != 1 {
		t.Fatal("recreated source wrote output", end, err)
	}
}

func TestMoreWorkersThanPartitionsIdleAndFinishIndependently(t *testing.T) {
	// Given
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	group := src + "-excess-workers"
	end := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','session.timeout.ms'='6000') INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", broker(t), dst, src, group, end.Format(time.RFC3339Nano))
	cfg := configuration(t, "idle_exit='5s'\n")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	results := make(chan workerResult, 3)
	for range 3 {
		go func() {

			// When
			results <- loggedWorker(ctx, statement, cfg)
		}()
	}
	waitMembers(t, a, group, 3, 1, results)
	for range 2 {
		select {
		case result := <-results:

			// Then
			if result.code != 3 || !strings.Contains(result.detail, "local idle exit") || !strings.Contains(result.detail, "worker held no partitions for 5s") {
				t.Fatalf("standby failed to idle-exit: %+v", result)
			}
		case <-ctx.Done():
			t.Fatal("excess workers failed to leave independently")
		}
	}
	p := producer(t)
	publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: []byte("work"), Timestamp: time.Now()})
	publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: []byte("trigger"), Timestamp: end.Add(time.Second)})
	select {
	case result := <-results:
		if result.code != 0 || strings.Contains(result.detail, "local idle exit") {
			t.Fatalf("assigned worker did not finish its partition: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("assigned worker waited for exited standbys")
	}
	waitMembers(t, a, group, 0, 0, nil)
	c := consumer(t)
	assign(t, c, dst, 1)
	if got := read(t, c); string(got.Value) != "work" {
		t.Fatal("work lost or trigger published", got)
	}
	require.Equal(t, int64(1), topicRecordCount(t, a, dst, 1), "stop trigger escaped the timestamp filter")
}

func relativeWindows(t *testing.T, parts []kadm.Offset) map[int32]plan.Window {
	t.Helper()
	result := make(map[int32]plan.Window)
	for _, part := range parts {
		require.NotEmpty(t, part.Metadata, "missing commit metadata")
		progress, err := commitmeta.NewCodec().Decode(part.Metadata)
		if err != nil || progress.Window == nil || progress.Window.Lower == nil || progress.Window.Upper == nil {
			t.Fatal("relative progress has no concrete window", part, err)
		}
		require.Equal(t, int64(2*time.Hour/time.Millisecond), *progress.Window.Upper-*progress.Window.Lower, "window bounds used different NOW instants")
		result[part.Partition] = *progress.Window
	}
	return result
}

func TestConcurrentNowWindowsPersistAcrossWorkersAndRestart(t *testing.T) {
	// Given
	a := admin(t)
	src, dst := topic(t, a, 2), topic(t, a, 1)
	group := src + "-now-workers"
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR AND RECORD_TIMESTAMP < NOW() + INTERVAL '1' HOUR ", broker(t), dst, src, group)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	results := make(chan workerResult, 2)
	for range 2 {
		go func() {

			// When
			results <- loggedWorker(ctx, statement, "")
		}()
	}
	waitMembers(t, a, group, 2, 2, results)
	before := relativeWindows(t, committedPartitions(t, a, group, src, 2))
	p := producer(t)
	for id := range 2 {
		publish(t, p, &kgo.Record{Topic: src, Partition: int32(id), Value: []byte(fmt.Sprintf("work-%d", id)), Timestamp: time.Now()})
		publish(t, p, &kgo.Record{Topic: src, Partition: int32(id), Value: []byte("trigger"), Timestamp: time.Now().Add(3 * time.Hour)})
	}
	for range 2 {
		select {
		case result := <-results:

			// Then
			if result.code != 0 || !strings.Contains(result.detail, "partition window positioned") {
				t.Fatal("relative worker failed or did not log its windows", result)
			}
		case <-ctx.Done():
			t.Fatal("relative workers did not finish")
		}
	}
	execute(t, statement, "")
	after := relativeWindows(t, committedPartitions(t, a, group, src, 2))
	require.Equal(t, after, before, "restart changed saved per-partition windows")
	end, err := a.ListEndOffsets(ctx, dst)
	info, ok := end.Lookup(dst, 0)
	if err != nil || !ok || info.Offset != 2 {
		t.Fatal("relative restart lost or recopied records", end, err)
	}
	waitMembers(t, a, group, 0, 0, nil)
	deleted, err := a.DeleteGroups(ctx, group)
	if err != nil || len(deleted) != 1 || deleted[group].Err != nil {
		t.Fatal("clear all fixture progress", deleted, err)
	}
	// Clearing all progress has the same startup semantics as offset retention expiry.
	result := loggedWorker(ctx, statement, "")
	if result.code != 0 || !strings.Contains(result.detail, `"partitions_with_progress":0`) {
		t.Fatal("empty group was not logged and initialized as a new run", result)
	}
	fresh := relativeWindows(t, committedPartitions(t, a, group, src, 2))
	if reflect.DeepEqual(before, fresh) {
		t.Fatal("cleared progress reused old NOW windows")
	}
}

func TestMissingTimestampWithoutFilterUsesProducerClockAndCountsIt(t *testing.T) {
	// Given
	a := admin(t)
	dst := topic(t, a, 1)
	path := filepath.Join(t.TempDir(), "source.avro")
	writeArchiveRecords(t, path, []core.Record{{Key: nil, Value: []byte{}, TimestampType: "unknown"}})
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM FILE avro('%s') WITH (GROUP='unstamped') ", dst, path)
	before := time.Now().UnixMilli()

	// When
	result := loggedWorker(context.Background(), statement, configuration(t))

	// Then
	if result.code != 0 || !strings.Contains(result.detail, `"msg":"records received producer timestamps","count":1`) {
		t.Fatal("producer timestamp was not counted and warned about", result)
	}
	c := consumer(t)
	assign(t, c, dst, 1)
	record := read(t, c)
	if record.Key != nil || record.Value == nil || record.Timestamp.UnixMilli() < before || record.Timestamp.UnixMilli() > time.Now().UnixMilli() || record.Attrs.TimestampType() != 0 {
		t.Fatal("producer clock or null/empty fidelity failed", record)
	}
}

// Intercept a real broker acknowledgment before the runner can process it.
// os.Exit bypasses drains/commits while other deliveries can remain outstanding.
type crashingClients struct{ adapter.Clients }

func (c crashingClients) Producer(options ...kgo.Opt) (adapter.ProducerClient, error) {
	producer, err := c.Clients.Producer(options...)
	if err != nil {
		return nil, err
	}
	return &crashingProducer{producer}, nil
}

type crashingProducer struct{ adapter.ProducerClient }

func (p *crashingProducer) TryProduce(ctx context.Context, record *kgo.Record, callback func(*kgo.Record, error)) {
	p.ProducerClient.TryProduce(ctx, record, func(record *kgo.Record, err error) {
		if err == nil {
			os.Exit(99)
		}
		callback(record, err)
	})
}

func TestWorkerCrashBeforeDeliveryProcessingReplaysWithoutLoss(t *testing.T) {
	// Given
	if os.Getenv("KAFERRY_KAFKA_CRASH_HELPER") == "1" {
		var command app.Command
		graph := fx.New(fx.NopLogger, app.RuntimeModules, fx.Decorate(func(clients adapter.Clients) adapter.Clients {
			return crashingClients{clients}
		}), fx.Populate(&command))
		require.NoError(t, graph.Err())
		args := os.Args[len(os.Args)-2:]
		require.NoError(t, command.Run(context.Background(), args[0], args[1], testShutdown{}))
		t.Fatal("crash hook not reached")
	}
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	group := src + "-crash"
	p := producer(t)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for i := range 32 {
		publish(t, p, &kgo.Record{Topic: src, Partition: 0, Key: []byte("stable"), Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','session.timeout.ms'='6000'), CONNECTION slow AS ('bootstrap.servers'='%s','linger.ms'='100') INSERT INTO TOPIC '%s@slow' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", broker(t), broker(t), dst, src, group, stamp.Add(time.Second).Format(time.RFC3339Nano))
	cfg := configuration(t, "record_cap=16\npartition_record_cap=16\n")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWorkerCrashBeforeDeliveryProcessingReplaysWithoutLoss$", "--", statement, cfg)
	child.Env = append(os.Environ(), "KAFERRY_KAFKA_CRASH_HELPER=1")

	// When
	data, err := child.CombinedOutput()

	// Then
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 99 {
		t.Fatalf("crash helper: %v %s", err, data)
	}
	parts := committedPartitions(t, a, group, src, 1)
	require.Zero(t, parts[0].At, "unprocessed delivery advanced the group")
	waitMembers(t, a, group, 0, 0, nil)
	execute(t, statement, cfg)
	end, err := a.ListEndOffsets(ctx, dst)
	info, ok := end.Lookup(dst, 0)
	if err != nil || !ok || info.Offset <= 32 || info.Offset > 48 {
		t.Fatal("crash duplicate exposure exceeded the partition cap", end, err)
	}
	c := consumer(t)
	assign(t, c, dst, 1)
	counts := make(map[string]int)
	for range int(info.Offset) {
		record := read(t, c)
		counts[string(record.Value)]++
		if !bytes.Equal(record.Key, []byte("stable")) || !record.Timestamp.Equal(stamp) {
			t.Fatal("replay changed the logical record", record)
		}
	}
	for i := range 32 {
		if n := counts[fmt.Sprint(i)]; n < 1 || n > 2 {
			t.Fatal("record lost or replayed more than once", i, n)
		}
	}
}

func TestRetentionOvertakesRunningConsumerFailsAndRestartUsesLogStart(t *testing.T) {
	// Given
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	group := src + "-running-retention"
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", broker(t), dst, src, group)
	binary := testBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	process := startCLI(t, ctx, binary, "", "run", "-e", statement)
	committedPartitions(t, a, group, src, 1)
	// When retention deletes records while the consumer is stopped.
	require.NoError(t, process.signal(syscall.SIGSTOP))
	p := producer(t)
	for i := range 10 {
		publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: []byte(fmt.Sprint(i)), Timestamp: time.Now()})
	}
	deleted, err := a.DeleteRecords(ctx, kadm.Offsets{src: {0: {Topic: src, Partition: 0, At: 5}}})
	if err != nil || deleted.Error() != nil {
		t.Fatal("delete unread records", deleted, err)
	}
	require.NoError(t, process.signal(syscall.SIGCONT))
	// Then the running consumer fails instead of silently resetting its offset.
	select {
	case <-process.done:
		err := process.wait()
		if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
			t.Fatalf("running offset loss did not fail: %v %s", err, process.logs())
		}
	case <-ctx.Done():
		t.Fatal("retention overtook the consumer without a failure")
	}
	if !strings.Contains(process.logs(), "offset") {
		t.Fatal("offset failure has no diagnostic", process.logs())
	}
	// Restart the same query and group; cancel only after retained output is durable.
	// When restarting the same query after retention has removed its saved position.
	process = startCLI(t, ctx, binary, "", "run", "-e", statement)
	waitEnd(t, a, dst, 5)
	require.NoError(t, process.signal(os.Interrupt))
	if err := process.wait(); err != nil {
		t.Fatalf("retention restart: %v %s", err, process.logs())
	}

	// Then retained records are copied and the retention gap is logged.
	if !strings.Contains(process.logs(), "expired source position") {
		t.Fatal("restart did not log the retention gap", process.logs())
	}
}

// Embedded component tests keep process termination under the test runner's control.
type testShutdown struct{ core.NoopStopObserver }

func (testShutdown) Configure(time.Duration) {}
func (testShutdown) Deadline() time.Time     { return time.Now().Add(time.Hour) }
