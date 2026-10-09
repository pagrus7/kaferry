//go:build integration

package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
	"kafka-fwd2/internal/app"
	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/file"
	adapter "kafka-fwd2/internal/kafka"
)

func loggedWorker(ctx context.Context, statement, cfg string) workerResult {
	var output bytes.Buffer
	var command app.Command
	graph := fx.New(fx.NopLogger, app.RuntimeModules, fx.Decorate(func(*slog.Logger) *slog.Logger {
		return slog.New(slog.NewJSONHandler(&output, nil))
	}), fx.Populate(&command))
	if err := graph.Err(); err != nil {
		return workerResult{2, err.Error()}
	}
	code := 0
	if err := command.Run(ctx, statement, cfg); err != nil {
		code = 1
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) {
			code = coded.ExitCode()
		}
		fmt.Fprintln(&output, err)
	}
	return workerResult{code, output.String()}
}

func committedPartitions(t *testing.T, a *k.AdminClient, group, topic string, count int) []k.TopicPartition {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		parts := make([]k.TopicPartition, count)
		for i := range parts {
			parts[i] = k.TopicPartition{Topic: &topic, Partition: int32(i)}
		}
		result, err := a.ListConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: parts}})
		if err == nil && len(result.ConsumerGroupsTopicPartitions) == 1 {
			parts = result.ConsumerGroupsTopicPartitions[0].Partitions
			valid := len(parts) == count
			for _, part := range parts {
				valid = valid && part.Error == nil && part.Offset >= 0 && part.Metadata != nil
			}
			if valid {
				return parts
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("initial progress not committed", err)
		case <-tick.C:
		}
	}
}

func TestRecreatedSourceFailsBeforeJoiningEvenWithValidOffsets(t *testing.T) {
	// Given
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	p := producer(t, nil)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte("original"), Timestamp: stamp})
	group := src + "-recreated"
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), group)
	execute(t, statement, "")
	old := committedPartitions(t, a, group, src, 1)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	deleted, err := a.DeleteTopics(ctx, []string{src})
	if err != nil || len(deleted) != 1 || deleted[0].Error.Code() != k.ErrNoError {
		t.Fatal("delete fixture topic", deleted, err)
	}
	// Kafka may remove offsets on deletion. Restore the old identity metadata so
	// the guard is exercised, with an offset inside the replacement topic's log.
	for {
		created, err := a.CreateTopics(ctx, []k.TopicSpecification{{Topic: src, NumPartitions: 1, ReplicationFactor: 1}})
		if err == nil && len(created) == 1 && created[0].Error.Code() == k.ErrNoError {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("replacement topic not created", created, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	waitTopicReady(t, a, src, 1)
	replacement := producer(t, nil)
	for range 2 {
		publish(t, replacement, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte("replacement"), Timestamp: time.Now()})
	}
	result, err := a.AlterConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: []k.TopicPartition{old}}})
	if err != nil || len(result.ConsumerGroupsTopicPartitions) != 1 || result.ConsumerGroupsTopicPartitions[0].Partitions[0].Error != nil {
		t.Fatal("restore old progress", result, err)
	}
	var output bytes.Buffer

	// When
	executeCode := app.Execute(ctx, []string{"run", "-e", statement}, nil, &output)

	// Then
	if executeCode != 2 || !strings.Contains(output.String(), "source topic identity mismatch") {
		t.Fatalf("recreation was not rejected before intake: exit=%d %s", executeCode, output.String())
	}
	description, err := a.DescribeConsumerGroups(ctx, []string{group})
	if err != nil || len(description.ConsumerGroupDescriptions) != 1 || len(description.ConsumerGroupDescriptions[0].Members) != 0 {
		t.Fatal("recreated source joined the group", description, err)
	}
	end, err := a.ListOffsets(ctx, map[k.TopicPartition]k.OffsetSpec{{Topic: &dst, Partition: 0}: k.LatestOffsetSpec})
	info, ok := testOffset(end, dst, 0)
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
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','session.timeout.ms'='6000') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), dst, src, end.Format(time.RFC3339Nano), group)
	cfg := configuration(t)
	f, err := os.OpenFile(cfg, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("idle_exit='5s'\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
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
			if result.code != 0 || !strings.Contains(result.detail, "local idle exit") {
				t.Fatalf("standby failed to idle-exit: %+v", result)
			}
		case <-ctx.Done():
			t.Fatal("excess workers failed to leave independently")
		}
	}
	p := producer(t, nil)
	publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte("work"), Timestamp: time.Now()})
	publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte("trigger"), Timestamp: end.Add(time.Second)})
	select {
	case result := <-results:
		if result.code != 0 || strings.Contains(result.detail, "local idle exit") {
			t.Fatalf("assigned worker did not finish its partition: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("assigned worker waited for exited standbys")
	}
	waitMembers(t, a, group, 0, 0, nil)
	execute(t, statement, cfg) // Sole-worker confirmation of the bounded group.
	c := consumer(t, dst+"-check")
	if err := c.Assign([]k.TopicPartition{{Topic: &dst, Partition: 0, Offset: 0}}); err != nil {
		t.Fatal(err)
	}
	if got := read(t, c); string(got.Value) != "work" {
		t.Fatal("work lost or trigger published", got)
	}
}

func relativeWindows(t *testing.T, parts []k.TopicPartition) map[int32]adapter.Progress {
	t.Helper()
	result := make(map[int32]adapter.Progress)
	for _, part := range parts {
		var progress adapter.Progress
		if part.Metadata == nil || !strings.HasPrefix(*part.Metadata, "kfwd2:1;") || json.Unmarshal([]byte(strings.TrimPrefix(*part.Metadata, "kfwd2:1;")), &progress) != nil || progress.Window == nil || progress.Window.Lower == nil || progress.Window.Upper == nil {
			t.Fatal("relative progress has no concrete window", part)
		}
		if *progress.Window.Upper-*progress.Window.Lower != int64(2*time.Hour/time.Millisecond) {
			t.Fatal("window bounds used different NOW instants", progress)
		}
		result[part.Partition] = progress
	}
	return result
}

func TestConcurrentNowWindowsPersistAcrossWorkersAndRestart(t *testing.T) {
	// Given
	a := admin(t)
	src, dst := topic(t, a, 2), topic(t, a, 1)
	group := src + "-now-workers"
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR AND RECORD_TIMESTAMP < NOW() + INTERVAL '1' HOUR OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), dst, src, group)
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
	p := producer(t, nil)
	for id := range 2 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: int32(id)}, Value: []byte(fmt.Sprintf("work-%d", id)), Timestamp: time.Now()})
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: int32(id)}, Value: []byte("trigger"), Timestamp: time.Now().Add(3 * time.Hour)})
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
	if !reflect.DeepEqual(before, after) {
		t.Fatal("restart changed saved per-partition windows", before, after)
	}
	end, err := a.ListOffsets(ctx, map[k.TopicPartition]k.OffsetSpec{{Topic: &dst, Partition: 0}: k.LatestOffsetSpec})
	info, ok := testOffset(end, dst, 0)
	if err != nil || !ok || info.Offset != 2 {
		t.Fatal("relative restart lost or recopied records", end, err)
	}
	waitMembers(t, a, group, 0, 0, nil)
	deleted, err := a.DeleteConsumerGroups(ctx, []string{group})
	if err != nil || len(deleted.ConsumerGroupResults) != 1 || deleted.ConsumerGroupResults[0].Error.Code() != k.ErrNoError {
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
	path := filepath.Join(t.TempDir(), "source.cbor")
	codec, err := file.NewCodec(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	out, err := file.NewOutput(file.NewPublisher(), path, "fixture", "fixture", "", 0, codec, 10, slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Submit(core.Record{Key: nil, Value: []byte{}, TimestampType: "unknown"}, core.Token{}); err != nil {
		t.Fatal(err)
	}
	if err := out.Flush(); err != nil {
		t.Fatal(err)
	}
	out.Close()
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='unstamped')", dst, path)
	before := time.Now().UnixMilli()

	// When
	result := loggedWorker(context.Background(), statement, configuration(t))

	// Then
	if result.code != 0 || !strings.Contains(result.detail, `"msg":"records received producer timestamps","count":1`) {
		t.Fatal("producer timestamp was not counted and warned about", result)
	}
	c := consumer(t, dst+"-verify")
	if err := c.Assign([]k.TopicPartition{{Topic: &dst, Partition: 0, Offset: 0}}); err != nil {
		t.Fatal(err)
	}
	record := read(t, c)
	if record.Key != nil || record.Value == nil || record.Timestamp.UnixMilli() < before || record.Timestamp.UnixMilli() > time.Now().UnixMilli() || record.TimestampType != k.TimestampCreateTime {
		t.Fatal("producer clock or null/empty fidelity failed", record)
	}
}

// Intercept a real broker acknowledgment before the runner can process it.
// os.Exit bypasses drains/commits while other deliveries can remain outstanding.
type crashingClients struct{ adapter.Clients }

func (c crashingClients) Producer(cfg k.ConfigMap) (adapter.ProducerClient, error) {
	p, err := c.Clients.Producer(cfg)
	if err != nil {
		return nil, err
	}
	return &crashingProducer{ProducerClient: p}, nil
}

type crashingProducer struct{ adapter.ProducerClient }

func (p *crashingProducer) Events() chan k.Event {
	events := make(chan k.Event)
	go func() {
		for event := range p.ProducerClient.Events() {
			if message, ok := event.(*k.Message); ok && message.TopicPartition.Error == nil {
				os.Exit(99)
			}
			events <- event
		}
		close(events)
	}()
	return events
}

func TestWorkerCrashBeforeDeliveryProcessingReplaysWithoutLoss(t *testing.T) {
	// Given
	if os.Getenv("KFWD_KAFKA_CRASH_HELPER") == "1" {
		var command app.Command
		graph := fx.New(fx.NopLogger, app.RuntimeModules, fx.Decorate(func(clients adapter.Clients) adapter.Clients {
			return crashingClients{clients}
		}), fx.Populate(&command))
		if graph.Err() != nil {
			t.Fatal(graph.Err())
		}
		args := os.Args[len(os.Args)-2:]
		if err := command.Run(context.Background(), args[0], args[1]); err != nil {
			t.Fatal(err)
		}
		t.Fatal("crash hook not reached")
	}
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	group := src + "-crash"
	p := producer(t, nil)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for i := range 32 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Key: []byte("stable"), Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','session.timeout.ms'='6000'), CONNECTION slow AS ('bootstrap.servers'='%s','linger.ms'='100') INSERT INTO TOPIC '%s@slow' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), broker(t), dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), group)
	cfg := configuration(t)
	f, err := os.OpenFile(cfg, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("record_cap=16\npartition_record_cap=16\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWorkerCrashBeforeDeliveryProcessingReplaysWithoutLoss$", "--", statement, cfg)
	child.Env = append(os.Environ(), "KFWD_KAFKA_CRASH_HELPER=1")

	// When
	data, err := child.CombinedOutput()

	// Then
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 99 {
		t.Fatalf("crash helper: %v %s", err, data)
	}
	parts := committedPartitions(t, a, group, src, 1)
	if parts[0].Offset != 0 {
		t.Fatal("unprocessed delivery advanced the group", parts)
	}
	waitMembers(t, a, group, 0, 0, nil)
	execute(t, statement, cfg)
	end, err := a.ListOffsets(ctx, map[k.TopicPartition]k.OffsetSpec{{Topic: &dst, Partition: 0}: k.LatestOffsetSpec})
	info, ok := testOffset(end, dst, 0)
	if err != nil || !ok || info.Offset <= 32 || info.Offset > 48 {
		t.Fatal("crash duplicate exposure exceeded the partition cap", end, err)
	}
	c := consumer(t, dst+"-verify")
	if err := c.Assign([]k.TopicPartition{{Topic: &dst, Partition: 0, Offset: 0}}); err != nil {
		t.Fatal(err)
	}
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
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", broker(t), dst, src, group)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	process := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement)
	committedPartitions(t, a, group, src, 1)
	// When retention deletes records while the consumer is stopped.
	if err := process.signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	p := producer(t, nil)
	for i := range 10 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte(fmt.Sprint(i)), Timestamp: time.Now()})
	}
	deleted, err := a.DeleteRecords(ctx, []k.TopicPartition{{Topic: &src, Partition: 0, Offset: 5}})
	if err != nil || len(deleted.DeleteRecordsResults) != 1 || deleted.DeleteRecordsResults[0].TopicPartition.Error != nil {
		t.Fatal("delete unread records", deleted, err)
	}
	if err := process.signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
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
	process = startCLI(t, ctx, testBinary(t), "", "run", "-e", statement)
	waitEnd(t, a, dst, 5)
	if err := process.signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := process.wait(); err != nil {
		t.Fatalf("retention restart: %v %s", err, process.logs())
	}

	// Then retained records are copied and the retention gap is logged.
	if !strings.Contains(process.logs(), "expired source position") {
		t.Fatal("restart did not log the retention gap", process.logs())
	}
}
