//go:build integration && franzspike

package franzspike

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type environment struct{}

func (environment) LookupEnv(key string) (string, bool) { return "", false }
func spikePlan(t *testing.T, q string) plan.Plan {
	t.Helper()
	ast, err := sql.NewParser().Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"default": {"bootstrap.servers": "127.0.0.1:39092"}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func execute(t *testing.T, p plan.Plan, home string) {
	t.Helper()
	limits := config.Defaults(home).Worker
	if err := os.MkdirAll(*limits.CheckpointDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := Execute(ctx(t), p, limits, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
}
func TestSpikeEngineResumeAndFilePipeline(t *testing.T) {
	// Given a bounded query with named group and preserved old timestamps.
	src, dst := topic(t, 1), topic(t, 1)
	home := t.TempDir()
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	upper := stamp.Add(time.Minute).Format(time.RFC3339Nano)
	query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", dst, src, upper, src+"-engine")
	p := spikePlan(t, query)
	producer := client(t)
	publish := func(value string) {
		t.Helper()
		if err := producer.ProduceSync(ctx(t), &kgo.Record{Topic: src, Key: []byte(value), Value: []byte(value), Timestamp: stamp}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	publish("first")
	// When running, appending data, then restarting the exact query.
	execute(t, p, home)
	publish("second")
	execute(t, p, home)
	reader := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{dst: {0: kgo.NewOffset().AtStart()}}))
	got := readN(t, reader, 2)
	// Then only the new record is copied on resume and the group metadata remains readable.
	if string(got[0].Value) != "first" || string(got[1].Value) != "second" {
		t.Fatal("resume output", got)
	}
	ends, err := kadm.NewClient(producer).ListEndOffsets(ctx(t), dst)
	if err != nil || ends[dst][0].Offset != 2 {
		t.Fatal("duplicate or missing output", ends, err)
	}
	saved, err := kadm.NewClient(producer).FetchOffsets(ctx(t), p.Group)
	if err != nil || saved[src][0].At != 2 || !strings.HasPrefix(saved[src][0].Metadata, "kfwd2:1;") {
		t.Fatal(saved, err)
	}
	// When copying the source to a real archive, then replaying that file through the same engine.
	path := filepath.Join(t.TempDir(), "archive.cbor")
	toFile := spikePlan(t, fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", path, src, upper, src+"-archive"))
	execute(t, toFile, home)
	replay := topic(t, 1)
	toTopic := spikePlan(t, fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='%s')", replay, path, src+"-replay"))
	execute(t, toTopic, home)
	output := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{replay: {0: kgo.NewOffset().AtStart()}}))
	// Then the file/Kafka ports interoperate with the copy engine and old timestamps survive.
	roundtrip := readN(t, output, 2)
	for i, r := range roundtrip {
		if string(r.Value) != string(got[i].Value) || r.Timestamp.UnixMilli() != stamp.UnixMilli() {
			t.Fatal("file round trip changed record", r)
		}
	}
}

type observeTip struct {
	once    sync.Once
	sampled chan struct{}
}

func (w *observeTip) Write(b []byte) (int, error) {
	if strings.Contains(string(b), "tip watermark sampled") {
		w.once.Do(func() { close(w.sampled) })
	}
	return len(b), nil
}
func TestSpikeEngineWaitsForOpenTransactionAndKeepsFixedEnd(t *testing.T) {
	// Given one open transactional record and a bounded query whose time limit has passed.
	src, dst := topic(t, 1), topic(t, 1)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	tx := client(t, kgo.TransactionalID(src+"-open"))
	if err := tx.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := tx.ProduceSync(ctx(t), &kgo.Record{Topic: src, Value: []byte("inside sampled end"), Timestamp: stamp}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	p := spikePlan(t, fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", dst, src, stamp.Add(time.Minute).Format(time.RFC3339Nano), src+"-open-group"))
	observer := &observeTip{sampled: make(chan struct{})}
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	home := t.TempDir()
	go func() {
		result <- Execute(deadline, p, config.Defaults(home).Worker, slog.New(slog.NewTextHandler(observer, nil)))
	}()
	await(t, observer.sampled, "sampling physical end")
	// When the physical end is sampled but read_committed cannot yet reach it.
	select {
	case err := <-result:
		t.Fatalf("completed across open transaction: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	producer := client(t)
	if err := producer.ProduceSync(ctx(t), &kgo.Record{Topic: src, Value: []byte("after sampled end"), Timestamp: stamp}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := tx.EndTransaction(ctx(t), kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.Done():
		t.Fatal("copy did not finish after transaction committed")
	}
	// Then the original record is copied and later data is left for another run.
	ends, err := kadm.NewClient(producer).ListEndOffsets(ctx(t), dst)
	if err != nil || ends[dst][0].Offset != 1 {
		t.Fatal(ends, err)
	}
	reader := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{dst: {0: kgo.NewOffset().AtStart()}}))
	if r := readN(t, reader, 1)[0]; string(r.Value) != "inside sampled end" {
		t.Fatal("wrong record copied", r)
	}
}

func TestSpikeSinkCloseDoesNotWaitForUndrainedReports(t *testing.T) {
	// Given a stopped core with a full delivery queue and a write accepted by the client.
	src, dst := topic(t, 1), topic(t, 1)
	p := spikePlan(t, fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", dst, src, src+"-close"))
	limits := config.Defaults(t.TempDir()).Worker
	limits.RecordCap = 1
	sink, err := NewSink(p, limits)
	if err != nil {
		t.Fatal(err)
	}
	for range cap(sink.reports) {
		sink.reports <- core.Delivery{}
	}
	if err := sink.Submit(core.Record{Value: []byte("one")}, core.Token{}); err != nil {
		t.Fatal(err)
	}
	// When closing without a delivery receiver.
	closed := make(chan struct{})
	go func() { sink.Close(); close(closed) }()
	// Then callback cleanup finishes instead of blocking on the full queue.
	await(t, closed, "sink close")
}
