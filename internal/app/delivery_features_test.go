package app

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPartitionAndOffsetFeatures(t *testing.T) {
	for _, example := range []struct {
		predicate string
		parts     []int
		lo, hi    int
	}{
		{"WHERE PARTITION IN (0,2) AND RECORD_OFFSET >= 3 AND RECORD_OFFSET < 8", []int{0, 2}, 3, 8},
		{"WHERE PARTITION NOT BETWEEN 1 AND 1 AND RECORD_OFFSET BETWEEN 2 AND 5", []int{0, 2}, 2, 6},
		{"WHERE (PARTITION IN (0) AND RECORD_OFFSET >= 3 AND RECORD_OFFSET < 8) OR (PARTITION BETWEEN 2 AND 2 AND RECORD_OFFSET >= 3 AND RECORD_OFFSET < 8)", []int{0, 2}, 3, 8},
	} {
		t.Run(example.predicate, func(t *testing.T) {
			// Given selected and excluded partitions, with explicit numeric offset ranges.
			source, destination := clusters(t, 3)
			seed(t, source, 3, 12)
			p := query(t, source, destination, example.predicate, "")
			cfg := config.Defaults("")
			cfg.Worker.RecordCap = 4
			cfg.Worker.PartitionRecordCap = 3
			// When copied and resumed through the production planner and runner.
			require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
			require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
			// Then only the authored partition/offset selections are written once.
			got := output(t, destination, 3)
			require.Len(t, got, len(example.parts)*(example.hi-example.lo))
			for _, part := range example.parts {
				for offset := example.lo; offset < example.hi; offset++ {
					require.Equal(t, 1, got[fmt.Sprintf("%d:%d", part, offset)])
				}
			}
		})
	}
}

type observations struct {
	mu    sync.Mutex
	last  core.Snapshot
	peak  int64
	saves []core.SaveResult
}

func (o *observations) Publish(snapshot core.Snapshot) {
	o.mu.Lock()
	o.last = snapshot
	o.peak = max(o.peak, snapshot.PendingEntries)
	o.mu.Unlock()
}
func (o *observations) Saved(result core.SaveResult) {
	o.mu.Lock()
	o.saves = append(o.saves, result)
	o.mu.Unlock()
}
func TestWorkerLimitsAndObserver(t *testing.T) {
	// Given a per-invocation worker stop and small uncommitted work caps.
	source, destination := clusters(t, 3)
	seed(t, source, 3, 40)
	p := query(t, source, destination, "WHERE RECORD_OFFSET < LATEST", "OPTIONS (WORKER_STOP_AFTER_RECORDS = 17)")
	cfg := config.Defaults("")
	cfg.Worker.RecordCap = 5
	cfg.Worker.PartitionRecordCap = 3
	observed := &observations{}
	// When one capped invocation runs, then its actual save and count observations are reported.
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress", Observer: observed}))
	observed.mu.Lock()
	require.EqualValues(t, 17, observed.last.Submitted)
	require.LessOrEqual(t, observed.last.PendingEntries, int64(5))
	require.NotEmpty(t, observed.saves)
	observed.mu.Unlock()
	require.Len(t, output(t, destination, 3), 17)
	// When the same group resumes without a worker allowance, then all input remains once.
	p.WorkerStopAfterRecords = 0
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	got := output(t, destination, 3)
	require.Len(t, got, 120)
	for _, count := range got {
		require.Equal(t, 1, count)
	}
}
func TestKeyRoutingAndCompression(t *testing.T) {
	for _, compression := range []string{"none", "gzip", "snappy", "lz4", "zstd"} {
		t.Run(compression, func(t *testing.T) {
			// Given three source partitions, three output partitions and repeated non-null keys.
			source, destination := clusters(t, 3)
			seed(t, source, 3, 12)
			ast, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO TOPIC 'output@dst' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC 'input@src' WITH (GROUP = 'key-%s') WHERE RECORD_OFFSET < LATEST;", compression))
			require.NoError(t, err)
			p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source, ",")}, "dst": {"bootstrap.servers": strings.Join(destination, ","), "compression.type": compression}})
			require.NoError(t, err)
			// When keyed records are transactionally copied with each supported codec.
			require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
			// Then repeated keys retain one destination partition and every occurrence survives.
			positions := map[string]int32{}
			total := 0
			for _, record := range committedRecords(t, destination, 3) {
				key := string(record.Key)
				if previous, ok := positions[key]; ok {
					require.Equal(t, previous, record.Partition)
				} else {
					positions[key] = record.Partition
				}
				total++
			}
			require.Equal(t, 36, total)
			require.Len(t, positions, 4)
		})
	}
}
func TestCommittedAbortedAndOpenTransactions(t *testing.T) {
	// Given ordinary records, an aborted transaction, a committed transaction and an open tail.
	source, destination := clusters(t, 1)
	seed(t, source, 1, 3)
	tx := client(t, source, kgo.TransactionalID("source-transactions"))
	for _, commit := range []bool{false, true} {
		require.NoError(t, tx.BeginTransaction())
		prefix := "aborted"
		if commit {
			prefix = "committed"
		}
		require.NoError(t, tx.ProduceSync(testContext(t), &kgo.Record{Topic: "input", Value: []byte(prefix + "-1")}, &kgo.Record{Topic: "input", Value: []byte(prefix + "-2")}).FirstErr())
		require.NoError(t, tx.EndTransaction(testContext(t), kgo.TransactionEndTry(commit)))
	}
	require.NoError(t, tx.BeginTransaction())
	require.NoError(t, tx.ProduceSync(testContext(t), &kgo.Record{Topic: "input", Value: []byte("open-tail")}).FirstErr())
	p := query(t, source, destination, "WHERE RECORD_OFFSET < LATEST", "")
	// When the first assignment freezes the stable end and a rerun follows tail commit.
	require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	require.NoError(t, tx.EndTransaction(testContext(t), kgo.TryCommit))
	require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	// Then aborted and initially open records remain excluded.
	got := output(t, destination, 1)
	require.Len(t, got, 5)
	for _, key := range []string{"0:0", "0:1", "0:2", "committed-1", "committed-2"} {
		require.Equal(t, 1, got[key])
	}
}
func TestRelativeTimestampWindowPersists(t *testing.T) {
	// Given two in-window records and a later offset carrying an older timestamp.
	source, destination := clusters(t, 1)
	producer := client(t, source)
	now := time.Now()
	require.NoError(t, producer.ProduceSync(testContext(t), &kgo.Record{Topic: "input", Value: []byte("inside-1"), Timestamp: now.Add(-time.Minute)}, &kgo.Record{Topic: "input", Value: []byte("inside-2"), Timestamp: now.Add(-time.Minute)}, &kgo.Record{Topic: "input", Value: []byte("outside"), Timestamp: now.Add(-2 * time.Hour)}).FirstErr())
	p := query(t, source, destination, "WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR AND RECORD_TIMESTAMP < NOW() AND RECORD_OFFSET < LATEST", "OPTIONS (WORKER_STOP_AFTER_RECORDS = 1)")
	// When a capped run resumes, then saved relative windows and offset ends remain authoritative.
	require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	p.WorkerStopAfterRecords = 0
	require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	require.Equal(t, map[string]int{"inside-1": 1, "inside-2": 1}, output(t, destination, 1))
}
func TestCompactedInput(t *testing.T) {
	// Given a compacted source containing gaps in original offsets.
	sourceCluster := kfake.MustCluster(kfake.NumBrokers(1))
	destinationCluster := kfake.MustCluster(kfake.NumBrokers(1))
	defer sourceCluster.Close()
	defer destinationCluster.Close()
	source, destination := sourceCluster.ListenAddrs(), destinationCluster.ListenAddrs()
	topic(t, source, "input", 1, true)
	topic(t, destination, "output", 1, false)
	topic(t, destination, "progress", 1, true)
	producer := client(t, source)
	require.NoError(t, producer.ProduceSync(testContext(t), &kgo.Record{Topic: "input", Key: []byte("a"), Value: []byte("old-a")}, &kgo.Record{Topic: "input", Key: []byte("b"), Value: []byte("old-b")}, &kgo.Record{Topic: "input", Key: []byte("a"), Value: []byte("new-a")}, &kgo.Record{Topic: "input", Key: []byte("b"), Value: []byte("new-b")}).FirstErr())
	require.NoError(t, producer.ProduceSync(testContext(t), &kgo.Record{Topic: "input", Key: []byte("sentinel"), Value: []byte("sentinel")}).FirstErr())
	sourceCluster.Compact()
	p := query(t, source, destination, "WHERE RECORD_OFFSET < LATEST", "")
	// When production gap detection copies and resumes through destination progress.
	require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	// Then only retained records are present, once each.
	require.Equal(t, map[string]int{"new-a": 1, "new-b": 1, "sentinel": 1}, output(t, destination, 1))
}

func TestGroupRebalanceAndPartitionAddition(t *testing.T) {
	// Given a following worker holding unprepared output from both source partitions.
	source, destination := clusters(t, 2)
	seed(t, source, 2, 20)
	p := query(t, source, destination, "WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR", "")
	cfg := config.Defaults("")
	cfg.Worker.RecordCap = 100
	cfg.Worker.PartitionRecordCap = 50
	firstCtx, firstCancel := context.WithCancel(testContext(t))
	defer firstCancel()
	secondCtx, secondCancel := context.WithCancel(testContext(t))
	defer secondCancel()
	buffered, release := make(chan struct{}), make(chan struct{})
	released := false
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	var secondAcquires atomic.Int64
	queued := map[int32]bool{}
	blocked := false
	go func() {
		firstDone <- Run(firstCtx, p, cfg, Options{ProgressTopic: "progress", Hook: func(point string, id int32) {
			if point == "queued" && !blocked {
				queued[id] = true
				if len(queued) == 2 {
					blocked = true
					close(buffered)
					<-release
				}
			}
		}})
	}()
	firstFinished, secondStarted, secondFinished := false, false, false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		firstCancel()
		secondCancel()
		if !firstFinished {
			select {
			case <-firstDone:
			case <-time.After(5 * time.Second):
				t.Error("first worker did not stop")
			}
		}
		if secondStarted && !secondFinished {
			select {
			case <-secondDone:
			case <-time.After(5 * time.Second):
				t.Error("second worker did not stop")
			}
		}
	})
	select {
	case <-buffered:
	case <-time.After(10 * time.Second):
		t.Fatal("fixture did not buffer both partitions before transfer")
	}
	// When a second worker joins while the first still holds unfinished work.
	secondStarted = true
	go func() {
		secondDone <- Run(secondCtx, p, cfg, Options{ProgressTopic: "progress", Hook: func(point string, _ int32) {
			if point == "acquired" {
				secondAcquires.Add(1)
			}
		}})
	}()
	membership := client(t, source)
	require.Eventually(t, func() bool {
		request := kmsg.NewPtrDescribeGroupsRequest()
		request.Groups = []string{p.Group}
		response, err := request.RequestWith(testContext(t), membership)
		if err != nil {
			return false
		}
		for _, group := range response.Groups {
			if group.State == "PreparingRebalance" || group.State == "CompletingRebalance" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)
	close(release)
	released = true
	require.Eventually(t, func() bool { return secondAcquires.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
	// Partition growth gives the new partition the original saved relative window.
	for _, endpoint := range []struct {
		brokers []string
		topic   string
	}{{destination, "output"}, {source, "input"}} {
		result, err := kadm.NewClient(client(t, endpoint.brokers)).CreatePartitions(testContext(t), 1, endpoint.topic)
		require.NoError(t, err)
		require.NoError(t, result.Error())
	}
	producer := client(t, source)
	records := []*kgo.Record{}
	for offset := range 20 {
		records = append(records, recordFor("input", 2, fmt.Sprintf("2:%d", offset)))
	}
	require.NoError(t, producer.ProduceSync(testContext(t), records...).FirstErr())
	admin := kadm.NewClient(client(t, destination))
	require.Eventually(t, func() bool {
		ends, err := admin.ListCommittedOffsets(testContext(t), "output")
		if err != nil {
			return false
		}
		for part := range 3 {
			if ends["output"][int32(part)].Offset < 21 {
				return false
			}
		}
		return true
	}, 15*time.Second, 20*time.Millisecond)
	firstCancel()
	firstErr := <-firstDone
	firstFinished = true
	secondCancel()
	secondErr := <-secondDone
	secondFinished = true
	// A member may stop on a failed ownership check; the replacement must retain every input once.
	if firstErr != nil {
		t.Logf("first member released unfinished work: %v", firstErr)
	}
	require.NoError(t, secondErr)
	got := output(t, destination, 3)
	require.Len(t, got, 60)
	for _, count := range got {
		require.Equal(t, 1, count)
	}
	report, err := (Inspection{Options: Options{ProgressTopic: "progress"}}).Status(testContext(t), p, cfg.Worker, core.NoopStopObserver{})
	require.NoError(t, err)
	require.Len(t, report.Partitions, 3)
	require.NotNil(t, report.Partitions[0].Window)
	for _, row := range report.Partitions {
		require.Equal(t, report.Partitions[0].Window, row.Window)
	}
}
