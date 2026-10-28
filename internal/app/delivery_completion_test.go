package app

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type controlledClock struct{ milliseconds atomic.Int64 }

func (c *controlledClock) Now() time.Time        { return time.UnixMilli(c.milliseconds.Load()) }
func (c *controlledClock) Sleep(d time.Duration) { c.milliseconds.Add(d.Milliseconds()) }
func TestWallClockEndSaveWithUnpreparedOutput(t *testing.T) {
	for _, destinationKind := range []string{"TOPIC", "ARCHIVE"} {
		t.Run(destinationKind, func(t *testing.T) {
			// Given buffered output when the timestamp arrival deadline passes.
			source, destination := clusters(t, 1)
			seed(t, source, 1, 5)
			root := ""
			target := "TOPIC 'output@dst'"
			if destinationKind == "ARCHIVE" {
				root = t.TempDir()
				target = "ARCHIVE '" + root + "'"
			}
			now := time.Now().Truncate(time.Second)
			upper := now.Add(time.Minute)
			text := fmt.Sprintf("INSERT INTO %s SELECT * FROM TOPIC 'input@src' WITH(GROUP='wall-clock-%d',MAX_STALENESS=INTERVAL '0' SECOND,MAX_OUT_OF_ORDER=OFF) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s'", target, time.Now().UnixNano(), upper.Format(time.RFC3339))
			ast, err := sql.NewParser().Parse(text)
			require.NoError(t, err)
			p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source, ",")}, "dst": {"bootstrap.servers": strings.Join(destination, ",")}})
			require.NoError(t, err)
			clock := &controlledClock{}
			clock.milliseconds.Store(now.UnixMilli())
			queued, flushed, metadataOnly := false, false, false
			options := Options{ProgressTopic: "progress", Clock: clock, Hook: func(point string, _ int32) {
				switch point {
				case "queued":
					queued = true
					clock.milliseconds.Store(upper.Add(time.Second).UnixMilli())
				case "flushed":
					flushed = true
				case "committed":
					if queued && !flushed {
						metadataOnly = true
					}
				}
			}}
			// When the runner saves its captured end before flushing those records.
			require.NoError(t, Run(testContext(t), p, config.Defaults(""), options))
			require.True(t, metadataOnly, "fixture must exercise a metadata-only save with unprepared output")
			require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
			// Then no input is skipped or duplicated, and the saved end remains fixed.
			var got map[string]int
			if destinationKind == "TOPIC" {
				got = output(t, destination, 1)
			} else {
				got = archiveValues(t, root)
			}
			require.Len(t, got, 5)
			for offset := range 5 {
				require.Equal(t, 1, got[fmt.Sprintf("0:%d", offset)])
			}
		})
	}
}
func TestTipWaitOpenTransaction(t *testing.T) {
	// Given an unfinished source transaction after a timestamp query's replicated end.
	source, destination := clusters(t, 1)
	producer := client(t, source, kgo.TransactionalID("open-source"))
	require.NoError(t, producer.BeginTransaction())
	stamp := time.Now().Add(-time.Minute)
	require.NoError(t, producer.ProduceSync(testContext(t), &kgo.Record{Topic: "input", Value: []byte("committed-later"), Timestamp: stamp}).FirstErr())
	p := query(t, source, destination, "WHERE RECORD_TIMESTAMP < NOW()", "")
	p.MaxStalenessMS = 0
	p.MaxOutOfOrderMS = new(int64(-1))
	cfg := config.Defaults("")
	cfg.Worker.TipWait = "100ms"
	// When tip_wait expires, then the uncommitted input is absent and the run fails safely.
	require.ErrorContains(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}), "tip wait exceeded")
	require.Empty(t, output(t, destination, 1))
	// When that transaction commits and the same query resumes, then it is copied once.
	require.NoError(t, producer.EndTransaction(testContext(t), kgo.TryCommit))
	require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	require.Equal(t, map[string]int{"committed-later": 1}, output(t, destination, 1))
}

func TestStartLatestFollowsAndResumesSavedCursor(t *testing.T) {
	// Given a source backlog and a fresh following job starting at LATEST.
	source, destination := clusters(t, 1)
	seed(t, source, 1, 3)
	p := query(t, source, destination, "", "")
	p.Start = "LATEST"
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	var commits atomic.Int64
	done := make(chan error, 1)
	finished := false
	go func() {
		done <- Run(ctx, p, config.Defaults(""), Options{ProgressTopic: "progress", Hook: func(point string, _ int32) {
			if point == "committed" {
				commits.Add(1)
			}
		}})
	}()
	t.Cleanup(func() {
		cancel()
		if !finished {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("following worker did not stop")
			}
		}
	})
	require.Eventually(t, func() bool { return commits.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
	// When a new record arrives, then it is copied and the previous backlog is excluded.
	producer := client(t, source)
	require.NoError(t, producer.ProduceSync(testContext(t), recordFor("input", 0, "later")).FirstErr())
	require.Eventually(t, func() bool { return commits.Load() > 1 }, 10*time.Second, 10*time.Millisecond)
	cancel()
	err := <-done
	finished = true
	require.NoError(t, err)
	require.Equal(t, map[string]int{"later": 1}, output(t, destination, 1))
	// A restart uses the saved cursor, including records already present at restart.
	require.NoError(t, producer.ProduceSync(testContext(t), recordFor("input", 0, "next")).FirstErr())
	p.WorkerStopAfterRecords = 1
	require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	require.Equal(t, map[string]int{"later": 1, "next": 1}, output(t, destination, 1))
}
