package app

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPreparedOutputSurvivesCoordinatorMoveAndClusterOutage(t *testing.T) {
	for _, fault := range []string{"group-coordinator", "transaction-coordinator", "cluster-outage"} {
		for _, direction := range []string{"TOPIC_to_TOPIC", "TOPIC_to_ARCHIVE", "ARCHIVE_to_TOPIC"} {
			if fault == "group-coordinator" && strings.HasPrefix(direction, "ARCHIVE") {
				continue
			}
			t.Run(fault+"/"+direction, func(t *testing.T) {
				// Given three-broker protocol clusters and one-record checkpoint batches.
				source, destination := kfake.MustCluster(kfake.NumBrokers(3)), kfake.MustCluster(kfake.NumBrokers(3))
				defer source.Close()
				defer destination.Close()
				topic(t, source.ListenAddrs(), "input", 1, false)
				topic(t, destination.ListenAddrs(), "output", 2, false)
				coordinated := direction == "TOPIC_to_ARCHIVE"
				authority := destination
				if coordinated {
					authority = source
				}
				topic(t, authority.ListenAddrs(), "progress", 1, true)
				var unavailable atomic.Bool
				var rejected atomic.Int64
				authority.Control(func(kmsg.Request) (kmsg.Response, error, bool) {
					authority.KeepControl()
					if unavailable.Load() {
						rejected.Add(1)
						return nil, io.EOF, true
					}
					return nil, nil, false
				})
				var transaction atomic.Pointer[string]
				authority.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
					authority.KeepControl()
					if id := request.(*kmsg.ProduceRequest).TransactionID; id != nil {
						value := *id
						transaction.Store(&value)
					}
					return nil, nil, false
				})
				input, output := "TOPIC 'input@src'", "TOPIC 'output@dst'"
				expected := map[string]int{}
				if direction == "ARCHIVE_to_TOPIC" {
					root := t.TempDir()
					expected = archiveInput(t, root)
					input = "ARCHIVE '" + root + "'"
				} else {
					seed(t, source.ListenAddrs(), 1, 6)
					for offset := range 6 {
						expected[fmt.Sprintf("0:%d", offset)] = 1
					}
				}
				root := ""
				if coordinated {
					root = t.TempDir()
					output = "ARCHIVE '" + root + "'"
				}
				ast, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH(GROUP='fault-%d') WHERE RECORD_OFFSET < LATEST", output, input, time.Now().UnixNano()))
				require.NoError(t, err)
				p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source.ListenAddrs(), ","), "session.timeout.ms": "6000", "heartbeat.interval.ms": "500"}, "dst": {"bootstrap.servers": strings.Join(destination.ListenAddrs(), ",")}})
				require.NoError(t, err)
				cfg := config.Defaults("")
				state := t.TempDir()
				cfg.Worker.CheckpointDir = &state
				cfg.Worker.RecordCap, cfg.Worker.PartitionRecordCap = 1, 1
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				var ready atomic.Bool
				injected := false
				var injectedAt time.Time
				var faultErr error
				options := Options{ProgressTopic: "progress", Hook: func(point string, _ int32) {
					if point == "flushed" {
						ready.Store(true)
					}
					if point != "prepared" || !ready.Load() || injected {
						return
					}
					injected = true
					injectedAt = time.Now()
					// When the coordinator moves or every request to the authoritative cluster fails after preparation.
					if fault == "cluster-outage" {
						unavailable.Store(true)
						time.AfterFunc(250*time.Millisecond, cancel)
						return
					}
					cluster, key := source, p.Group
					if fault == "transaction-coordinator" {
						cluster = authority
						id := transaction.Load()
						if id == nil {
							faultErr = fmt.Errorf("no transactional produce observed before fault")
							return
						}
						key = *id
					}
					old := cluster.CoordinatorFor(key)
					for range 100 {
						cluster.RehashCoordinators()
						if cluster.CoordinatorFor(key) != old {
							return
						}
					}
					faultErr = fmt.Errorf("coordinator did not move")
				}}
				attemptErr := Run(ctx, p, cfg, options)
				require.True(t, injected, "attempt stopped before fault: %v", attemptErr)
				require.NoError(t, faultErr)
				if fault == "cluster-outage" {
					require.Error(t, attemptErr)
					require.Positive(t, rejected.Load(), "no request encountered the outage")
					require.Less(t, time.Since(injectedAt), 25*time.Second, "checkpoint outage ignored its deadline")
				}
				unavailable.Store(false)
				if p.Source.Kind == "TOPIC" {
					removeDeadMember(t, source.ListenAddrs(), p.Group)
				}
				// Then a replacement fences the old producer and emits every authored occurrence exactly once.
				require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
				if coordinated {
					require.Equal(t, expected, archiveValues(t, root))
				} else {
					require.Equal(t, expected, recordValues(committedRecords(t, destination.ListenAddrs(), 2)))
				}
			})
		}
	}
}
