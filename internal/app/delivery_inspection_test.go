package app

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestAuthoritativeReadOnlyInspectionAllEndpoints(t *testing.T) {
	for _, src := range []string{"TOPIC", "ARCHIVE"} {
		for _, dst := range []string{"TOPIC", "ARCHIVE"} {
			t.Run(src+"_to_"+dst, func(t *testing.T) {
				// Given a completed copy and an absent advisory replay mirror.
				source := kfake.MustCluster(kfake.NumBrokers(1))
				destination := kfake.MustCluster(kfake.NumBrokers(1))
				defer source.Close()
				defer destination.Close()
				topic(t, source.ListenAddrs(), "input", 2, false)
				topic(t, source.ListenAddrs(), "progress", 2, true)
				topic(t, destination.ListenAddrs(), "output", 2, false)
				topic(t, destination.ListenAddrs(), "progress", 2, true)
				input := "TOPIC 'input@src'"
				output := "TOPIC 'output@dst'"
				if src == "TOPIC" {
					seed(t, source.ListenAddrs(), 2, 3)
				} else {
					root := t.TempDir()
					archiveInput(t, root)
					input = "ARCHIVE '" + root + "'"
				}
				if dst == "ARCHIVE" {
					output = "ARCHIVE '" + t.TempDir() + "'"
				}
				statement := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH (GROUP='inspection') WHERE RECORD_OFFSET < LATEST", output, input)
				ast, err := sql.NewParser().Parse(statement)
				require.NoError(t, err)
				p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source.ListenAddrs(), ",")}, "dst": {"bootstrap.servers": strings.Join(destination.ListenAddrs(), ",")}})
				require.NoError(t, err)
				cfg := config.Defaults("")
				directory := t.TempDir()
				cfg.Worker.CheckpointDir = &directory
				options := Options{ProgressTopic: "progress"}
				require.NoError(t, Run(testContext(t), p, cfg, options))
				if src == "ARCHIVE" {
					entries, err := os.ReadDir(directory)
					require.NoError(t, err)
					for _, entry := range entries {
						if strings.HasSuffix(entry.Name(), ".json") {
							require.NoError(t, os.Remove(filepath.Join(directory, entry.Name())))
						}
					}
				}
				var mutations atomic.Int64
				for _, cluster := range []*kfake.Cluster{source, destination} {
					for _, key := range []int16{int16(kmsg.Produce), int16(kmsg.InitProducerID), int16(kmsg.JoinGroup), int16(kmsg.OffsetCommit), int16(kmsg.CreatePartitions)} {
						cluster.ControlKey(key, func(kmsg.Request) (kmsg.Response, error, bool) {
							cluster.KeepControl()
							mutations.Add(1)
							return nil, nil, false
						})
					}
				}
				// When status/explain inspect destination progress, then both agree without Kafka writes.
				inspector := Inspection{Options: options}
				status, err := inspector.Status(testContext(t), p, cfg.Worker, core.NoopStopObserver{})
				require.NoError(t, err)
				require.Equal(t, "stopped", status.Verdict)
				report, err := inspector.Explain(testContext(t), p, cfg.Worker, core.NoopStopObserver{})
				require.NoError(t, err)
				if src == "TOPIC" {
					for _, row := range report.Partitions {
						require.True(t, row.Saved)
						require.Equal(t, int64(3), row.Start)
					}
				} else {
					require.True(t, *report.Source.File.CheckpointExists)
				}
				require.Zero(t, mutations.Load())
				// The missing mirror is repaired by a resume without duplicating output.
				require.NoError(t, Run(testContext(t), p, cfg, options))
			})
		}
	}
}
