package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/kafkaport"
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
				expected := map[string]int{}
				input := "TOPIC 'input@src'"
				output := "TOPIC 'output@dst'"
				if src == "TOPIC" {
					seed(t, source.ListenAddrs(), 2, 3)
					for part := range 2 {
						for offset := range 3 {
							expected[fmt.Sprintf("%d:%d", part, offset)] = 1
						}
					}
				} else {
					root := t.TempDir()
					expected = archiveInput(t, root)
					input = "ARCHIVE '" + root + "'"
				}
				outputRoot := ""
				if dst == "ARCHIVE" {
					outputRoot = t.TempDir()
					output = "ARCHIVE '" + outputRoot + "'"
				}
				statement := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH (GROUP='inspection') WHERE RECORD_OFFSET < LATEST", output, input)
				ast, err := sql.NewParser().Parse(statement)
				require.NoError(t, err)
				p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source.ListenAddrs(), ",")}, "dst": {"bootstrap.servers": strings.Join(destination.ListenAddrs(), ",")}})
				require.NoError(t, err)
				cfg := config.Defaults("")
				cfg.Worker.ProgressTopic = "progress"
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
				inspector := (Inspection{Options: options}).service()
				connections := &inspectionConnections{Factory: inspector.kafka}
				inspector.kafka = connections
				status, err := inspector.Status(testContext(t), p, cfg.Worker, core.NoopStopObserver{})
				require.NoError(t, err)
				require.Equal(t, "stopped", status.Verdict)
				report, err := inspector.Explain(testContext(t), p, cfg.Worker, core.NoopStopObserver{})
				require.NoError(t, err)
				perCall := 0
				if src == "TOPIC" {
					perCall++
				}
				if dst == "TOPIC" {
					perCall++
				}
				require.Len(t, connections.opened, 2*perCall)
				for _, connection := range connections.opened {
					require.Equal(t, 1, connection.closes)
				}
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
				if dst == "ARCHIVE" {
					require.Equal(t, expected, archiveValues(t, outputRoot))
				} else {
					require.Equal(t, expected, recordValues(committedRecords(t, destination.ListenAddrs(), 2)))
				}
				if src == "ARCHIVE" {
					mirror, err := replay.ReadCheckpoint(directory, p.Group, replay.NativeGroupFiles{})
					require.NoError(t, err)
					require.NotNil(t, mirror)
					var count int64
					for _, n := range expected {
						count += int64(n)
					}
					require.Equal(t, count, mirror.Position)
					require.True(t, mirror.Complete)
				}
			})
		}
	}
}

type inspectionConnections struct {
	kafkaport.Factory
	opened                           []*countedInspectionConnection
	failOpen, failMetadata, failRead int
	failure, closeFailure            error
}

func (f *inspectionConnections) ForQuery(s *delivery.Session) kafkaport.Factory {
	f.Factory = f.Factory.ForQuery(s)
	return f
}

func (f *inspectionConnections) Open(ctx context.Context, e plan.Endpoint) (kafkaport.Connection, error) {
	index := len(f.opened) + 1
	if index == f.failOpen {
		return nil, f.failure
	}
	c, err := f.Factory.Open(ctx, e)
	if err != nil {
		return nil, err
	}
	view := &countedInspectionConnection{DeliveryConnection: c.(kafkaport.DeliveryConnection), InspectionConnection: c.(kafkaport.InspectionConnection), closeErr: f.closeFailure}
	if index == f.failMetadata {
		view.metadataErr = f.failure
	}
	if index == f.failRead {
		view.readErr = f.failure
	}
	f.opened = append(f.opened, view)
	return view, nil
}

type countedInspectionConnection struct {
	kafkaport.DeliveryConnection
	kafkaport.InspectionConnection
	closes                         int
	metadataErr, readErr, closeErr error
}

func (c *countedInspectionConnection) ClusterID(ctx context.Context) (string, error) {
	if c.metadataErr != nil {
		return "", c.metadataErr
	}
	return c.DeliveryConnection.ClusterID(ctx)
}
func (c *countedInspectionConnection) Describe(ctx context.Context, config bool) (kafkaport.TopicDescription, error) {
	if c.readErr != nil {
		return kafkaport.TopicDescription{}, c.readErr
	}
	return c.InspectionConnection.Describe(ctx, config)
}
func (c *countedInspectionConnection) CheckMembers(ctx context.Context, group, protocol string) error {
	return c.InspectionConnection.CheckMembers(ctx, group, protocol)
}
func (c *countedInspectionConnection) Close(ctx context.Context) error {
	c.closes++
	return errors.Join(c.DeliveryConnection.Close(ctx), c.closeErr)
}

func TestInspectionClosesSharedConnectionsOnceOnFailure(t *testing.T) {
	for _, stage := range []string{"second open", "second metadata", "destination describe", "close"} {
		t.Run(stage, func(t *testing.T) {
			// Given Kafka endpoints and a failure during acquisition, inspection or cleanup.
			source, destination := clusters(t, 1)
			p := query(t, source, destination, "WHERE RECORD_OFFSET < LATEST", "")
			inspector := (Inspection{Options: Options{ProgressTopic: "progress"}}).service()
			failure, closeFailure := errors.New("inspection failed"), errors.New("close failed")
			connections := &inspectionConnections{Factory: inspector.kafka, failure: failure, closeFailure: closeFailure}
			switch stage {
			case "second open":
				connections.failOpen = 2
			case "second metadata":
				connections.failMetadata = 2
			case "destination describe":
				connections.failRead = 2
			}
			inspector.kafka = connections
			// When explain fails, then every acquired connection closes once and all close errors survive.
			worker := config.Defaults("").Worker
			worker.ProgressTopic = "progress"
			_, err := inspector.Explain(testContext(t), p, worker, core.NoopStopObserver{})
			require.ErrorIs(t, err, closeFailure)
			if stage != "close" {
				require.ErrorIs(t, err, failure)
			}
			want := 2
			if stage == "second open" {
				want = 1
			}
			require.Len(t, connections.opened, want)
			for _, c := range connections.opened {
				require.Equal(t, 1, c.closes)
			}
		})
	}
}
