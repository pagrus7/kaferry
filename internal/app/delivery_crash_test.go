package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestStreamingCrashWorkerProcess(t *testing.T) {
	if os.Getenv("KAFERRY_STREAM_CHILD") == "" {
		return
	}
	ast, err := sql.NewParser().Parse(os.Getenv("KAFERRY_STREAM_SQL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var profiles plan.Profiles
	if err = json.Unmarshal([]byte(os.Getenv("KAFERRY_STREAM_PROFILES")), &profiles); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	p, err := plan.NewPlanner(environment{}).Build(ast, profiles)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg := config.Defaults("")
	cfg.Worker.RecordCap = 2
	cfg.Worker.PartitionRecordCap = 1
	cfg.Worker.ArchiveCaptureOpenFiles = 1
	directory := os.Getenv("KAFERRY_STREAM_STATE")
	cfg.Worker.CheckpointDir = &directory
	var data atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	err = Run(ctx, p, cfg, Options{ProgressTopic: "progress", Hook: func(point string, _ int32) {
		if point == "flushed" {
			data.Store(true)
		}
		if data.Load() && point == os.Getenv("KAFERRY_STREAM_CRASH") {
			os.Exit(86)
		}
	}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
func TestStreamingProcessCrashMatrix(t *testing.T) {
	for _, sourceKind := range []string{"TOPIC", "ARCHIVE"} {
		for _, destinationKind := range []string{"TOPIC", "ARCHIVE"} {
			points := []string{"prepared", "committed"}
			if destinationKind == "ARCHIVE" {
				points = append(points, "checkpointed", "published")
			}
			for _, point := range points {
				t.Run(sourceKind+"_to_"+destinationKind+"/"+point, func(t *testing.T) {
					// Given the streaming implementation with a small batch and durable progress surviving a process crash.
					source, destination := clusters(t, 2)
					input, target := "TOPIC 'input@src'", "TOPIC 'output@dst'"
					expected := map[string]int{}
					if sourceKind == "TOPIC" {
						seed(t, source, 2, 4)
						for part := range 2 {
							for offset := range 4 {
								expected[fmt.Sprintf("%d:%d", part, offset)] = 1
							}
						}
					} else {
						root := t.TempDir()
						expected = archiveInput(t, root)
						input = "ARCHIVE '" + root + "'"
					}
					outputRoot := ""
					if destinationKind == "ARCHIVE" {
						outputRoot = t.TempDir()
						target = "ARCHIVE '" + outputRoot + "' WITH(ROTATE_AFTER_SIZE=1000)"
					}
					statement := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH(GROUP='stream-crash-%d') WHERE RECORD_OFFSET < LATEST", target, input, time.Now().UnixNano())
					profiles := plan.Profiles{"src": {"bootstrap.servers": strings.Join(source, ","), "session.timeout.ms": "6000", "heartbeat.interval.ms": "500"}, "dst": {"bootstrap.servers": strings.Join(destination, ",")}}
					ast, err := sql.NewParser().Parse(statement)
					require.NoError(t, err)
					p, err := plan.NewPlanner(environment{}).Build(ast, profiles)
					require.NoError(t, err)
					directory := t.TempDir()
					encoded, err := json.Marshal(profiles)
					require.NoError(t, err)
					binary, err := os.Executable()
					require.NoError(t, err)
					// When the worker exits at a data-bearing commit boundary and another process resumes.
					command := exec.CommandContext(testContext(t), binary, "-test.run=^TestStreamingCrashWorkerProcess$")
					command.Env = append(os.Environ(), "KAFERRY_STREAM_CHILD=1", "KAFERRY_STREAM_SQL="+statement, "KAFERRY_STREAM_PROFILES="+string(encoded), "KAFERRY_STREAM_STATE="+directory, "KAFERRY_STREAM_CRASH="+point)
					logs, err := command.CombinedOutput()
					var exited *exec.ExitError
					require.ErrorAs(t, err, &exited, "%s", logs)
					require.Equal(t, 86, exited.ExitCode(), "%s", logs)
					if sourceKind == "TOPIC" {
						removeDeadMember(t, source, p.Group)
					}
					cfg := config.Defaults("")
					cfg.Worker.RecordCap = 2
					cfg.Worker.PartitionRecordCap = 1
					cfg.Worker.ArchiveCaptureOpenFiles = 1
					cfg.Worker.CheckpointDir = &directory
					require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
					require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
					// Then every selected physical occurrence remains exactly once in committed output.
					if destinationKind == "TOPIC" {
						require.Equal(t, expected, output(t, destination, 2))
					} else {
						require.Equal(t, expected, archiveValues(t, outputRoot))
					}
				})
			}
		}
	}
}
