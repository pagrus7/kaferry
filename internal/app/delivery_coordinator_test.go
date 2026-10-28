package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/pagrus7/kaferry/internal/franz"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func capturePlan(t *testing.T, source []string, root, group string, follow bool) (plan.Plan, string) {
	t.Helper()
	predicate := "WHERE RECORD_OFFSET < LATEST"
	if follow {
		predicate = ""
	}
	text := fmt.Sprintf("INSERT INTO ARCHIVE '%s' WITH (ROTATE_AFTER_SIZE = 1000) SELECT * FROM TOPIC 'input@src' WITH (GROUP = '%s') %s;", root, group, predicate)
	ast, err := sql.NewParser().Parse(text)
	require.NoError(t, err)
	p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source, ","), "session.timeout.ms": "6000", "heartbeat.interval.ms": "500", "metadata.max.age.ms": "10"}})
	require.NoError(t, err)
	return p, text
}
func TestCaptureWorkerProcess(t *testing.T) {
	if os.Getenv("KAFERRY_CAPTURE_CHILD") == "" {
		return
	}
	ast, err := sql.NewParser().Parse(os.Getenv("KAFERRY_CAPTURE_SQL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": os.Getenv("KAFERRY_CAPTURE_BROKERS"), "session.timeout.ms": "6000", "heartbeat.interval.ms": "500", "metadata.max.age.ms": "10"}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg := config.Defaults("")
	cfg.Worker.RecordCap = 1
	cfg.Worker.PartitionRecordCap = 1
	var hasData atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	clients := franz.NewClients()
	if os.Getenv("KAFERRY_CAPTURE_OUTCOME") == "abort" {
		clients = abortDecisionClients{Clients: clients, prepared: &hasData}
	}
	err = Run(ctx, p, cfg, Options{Clients: clients, ProgressTopic: os.Getenv("KAFERRY_CAPTURE_PROGRESS"), Logger: slog.New(slog.NewJSONHandler(os.Stderr, nil)), Hook: func(point string, _ int32) {
		fmt.Fprintln(os.Stderr, "capture hook", point)
		if point == "flushed" {
			hasData.Store(true)
		}
		if gate := os.Getenv("KAFERRY_CAPTURE_GATE"); gate != "" && hasData.Load() && point == "prepared" {
			if err := os.WriteFile(gate+".ready", nil, 0600); err != nil {
				os.Exit(1)
			}
			for {
				if _, err := os.Stat(gate + ".continue"); err == nil {
					break
				}
				select {
				case <-ctx.Done():
					os.Exit(1)
				case <-time.After(time.Millisecond):
				}
			}
		}
		if hasData.Load() && point == os.Getenv("KAFERRY_CAPTURE_CRASH") {
			os.Exit(86)
		}
	}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
func removeDeadMember(t *testing.T, brokers []string, group string) {
	t.Helper()
	c := client(t, brokers)
	request := kmsg.NewPtrDescribeGroupsRequest()
	request.Groups = []string{group}
	response, err := request.RequestWith(testContext(t), c)
	require.NoError(t, err)
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group = group
	for _, g := range response.Groups {
		for _, member := range g.Members {
			leave.Members = append(leave.Members, kmsg.LeaveGroupRequestMember{MemberID: member.MemberID})
		}
	}
	if len(leave.Members) > 0 {
		_, err = leave.RequestWith(testContext(t), c)
		require.NoError(t, err)
	}
}
func TestIsolatedCaptureDisksAndIndependentRecovery(t *testing.T) {
	for _, point := range []string{"prepared", "coordinated", "published"} {
		t.Run(point, func(t *testing.T) {
			// Given two separate local disks and a broker surviving the first worker's crash.
			source, _ := clusters(t, 1)
			progress := fmt.Sprintf("capture-progress-%d", time.Now().UnixNano())
			topic(t, source, progress, 1, true)
			admin := kadm.NewClient(client(t, source))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, _ = admin.DeleteTopics(ctx, progress)
			})
			seed(t, source, 1, 6)
			firstRoot, secondRoot := t.TempDir(), t.TempDir()
			group := fmt.Sprintf("isolated-%s-%d", point, time.Now().UnixNano())
			first, statement := capturePlan(t, source, firstRoot, group, true)
			binary, err := os.Executable()
			require.NoError(t, err)
			command := exec.CommandContext(testContext(t), binary, "-test.run=^TestCaptureWorkerProcess$")
			command.Env = append(os.Environ(), "KAFERRY_CAPTURE_CHILD=1", "KAFERRY_CAPTURE_SQL="+statement, "KAFERRY_CAPTURE_BROKERS="+strings.Join(source, ","), "KAFERRY_CAPTURE_CRASH="+point, "KAFERRY_CAPTURE_PROGRESS="+progress)
			logs, err := command.CombinedOutput()
			var exited *exec.ExitError
			require.ErrorAs(t, err, &exited, "%s", logs)
			require.Equal(t, 86, exited.ExitCode(), "%s", logs)
			removeDeadMember(t, source, group)
			// When another host resumes without access to the old disk.
			second, _ := capturePlan(t, source, secondRoot, group, true)
			cfg := config.Defaults("")
			cfg.Worker.RecordCap = 1
			cfg.Worker.PartitionRecordCap = 1
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			paused, resume := make(chan struct{}), make(chan struct{})
			var hasData, stoppedOnce atomic.Bool
			released, finished := false, false
			done := make(chan error, 1)
			go func() {
				done <- Run(ctx, second, cfg, Options{ProgressTopic: progress, Hook: func(event string, _ int32) {
					if event == "flushed" {
						hasData.Store(true)
					}
					if event == "checkpoint-chunks" && hasData.Load() && stoppedOnce.CompareAndSwap(false, true) {
						close(paused)
						<-resume
					}
				}})
			}()
			t.Cleanup(func() {
				if !released {
					close(resume)
				}
				cancel()
				if !finished {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("replacement did not stop")
					}
				}
			})
			select {
			case <-paused:
			case <-time.After(10 * time.Second):
				t.Fatal("replacement did not pause between checkpoint chunks and head")
			}
			// The first host recovers while the replacement still owns the source partition.
			if binary := os.Getenv("KAFERRY_LAB_BINARY"); binary != "" && point == "coordinated" {
				path := filepath.Join(t.TempDir(), "recovery.toml")
				require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf("[connections.src]\n'bootstrap.servers'=%q\n", strings.Join(source, ","))), 0600))
				command := exec.CommandContext(testContext(t), binary, "--archive-coordinator", "--recover-local", "--progress-topic", progress, "--", "run", "--config", path, "--file", "-")
				command.Stdin = strings.NewReader(statement)
				logs, err := command.CombinedOutput()
				require.NoError(t, err, "%s", logs)
			} else {
				require.NoError(t, RecoverLocal(testContext(t), first, cfg, Options{ProgressTopic: progress}))
			}

			close(resume)
			released = true
			// A new record proves recovery did not fence or rewind the active replacement.
			seedClient := client(t, source)
			require.NoError(t, seedClient.ProduceSync(testContext(t), recordFor("input", 0, "after-recovery")).FirstErr())
			require.Eventually(t, func() bool {
				values, _, err := scanArchive(secondRoot)
				return err == nil && values["after-recovery"] == 1
			}, 10*time.Second, 20*time.Millisecond)
			cancel()
			workerErr := <-done
			finished = true
			require.NoError(t, workerErr)
			// Then collecting completed files from both disks yields each input occurrence once.
			combined := archiveValues(t, firstRoot)
			for value, count := range archiveValues(t, secondRoot) {
				combined[value] += count
			}
			require.Len(t, combined, 7)
			for offset := range 6 {
				require.Equal(t, 1, combined[fmt.Sprintf("0:%d", offset)])
			}
			require.Equal(t, 1, combined["after-recovery"])
		})
	}
}

func recordFor(topic string, part int32, value string) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: part, Value: []byte(value), Timestamp: time.Now()}
}

func TestLocalRecoveryRejectsUnresolvedDeadProducer(t *testing.T) {
	// Given a dead capture process whose data-bearing EndTxn has not been applied.
	source := kfake.MustCluster(kfake.NumBrokers(1))
	t.Cleanup(source.Close)
	topic(t, source.ListenAddrs(), "input", 1, false)
	topic(t, source.ListenAddrs(), "progress", 1, true)
	seed(t, source.ListenAddrs(), 1, 6)
	root := t.TempDir()
	p, statement := capturePlan(t, source.ListenAddrs(), root, "unresolved-capture", false)
	gate := filepath.Join(t.TempDir(), "checkpoint")
	var armed atomic.Bool
	blocked, release := make(chan struct{}), make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	source.ControlKey(int16(kmsg.EndTxn), func(request kmsg.Request) (kmsg.Response, error, bool) {
		source.KeepControl()
		if request.(*kmsg.EndTxnRequest).Commit && armed.CompareAndSwap(true, false) {
			close(blocked)
			source.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})
	binary, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(testContext(t), binary, "-test.run=^TestCaptureWorkerProcess$")
	command.Env = append(os.Environ(), "KAFERRY_CAPTURE_CHILD=1", "KAFERRY_CAPTURE_SQL="+statement, "KAFERRY_CAPTURE_BROKERS="+strings.Join(source.ListenAddrs(), ","), "KAFERRY_CAPTURE_PROGRESS=progress", "KAFERRY_CAPTURE_GATE="+gate)
	var logs bytes.Buffer
	command.Stdout, command.Stderr = &logs, &logs
	require.NoError(t, command.Start())
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = command.Process.Kill()
			<-done
		}
		if t.Failed() {
			t.Logf("capture process: %s", logs.String())
		}
	})
	require.Eventually(t, func() bool { _, err := os.Stat(gate + ".ready"); return err == nil }, 10*time.Second, 10*time.Millisecond)
	armed.Store(true)
	require.NoError(t, os.WriteFile(gate+".continue", nil, 0600))
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("data checkpoint EndTxn did not reach the broker")
	}
	require.NoError(t, command.Process.Kill())
	require.Error(t, <-done)
	waited = true
	before := captureDiskSnapshot(t, root)
	cfg := config.Defaults("")
	// When publication-only recovery runs before any replacement has fenced A.
	recoveryErr := RecoverLocal(testContext(t), p, cfg, Options{ProgressTopic: "progress"})
	// Then it refuses without changing disk, even though the lock is now available.
	require.Equal(t, before, captureDiskSnapshot(t, root))
	require.ErrorContains(t, recoveryErr, "replacement capture owner")
	// When the delayed broker commit completes, ordinary resume fences before recovering.
	paths, err := filepath.Glob(filepath.Join(root, ".kaferry", "writers", "*", "state.json"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	data, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	envelope, err := checkpoint.Decode[archiveEnvelope](data, 96<<20)
	require.NoError(t, err)
	j := productionReader(t, p, cfg, envelope.Identity, "progress")
	close(release)
	released = true
	require.Eventually(t, func() bool {
		frame, err := j.Read(testContext(t), 0)
		return err == nil && frame != nil && frame.Cursor.Position == 1
	}, 5*time.Second, 10*time.Millisecond)
	removeDeadMember(t, source.ListenAddrs(), p.Group)
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	expected := map[string]int{}
	for offset := range 6 {
		expected[fmt.Sprintf("0:%d", offset)] = 1
	}
	require.Equal(t, expected, archiveValues(t, root))
}

func captureDiskSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[name] = delivery.Hash(string(data))
		return nil
	}))
	return result
}

type abortDecisionClients struct {
	franz.Clients
	prepared *atomic.Bool
}

func (c abortDecisionClients) Transaction(opts ...kgo.Opt) (franz.TransactionClient, error) {
	client, err := c.Clients.Transaction(opts...)
	if err != nil {
		return nil, err
	}
	return abortDecision{TransactionClient: client, prepared: c.prepared}, nil
}

type abortDecision struct {
	franz.TransactionClient
	prepared *atomic.Bool
}

func (c abortDecision) EndTransaction(ctx context.Context, outcome kgo.TransactionEndTry) error {
	if outcome == kgo.TryCommit && c.prepared.Load() {
		if err := c.TransactionClient.EndTransaction(ctx, kgo.TryAbort); err != nil {
			return err
		}
		return errors.New("fixture coordinator decided abort")
	}
	return c.TransactionClient.EndTransaction(ctx, outcome)
}
