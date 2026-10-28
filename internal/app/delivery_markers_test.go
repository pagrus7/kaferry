package app

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// markerNetwork delays actual inter-broker WriteTxnMarkers requests. Client
// traffic still reaches brokers directly, including coordinator decisions and
// read_committed fetches. Releasing the gate applies the original broker frames.
type markerNetwork struct {
	armed       atomic.Bool
	seen        chan struct{}
	release     chan struct{}
	once        sync.Once
	mu          sync.Mutex
	connections []net.Conn
	listeners   []net.Listener
	workers     sync.WaitGroup
}

func newMarkerNetwork(t *testing.T) *markerNetwork {
	t.Helper()
	g := &markerNetwork{seen: make(chan struct{}, 8), release: make(chan struct{})}
	for id := 1; id <= 3; id++ {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", 48100+id))
		require.NoError(t, err)
		g.listeners = append(g.listeners, listener)
		g.workers.Go(func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				g.mu.Lock()
				g.connections = append(g.connections, conn)
				g.mu.Unlock()
				g.workers.Go(func() { g.forward(conn, fmt.Sprintf("127.0.0.1:%d", 48110+id)) })
			}
		})
	}
	t.Cleanup(func() {
		g.unblock()
		for _, l := range g.listeners {
			_ = l.Close()
		}
		g.mu.Lock()
		for _, c := range g.connections {
			_ = c.Close()
		}
		g.mu.Unlock()
		g.workers.Wait()
	})
	return g
}
func (g *markerNetwork) unblock() { g.once.Do(func() { close(g.release) }) }
func (g *markerNetwork) forward(front net.Conn, target string) {
	defer front.Close()
	back, err := net.DialTimeout("tcp", target, time.Second)
	if err != nil {
		return
	}
	defer back.Close()
	g.mu.Lock()
	g.connections = append(g.connections, back)
	g.mu.Unlock()
	finished := make(chan struct{})
	go func() { _, _ = io.Copy(front, back); _ = front.Close(); close(finished) }()
	defer func() { _ = back.Close(); <-finished }()
	for {
		var prefix [4]byte
		if _, err = io.ReadFull(front, prefix[:]); err != nil {
			return
		}
		size := int(binary.BigEndian.Uint32(prefix[:]))
		if size < 8 || size > 100<<20 {
			return
		}
		body := make([]byte, size)
		if _, err = io.ReadFull(front, body); err != nil {
			return
		}
		if binary.BigEndian.Uint16(body[:2]) == uint16(kmsg.WriteTxnMarkers) && g.armed.Load() {
			select {
			case g.seen <- struct{}{}:
			default:
			}
			g.mu.Lock()
			release := g.release
			g.mu.Unlock()
			<-release
		}
		if _, err = back.Write(prefix[:]); err != nil {
			return
		}
		if _, err = back.Write(body); err != nil {
			return
		}
	}
}
func markerCompose(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-p", "kaferry-marker-qualification", "-f", "../../tests/transactions/compose.yaml"}, args...)...)
	logs, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", logs)
}
func TestCoordinatorDecidedCaptureRecoveryWithDelayedMarkers(t *testing.T) {
	network, brokers := startMarkerFixture(t)
	for _, commit := range []bool{true, false} {
		t.Run(map[bool]string{true: "commit", false: "abort"}[commit], func(t *testing.T) {
			// Given an acknowledged prefix on disk and one more synced capture batch.
			topic(t, brokers, "input", 1, false)
			topic(t, brokers, "progress", 1, true)
			admin := kadm.NewClient(client(t, brokers))
			t.Cleanup(func() { _, _ = admin.DeleteTopics(context.Background(), "input", "progress") })
			seed(t, brokers, 1, 6)
			oldRoot, newRoot := t.TempDir(), t.TempDir()
			group := fmt.Sprintf("marker-%t-%d", commit, time.Now().UnixNano())
			p, statement := capturePlan(t, brokers, oldRoot, group, true)
			cfg := config.Defaults("")
			cfg.Worker.RecordCap = 1
			cfg.Worker.PartitionRecordCap = 1
			p.WorkerStopAfterRecords = 2
			require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
			p.WorkerStopAfterRecords = 0
			acknowledged := captureDiskSnapshot(t, oldRoot)
			binaryPath, err := os.Executable()
			require.NoError(t, err)
			gate := filepath.Join(t.TempDir(), "prepare")
			command := exec.CommandContext(t.Context(), binaryPath, "-test.run=^TestCaptureWorkerProcess$")
			command.Env = append(os.Environ(), "KAFERRY_CAPTURE_CHILD=1", "KAFERRY_CAPTURE_SQL="+statement, "KAFERRY_CAPTURE_BROKERS="+strings.Join(brokers, ","), "KAFERRY_CAPTURE_PROGRESS=progress", "KAFERRY_CAPTURE_GATE="+gate, "KAFERRY_CAPTURE_OUTCOME="+map[bool]string{true: "commit", false: "abort"}[commit])
			var logs bytes.Buffer
			command.Stderr = &logs
			require.NoError(t, command.Start())
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			waited := false
			t.Cleanup(func() {
				if !waited {
					_ = command.Process.Kill()
					<-done
				}
			})
			require.Eventually(t, func() bool { _, err := os.Stat(gate + ".ready"); return err == nil }, 15*time.Second, 10*time.Millisecond)
			network.armed.Store(true)
			require.NoError(t, os.WriteFile(gate+".continue", nil, 0600))
			select {
			case <-network.seen:
			case <-time.After(10 * time.Second):
				t.Fatal("coordinator did not send transaction markers")
			}
			identity := captureIdentity(t, p)
			s := delivery.New(p, cfg.Worker)
			s.Identity = identity
			txid := s.TransactionID(0, "progress")
			decision := "PrepareCommit"
			if !commit {
				decision = "PrepareAbort"
			}
			require.Eventually(t, func() bool {
				result, err := admin.DescribeTransactions(testContext(t), txid)
				return err == nil && result[txid].State == decision
			}, 5*time.Second, 10*time.Millisecond, "coordinator must have durably decided the requested outcome")
			t.Logf("coordinator=%s; transaction markers held; prior acknowledged cursor=2", decision)
			if err := command.Process.Kill(); err != nil {
				require.ErrorIs(t, err, os.ErrProcessDone)
			}
			require.Error(t, <-done, "%s", logs.String())
			waited = true
			before := captureDiskSnapshot(t, oldRoot)
			// When marker application is stalled, neither publication nor takeover may use a partial authority prefix.
			recoveryCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			started := time.Now()
			err = RecoverLocal(recoveryCtx, p, cfg, Options{ProgressTopic: "progress"})
			cancel()
			require.Error(t, err)
			require.Less(t, time.Since(started), 3*time.Second)
			require.Equal(t, before, captureDiskSnapshot(t, oldRoot))
			removeDeadMember(t, brokers, group)
			replacement, _ := capturePlan(t, brokers, newRoot, group, true)
			replacement.WorkerStopAfterRecords = 4
			if commit {
				replacement.WorkerStopAfterRecords = 3
			}
			recoveryCtx, cancel = context.WithTimeout(t.Context(), time.Second)
			err = Run(recoveryCtx, replacement, cfg, Options{ProgressTopic: "progress"})
			cancel()
			require.Error(t, err)
			require.Equal(t, before, captureDiskSnapshot(t, oldRoot))
			// When the actual commit/abort markers apply, then both disks converge to one output per occurrence.
			network.armed.Store(false)
			network.unblock()
			require.NoError(t, Run(testContext(t), replacement, cfg, Options{ProgressTopic: "progress"}), "child: %s", logs.String())
			cursorBefore := productionReader(t, replacement, cfg, identity, "progress")
			frame, err := cursorBefore.Read(testContext(t), 0)
			require.NoError(t, err)
			require.EqualValues(t, 6, frame.Cursor.Position)
			require.NoError(t, RecoverLocal(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
			frame, err = cursorBefore.Read(testContext(t), 0)
			require.NoError(t, err)
			require.EqualValues(t, 6, frame.Cursor.Position, "publication acknowledgements cannot change the global cursor")
			got := archiveValues(t, oldRoot)
			for value, n := range archiveValues(t, newRoot) {
				got[value] += n
			}
			expected := map[string]int{}
			for offset := range 6 {
				expected[fmt.Sprintf("0:%d", offset)] = 1
			}
			require.Equal(t, expected, got)
			t.Log("six committed occurrences across both disks; global cursor=6 before/after publication recovery")
			after := captureDiskSnapshot(t, oldRoot)
			for path, digest := range acknowledged {
				if strings.HasSuffix(path, ".avro") {
					require.Equal(t, digest, after[path], "acknowledged file bytes changed")
				}
			}
		})
		// Each arm needs a fresh gate; the forwarding goroutines read this same release channel only while armed.
		if commit {
			network.mu.Lock()
			network.once = sync.Once{}
			network.release = make(chan struct{})
			network.mu.Unlock()
			for len(network.seen) > 0 {
				<-network.seen
			}
		}
	}
}
func captureIdentity(t *testing.T, p plan.Plan) delivery.Identity {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(p.Destination.Name, ".kaferry", "writers", "*", "state.json"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	data, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	e, err := checkpoint.Decode[archiveEnvelope](data, 96<<20)
	require.NoError(t, err)
	return e.Identity
}

func startMarkerFixture(t *testing.T) (*markerNetwork, []string) {
	t.Helper()
	if os.Getenv("KAFERRY_MARKER_FIXTURE") != "1" {
		t.Skip("set KAFERRY_MARKER_FIXTURE=1 for the disposable replicated transaction fixture")
	}
	network := newMarkerNetwork(t)
	t.Cleanup(func() {
		network.unblock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "docker", "compose", "-p", "kaferry-marker-qualification", "-f", "../../tests/transactions/compose.yaml", "logs", "--no-color")
		brokerLogs, logErr := cmd.CombinedOutput()
		if logErr != nil {
			t.Errorf("fixture logs: %v", logErr)
		}
		if directory := os.Getenv("KAFERRY_REPORT_DIR"); directory != "" {
			if err := os.WriteFile(filepath.Join(directory, "transaction-"+t.Name()+"-brokers.log"), brokerLogs, 0600); err != nil {
				t.Errorf("save fixture logs: %v", err)
			}
		}
		cmd = exec.CommandContext(ctx, "docker", "compose", "-p", "kaferry-marker-qualification", "-f", "../../tests/transactions/compose.yaml", "down", "-v")
		logs, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("fixture cleanup: %v %s", err, logs)
		}
	})
	markerCompose(t, "up", "-d", "--wait", "--wait-timeout", "90")
	brokers := []string{"127.0.0.1:45092", "127.0.0.1:46092", "127.0.0.1:47092"}
	return network, brokers
}

func TestReplicatedCopyContinuesAcrossCoordinatorOutage(t *testing.T) {
	_, brokers := startMarkerFixture(t)
	admin := kadm.NewClient(client(t, brokers))
	created, err := admin.CreateTopics(testContext(t), 1, 3, map[string]*string{"min.insync.replicas": kadm.StringPtr("2")}, "input", "output")
	require.NoError(t, err)
	require.NoError(t, created.Error())
	created, err = admin.CreateTopics(testContext(t), 1, 3, map[string]*string{"min.insync.replicas": kadm.StringPtr("2"), "cleanup.policy": kadm.StringPtr("compact")}, "progress")
	require.NoError(t, err)
	require.NoError(t, created.Error())
	awaitReplicatedTopics(t, brokers, nil, "input", "output", "progress")
	// Given a running copy with an acknowledged sixteen-record prefix.
	seed(t, brokers, 1, 16)
	p := query(t, brokers, brokers, "", "")
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	var logs bytes.Buffer
	var prepared atomic.Bool
	ready := make(chan struct{})
	var readyOnce sync.Once
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, p, config.Defaults(""), Options{ProgressTopic: "progress", Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Hook: func(point string, _ int32) {
			if point == "flushed" {
				prepared.Store(true)
			}
			if point == "committed" && prepared.Load() {
				readyOnce.Do(func() { close(ready) })
			}
		}})
	}()
	finished := false
	t.Cleanup(func() {
		cancel()
		if !finished {
			<-done
		}
	})
	select {
	case <-ready:
	case err := <-done:
		finished = true
		t.Fatalf("worker stopped before initial commit: %v %s", err, logs.String())
	case <-ctx.Done():
		t.Fatal("initial output was not committed")
	}
	metadata, err := admin.BrokerMetadata(testContext(t))
	require.NoError(t, err)
	identity := delivery.Identity{SourceCluster: metadata.Cluster}
	session := delivery.New(p, config.Defaults("").Worker)
	session.Identity = identity
	id := session.TransactionID(0, "progress")
	coordinator := admin.FindTxnCoordinators(testContext(t), id)[id]
	require.NoError(t, coordinator.Err)
	container := fmt.Sprintf("kaferry-marker-qualification-marker-%d-1", coordinator.NodeID)
	require.Contains(t, []int32{1, 2, 3}, coordinator.NodeID)
	// When its transaction coordinator stays down while more selected records arrive.
	data, err := exec.Command("docker", "stop", "-t", "1", container).CombinedOutput()
	require.NoError(t, err, "%s", data)
	t.Cleanup(func() {
		data, err := exec.Command("docker", "start", container).CombinedOutput()
		require.NoError(t, err, "%s", data)
	})
	producer := client(t, brokers)
	for offset := 16; offset < 64; offset++ {
		require.NoError(t, producer.ProduceSync(testContext(t), recordFor("input", 0, fmt.Sprintf("0:%d", offset))).FirstErr())
	}
	reader := client(t, brokers, kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"output": {0: kgo.NewOffset().AtStart()}}))
	got := map[string]int{}
	for len(got) < 64 {
		select {
		case err := <-done:
			finished = true
			t.Fatalf("worker stopped during coordinator outage: %v %s", err, logs.String())
		default:
		}
		poll, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		fetch := reader.PollRecords(poll, 100)
		stop()
		if err := fetch.Err(); err != nil && err != context.DeadlineExceeded {
			require.NoError(t, err)
		}
		for _, record := range fetch.Records() {
			got[string(record.Value)]++
		}
		require.NoError(t, ctx.Err())
	}
	cancel()
	err = <-done
	finished = true
	require.NoError(t, err, "%s", logs.String())
	// Then every selected occurrence is committed once without restarting the worker.
	for offset := range 64 {
		require.Equal(t, 1, got[fmt.Sprintf("0:%d", offset)])
	}
}
