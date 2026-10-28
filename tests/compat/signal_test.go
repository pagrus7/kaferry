//go:build integration

package compat

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testBinary(t *testing.T) string {
	t.Helper()
	path := os.Getenv("KAFERRY_TEST_BINARY")
	if path != "" {
		require.NoError(t, verifyBinary(path, os.Getenv("KAFERRY_EXPECT_BINARY_SHA256")))
		return path
	}
	path = filepath.Join(t.TempDir(), "kaferry")
	cmd := exec.Command("go", "build", "-trimpath", "-o", path, "../../cmd/kaferry")
	cmd.Env = append(cmd.Environ(), "CGO_ENABLED=0")
	if data, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, data)
	}
	require.NoError(t, verifyBinary(path, os.Getenv("KAFERRY_EXPECT_BINARY_SHA256")))
	return path
}

func verifyBinary(path, expected string) error {
	if expected == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != expected {
		return fmt.Errorf("test binary SHA-256 does not match the release artifact")
	}
	return nil
}

func TestExpectedBinaryHashRejectsSubstitution(t *testing.T) {
	// Given a test binary whose expected digest was recorded after archive extraction.
	path := filepath.Join(t.TempDir(), "binary")
	data := []byte("release binary")
	require.NoError(t, os.WriteFile(path, data, 0600))
	expected := fmt.Sprintf("%x", sha256.Sum256(data))
	require.NoError(t, verifyBinary(path, expected))
	// When the path is replaced, then the artifact tests reject the substitute.
	require.NoError(t, os.WriteFile(path, []byte("rebuilt binary"), 0600))
	require.ErrorContains(t, verifyBinary(path, expected), "does not match")
	require.Error(t, verifyBinary(path+"-missing", expected), "missing artifact must fail")
}
func waitEnd(t *testing.T, a *kadm.Client, topic string, end int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		result, err := a.ListEndOffsets(ctx, topic)
		if err == nil {
			if v, ok := result.Lookup(topic, 0); ok && v.Err == nil && int64(v.Offset) >= end {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("destination did not reach expected end", err)
		case <-tick.C:
		}
	}
}
func TestCLIInterruptCommitsAndResumeDoesNotRecopy(t *testing.T) {
	// Given
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	p := producer(t)
	binary := testBinary(t)
	group := src + "-signal"
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','topic.metadata.refresh.interval.ms'='500') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=LATEST) ", broker(t), dst, src, group)
	publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: []byte("before latest"), Timestamp: time.Now()})
	for run := range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		t.Cleanup(cancel)
		// When starting a copy with the saved group position.
		process := startCLI(t, ctx, binary, statement, "run", "-f", "-")
		waitMembers(t, a, group, 1, 1, nil)
		// Then the source position is saved before records arrive for START=LATEST.
		start := committedPartitions(t, a, group, src, 1)
		require.EqualValues(t, run*10+1, start[0].At, "initial source position")
		if run == 1 {

			// When
			signalErr := process.signal(syscall.SIGSTOP)

			require.NoError(t, signalErr)

			// When both topics gain a partition while the resumed worker is stopped.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			results, err := a.UpdatePartitions(ctx, 2, dst, src)
			cancel()

			// Then it joins the new assignment after continuing.
			require.NoError(t, err, "partition expansion")
			require.Len(t, results, 2)
			for _, name := range []string{dst, src} {
				require.NoError(t, results[name].Err, "expand %s", name)
				waitTopicReady(t, a, name, 2)
			}
			expanded := producer(t)
			publish(t, expanded, &kgo.Record{Topic: src, Partition: 1, Value: []byte("new partition before assignment"), Timestamp: time.Now()})
			require.NoError(t, process.signal(syscall.SIGCONT))
			waitMembers(t, a, group, 1, 2, nil)
		}

		// When new records arrive and the worker is interrupted after publishing them.
		for i := range 10 {
			publish(t, p, &kgo.Record{Topic: src, Partition: 0, Key: []byte("key"), Value: []byte(fmt.Sprintf("run-%d-%d", run, i)), Timestamp: time.Now()})
		}
		waitTopicCount(t, a, dst, run+1, int64((run+1)*10+run))
		require.NoError(t, process.signal(os.Interrupt))
		select {
		case <-process.done:
			if err := process.wait(); err != nil {
				t.Fatalf("interrupt exit: %v %s", err, process.logs())
			}
		case <-ctx.Done():
			t.Fatal("signal shutdown timed out")
		}

		// Then shutdown persists its confirmed progress.
		cancel()
		progress := committedPartitions(t, a, group, src, run+1)
		require.EqualValues(t, (run+1)*10+1, progress[0].At, "signal saved partition zero")
		if run == 1 {
			require.EqualValues(t, 1, progress[1].At, "signal saved new partition")
		}

	}
	counts := topicRecordCounts(t, a, dst, 2)
	for partition, want := range map[int32]int64{0: 20, 1: 1} {
		require.Equal(t, want, counts[partition], "clean restart retains original partition without recopying")
	}
}
