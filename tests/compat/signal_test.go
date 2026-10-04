//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func testBinary(t *testing.T) string {
	t.Helper()
	path := os.Getenv("KFWD_TEST_BINARY")
	if path != "" {
		return path
	}
	path = filepath.Join(t.TempDir(), "kafka-fwd2")
	cmd := exec.Command("go", "build", "-trimpath", "-o", path, "../../cmd/kafka-fwd2")
	if data, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, data)
	}
	return path
}
func waitEnd(t *testing.T, a *k.AdminClient, topic string, end int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		key := k.TopicPartition{Topic: &topic, Partition: 0}
		result, err := a.ListOffsets(ctx, map[k.TopicPartition]k.OffsetSpec{key: k.LatestOffsetSpec})
		if err == nil {
			if v, ok := testOffset(result, topic, 0); ok && v.Error.Code() == k.ErrNoError && int64(v.Offset) >= end {
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
	p := producer(t, nil)
	binary := testBinary(t)
	group := src + "-signal"
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','topic.metadata.refresh.interval.ms'='500') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=LATEST)", broker(t), dst, src, group)
	publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte("before latest"), Timestamp: time.Now()})
	for run := range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cmd := exec.CommandContext(ctx, binary, "run", "-f", "-")
		cmd.Stdin = strings.NewReader(statement)
		var output bytes.Buffer
		cmd.Stderr = &output
		if err := cmd.Start(); err != nil {
			cancel()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		finished := false
		t.Cleanup(func() {
			cancel()
			if !finished {
				_ = cmd.Process.Kill()
				<-done
			}
			if t.Failed() {
				t.Logf("copy process stderr: %s", output.String())
			}
		})
		waitMembers(t, a, group, 1, 1, nil)
		if run == 1 {

			// When
			signalErr := cmd.Process.Signal(syscall.SIGSTOP)

			// Then
			if signalErr != nil {
				t.Fatal(signalErr)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			results, err := a.CreatePartitions(ctx, []k.PartitionsSpecification{{Topic: src, IncreaseTo: 2}})
			cancel()
			if err != nil || results[0].Error.Code() != k.ErrNoError {
				t.Fatal("partition expansion", err, results)
			}
			if _, err := p.GetMetadata(&src, false, 5000); err != nil {
				t.Fatal("fixture metadata refresh", err)
			}
			publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 1}, Value: []byte("new partition before assignment"), Timestamp: time.Now()})
			if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
				t.Fatal(err)
			}
			waitMembers(t, a, group, 1, 2, nil)
		}
		for i := range 10 {
			publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Key: []byte("key"), Value: []byte(fmt.Sprintf("run-%d-%d", run, i)), Timestamp: time.Now()})
		}
		waitEnd(t, a, dst, int64((run+1)*10+run))
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			finished = true
			if err != nil {
				t.Fatalf("interrupt exit: %v %s", err, output.String())
			}
		case <-ctx.Done():
			t.Fatal("signal shutdown timed out")
		}
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		result, err := a.ListConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: []k.TopicPartition{{Topic: &src, Partition: 0}}}})
		cancel()
		if err != nil || len(result.ConsumerGroupsTopicPartitions) != 1 || int64(result.ConsumerGroupsTopicPartitions[0].Partitions[0].Offset) != int64((run+1)*10+1) {
			t.Fatal("signal did not persist confirmed progress", err, result)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := k.TopicPartition{Topic: &dst, Partition: 0}
	result, err := a.ListOffsets(ctx, map[k.TopicPartition]k.OffsetSpec{key: k.LatestOffsetSpec})
	info, ok := testOffset(result, dst, 0)
	if err != nil || !ok || info.Offset != 21 {
		t.Fatal("clean restart duplicated output", err, result)
	}
}

func testOffset(result k.ListOffsetsResult, topic string, partition int32) (k.ListOffsetsResultInfo, bool) {
	for key, info := range result.ResultInfos {
		if key.Topic != nil && *key.Topic == topic && key.Partition == partition {
			return info, true
		}
	}
	return k.ListOffsetsResultInfo{}, false
}
