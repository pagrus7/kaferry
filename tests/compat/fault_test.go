//go:build integration && fault

package compat

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ownedContainer(t *testing.T) string {
	t.Helper()
	name := os.Getenv("KAFKA_FAULT_CONTAINER")
	if name == "" {
		t.Skip("set KAFKA_FAULT_CONTAINER for broker pause tests")
	}
	data, err := exec.Command("docker", "inspect", "--format", "{{index .Config.Labels \"com.docker.compose.project\"}}", name).Output()
	if err != nil || strings.TrimSpace(string(data)) != "kaferry-compat" {
		t.Fatal("fault tests require the project's disposable compose broker")
	}
	return name
}
func TestBlockingCommitAcrossBrokerOutage(t *testing.T) {
	// Given
	container := ownedContainer(t)
	a := admin(t)
	name := topic(t, a, 1)
	c, err := k.NewConsumer(&k.ConfigMap{"bootstrap.servers": broker(t), "group.id": name + "-fault", "enable.auto.commit": false, "session.timeout.ms": 6000, "socket.timeout.ms": 1000, "fetch.wait.max.ms": 0, "partition.assignment.strategy": "cooperative-sticky"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	assigned := false
	if err := c.Subscribe(name, func(c *k.Consumer, e k.Event) error {
		switch e := e.(type) {
		case k.AssignedPartitions:
			assigned = true
			return c.IncrementalAssign(e.Partitions)
		case k.RevokedPartitions:
			return c.IncrementalUnassign(e.Partitions)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !assigned && time.Now().Before(deadline) {
		c.Poll(50)
	}
	if !assigned {
		t.Fatal("assignment timed out")
	}

	// When
	combinedOutputData, combinedOutputErr := exec.Command("docker", "pause", container).CombinedOutput()

	// Then
	if combinedOutputErr != nil {
		t.Fatalf("pause: %v %s", combinedOutputErr, combinedOutputData)
	}
	unpaused := false
	defer func() {
		if !unpaused {
			_ = exec.Command("docker", "unpause", container).Run()
		}
	}()
	type result struct {
		parts    []k.TopicPartition
		err      error
		duration time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		parts, err := c.CommitOffsets([]k.TopicPartition{{Topic: &name, Partition: 0, Offset: 0}})
		done <- result{parts, err, time.Since(start)}
	}()
	var early *result
	select {
	case r := <-done:
		early = &r
	case <-time.After(2500 * time.Millisecond):
	}
	if data, err := exec.Command("docker", "unpause", container).CombinedOutput(); err != nil {
		t.Fatalf("unpause: %v %s", err, data)
	}
	unpaused = true
	var r result
	if early != nil {
		r = *early
	} else {
		select {
		case r = <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("blocking commit did not return after broker recovery")
		}
	}
	t.Logf("CommitOffsets elapsed=%s err=%v per-partition=%+v (socket.timeout.ms=1000; broker paused 2.5s)", r.duration, r.err, r.parts)
	if r.err == nil {
		for _, p := range r.parts {
			if p.Error != nil {
				t.Log(fmt.Sprintf("partition commit error: %v", p.Error))
			}
		}
	}
}

func TestAssignmentLostOnPollDeadline(t *testing.T) {
	// Given
	a := admin(t)
	name := topic(t, a, 1)
	c, err := k.NewConsumer(&k.ConfigMap{"bootstrap.servers": broker(t), "group.id": name + "-lost", "enable.auto.commit": false, "session.timeout.ms": 6000, "max.poll.interval.ms": 6000, "partition.assignment.strategy": "cooperative-sticky"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	assigned, lost := false, false
	_ = c.Subscribe(name, func(c *k.Consumer, e k.Event) error {
		switch e := e.(type) {
		case k.AssignedPartitions:
			assigned = true
			return c.IncrementalAssign(e.Partitions)
		case k.RevokedPartitions:
			lost = c.AssignmentLost()
			return c.IncrementalUnassign(e.Partitions)
		}
		return nil
	})
	deadline := time.Now().Add(10 * time.Second)
	for !assigned && time.Now().Before(deadline) {

		// When
		c.Poll(20)
	}

	// Then
	if !assigned {
		t.Fatal("assignment timed out")
	}
	<-time.After(7 * time.Second)
	deadline = time.Now().Add(5 * time.Second)
	for !lost && time.Now().Before(deadline) {
		c.Poll(20)
	}
	if !lost {
		t.Fatal("lost ownership was not exposed during revocation")
	}
}
