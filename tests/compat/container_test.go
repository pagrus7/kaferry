//go:build integration && container

package compat

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestContainerShutdownAndResume(t *testing.T) {
	// Given a container image and a copy group that saves progress in Kafka.
	image := os.Getenv("KAFERRY_TEST_IMAGE")
	require.NotEmpty(t, image, "set KAFERRY_TEST_IMAGE to the tested image")
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	producer := producer(t)
	group := src + "-container"
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s')", broker(t), dst, src, group)
	for round := range 2 {
		name := fmt.Sprintf("%s-%d", group, round)
		command := exec.Command("docker", "run", "--rm", "-i", "--network=host", "--name", name, image, "run", "--file", "-")
		command.Stdin = strings.NewReader(statement)
		var logs bytes.Buffer
		command.Stdout, command.Stderr = &logs, &logs
		require.NoError(t, command.Start())
		t.Cleanup(func() {
			if command.ProcessState == nil {
				_ = exec.Command("docker", "rm", "--force", name).Run()
				_ = command.Wait()
			}
		})
		waitMembers(t, a, group, 1, 1, nil)
		publish(t, producer, &kgo.Record{Topic: src, Value: []byte(fmt.Sprintf("round-%d", round)), Timestamp: time.Now()})
		waitEnd(t, a, dst, int64(round+1))
		// When Docker sends SIGTERM and waits for the process to stop.
		stop := exec.Command("docker", "stop", "--time", "15", name)
		data, err := stop.CombinedOutput()
		require.NoError(t, err, "%s", data)
		require.NoError(t, command.Wait(), "%s", logs.String())
		// Then the next container resumes after exactly the confirmed record.
		progress := committedPartitions(t, a, group, src, 1)
		require.EqualValues(t, round+1, progress[0].At)
		require.EqualValues(t, round+1, topicRecordCount(t, a, dst, 1), "clean restart must not recopy")
	}
}
