//go:build integration

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRecreatedDestinationRejectsResumeAndSupportsExplicitRecovery(t *testing.T) {
	broker(t)
	binary := testBinary(t)
	for _, recovery := range []string{"fresh group", "saved positions"} {
		t.Run(recovery, func(t *testing.T) {
			// Given two source partitions and a partially copied, durably checkpointed job.
			a := admin(t)
			source, destination := topic(t, a, 2), topic(t, a, 2)
			producer := producer(t)
			for partition := range int32(2) {
				for offset := range 4 {
					publish(t, producer, &kgo.Record{Topic: source, Partition: partition, Value: []byte(fmt.Sprintf("%d:%d", partition, offset))})
				}
			}
			config := configuration(t)
			group := source + "-original"
			query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s') WHERE RECORD_OFFSET < LATEST", destination, source, group)
			invoke := func(command, statement string, want int, extra ...string) string {
				t.Helper()
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				args := append([]string{command, "-e", statement, "-c", config}, extra...)
				process := startCLI(t, ctx, binary, "", args...)
				err := process.wait()
				code := 0
				if err != nil {
					var exit *exec.ExitError
					require.ErrorAs(t, err, &exit)
					code = exit.ExitCode()
				}
				require.NoError(t, ctx.Err(), process.logs())
				require.Equal(t, want, code, process.logs())
				return process.logs()
			}
			invoke("run", query+" OPTIONS(WORKER_STOP_AFTER_RECORDS=2)", 0)
			waitMembers(t, a, group, 0, 0, nil)
			before := committedPartitions(t, a, group, source, 2)
			require.Equal(t, int64(2), before[0].At+before[1].At)
			require.Equal(t, int64(2), topicRecordCount(t, a, destination, 2))
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			original, err := a.ListTopics(ctx, destination)
			require.NoError(t, err)
			deleted, err := a.DeleteTopics(ctx, destination)
			require.NoError(t, err)
			require.NoError(t, deleted[destination].Err)
			for {
				_, err = a.CreateTopic(ctx, 3, 1, nil, destination)
				if err == nil {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("replacement topic was not created", err)
				case <-time.After(20 * time.Millisecond):
				}
			}
			waitTopicReady(t, a, destination, 3)
			replacement, err := a.ListTopics(ctx, destination)
			require.NoError(t, err)
			require.NotEqual(t, original[destination].ID, replacement[destination].ID)

			// When the same job resumes against a replacement UUID, then no records or source progress change.
			require.Contains(t, invoke("run", query, 2), "query fingerprint mismatch")
			require.Equal(t, int64(0), topicRecordCount(t, a, destination, 3))
			require.Equal(t, before, committedPartitions(t, a, group, source, 2))
			var report inspect.StatusReport
			require.NoError(t, json.Unmarshal([]byte(invoke("status", query, 5, "--json")), &report))
			require.Equal(t, "unknown", report.Verdict)
			require.Contains(t, report.Activity, "idle twice")
			require.Len(t, report.Partitions, 2)
			for n, saved := range before {
				metadata, err := commitmeta.NewCodec().Decode(saved.Metadata)
				require.NoError(t, err)
				require.Equal(t, saved.Partition, report.Partitions[n].ID)
				require.Equal(t, new(saved.At), report.Partitions[n].Position)
				require.Equal(t, new(int64(4)), report.Partitions[n].End)
				require.Equal(t, metadata.Fingerprint, report.Partitions[n].Fingerprint)
				require.NotEqual(t, report.Fingerprint, report.Partitions[n].Fingerprint)
			}
			require.Equal(t, before, committedPartitions(t, a, group, source, 2))

			// When the client starts a new group, optionally translating the inspected positions into explicit bounds.
			var expected, branches []string
			for _, row := range report.Partitions {
				start := int64(0)
				if recovery == "saved positions" {
					start = *row.Position
					branches = append(branches, fmt.Sprintf("(PARTITION IN (%d) AND RECORD_OFFSET >= %d AND RECORD_OFFSET < %d)", row.ID, start, *row.End))
				}
				for offset := start; offset < 4; offset++ {
					expected = append(expected, fmt.Sprintf("%d:%d", row.ID, offset))
				}
			}
			recovered := strings.Replace(query, group, group+"-recovery", 1)
			if len(branches) > 0 {
				recovered = strings.Replace(recovered, "RECORD_OFFSET < LATEST", strings.Join(branches, " OR "), 1)
			}
			invoke("run", recovered, 0)
			// Then the replacement contains exactly the intended fresh or remaining records, and old progress stays intact.
			require.EqualValues(t, len(expected), topicRecordCount(t, a, destination, 3))
			reader := consumer(t)
			assign(t, reader, destination, 3)
			var actual []string
			for range expected {
				actual = append(actual, string(read(t, reader).Value))
			}
			require.ElementsMatch(t, expected, actual)
			require.Equal(t, before, committedPartitions(t, a, group, source, 2))
			var completed inspect.StatusReport
			require.NoError(t, json.Unmarshal([]byte(invoke("status", recovered, 0, "--json")), &completed))
			require.Equal(t, "complete", completed.Verdict)
		})
	}
}
