package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func fileTree(t *testing.T, directory string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			out[relative+"/"] = ""
			return nil
		}
		data, err := os.ReadFile(path)
		if err == nil {
			out[relative] = string(data)
		}
		return err
	}))
	return out
}

func TestStatusRejectsOneOffAndCorruptFileProgress(t *testing.T) {
	for _, mode := range []string{"default", "ONE_OFF", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			// Given an anonymous group or corrupt saved progress.
			f := filePreview(t)
			query := f.query
			want := 2
			if mode == "ONE_OFF" {
				query = strings.Replace(query, "'preview'", "ONE_OFF", 1)
			} else if mode == "default" {
				query = strings.Replace(query, " WITH (GROUP='preview')", "", 1)
			} else {
				var diagnostics bytes.Buffer
				require.Zero(t, Execute(t.Context(), []string{"run", "-e", query, "-c", f.config}, nil, io.Discard, &diagnostics))
				files, err := filepath.Glob(filepath.Join(f.destination, ".kaferry", "writers", "*", "state.json"))
				require.NoError(t, err)
				require.Len(t, files, 1)
				require.NoError(t, os.WriteFile(files[0], []byte("broken"), 0600))
				want = 5
			}
			before := fileTree(t, f.directory)
			var out, errout bytes.Buffer
			// When inspecting, then ONE_OFF is rejected with an actionable hint; corrupt state is unknown.
			code := Execute(t.Context(), []string{"status", "-e", query, "-c", f.config}, nil, &out, &errout)
			require.Equal(t, want, code)
			if mode != "corrupt" {
				require.Contains(t, errout.String(), "logged group name")
			} else {
				require.Contains(t, out.String(), "Status: unknown")
			}
			require.Equal(t, fileTree(t, f.directory), before)
		})
	}
}

func TestStatusKafkaRequestsAreReadOnly(t *testing.T) {
	// Given a source topic, destination and a group without saved progress.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
	defer cluster.Close()
	topic(t, cluster.ListenAddrs(), "kaferry-progress", 4, true)
	var writes atomic.Int64
	for _, key := range []kmsg.Key{kmsg.Produce, kmsg.Fetch, kmsg.JoinGroup, kmsg.OffsetCommit, kmsg.ConsumerGroupHeartbeat, kmsg.DescribeConfigs} {
		cluster.ControlKey(int16(key), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			writes.Add(1)
			return nil, nil, false
		})
	}

	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, selfCopy := range []bool{false, true} {
		t.Run(fmt.Sprint(selfCopy), func(t *testing.T) {
			// Given either independent topics or a source and destination that resolve to the same topic.
			destination := "output"
			if selfCopy {
				destination = "input"
			}
			query := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC 'input' WITH (GROUP='status', START=EARLIEST) ", strings.Join(cluster.ListenAddrs(), ","), destination)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			before := fileTree(t, home)
			var out, errout bytes.Buffer
			// When the complete Fx-composed status command inspects the resolved endpoints.
			code := Execute(ctx, []string{"status", "-e", query, "--json"}, nil, &out, &errout)
			// Then self-copy is an input conflict, and inspections issue no execution requests or file writes.
			if selfCopy {
				require.Equal(t, 2, code)
				require.Empty(t, out.String())
				require.Contains(t, errout.String(), "source and destination are the same Kafka topic")
			} else {
				require.Equal(t, 5, code)
				require.Empty(t, errout.String())
				var report inspect.StatusReport
				require.NoError(t, json.Unmarshal(out.Bytes(), &report))
				require.Equal(t, "no saved progress", report.Reason)
			}
			require.Zero(t, writes.Load())
			require.Equal(t, before, fileTree(t, home))
		})
	}
}
