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

func TestFileStatusObservesSavedStopsWithoutChangingFiles(t *testing.T) {
	// Given a file whose partial final frame already exists when copying starts.
	f := filePreview(t)
	source := filepath.Join(f.directory, "source.cbor")
	appendBytes := func(data []byte) {
		t.Helper()
		w, err := os.OpenFile(source, os.O_APPEND|os.O_WRONLY, 0600)
		require.NoError(t, err)
		defer w.Close()
		_, err = w.Write(data)
		require.NoError(t, err)
	}
	appendBytes([]byte{0, 0, 0})
	status := func(want string, code int) {
		t.Helper()
		before := fileTree(t, f.directory)
		var out, errout bytes.Buffer
		got := Execute(t.Context(), []string{"status", "-e", f.query, "-c", f.config, "--json"}, nil, &out, &errout)
		var report inspect.StatusReport
		require.NoError(t, json.Unmarshal(out.Bytes(), &report))
		require.Equal(t, code, got)
		require.Equal(t, want, report.Verdict)
		require.Equal(t, fileTree(t, f.directory), before)
	}
	status("unknown", 5)
	// When copying reaches EOF and saves its stop, including an unchanged source cursor.
	var diagnostics bytes.Buffer
	for range 2 {
		require.Zero(t, Execute(t.Context(), []string{"run", "-e", f.query, "-c", f.config}, nil, io.Discard, &diagnostics))
		// Then the saved opening size, rather than the shorter EOF position, yields stopped.
		status("stopped", 0)
	}
	// When bytes are appended after that stop, then the saved stop no longer covers the current file.
	appendBytes([]byte{0})
	status("incomplete", 4)
}
func fileTree(t *testing.T, directory string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	out := map[string]string{}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(directory, e.Name()))
		require.NoError(t, err)
		out[e.Name()] = string(data)
	}
	return out
}

func TestStatusRejectsOneOffAndCorruptFileProgress(t *testing.T) {
	for _, mode := range []string{"ONE_OFF", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			// Given an anonymous group or corrupt saved progress.
			f := filePreview(t)
			query := f.query
			want := 2
			if mode == "ONE_OFF" {
				query = strings.Replace(query, "'preview'", "ONE_OFF", 1)
			} else {
				var diagnostics bytes.Buffer
				require.Zero(t, Execute(t.Context(), []string{"run", "-e", query, "-c", f.config}, nil, io.Discard, &diagnostics))
				files, err := filepath.Glob(filepath.Join(f.directory, "*.json"))
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
			if mode == "ONE_OFF" {
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
			query := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC 'input' OPTIONS(GROUP='status',START=EARLIEST)", strings.Join(cluster.ListenAddrs(), ","), destination)
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
