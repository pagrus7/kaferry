package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/inspect"
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
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		if _, err = w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	appendBytes([]byte{0, 0, 0})
	status := func(want string, code int) {
		t.Helper()
		before := fileTree(t, f.directory)
		var out, errout bytes.Buffer
		got := Execute(t.Context(), []string{"status", "-e", f.query, "-c", f.config, "--json"}, nil, &out, &errout)
		var report inspect.StatusReport
		if err := json.Unmarshal(out.Bytes(), &report); err != nil || got != code || report.Verdict != want {
			t.Fatal(got, out.String(), errout.String(), err)
		}
		if after := fileTree(t, f.directory); !reflect.DeepEqual(before, after) {
			t.Fatal("status changed files")
		}
	}
	status("unknown", 5)
	// When copying reaches EOF and saves its stop, including an unchanged source cursor.
	var diagnostics bytes.Buffer
	for range 2 {
		if code := Execute(t.Context(), []string{"run", "-e", f.query, "-c", f.config}, nil, io.Discard, &diagnostics); code != 0 {
			t.Fatal(code, diagnostics.String())
		}
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
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(directory, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
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
				if code := Execute(t.Context(), []string{"run", "-e", query, "-c", f.config}, nil, io.Discard, &diagnostics); code != 0 {
					t.Fatal(code, diagnostics.String())
				}
				files, err := filepath.Glob(filepath.Join(f.directory, "*.json"))
				if err != nil || len(files) != 1 {
					t.Fatal(files, err)
				}
				if err := os.WriteFile(files[0], []byte("broken"), 0600); err != nil {
					t.Fatal(err)
				}
				want = 5
			}
			before := fileTree(t, f.directory)
			var out, errout bytes.Buffer
			// When inspecting, then ONE_OFF is rejected with an actionable hint; corrupt state is unknown.
			code := Execute(t.Context(), []string{"status", "-e", query, "-c", f.config}, nil, &out, &errout)
			if code != want || mode == "ONE_OFF" && !strings.Contains(errout.String(), "logged group name") || mode == "corrupt" && !strings.Contains(out.String(), "Status: unknown") || !reflect.DeepEqual(before, fileTree(t, f.directory)) {
				t.Fatal(code, out.String(), errout.String())
			}
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
	query := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC 'output' SELECT * FROM TOPIC 'input' OPTIONS(GROUP='status',START=EARLIEST)", strings.Join(cluster.ListenAddrs(), ","))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var out, errout bytes.Buffer
	// When the complete Fx-composed status command inspects the group.
	code := Execute(ctx, []string{"status", "-e", query, "--json"}, nil, &out, &errout)
	// Then it reports no saved progress without joining, fetching, committing, producing or reading configs.
	var report inspect.StatusReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || code != 5 || report.Reason != "no saved progress" || writes.Load() != 0 {
		t.Fatal(code, out.String(), errout.String(), writes.Load(), err)
	}
}
