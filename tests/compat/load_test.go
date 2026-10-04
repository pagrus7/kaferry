//go:build integration && load

package compat

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestLoadBoundedKafkaCopy(t *testing.T) {
	// Given default caps and a small-cap slow-destination scenario.
	scenarios := []struct {
		name           string
		count, records int
		bytes          int64
		config         string
	}{
		{"default", 16000, 20000, 128 << 20, ""},
		{"slow destination small caps", 128, 32, 128 << 10, "[worker]\nrecord_cap=32\nbyte_cap=131072\npartition_record_cap=8\npartition_byte_cap=32768\n"},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			count, recordCap, byteCap, configuration := scenario.count, scenario.records, scenario.bytes, scenario.config
			// Given seeded topics, a compiled CLI and worker limits.
			a := admin(t)
			src, dst := topic(t, a, 4), topic(t, a, 5)
			p := producer(t, nil)
			reports := make(chan k.Event, count)
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			for i := range count {
				payload := make([]byte, 1024+i%4096)
				binary.BigEndian.PutUint32(payload, uint32(i))
				key := []byte(fmt.Sprintf("key-%d", i%257))
				if i%7 == 0 {
					key = nil
				}
				if err := p.Produce(&k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: int32(i % 4)}, Key: key, Value: payload, Timestamp: stamp, Headers: []k.Header{{Key: "trace", Value: []byte{0, 255}}, {Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}}, reports); err != nil {
					t.Fatal(err)
				}
			}
			for range count {
				select {
				case e := <-reports:
					if r, ok := e.(*k.Message); !ok || r.TopicPartition.Error != nil {
						t.Fatal("fixture publish failed", e)
					}
				case <-time.After(15 * time.Second):
					t.Fatal("fixture delivery timed out")
				}
			}
			binaryPath := os.Getenv("KFWD_TEST_BINARY")
			if binaryPath == "" {
				binaryPath = filepath.Join(t.TempDir(), "kafka-fwd2")
				command := exec.Command("go", "build", "-trimpath", "-o", binaryPath, "../../cmd/kafka-fwd2")
				if data, err := command.CombinedOutput(); err != nil {
					t.Fatalf("build: %v %s", err, data)
				}
			}
			statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), src+"-load")
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			args := []string{"run", "--file", "-"}
			if configuration != "" {
				path := filepath.Join(t.TempDir(), "worker.toml")
				if err := os.WriteFile(path, []byte(configuration), 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--config", path)
				statement = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s'), CONNECTION slow AS ('bootstrap.servers'='%s','linger.ms'='100') INSERT INTO TOPIC '%s@slow' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), broker(t), dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), src+"-load")
			}
			command := exec.CommandContext(ctx, binaryPath, args...)
			command.Stdin = strings.NewReader(statement)
			var output bytes.Buffer
			command.Stderr = &output

			// When executing the bounded copy and sampling its memory.
			start := time.Now()
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			stop := make(chan struct{})
			peakResult := make(chan int64, 1)
			go func() {
				var peak int64
				tick := time.NewTicker(50 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-stop:
						peakResult <- peak
						return
					case <-tick.C:
						data, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(command.Process.Pid)).Output()
						if err == nil {
							rss, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
							if rss > peak {
								peak = rss
							}
						}
					}
				}
			}()
			err := command.Wait()
			elapsed := time.Since(start)
			close(stop)
			peak := <-peakResult
			if err != nil {
				t.Fatalf("load copy: %v %s", err, output.String())
			}
			var summary struct {
				Submitted   int   `json:"submitted"`
				Confirmed   int   `json:"confirmed"`
				PeakRecords int   `json:"peak_uncommitted_records"`
				PeakBytes   int64 `json:"peak_uncommitted_bytes"`
			}
			for _, line := range strings.Split(output.String(), "\n") {
				var event map[string]json.RawMessage
				if json.Unmarshal([]byte(line), &event) == nil && string(event["msg"]) == `"copy summary"` {
					if err := json.Unmarshal([]byte(line), &summary); err != nil {
						t.Fatal(err)
					}
				}
			}

			// Then the CLI resolves every record within its work caps and preserves output.
			if summary.Submitted != count || summary.Confirmed != count || summary.PeakRecords > recordCap || summary.PeakBytes > byteCap {
				t.Fatalf("load counts/caps wrong: %+v", summary)
			}
			c := consumer(t, dst+"-reader")
			var parts []k.TopicPartition
			for i := range 5 {
				parts = append(parts, k.TopicPartition{Topic: &dst, Partition: int32(i), Offset: 0})
			}
			if err := c.Assign(parts); err != nil {
				t.Fatal(err)
			}
			seen := make([]bool, count)
			for range count {
				r := read(t, c)
				if len(r.Value) < 4 {
					t.Fatal("truncated record")
				}
				i := int(binary.BigEndian.Uint32(r.Value))
				if i >= count || seen[i] {
					t.Fatal("lost or duplicate record")
				}
				seen[i] = true
				if len(r.Value) != 1024+i%4096 || !r.Timestamp.Equal(stamp) || len(r.Headers) != 3 || r.Headers[1].Value != nil || r.Headers[2].Value == nil {
					t.Fatal("load copy changed record")
				}
			}
			cpu := command.ProcessState.UserTime() + command.ProcessState.SystemTime()
			t.Logf("records=%d elapsed=%s records/s=%.0f child_cpu=%s sampled_peak_rss_MiB=%.1f peak_uncommitted_records=%d peak_uncommitted_bytes=%d", count, elapsed, float64(count)/elapsed.Seconds(), cpu, float64(peak)/1024, summary.PeakRecords, summary.PeakBytes)
		})
	}
}
