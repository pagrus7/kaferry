//go:build integration

package compat

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/app"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
)

func execute(t *testing.T, statement, config string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var output bytes.Buffer
	if code := app.Execute(ctx, []string{"run", "--sql", statement, "--config", config}, nil, &output); code != 0 {
		t.Fatalf("exit=%d: %s", code, output.String())
	}
}
func configuration(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	text := fmt.Sprintf("[connections.default]\n'bootstrap.servers' = %q\n[worker]\ncheckpoint_dir = %q\n", broker(t), t.TempDir())
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func archiveRecords(t *testing.T, path string) []core.Record {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := file.NewCodec(64 << 20)
	if err != nil {
		t.Fatal(err)
	}
	_, pos, err := codec.Header(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var records []core.Record
	for pos < int64(len(data)) {
		if pos+4 > int64(len(data)) {
			t.Fatal("partial length")
		}
		n := int64(binary.BigEndian.Uint32(data[pos : pos+4]))
		if pos+8+n > int64(len(data)) {
			t.Fatal("partial record")
		}
		r, err := codec.DecodeRecord(data[pos+4 : pos+4+n])
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, r)
		pos += 8 + n
	}
	return records
}
func TestAllFileDirectionsAndResume(t *testing.T) {
	// Given
	a := admin(t)
	src := topic(t, a, 1)
	dst := topic(t, a, 3)
	p := producer(t, nil)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	var expected []core.Record
	for i := range 24 {
		key, value := []byte{0, byte(i)}, []byte{255, byte(i)}
		if i%3 == 0 {
			key = nil
		}
		if i%3 == 1 {
			key = []byte{}
		}
		if i%4 == 0 {
			value = nil
		}
		if i%4 == 1 {
			value = []byte{}
		}
		headers := []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}, {Key: "binary", Value: []byte{0, 255}}}
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Key: key, Value: value, Headers: headers, Timestamp: stamp})
		ms := stamp.UnixMilli()
		expected = append(expected, core.Record{Key: key, Value: value, Headers: []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}, {Name: "binary", Value: []byte{0, 255}}}, TimestampMS: &ms, TimestampType: "create_time", Source: &core.Provenance{Topic: src, Partition: 0, Offset: int64(i)}})
	}
	dir := t.TempDir()
	archive, clone := filepath.Join(dir, "archive.cbor"), filepath.Join(dir, "clone.cbor")
	cfg := configuration(t)
	statement := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", archive, src, stamp.Add(time.Second).Format(time.RFC3339Nano), src+"-archive")

	// When
	execute(t, statement, cfg)

	// Then
	if actual := archiveRecords(t, archive); !reflect.DeepEqual(actual, expected) {
		t.Fatalf("topic→file changed records\nactual=%+v\nexpected=%+v", actual, expected)
	}
	execute(t, statement, cfg)
	if len(archiveRecords(t, archive)) != 24 {
		t.Fatal("completed archive was recopied")
	}
	statement = fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='clone')", clone, archive)
	execute(t, statement, cfg)
	execute(t, statement, cfg)
	if actual := archiveRecords(t, clone); !reflect.DeepEqual(actual, expected) {
		t.Fatal("file→file changed records or repeated acknowledged output")
	}
	statement = fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='restore')", dst, archive)
	execute(t, statement, cfg)
	execute(t, statement, cfg)
	c := consumer(t, dst+"-reader")
	var parts []k.TopicPartition
	for i := range 3 {
		parts = append(parts, k.TopicPartition{Topic: &dst, Partition: int32(i), Offset: 0})
	}
	if err := c.Assign(parts); err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for range 24 {
		r := read(t, c)
		signature := fmt.Sprintf("%v/%v/%v/%v", r.Key == nil, r.Key, r.Value == nil, r.Value)
		counts[signature]++
		if !r.Timestamp.Equal(stamp) || !reflect.DeepEqual(r.Headers, []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}, {Key: "binary", Value: []byte{0, 255}}}) {
			t.Fatal("file→topic changed timestamp or headers")
		}
	}
	for _, r := range expected {
		signature := fmt.Sprintf("%v/%v/%v/%v", r.Key == nil, r.Key, r.Value == nil, r.Value)
		counts[signature]--
	}
	for _, n := range counts {
		if n != 0 {
			t.Fatal("file→topic changed null/empty bytes")
		}
	}
	for i := range 3 {
		low, high, err := c.QueryWatermarkOffsets(dst, int32(i), 5000)
		if err != nil {
			t.Fatal(err)
		}
		_ = low
		if high < 0 {
			t.Fatal("invalid high watermark")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, part := range parts {
		result, err := a.ListOffsets(ctx, map[k.TopicPartition]k.OffsetSpec{part: k.LatestOffsetSpec})
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range result.ResultInfos {
			if v.Error.Code() != k.ErrNoError {
				t.Fatal(v.Error)
			}
			counts["total"] += int(v.Offset)
		}
	}
	if counts["total"] != 24 {
		t.Fatal("resumed file→topic produced extra records")
	}
}
