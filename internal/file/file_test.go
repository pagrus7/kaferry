package file

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
)

func logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func codec(t *testing.T) *Codec {
	t.Helper()
	c, err := NewCodec(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func sample() core.Record {
	stamp := int64(1791000000123)
	return core.Record{Key: nil, Value: []byte{}, Headers: []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}, {Name: string([]byte{255, 0}), Value: []byte{0, 255}}}, TimestampMS: &stamp, TimestampType: "create_time"}
}

func TestRecordCodecFullFidelity(t *testing.T) {
	// Given
	c := codec(t)
	input := sample()

	// When
	data, err := c.EncodeRecord(input)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	payload, _, _, err := readFrame(bytes.NewReader(data), 0, int64(len(data)), c.limit)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.DecodeRecord(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("codec lost data: %#v", got)
	}
	data[len(data)-1] ^= 1
	if _, _, _, err = readFrame(bytes.NewReader(data), 0, int64(len(data)), c.limit); err == nil {
		t.Fatal("CRC damage accepted")
	}
	if _, _, _, err = readFrame(bytes.NewReader(data), 0, int64(len(data)-1), c.limit); !errors.Is(err, io.EOF) {
		t.Fatal("partial frame not EOF")
	}
}

func TestOutputAcknowledgmentAndRecovery(t *testing.T) {
	// Given
	c := codec(t)
	path := filepath.Join(t.TempDir(), "archive.cbor")
	out, err := NewOutput(NewPublisher(), path, "g", "fp", "", 0, c, 10, logger())
	if err != nil {
		t.Fatal(err)
	}
	id, headerEnd := out.DurableOutput()

	// When
	err = out.Submit(sample(), core.Token{Partition: 0, Generation: 1, Sequence: 1})

	// Then
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-out.Deliveries():
		t.Fatal("acknowledged before fsync")
	default:
	}

	// When flushing the pending record.
	err = out.Flush()

	// Then its acknowledgment follows successful durability.
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-out.Deliveries():
	default:
		t.Fatal("missing acknowledgment after fsync")
	}
	_, extent := out.DurableOutput()
	if extent <= headerEnd {
		t.Fatal("durable extent did not advance")
	}
	out.Close()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte{0, 0, 0, 100, 1, 2}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// When reopening after an unacknowledged partial tail.
	out, err = NewOutput(NewPublisher(), path, "g", "fp", id, extent, c, 10, logger())

	// Then recovery truncates only the unacknowledged tail.
	if err != nil {
		t.Fatal(err)
	}
	out.Close()
	info, _ := os.Stat(path)
	if info.Size() != extent {
		t.Fatal("unacknowledged tail not truncated")
	}
	f, err = os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte{1}, headerEnd+4); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err = NewOutput(NewPublisher(), path, "g", "fp", id, extent, c, 10, logger()); err == nil {
		t.Fatal("acknowledged corruption accepted")
	}
}

func TestOutputLocksAndUnownedHeader(t *testing.T) {
	// Given
	c := codec(t)
	path := filepath.Join(t.TempDir(), "archive.cbor")

	// When
	out, err := NewOutput(NewPublisher(), path, "g", "fp", "", 0, c, 10, logger())

	// Then
	if err != nil {
		t.Fatal(err)
	}
	id, extent := out.DurableOutput()
	if _, err = NewOutput(NewPublisher(), path, "g", "fp", id, extent, c, 10, logger()); err == nil {
		t.Fatal("two output writers admitted")
	}
	out.Close()
	if _, err = NewOutput(NewPublisher(), path, "g", "fp", "", 0, c, 10, logger()); err == nil {
		t.Fatal("unowned header-only output adopted")
	}
	if err = os.Truncate(path, extent-1); err != nil {
		t.Fatal(err)
	}
	if _, err = NewOutput(NewPublisher(), path, "g", "fp", id, extent, c, 10, logger()); err == nil {
		t.Fatal("output shorter than durable extent accepted")
	}
}

type failureWriter struct{}

func (failureWriter) Replace(string, []byte) error { return errors.New("injected checkpoint failure") }

type lifecycle struct{ assignments []core.Assignment }

func (l *lifecycle) Assign(a []core.Assignment) error { l.assignments = a; return nil }
func (*lifecycle) Revoke([]int32, bool) error         { return nil }

func TestCheckpointAndSourceSnapshot(t *testing.T) {
	// Given
	c := codec(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "input.cbor")
	out, err := NewOutput(NewPublisher(), path, "archive", "original-query", "", 0, c, 10, logger())
	if err != nil {
		t.Fatal(err)
	}
	if err = out.Submit(sample(), core.Token{}); err != nil {
		t.Fatal(err)
	}
	if err = out.Flush(); err != nil {
		t.Fatal(err)
	}
	out.Close()
	checkpoints, err := NewCheckpoints(filepath.Join(dir, "checkpoints"), "../g", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewCheckpoints(filepath.Join(dir, "checkpoints"), "../g", DurableWriter{}); err == nil {
		t.Fatal("exclusive checkpoint lock not enforced")
	}
	source, err := NewSource(path, "copy-query", plan.WindowSpec{}, c, checkpoints, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	l := &lifecycle{}
	if err = source.Start(l); err != nil {
		t.Fatal(err)
	}
	// Appends after open belong to the next run.
	extra, err := c.EncodeRecord(sample())
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(extra); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// When
	event, err := source.Poll(0)

	// Then
	if err != nil || event.Input == nil {
		t.Fatalf("source read: %v", err)
	}
	if _, err = checkpoints.Save(context.Background(), []core.Cursor{{Partition: 0, Position: event.Input.Next}}); err != nil {
		t.Fatal(err)
	}
	event, err = source.Poll(0)
	if err != nil || event.EOF == nil {
		t.Fatal("source followed an append past its open-time snapshot")
	}
	source.Close()
	checkpoints.Close()
	checkpoints, err = NewCheckpoints(filepath.Join(dir, "checkpoints"), "../g", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoints.Close()
	source, err = NewSource(path, "copy-query", plan.WindowSpec{}, c, checkpoints, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err = source.Start(&lifecycle{}); err != nil {
		t.Fatal(err)
	}
	event, err = source.Poll(0)
	if err != nil || event.Input == nil {
		t.Fatal("resume did not read the appended record")
	}
	checkpoints.writer = failureWriter{}
	before := checkpoints.current.Position
	if _, err = checkpoints.Save(context.Background(), []core.Cursor{{Partition: 0, Position: event.Input.Next}}); err == nil || checkpoints.current.Position != before {
		t.Fatal("failed checkpoint advanced durable progress")
	}
}

func TestSourceReplacementFails(t *testing.T) {
	// Given
	c := codec(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "input")
	out, err := NewOutput(NewPublisher(), path, "archive", "fp", "", 0, c, 10, logger())
	if err != nil {
		t.Fatal(err)
	}
	out.Close()
	cp, err := NewCheckpoints(filepath.Join(dir, "cp"), "group", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSource(path, "query", plan.WindowSpec{}, c, cp, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Start(&lifecycle{}); err != nil {
		t.Fatal(err)
	}
	source.Close()
	cp.Close()

	// When
	err = os.Remove(path)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	out, err = NewOutput(NewPublisher(), path, "archive", "fp", "", 0, c, 10, logger())
	if err != nil {
		t.Fatal(err)
	}
	out.Close()
	cp, err = NewCheckpoints(filepath.Join(dir, "cp"), "group", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	if _, err = NewSource(path, "query", plan.WindowSpec{}, c, cp, core.RealClock{}); err == nil {
		t.Fatal("source replacement resumed at unrelated byte position")
	}
}

func FuzzFraming(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _, _ = readFrame(bytes.NewReader(data), 0, int64(len(data)), 1<<20)
	})
}
func TestTimestampNull(t *testing.T) {
	// Given
	c := codec(t)
	record := core.Record{TimestampType: "unknown", Value: []byte("x")}

	// When
	data, err := c.EncodeRecord(record)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	payload, _, _, _ := readFrame(bytes.NewReader(data), 0, int64(len(data)), c.limit)
	result, err := c.DecodeRecord(payload)
	if err != nil || result.TimestampMS != nil {
		t.Fatal("timestamp null changed")
	}
}
