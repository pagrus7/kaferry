package file

import (
	"bytes"
	"encoding/hex"
	"errors"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestPhysicalShortReadsCannotBecomeSnapshotEOF(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		length int
	}{{"prefix", 2}, {"body", 7}} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given a complete frame in the open-time snapshot, now physically shorter.
			data := frame([]byte{1, 2, 3, 4})
			reader := bytes.NewReader(data[:scenario.length])

			// When reading the advertised snapshot extent.
			_, _, _, err := readFrame(reader, 0, int64(len(data)), 1024)

			// Then a changed-source conflict fails, without matching normal EOF.
			var changed *Conflict
			if err == nil || errors.Is(err, io.EOF) || !errors.As(err, &changed) {
				t.Fatal("short read treated as snapshot completion", err)
			}
		})
	}
	// Given an originally partial trailing frame.
	data := frame([]byte{1, 2, 3, 4})

	// When its original snapshot ends before the complete frame.
	_, _, _, err := readFrame(bytes.NewReader(data[:7]), 0, 7, 1024)

	// Then the partial trailing frame retains normal snapshot EOF semantics.
	if !errors.Is(err, io.EOF) {
		t.Fatal("original partial tail no longer ends the snapshot", err)
	}
}

func TestProvenanceArchiveWireBytesAreStable(t *testing.T) {
	// Given the documented nullable record fields and diagnostic provenance.
	c := codec(t)
	record := core.Record{Key: nil, Value: []byte{}, TimestampType: "unknown", Source: &core.Provenance{Topic: "orders", Partition: 3, Offset: 42}}

	// When encoding the full logical record.
	data, err := c.EncodeRecord(record)

	// Then the archive wire representation remains byte-identical.
	require.NoError(t, err)
	const golden = "00000064a6636b6579f66576616c75654066736f75726365a365746f706963666f7264657273666f6666736574182a69706172746974696f6e036768656164657273806c74696d657374616d705f6d73f66e74696d657374616d705f7479706567756e6b6e6f776e3b069d95"
	if hex.EncodeToString(data) != golden {
		t.Fatalf("archive provenance wire bytes changed: %x", data)
	}
	decoded, err := c.DecodeRecord(data[4 : len(data)-4])
	require.NoError(t, err)
	require.Equal(t, record, decoded, "provenance survives archive round trip")
}

type failedReader struct{ err error }

func (r failedReader) ReadAt([]byte, int64) (int, error) { return 0, r.err }

func TestFrameReadPreservesNonEOFFailure(t *testing.T) {
	// Given an input device failure rather than a shortened file.
	cause := errors.New("injected device failure")
	// When reading the snapshot prefix.
	_, _, _, err := readFrame(failedReader{err: cause}, 0, 12, 1024)
	// Then the original cause remains available to the caller.
	var changed *Conflict
	if !errors.Is(err, cause) || errors.Is(err, io.EOF) || errors.As(err, &changed) {
		t.Fatal("device failure misclassified as truncation", err)
	}
}
