package avro

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInput = errors.New("input read failed")

type failingInput struct {
	*bytes.Reader
	after    int64
	complete bool
}

func (f failingInput) ReadAt(data []byte, offset int64) (int, error) {
	if offset < f.after {
		return f.Reader.ReadAt(data, offset)
	}
	if !f.complete {
		return 0, errInput
	}
	n, _ := f.Reader.ReadAt(data, offset)
	return n, errInput
}

func TestInputErrorsSurviveDecoderAndCompleteReadHandling(t *testing.T) {
	// Given reads that fail either before data or together with a complete requested buffer.
	c := codec(t, 4<<20)
	data := write(t, c, records(), 4)
	valid, err := c.Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	for _, complete := range []bool{false, true} {
		// When reading a header, then the Avro decoder cannot replace the I/O cause with EOF.
		_, err := c.Open(failingInput{Reader: bytes.NewReader(data), complete: complete}, int64(len(data)))
		require.ErrorIs(t, err, errInput)
		// When reading a block, then a full read does not hide a simultaneous input failure.
		r, err := c.Open(failingInput{Reader: bytes.NewReader(data), after: valid.Start, complete: complete}, int64(len(data)))
		require.NoError(t, err)
		_, err = r.Block(r.Start)
		require.ErrorIs(t, err, errInput)
	}
}
