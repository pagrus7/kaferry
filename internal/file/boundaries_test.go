package file

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	core "kafka-fwd2/internal/copy"
)

type decoderMock struct {
	ArchiveDecoder
	record  core.Record
	err     error
	payload []byte
}

func (*decoderMock) FrameLimit() int64 { return 1024 }
func (d *decoderMock) DecodeRecord(payload []byte) (core.Record, error) {
	d.payload = payload
	return d.record, d.err
}

type readerCloser struct{ *bytes.Reader }

func (readerCloser) Close() error { return nil }

type checkpointsMock struct {
	SourceCheckpoints
	err        error
	saved      []core.Cursor
	remembered map[int64]FrameIdentity
}

func (c *checkpointsMock) Save(_ context.Context, cursors []core.Cursor) (map[int32]error, error) {
	c.saved = append(c.saved, cursors...)
	return map[int32]error{0: nil}, c.err
}
func (c *checkpointsMock) RememberFrame(position int64, identity FrameIdentity) {
	c.remembered[position] = identity
}
func TestFileSourceUsesDecoderAndRemembersOnlyDecodedFrames(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "decode-failure"}[fail], func(t *testing.T) {
			payload := []byte("opaque payload")
			data := append(frame(payload), []byte{0, 0, 0, 5, 1}...)
			decoder := &decoderMock{record: core.Record{Key: nil, Value: []byte{}, TimestampType: "unknown"}}
			if fail {
				decoder.err = errors.New("invalid record")
			}
			checkpoints := &checkpointsMock{remembered: make(map[int64]FrameIdentity)}
			source := &Source{file: readerCloser{bytes.NewReader(data)}, codec: decoder, checkpoints: checkpoints, size: int64(len(data))}
			event, err := source.Poll(0)
			if fail {
				if !errors.Is(err, decoder.err) || source.position != 0 || len(checkpoints.remembered) != 0 {
					t.Fatal("failed decode advanced source progress", err)
				}
				return
			}
			if err != nil || event.Input == nil || !bytes.Equal(decoder.payload, payload) || event.Input.Record.Value == nil || event.Input.Record.Key != nil || len(checkpoints.remembered) != 1 || checkpoints.remembered[event.Input.Next].Position != 0 {
				t.Fatal("decoded record or frame identity lost", event, err)
			}
			event, err = source.Poll(0)
			if err != nil || event.EOF == nil || len(checkpoints.remembered) != 1 {
				t.Fatal("partial tail was emitted or remembered", event, err)
			}
		})
	}
}
func TestFileSourceCannotAssignBeforeInitialCheckpoint(t *testing.T) {
	checkpoints := &checkpointsMock{err: errors.New("checkpoint failure")}
	source := &Source{position: 100, checkpoints: checkpoints}
	lifecycle := &lifecycle{}
	if err := source.Start(lifecycle); !errors.Is(err, checkpoints.err) || len(lifecycle.assignments) != 0 || len(checkpoints.saved) != 1 || checkpoints.saved[0].Position != 100 {
		t.Fatal("intake began without initial progress", err)
	}
}

type encoderMock struct{ err error }

func (e encoderMock) EncodeRecord(core.Record) ([]byte, error) { return []byte("frame"), e.err }

type bufferMock struct {
	writeErr, flushErr error
	short              bool
	flushes            int
}

func (b *bufferMock) Write(data []byte) (int, error) {
	if b.short {
		return len(data) - 1, nil
	}
	return len(data), b.writeErr
}
func (b *bufferMock) Flush() error { b.flushes++; return b.flushErr }
func TestFileOutputAcknowledgesOnlyAfterBufferAndSyncSucceed(t *testing.T) {
	for _, stage := range []string{"encode", "write", "short", "flush", "sync", "success"} {
		t.Run(stage, func(t *testing.T) {
			failure := errors.New("I/O failure")
			encoder, buffer := encoderMock{}, &bufferMock{}
			switch stage {
			case "encode":
				encoder.err = failure
			case "write":
				buffer.writeErr = failure
			case "short":
				buffer.short = true
				failure = io.ErrShortWrite
			case "flush":
				buffer.flushErr = failure
			}
			syncs := 0
			output := &Output{buffer: buffer, codec: encoder, extent: 100, position: 100, deliveries: make(chan core.Delivery, 1), sync: func() error {
				syncs++
				if stage == "sync" {
					return failure
				}
				return nil
			}}
			token := core.Token{Partition: 0, Sequence: 1}
			err := output.Submit(core.Record{}, token)
			if len(output.deliveries) != 0 {
				t.Fatal("acknowledged before durability")
			}
			if err == nil {
				err = output.Flush()
			}
			if stage == "success" {
				if err != nil || output.extent != 105 || syncs != 1 || buffer.flushes != 1 || len(output.deliveries) != 1 || (<-output.deliveries).Token != token {
					t.Fatal("durable acknowledgment missing", err)
				}
				return
			}
			if !errors.Is(err, failure) || output.extent != 100 || len(output.deliveries) != 0 {
				t.Fatal("failed write acknowledged", err)
			}
			if stage == "encode" {
				var recordError *core.RecordError
				if !errors.As(err, &recordError) || output.failed != nil {
					t.Fatal("validation failure poisoned output", err)
				}
				return
			}
			beforeSyncs, beforeFlushes := syncs, buffer.flushes
			if err := output.Flush(); !errors.Is(err, failure) || syncs != beforeSyncs || buffer.flushes != beforeFlushes {
				t.Fatal("failed output retried I/O", err)
			}
		})
	}
}
