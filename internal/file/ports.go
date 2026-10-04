package file

import (
	"context"
	"io"

	copy "kafka-fwd2/internal/copy"
)

type ArchiveDecoder interface {
	Header(io.ReaderAt, int64) (Header, int64, error)
	DecodeRecord([]byte) (copy.Record, error)
	FrameLimit() int64
}
type RecordEncoder interface {
	EncodeRecord(copy.Record) ([]byte, error)
}
type ArchiveEncoder interface {
	RecordEncoder
	Header(io.ReaderAt, int64) (Header, int64, error)
	EncodeHeader(Header) ([]byte, error)
	FrameLimit() int64
}
type SourceCheckpoints interface {
	Existing() *Checkpoint
	Initialize(Checkpoint)
	RememberFrame(int64, FrameIdentity)
	Save(context.Context, []copy.Cursor) (map[int32]error, error)
}
type CheckpointStore interface {
	SourceCheckpoints
	BindOutput(copy.OutputProgress)
	Delete() error
	Close()
}
type sourceFile interface {
	io.ReaderAt
	io.Closer
}
type outputBuffer interface {
	io.Writer
	Flush() error
}
