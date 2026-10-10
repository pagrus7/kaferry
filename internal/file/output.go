package file

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	copy "github.com/pagrus7/kaferry/internal/copy"
)

type Output struct {
	file       io.Closer
	buffer     outputBuffer
	codec      RecordEncoder
	header     Header
	position   int64
	extent     int64
	pending    []copy.Token
	deliveries chan copy.Delivery
	logger     *slog.Logger
	sync       func() error
	failed     error // A retried fsync can succeed after the kernel dropped failed pages.
}

func NewOutput(publisher Publisher, path, group, fp, expectedID string, extent int64, codec ArchiveEncoder, capacity int, logger *slog.Logger) (*Output, error) {
	f, created, err := openOutput(publisher, path, group, fp, expectedID, codec)
	if err != nil {
		return nil, err
	}
	output, err := initializeOutput(f, created, group, fp, expectedID, extent, codec, capacity, logger)
	if err != nil {
		f.Close()
	}
	return output, err
}

func openOutput(publisher Publisher, path, group, fp, expectedID string, codec ArchiveEncoder) (*os.File, bool, error) {
	if err := cleanupTemporary(path); err != nil {
		return nil, false, err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if !os.IsNotExist(err) {
		return f, false, err
	}
	if expectedID != "" {
		return nil, false, conflict("output is missing while progress names it")
	}
	return publisher.Create(path, group, fp, codec)
}

func initializeOutput(f *os.File, created bool, group, fp, expectedID string, extent int64, codec ArchiveEncoder, capacity int, logger *slog.Logger) (*Output, error) {
	if err := lock(f); err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	header, start, err := codec.Header(f, info.Size())
	if err != nil {
		return nil, conflict("%v", err)
	}
	extent, err = outputIdentity(header, start, info.Size(), created, group, fp, expectedID, extent)
	if err != nil {
		return nil, err
	}
	position, err := recoverOutput(f, start, info.Size(), extent, codec.FrameLimit(), logger)
	if err != nil {
		return nil, err
	}
	if _, err = f.Seek(position, io.SeekStart); err != nil {
		return nil, err
	}
	return &Output{file: f, buffer: bufio.NewWriterSize(f, 65536), codec: codec, header: header, position: position, extent: extent, deliveries: make(chan copy.Delivery, capacity+1), logger: logger, sync: f.Sync}, nil
}

func outputIdentity(header Header, start, size int64, created bool, group, fp, expectedID string, extent int64) (int64, error) {
	if header.Fingerprint != fp || header.Group != group {
		return 0, conflict("output query identity mismatch")
	}
	if expectedID == "" {
		if size != start || !created {
			return 0, conflict("output has no stored destination identity; delete the unowned output")
		}
		expectedID, extent = header.ID, start
	}
	if header.ID != expectedID || extent < start || size < extent {
		return 0, conflict("output identity or acknowledged extent mismatch")
	}
	return extent, nil
}

func recoverOutput(f *os.File, start, size, extent, limit int64, logger *slog.Logger) (int64, error) {
	position := start
	for position < size {
		_, next, _, err := readFrame(f, position, size, limit)
		if err != nil {
			var frameLimit *FrameLimit
			if errors.As(err, &frameLimit) {
				return 0, conflict("%v; raise worker.max_frame_bytes", err)
			}
			if position < extent {
				return 0, conflict("acknowledged output damaged: %v", err)
			}
			if err = f.Truncate(position); err != nil {
				return 0, err
			}
			if err = f.Sync(); err != nil {
				return 0, err
			}
			logger.Warn("unacknowledged output tail truncated", "bytes", size-position)
			break
		}
		if position < extent && next > extent {
			return 0, conflict("acknowledged extent is not a frame boundary")
		}
		position = next
	}
	return position, nil
}

func temporaryPrefix(path string) string {
	hash := sha256.Sum256([]byte(filepath.Base(path)))
	return ".kaferry-" + hex.EncodeToString(hash[:8]) + "-"
}
func cleanupTemporary(path string) error {
	dir := filepath.Dir(path)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	removed := false
	prefix := temporaryPrefix(path)
	// Recover interrupted publications made before the tool was renamed.
	legacyPrefix := strings.Replace(prefix, ".kaferry-", ".kafka-fwd2-", 1)
	for _, entry := range entries {
		if !entry.Type().IsRegular() || (!strings.HasPrefix(entry.Name(), prefix) && !strings.HasPrefix(entry.Name(), legacyPrefix)) {
			continue
		}
		name := filepath.Join(dir, entry.Name())
		f, err := os.OpenFile(name, os.O_RDWR, 0600)
		if err != nil {
			continue
		}
		if lock(f) == nil {
			if err := os.Remove(name); err == nil {
				removed = true
			}
		}
		_ = f.Close()
	}
	if removed {
		return syncDirectory(path)
	}
	return nil
}
func (o *Output) Submit(record copy.Record, token copy.Token) error {
	if o.failed != nil {
		return o.failed
	}
	data, err := o.codec.EncodeRecord(record)
	if err != nil {
		return &copy.RecordError{Class: "validation", Err: err}
	}
	n, err := o.buffer.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return o.fail(err)
	}
	o.position += int64(n)
	o.pending = append(o.pending, token)
	return nil
}
func (o *Output) Flush() error {
	if o.failed != nil {
		return o.failed
	}
	if len(o.pending) == 0 {
		return nil
	}
	if err := o.buffer.Flush(); err != nil {
		return o.fail(err)
	}
	if err := o.sync(); err != nil {
		return o.fail(err)
	}
	o.extent = o.position
	for _, token := range o.pending {
		o.deliveries <- copy.Delivery{Token: token}
	}
	o.pending = nil
	return nil
}
func (o *Output) fail(err error) error {
	o.failed = err
	o.pending = nil
	return err
}
func (o *Output) Deliveries() <-chan copy.Delivery { return o.deliveries }
func (o *Output) DurableOutput() (string, int64)   { return o.header.ID, o.extent }
func (o *Output) Close()                           { _ = o.file.Close() }
