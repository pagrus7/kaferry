package file

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	copy "kafka-fwd2/internal/copy"
)

type Output struct {
	file       *os.File
	buffer     *bufio.Writer
	codec      *Codec
	header     Header
	position   int64
	extent     int64
	pending    []copy.Token
	deliveries chan copy.Delivery
	logger     *slog.Logger
	sync       func() error
	failed     error // A retried fsync can succeed after the kernel dropped failed pages.
}

func NewOutput(path, group, fp, expectedID string, extent int64, codec *Codec, capacity int, logger *slog.Logger) (*Output, error) {
	if err := cleanupTemporary(path); err != nil {
		return nil, err
	}
	created := false
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if os.IsNotExist(err) {
		if expectedID != "" {
			return nil, conflict("output is missing while progress names it")
		}
		f, created, err = createOutput(path, group, fp, codec)
	}
	if err != nil {
		return nil, err
	}
	if err = lock(f); err != nil {
		f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	header, start, err := codec.Header(f, info.Size())
	if err != nil {
		f.Close()
		return nil, conflict("%v", err)
	}
	if header.Fingerprint != fp || header.Group != group {
		f.Close()
		return nil, conflict("output query identity mismatch")
	}
	if expectedID == "" {
		// A newly created file is identifiable by its creation result below.
		if info.Size() != start || !created {
			f.Close()
			return nil, conflict("output has no stored destination identity; delete the unowned output")
		}
		expectedID = header.ID
		extent = start
	}
	if header.ID != expectedID || extent < start || info.Size() < extent {
		f.Close()
		return nil, conflict("output identity or acknowledged extent mismatch")
	}
	position := start
	for position < info.Size() {
		_, next, _, err := readFrame(f, position, info.Size(), codec.limit)
		if err != nil {
			var limit *FrameLimit
			if errors.As(err, &limit) {
				f.Close()
				return nil, conflict("%v; raise worker.max_frame_bytes", err)
			}
			if position < extent {
				f.Close()
				return nil, conflict("acknowledged output damaged: %v", err)
			}
			if err = f.Truncate(position); err != nil {
				f.Close()
				return nil, err
			}
			if err = f.Sync(); err != nil {
				f.Close()
				return nil, err
			}
			logger.Warn("unacknowledged output tail truncated", "bytes", info.Size()-position)
			break
		}
		if position < extent && next > extent {
			f.Close()
			return nil, conflict("acknowledged extent is not a frame boundary")
		}
		position = next
	}
	if _, err = f.Seek(position, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &Output{file: f, buffer: bufio.NewWriterSize(f, 65536), codec: codec, header: header, position: position, extent: extent, deliveries: make(chan copy.Delivery, capacity+1), logger: logger, sync: f.Sync}, nil
}

func createOutput(path, group, fp string, codec *Codec) (*os.File, bool, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, false, err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), temporaryPrefix(path)+"*")
	if err != nil {
		return nil, false, err
	}
	name := temporary.Name()
	defer os.Remove(name)
	fail := func(err error) (*os.File, bool, error) { temporary.Close(); return nil, false, err }
	if err = lock(temporary); err != nil {
		return fail(err)
	}
	data, err := codec.EncodeHeader(Header{Version: 1, ID: hex.EncodeToString(id[:]), Group: group, Fingerprint: fp})
	if err != nil {
		return fail(err)
	}
	if _, err = temporary.Write(data); err != nil {
		return fail(err)
	}
	if err = temporary.Sync(); err != nil {
		return fail(err)
	}
	if err = os.Link(name, path); err != nil {
		temporary.Close()
		if errors.Is(err, os.ErrExist) {
			existing, err := os.OpenFile(path, os.O_RDWR, 0600)
			return existing, false, err
		}
		return nil, false, err
	}
	if err = os.Remove(name); err != nil {
		return fail(err)
	}
	if err = syncDirectory(path); err != nil {
		return fail(err)
	}
	return temporary, true, nil
}

func temporaryPrefix(path string) string {
	hash := sha256.Sum256([]byte(filepath.Base(path)))
	return ".kafka-fwd2-" + hex.EncodeToString(hash[:8]) + "-"
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
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasPrefix(entry.Name(), temporaryPrefix(path)) {
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
