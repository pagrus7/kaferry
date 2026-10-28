package archiveio

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/capture"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
)

type archiveWrite struct {
	prepared bool
	record   avro.Record
	next     int64
}
type archiveEnvelope struct {
	Partition int32
	Version   int
	Identity  delivery.Identity
	Frame     *delivery.Frame
	Capture   capture.State
}
type archiveLease struct {
	used               uint64
	authority          *capture.State
	coordinator        delivery.Transaction
	authoritativeFrame *delivery.Frame
	failed             error
	recoveryOnly       bool
	journal            *captureOutput
	id                 int32
	writerID, path     string
	lock               io.Closer
	store              *capture.Store
	writer             *capture.Capture
	contents           capture.State
	frame              *delivery.Frame
	staged             *delivery.Frame
	receipt            int64
	batchFrame         delivery.Frame
}

func (j *captureOutput) Acquire(ctx context.Context, id int32) (part delivery.Partition, result error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var acquiring *archiveLease
	defer func() {
		if acquiring == nil {
			return
		}
		if acquiring.store != nil {
			result = errors.Join(result, acquiring.store.Close())
		}
		if acquiring.coordinator != nil {
			_ = acquiring.coordinator.Close()
		}
		result = errors.Join(result, acquiring.lock.Close())
	}()
	{
		if j.leases[id] != nil {
			return nil, errors.New("archive partition lease already held")
		}
		root := j.session.Plan.Destination.Name
		for _, path := range []string{root, filepath.Join(root, ".kaferry"), filepath.Join(root, ".kaferry", "writers"), filepath.Join(root, ".kaferry", "eos-leases")} {
			if err := mkdirDurable(path); err != nil {
				return nil, err
			}
		}
		logicalID := j.session.ArchiveNamespace(id)
		writerID := logicalID
		lock, err := (replay.NativeGroupFiles{}).Acquire(filepath.Join(root, ".kaferry", "eos-leases", logicalID+".lock"))
		if err != nil {
			return nil, err
		}
		acquiring = &archiveLease{lock: lock}
		if j.log != nil {
			writerID, err = j.localWriter(root, logicalID, id)
			if err != nil {
				return nil, err
			}
		}
		a := &archiveLease{journal: j, id: id, writerID: writerID, path: filepath.Join(root, ".kaferry", "writers", writerID, "state.json"), lock: lock}
		acquiring = a
		if j.log != nil {
			a.coordinator, a.authoritativeFrame, err = j.coordinatorLease(ctx, id, writerID)
			if err != nil {
				return nil, err
			}
			a.authority, err = j.readOwner(ctx, id, writerID, true)
			if err != nil {
				return nil, err
			}
		}
		if err = a.initialize(); err != nil {
			return nil, err
		}
		if a.coordinator != nil {
			a.frame = a.authoritativeFrame
		}
		j.leases[id] = &archivePartition{archive: a, frame: a.frame}
		acquiring = nil
		j.session.Hook("acquired", id)
	}
	return j.leases[id], ctx.Err()
}
func mkdirDurable(path string) error {
	if err := os.Mkdir(path, 0700); err != nil {
		if errors.Is(err, os.ErrNotExist) && filepath.Dir(path) != path {
			if err := mkdirDurable(filepath.Dir(path)); err != nil {
				return err
			}
			return mkdirDurable(path)
		}
		if errors.Is(err, os.ErrExist) {
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
		}
		return err
	}
	return (capture.NativeFiles{}).SyncDirectory(filepath.Dir(path))
}
func (a *archiveLease) initialize() error {
	data, err := (replay.NativeGroupFiles{}).Read(a.path, 96<<20)
	if err == nil {
		envelope, err := checkpoint.Decode[archiveEnvelope](data, 96<<20)
		if err != nil {
			return err
		}
		if envelope.Version != delivery.Version || envelope.Partition != a.id || envelope.Identity != a.journal.session.Identity {
			return errors.New("archive checkpoint identity changed")
		}
		if envelope.Capture.Version != 1 || envelope.Capture.WriterID != a.writerID || envelope.Capture.Files == nil || envelope.Frame == nil && len(envelope.Capture.Files) > 0 {
			return errors.New("incomplete archive checkpoint")
		}
		a.frame = envelope.Frame
		if a.frame != nil && a.frame.Cursor.Partition != a.id {
			return errors.New("archive checkpoint partition changed")
		}
		if a.frame != nil {
			return a.journal.session.Validate(*a.frame)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	namespace := filepath.Dir(a.path)
	if _, err := os.Stat(namespace); !errors.Is(err, os.ErrNotExist) {
		return errors.New("archive namespace exists without checkpoint")
	}
	writers := filepath.Dir(namespace)
	temporary, err := os.MkdirTemp(writers, ".eos-init-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporary)
	lock, err := os.OpenFile(filepath.Join(temporary, "lock"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	err = errors.Join(lock.Sync(), lock.Close())
	if err != nil {
		return err
	}
	contents := capture.State{Version: 1, WriterID: a.writerID, Files: map[string]capture.Boundary{}}
	envelope := archiveEnvelope{Partition: a.id, Version: delivery.Version, Identity: a.journal.session.Identity, Capture: contents}
	encoded, err := checkpoint.Encode(envelope, 96<<20)
	if err != nil {
		return err
	}
	if err = a.journal.writer.Replace(filepath.Join(temporary, "state.json"), encoded); err != nil {
		return err
	}
	if err = os.Rename(temporary, namespace); err != nil {
		return err
	}
	return (capture.NativeFiles{}).SyncDirectory(writers)
}
func (a *archiveLease) Load(path string) (capture.State, error) {
	data, err := (replay.NativeGroupFiles{}).Read(path, 96<<20)
	if err != nil {
		return capture.State{}, err
	}
	envelope, err := checkpoint.Decode[archiveEnvelope](data, 96<<20)
	if err != nil {
		return capture.State{}, err
	}
	if envelope.Version != delivery.Version || envelope.Partition != a.id || envelope.Identity != a.journal.session.Identity {
		return capture.State{}, errors.New("archive checkpoint identity changed")
	}
	if envelope.Frame == nil && len(envelope.Capture.Files) > 0 {
		return capture.State{}, errors.New("incomplete archive checkpoint")
	}
	if envelope.Frame != nil {
		if err := a.journal.session.Validate(*envelope.Frame); err != nil {
			return capture.State{}, err
		}
	}
	a.contents = envelope.Capture
	if a.coordinator != nil || a.recoveryOnly {
		if a.authority == nil {
			if envelope.Frame != nil {
				return capture.State{}, errors.New("coordinator lost an initialized owner checkpoint")
			}
			a.contents = capture.State{Version: 1, WriterID: a.writerID, Files: map[string]capture.Boundary{}}
		} else {
			a.contents = *a.authority
		}
		return a.contents, nil
	}
	a.frame = envelope.Frame
	return envelope.Capture, nil
}
func (a *archiveLease) Save(path string, contents capture.State) error {
	frame := a.frame
	if a.staged != nil {
		frame = a.staged
	}
	if frame != nil && a.journal.session.StageReplay != nil {
		saved, err := a.journal.session.StageReplay(frame.Cursor)
		if err != nil {
			return err
		}
		copy := *frame
		copy.Replay = saved
		frame = &copy
	}
	if frame != nil {
		if err := a.journal.session.Validate(*frame); err != nil {
			return err
		}
	}
	encoded, err := checkpoint.Encode(archiveEnvelope{Partition: a.id, Version: delivery.Version, Identity: a.journal.session.Identity, Frame: frame, Capture: contents}, 96<<20)
	if err != nil {
		return err
	}
	a.journal.session.Hook("prepared", a.id)
	if frame != nil && a.coordinator != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = a.journal.writeFrame(ctx, a.coordinator, *frame, a.writerID, &contents)
		cancel()
		if err != nil {
			a.failed = err
			return err
		}
	}
	if a.recoveryOnly {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = a.acknowledge(ctx, contents)
		cancel()
		if err != nil {
			return err
		}
	}
	if err = a.journal.writer.Replace(path, encoded); err != nil {
		return err
	}
	a.contents = contents
	a.frame = frame
	if err = a.journal.saveMirror(frame); err != nil {
		return err
	}
	a.journal.session.Hook("checkpointed", a.id)
	return nil
}
func (a *archiveLease) stage(cursor core.Cursor, metadata string) error {
	frame := delivery.Frame{Version: delivery.Version, Identity: a.journal.session.Identity, Cursor: cursor, Metadata: metadata}
	if metadata != "" {
		text, err := metadataAt(metadata, cursor)
		if err != nil {
			return err
		}
		frame.Metadata = text
	}

	a.staged = &frame
	return nil
}
func (j *captureOutput) commitArchive(ctx context.Context, l *archivePartition, frame delivery.Frame) error {
	cursor, metadata := frame.Cursor, frame.Metadata
	a := l.archive
	if err := a.openWriter(); err != nil {
		l.failed = err
		return err
	}
	if j.session.Plan.Source.Kind == "TOPIC" && len(l.archiveQueue) > 0 {
		if len(a.contents.Files) == 0 {
			total := 0
			var victim *archiveLease
			for _, other := range j.leases {
				if other.archive == nil {
					continue
				}
				total += len(other.archive.contents.Files)
				if other != l && len(other.archive.contents.Files) > 0 && (victim == nil || other.archive.used < victim.used) {
					victim = other.archive
				}
			}
			if total >= j.session.Limits.ArchiveCaptureOpenFiles && victim != nil {
				if err := victim.writer.Finish(); err != nil {
					return err
				}
			}
		}
		j.archiveUse++
		a.used = j.archiveUse
	}
	a.batchFrame = delivery.Frame{Version: delivery.Version, Identity: j.session.Identity, Cursor: cursor, Metadata: metadata}
	remaining := make([]archiveWrite, 0)
	for _, entry := range l.archiveQueue {
		if !entry.prepared || entry.next > cursor.Position {
			remaining = append(remaining, entry)
			continue
		}
		a.receipt = entry.next
		if err := a.writer.Append(entry.record); err != nil {
			l.failed = err
			return err
		}
	}
	l.archiveQueue = remaining
	if err := a.stage(cursor, metadata); err != nil {
		l.failed = err
		return err
	}
	if err := a.writer.Flush(); err != nil {
		l.failed = err
		return err
	}
	// A filter-only or metadata-only save can have no changed file boundary.
	if err := a.Save(a.path, a.contents); err != nil {
		l.failed = err
		return err
	}
	l.frame = a.frame
	l.ready = 0
	j.session.Hook("committed", cursor.Partition)
	return ctx.Err()
}

type archiveSegments struct {
	*capture.Store
	lease *archiveLease
}

func (s archiveSegments) Append(id string, record avro.Record) (capture.Segment, error) {
	segment, err := s.Store.Append(id, record)
	if err != nil {
		return segment, err
	}
	cursor := s.lease.batchFrame.Cursor
	cursor.Position = s.lease.receipt
	if cursor.Stop != nil && cursor.Stop.At > cursor.Position {
		cursor.Stop = nil
	}
	if err = s.lease.stage(cursor, s.lease.batchFrame.Metadata); err != nil {
		return segment, err
	}
	return segment, nil
}
func (s archiveSegments) Publish(id string) error { return s.PublishMany([]string{id}) }
func (s archiveSegments) PublishMany(ids []string) error {
	// Replay has one cursor across original streams. Every earlier stream must
	// be synced before rotating any file can publish a cursor covering them.
	if err := s.Store.Checkpoint(); err != nil {
		return err
	}
	return s.Store.PublishMany(ids)
}

type archiveFiles struct {
	capture.NativeFiles
	writerID string
	journal  *captureOutput
	id       int32
}

func (f archiveFiles) ReadDir(path string) ([]os.DirEntry, error) {
	entries, err := f.NativeFiles.ReadDir(path)
	if err != nil {
		return nil, err
	}
	if filepath.Base(path) != "writers" {
		return entries, nil
	}
	selected := make([]os.DirEntry, 0, 1)
	for _, entry := range entries {
		if entry.Name() == f.writerID {
			selected = append(selected, entry)
		}
	}
	if len(selected) != 1 {
		return nil, errors.New("owned archive namespace missing")
	}
	return selected, nil
}
func (f archiveFiles) TryLock(path string) (io.Closer, error) {
	lock, err := f.NativeFiles.TryLock(path)
	if errors.Is(err, capture.ErrBusy) {
		return nil, errors.New("archive namespace busy; refusing to fork")
	}
	return lock, err
}
func (f archiveFiles) RenameExclusive(old, next string) error {
	if err := f.NativeFiles.RenameExclusive(old, next); err != nil {
		return err
	}
	f.journal.session.Hook("published", f.id)
	return nil
}

type captureBuffer struct {
	journal *captureOutput
	codec   *avro.Codec
}

func (b *captureBuffer) Append(record avro.Record, cursor core.Cursor) error {
	// Validate using the production codec before accepting a skippable record.
	writer, err := b.codec.NewWriter(io.Discard, avro.Identity{Version: 1, WriterID: delivery.Hash("validate")[:32], FileID: delivery.Hash("record")[:32], Source: record.Source, FirstOffset: record.Offset})
	if err != nil {
		return err
	}
	if err = writer.Append(record); err != nil {
		return err
	}
	b.journal.mu.Lock()
	defer b.journal.mu.Unlock()
	l := b.journal.leases[cursor.Partition]
	if l == nil {
		return errors.New("archive submission is not owned")
	}
	l.archiveQueue = append(l.archiveQueue, archiveWrite{record: record, next: cursor.Position})
	b.journal.session.Hook("queued", cursor.Partition)
	return nil
}
func (b *captureBuffer) Flush() error {
	b.journal.mu.Lock()
	defer b.journal.mu.Unlock()
	for _, l := range b.journal.leases {
		for i := range l.archiveQueue {
			l.archiveQueue[i].prepared = true
			l.ready = max(l.ready, l.archiveQueue[i].next)
		}
		if len(l.archiveQueue) > 0 {
			b.journal.session.Hook("flushed", l.archiveQueue[0].record.Source.Partition)
		}
	}
	return nil
}
func (b *captureBuffer) Finish() error {
	b.journal.mu.Lock()
	defer b.journal.mu.Unlock()
	for _, l := range b.journal.leases {
		if l.failed != nil {
			return l.failed
		}
		if l.archive.writer == nil {
			continue
		}
		if err := l.archive.writer.Finish(); err != nil {
			return err
		}
	}
	return nil
}
func (b *captureBuffer) Close() error { return nil }
func (j *captureOutput) Sink(limits config.Worker) (*Sink, error) {
	codec, err := avro.New(limits.ArchiveBlockMaxBytes)
	if err != nil {
		return nil, err
	}
	buffer := &captureBuffer{journal: j, codec: codec}
	return NewSink(buffer, limits.RecordCap), nil
}

func (f archiveFiles) OpenFile(path string, flags int, mode os.FileMode) (capture.DataFile, error) {
	file, err := f.NativeFiles.OpenFile(path, flags, mode)
	if err != nil {
		return nil, err
	}
	if flags&os.O_CREATE == 0 {
		return file, nil
	}
	f.journal.session.Hook("file-open", f.id)
	return &observedCaptureFile{DataFile: file, journal: f.journal, id: f.id}, nil
}

type observedCaptureFile struct {
	capture.DataFile
	journal *captureOutput
	id      int32
	once    sync.Once
}

func (f *observedCaptureFile) Close() error {
	err := f.DataFile.Close()
	f.once.Do(func() { f.journal.session.Hook("file-close", f.id) })
	return err
}

func (a *archiveLease) openWriter() error {
	if a.writer != nil {
		return nil
	}
	codec, err := avro.New(a.journal.session.Limits.ArchiveBlockMaxBytes)
	if err != nil {
		return err
	}
	a.store, err = capture.Open(a.journal.session.Plan.Destination.Name, codec, archiveFiles{writerID: a.writerID, journal: a.journal, id: a.id}, a)
	if err != nil {
		return err
	}
	a.writer, err = capture.New(archiveSegments{Store: a.store, lease: a}, a.journal.clock, capture.Policy{RotateAfterSize: a.journal.session.Plan.Destination.RotateAfterSize, RotateAfterAge: time.Duration(a.journal.session.Plan.Destination.RotateAfterAgeMS) * time.Millisecond, MaxOpenFiles: a.journal.session.Limits.ArchiveCaptureOpenFiles})
	if err != nil {
		return err
	}

	return nil
}
