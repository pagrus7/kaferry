package archiveio

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/pagrus7/kaferry/internal/archive/capture"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/commitmeta"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"os"
	"path/filepath"
	"sync"
)

type captureOutput struct {
	session    *delivery.Session
	clock      core.Clock
	log        delivery.Log
	writer     checkpoint.AtomicWriter
	mu         sync.Mutex
	leases     map[int32]*archivePartition
	archiveUse uint64
}
type archivePartition struct {
	archive      *archiveLease
	archiveQueue []archiveWrite
	frame        *delivery.Frame
	ready        int64
	failed       error
}

func NewCapture(s *delivery.Session, clock core.Clock, log delivery.Log, writer checkpoint.AtomicWriter) *captureOutput {
	return &captureOutput{session: s, clock: clock, log: log, writer: writer, leases: make(map[int32]*archivePartition)}
}
func (p *archivePartition) Saved() *delivery.Frame { return p.frame }
func (p *archivePartition) Commit(ctx context.Context, f delivery.Frame) error {
	j := p.archive.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	if p.failed != nil {
		return p.failed
	}
	if p.archive.failed != nil {
		return p.archive.failed
	}
	if p.ready > f.Cursor.Position {
		return errors.New("archive contains output past confirmed cursor")
	}
	return j.commitArchive(ctx, p, f)
}
func (p *archivePartition) Close() error {
	a := p.archive
	j := a.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	var failure error
	if a.writer != nil && p.failed == nil && a.failed == nil {
		failure = a.writer.Finish()
	}
	if a.writer != nil {
		failure = errors.Join(failure, a.writer.Close())
	} else if a.store != nil {
		failure = errors.Join(failure, a.store.Close())
	}
	failure = errors.Join(failure, a.lock.Close())
	if a.coordinator != nil {
		failure = errors.Join(failure, a.coordinator.Close())
	}
	delete(j.leases, a.id)
	return failure
}
func (j *captureOutput) Read(ctx context.Context, id int32) (*delivery.Frame, error) {
	if j.log != nil {
		return j.readFrame(ctx, id, false)
	}
	path := filepath.Join(j.session.Plan.Destination.Name, ".kaferry", "writers", j.session.ArchiveNamespace(id), "state.json")
	data, err := (replay.NativeGroupFiles{}).Read(path, delivery.MaxStateBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	e, err := checkpoint.Decode[archiveEnvelope](data, delivery.MaxStateBytes)
	if err != nil {
		return nil, err
	}
	if e.Version != delivery.Version || e.Partition != id || e.Identity != j.session.Identity {
		return nil, errors.New("archive checkpoint identity or version changed")
	}
	if e.Frame != nil {
		if err = j.session.Validate(*e.Frame); err != nil {
			return nil, err
		}
		if e.Frame.Cursor.Partition != id {
			return nil, errors.New("archive checkpoint partition changed")
		}
	}
	return e.Frame, nil
}
func (j *captureOutput) readFrame(ctx context.Context, id int32, barrier bool) (*delivery.Frame, error) {
	data, err := j.log.ReadValue(ctx, id, j.session.Key("state"), barrier)
	if err != nil || data == nil {
		return nil, err
	}
	f, err := j.session.Decode(data)
	if err != nil {
		return nil, err
	}
	if f.Cursor.Partition != id {
		return nil, errors.New("checkpoint partition disagrees with key")
	}
	return &f, nil
}
func (j *captureOutput) coordinatorLease(ctx context.Context, id int32, writer string) (delivery.Transaction, *delivery.Frame, error) {
	tx, err := j.log.Open(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	f, err := j.readFrame(ctx, id, true)
	if err == nil {
		err = tx.Write(ctx, j.session.Key("capture-owner"), []byte(writer))
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		_ = tx.Close()
		return nil, nil, err
	}
	return tx, f, nil
}
func (j *captureOutput) writeFrame(ctx context.Context, tx delivery.Transaction, f delivery.Frame, owner string, contents *capture.State) error {
	encoded, err := delivery.Encode(f)
	if err != nil {
		return err
	}
	if contents != nil {
		value, err := checkpoint.Encode(*contents, 32<<20)
		if err != nil {
			return err
		}
		if err = tx.Write(ctx, j.session.Key("owner/"+owner), value); err != nil {
			return err
		}
	}
	if err = tx.Write(ctx, j.session.Key("state"), encoded); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	j.session.Hook("coordinated", f.Cursor.Partition)
	return nil
}
func (j *captureOutput) readOwner(ctx context.Context, id int32, writer string, barrier bool) (*capture.State, error) {
	value, err := j.log.ReadValue(ctx, id, j.session.Key("owner/"+writer), barrier)
	if err != nil || value == nil {
		return nil, err
	}
	state, err := checkpoint.Decode[capture.State](value, 32<<20)
	if err != nil {
		return nil, err
	}
	if state.Version != 1 || state.WriterID != writer || state.Files == nil {
		return nil, errors.New("coordinator owner identity or version changed")
	}
	return &state, nil
}
func (j *captureOutput) saveMirror(f *delivery.Frame) error {
	if j.session.SaveMirror != nil {
		return j.session.SaveMirror(f)
	}
	return nil
}
func metadataAt(text string, cursor core.Cursor) (string, error) {
	p, err := commitmeta.NewCodec().Decode(text)
	if err != nil {
		return "", err
	}
	p.Stop = cursor.Stop
	p.End = cursor.End
	return commitmeta.NewCodec().Encode(p)
}
func (a *archiveLease) acknowledge(ctx context.Context, contents capture.State) error {
	for id, boundary := range contents.Files {
		old, ok := a.contents.Files[id]
		if !ok || old != boundary {
			return errors.New("recovery attempted to add an archive boundary")
		}
	}
	value, err := checkpoint.Encode(contents, 32<<20)
	if err != nil {
		return err
	}
	return a.journal.log.Publish(ctx, a.id, a.writerID, a.journal.session.Key("owner/"+a.writerID), value)
}

type localWriterIdentity struct {
	Version  int
	WriterID string
}

func (j *captureOutput) localWriter(root, logical string, partition int32) (string, error) {
	path := filepath.Join(root, ".kaferry", "eos-leases", logical+".owner")
	data, err := (replay.NativeGroupFiles{}).Read(path, 1024)
	if err == nil {
		owner, err := checkpoint.Decode[localWriterIdentity](data, 1024)
		id := owner.WriterID
		if err != nil {
			return "", err
		}
		bytes, err := hex.DecodeString(id)
		if err != nil || len(bytes) != 16 || owner.Version != 1 {
			return "", errors.New("invalid local writer identity")
		}
		return id, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	entries, err := os.ReadDir(filepath.Join(root, ".kaferry", "writers"))
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) != 32 {
			continue
		}
		data, err := (replay.NativeGroupFiles{}).Read(filepath.Join(root, ".kaferry", "writers", entry.Name(), "state.json"), delivery.MaxStateBytes)
		if err != nil {
			return "", err
		}
		envelope, err := checkpoint.Decode[archiveEnvelope](data, delivery.MaxStateBytes)
		if err != nil {
			return "", err
		}
		if envelope.Identity == j.session.Identity && envelope.Partition == partition {
			return "", errors.New("local writer binding missing for initialized archive")
		}
	}
	var bytes [16]byte
	if _, err = rand.Read(bytes[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(bytes[:])
	encoded, err := checkpoint.Encode(localWriterIdentity{Version: 1, WriterID: id}, 1024)
	if err != nil {
		return "", err
	}
	if err = j.writer.Replace(path, encoded); err != nil {
		return "", err
	}
	return id, nil
}

func (j *captureOutput) Prepare(ctx context.Context, ids []int32) error {
	if j.log != nil {
		return j.log.Ensure(ctx, ids)
	}
	return nil
}
