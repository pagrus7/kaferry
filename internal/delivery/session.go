// Package delivery binds confirmed input cursors to exactly-once output commits.
package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

const Version = 1
const MaxStateBytes = 96 << 20

type Identity struct {
	ReplayJob                         string
	ProgressTopic                     [16]byte
	Group, Fingerprint                string
	SourceCluster, DestinationCluster string
	SourceTopic, DestinationTopic     [16]byte
}
type Frame struct {
	Version  int
	Identity Identity
	Cursor   core.Cursor
	Metadata string
	Replay   *replay.State
}

type Partition interface {
	Saved() *Frame
	Commit(context.Context, Frame) error
	Close() error
}
type Backend interface {
	Prepare(context.Context, []int32) error
	Acquire(context.Context, int32) (Partition, error)
	Read(context.Context, int32) (*Frame, error)
}

// Log is the Kafka authority used by output transactions and coordinated capture.
// Open fences the prior stable producer before the caller reads saved state.
type Log interface {
	Ensure(context.Context, []int32) error
	Open(context.Context, int32) (Transaction, error)
	ReadValue(context.Context, int32, string, bool) ([]byte, error)
	Publish(context.Context, int32, string, string, []byte) error
}
type Transaction interface {
	Write(context.Context, string, []byte) error
	Commit(context.Context) error
	Close() error
}

type Session struct {
	Plan        plan.Plan
	Limits      config.Worker
	Identity    Identity
	Backend     Backend
	StageReplay func(core.Cursor) (*replay.State, error)
	SaveMirror  func(*Frame) error
	// Trace supports crash/fault evidence at commit boundaries.
	Trace      func(string, int32)
	mu         sync.Mutex
	traceMu    sync.Mutex
	partitions map[int32]Partition
}

func New(p plan.Plan, w config.Worker) *Session {
	return &Session{Plan: p, Limits: w, partitions: make(map[int32]Partition)}
}
func Hash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func (s *Session) Key(kind string) string {
	return Hash(fmt.Sprintf("%q/%q/%q", s.Plan.Group, s.Identity.SourceCluster, s.Identity.ReplayJob))[:32] + "/" + kind
}
func (s *Session) TransactionID(id int32, topic string) string {
	return "kaferry-v1-" + Hash(fmt.Sprintf("%q/%q/%q/%q/%d", topic, s.Identity.SourceCluster, s.Plan.Group, s.Identity.ReplayJob, id))[:32]
}
func (s *Session) ArchiveNamespace(id int32) string {
	return Hash(fmt.Sprintf("%q/%q/%q/%q/%d", s.Plan.Group, s.Identity.SourceCluster, s.Plan.Source.Kind, s.Identity.ReplayJob, id))[:32]
}
func (s *Session) Protocol(name string) string {
	return "kaferry-eos-v1-" + Hash(name + fmt.Sprint(s.Identity))[:16]
}
func (s *Session) Hook(point string, id int32) {
	if s.Trace != nil {
		s.traceMu.Lock()
		defer s.traceMu.Unlock()
		s.Trace(point, id)
	}
}
func Encode(frame Frame) ([]byte, error) { return checkpoint.Encode(frame, MaxStateBytes) }
func (s *Session) Decode(data []byte) (Frame, error) {
	f, err := checkpoint.Decode[Frame](data, MaxStateBytes)
	if err != nil {
		return f, err
	}
	return f, s.Validate(f)
}
func (s *Session) Validate(f Frame) error {
	if f.Version != Version {
		return errors.New("incompatible delivery checkpoint version")
	}
	if f.Identity != s.Identity {
		return errors.New("delivery checkpoint identity changed")
	}
	if f.Cursor.Position < 0 || f.Cursor.Partition < 0 {
		return errors.New("negative delivery checkpoint cursor")
	}
	if f.Cursor.End != nil && f.Cursor.Position > *f.Cursor.End {
		return errors.New("delivery cursor exceeds saved end")
	}
	if f.Cursor.Stop != nil && (f.Cursor.Stop.At < 0 || f.Cursor.Stop.At > f.Cursor.Position) {
		return errors.New("delivery stop exceeds saved cursor")
	}
	if s.Plan.Source.Kind == "ARCHIVE" {
		if f.Cursor.Partition != 0 || f.Metadata != "" || f.Replay == nil || f.Replay.Version != 1 || f.Replay.Group != s.Identity.Group || f.Replay.Fingerprint != s.Identity.Fingerprint || f.Replay.Files == nil || f.Replay.Position != f.Cursor.Position {
			return errors.New("delivery checkpoint lacks a matching physical replay cursor")
		}
	} else if f.Replay != nil || f.Metadata == "" {
		return errors.New("incomplete Kafka source checkpoint")
	}
	return nil
}
func (s *Session) Acquire(ctx context.Context, ids []int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		if s.partitions[id] != nil {
			return fmt.Errorf("partition %d already owned", id)
		}
	}
	if err := s.Backend.Prepare(ctx, ids); err != nil {
		return err
	}
	obtained := make([]Partition, len(ids))
	failures := make([]error, len(ids))
	var workers sync.WaitGroup
	jobs := make(chan int)
	concurrency := min(8, len(ids))
	if s.Plan.Destination.Kind == "ARCHIVE" {
		concurrency = min(1, len(ids))
	}
	for range concurrency {
		workers.Go(func() {
			for i := range jobs {
				obtained[i], failures[i] = s.Backend.Acquire(ctx, ids[i])
			}
		})
	}
	for i := range ids {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	var failure error
	for _, err := range failures {
		failure = errors.Join(failure, err)
	}
	if failure != nil {
		for _, p := range obtained {
			if p != nil {
				failure = errors.Join(failure, p.Close())
			}
		}
		return failure
	}
	for i, id := range ids {
		s.partitions[id] = obtained[i]
		s.Hook("acquired", id)
	}
	return nil
}
func (s *Session) Partition(id int32) Partition {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.partitions[id]
}
func (s *Session) Commit(ctx context.Context, cursor core.Cursor, metadata string) error {
	return s.CommitBatch(ctx, []Frame{{Cursor: cursor, Metadata: metadata}})
}
func (s *Session) CommitBatch(ctx context.Context, updates []Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	frames := make([]Frame, len(updates))
	for i, u := range updates {
		if s.partitions[u.Cursor.Partition] == nil {
			return errors.New("delivery partition is not owned")
		}
		f := Frame{Version: Version, Identity: s.Identity, Cursor: u.Cursor, Metadata: u.Metadata}
		if s.StageReplay != nil && s.Plan.Destination.Kind != "ARCHIVE" {
			saved, err := s.StageReplay(u.Cursor)
			if err != nil {
				return err
			}
			f.Replay = saved
		}
		if err := s.Validate(f); err != nil && s.Plan.Destination.Kind != "ARCHIVE" {
			return err
		}
		frames[i] = f
	}
	commit := func(f Frame) error {
		if err := s.partitions[f.Cursor.Partition].Commit(ctx, f); err != nil {
			return err
		}
		if s.SaveMirror != nil {
			return s.SaveMirror(&f)
		}
		return nil
	}
	if s.Plan.Destination.Kind == "ARCHIVE" || len(frames) < 2 {
		for _, f := range frames {
			if err := commit(f); err != nil {
				return err
			}
		}
		return nil
	}
	failures := make([]error, len(frames))
	var workers sync.WaitGroup
	jobs := make(chan int)
	for range min(8, len(frames)) {
		workers.Go(func() {
			for i := range jobs {
				failures[i] = commit(frames[i])
			}
		})
	}
	for i := range frames {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	var failure error
	for _, err := range failures {
		failure = errors.Join(failure, err)
	}
	return failure
}
func (s *Session) Release(ids []int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result error
	for _, id := range ids {
		if p := s.partitions[id]; p != nil {
			result = errors.Join(result, p.Close())
			delete(s.partitions, id)
		}
	}
	return result
}
func (s *Session) Close() error {
	s.mu.Lock()
	ids := make([]int32, 0, len(s.partitions))
	for id := range s.partitions {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	return s.Release(ids)
}

type Reader struct {
	Session *Session
	Log     Log
}

func (r Reader) Read(ctx context.Context, id int32) (*Frame, error) {
	return r.ReadBarrier(ctx, id, false)
}

func (r Reader) ReadBarrier(ctx context.Context, id int32, barrier bool) (*Frame, error) {
	data, err := r.Log.ReadValue(ctx, id, r.Session.Key("state"), barrier)
	if err != nil || data == nil {
		return nil, err
	}
	f, err := r.Session.Decode(data)
	if err != nil {
		return nil, err
	}
	if f.Cursor.Partition != id {
		return nil, errors.New("checkpoint partition disagrees with key")
	}
	return &f, nil
}
