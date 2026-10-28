package archiveio

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/pagrus7/kaferry/internal/archive/capture"
	core "github.com/pagrus7/kaferry/internal/copy"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/delivery"
)

type Binding struct {
	clock                   core.Clock
	log                     *slog.Logger
	session                 *delivery.Session
	writer                  checkpoint.AtomicWriter
	mirror                  *replay.Journal
	mirrorState             *replay.State
	mirrorPath, bindingPath string
}

func NewBinding(s *delivery.Session, writer checkpoint.AtomicWriter, clock core.Clock, log *slog.Logger) *Binding {
	b := &Binding{session: s, writer: writer, clock: clock, log: log}
	s.SaveMirror = b.saveMirror
	return b
}
func (b *Binding) Close() error {
	if b.mirror != nil {
		return b.mirror.Close()
	}
	return nil
}
func (b *Binding) Delete() error {
	for _, path := range []string{b.mirrorPath, b.bindingPath} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return (capture.NativeFiles{}).SyncDirectory(filepath.Dir(b.mirrorPath))
}

type replayBinding struct {
	Format   string
	Version  int
	Identity delivery.Identity
}

func (j *Binding) Open(limits config.Worker) error {
	directory, err := checkpointDirectory(limits)
	if err != nil {
		return err
	}
	if err = separatePaths(j.session.Plan, directory); err != nil {
		return err
	}
	_, lockErr := os.Stat(checkpoint.Path(directory, j.session.Plan.Group) + ".lock")
	if lockErr != nil && !errors.Is(lockErr, os.ErrNotExist) {
		return lockErr
	}
	mirror, err := replay.NewJournal(directory, j.session.Plan.Group, replay.NativeGroupFiles{}, j.writer)
	if err != nil {
		return err
	}
	j.mirror = mirror
	saved, err := mirror.Load()
	if err != nil {
		return err
	}
	j.mirrorState = saved
	j.mirrorPath = checkpoint.Path(directory, j.session.Plan.Group)
	j.bindingPath = j.mirrorPath + ".eos"
	data, err := (replay.NativeGroupFiles{}).Read(j.bindingPath, 32<<10)
	if err == nil {
		binding, err := checkpoint.Decode[replayBinding](data, 32<<10)
		if err != nil {
			return err
		}
		if binding.Format != "kaferry.delivery/v1" || binding.Version != 1 || binding.Identity.ReplayJob == "" {
			return errors.New("invalid replay binding")
		}
		j.session.Identity.ReplayJob = binding.Identity.ReplayJob
		if binding.Identity.Fingerprint != j.session.Identity.Fingerprint {
			return errors.New("replay checkpoint query identity mismatch")
		}
		if binding.Identity != j.session.Identity {
			return errors.New("replay query, destination or coordinator identity changed")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if saved != nil {
		return errors.New("legacy replay checkpoint cannot establish exactly-once output; use an independent copy")
	}
	if lockErr == nil {
		return errors.New("replay binding missing for initialized checkpoint directory")
	}
	var id [16]byte
	_, _ = rand.Read(id[:])
	j.session.Identity.ReplayJob = hex.EncodeToString(id[:])
	encoded, err := checkpoint.Encode(replayBinding{Format: "kaferry.delivery/v1", Version: 1, Identity: j.session.Identity}, 32<<10)
	if err != nil {
		return err
	}
	return j.writer.Replace(j.bindingPath, encoded)
}
func (j *Binding) saveMirror(frame *delivery.Frame) error {
	if j.mirror != nil && frame != nil && frame.Replay != nil {
		return j.mirror.Save(*frame.Replay)
	}
	return nil
}

// InspectBinding reads job identity without acquiring its execution lock.
func InspectBinding(s *delivery.Session, w config.Worker) (bool, error) {
	directory, err := checkpointDirectory(w)
	if err != nil {
		return false, err
	}
	data, err := (replay.NativeGroupFiles{}).Read(checkpoint.Path(directory, s.Plan.Group)+".eos", 32<<10)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	b, err := checkpoint.Decode[replayBinding](data, 32<<10)
	if err != nil {
		return false, err
	}
	s.Identity.ReplayJob = b.Identity.ReplayJob
	if b.Identity.Fingerprint != s.Identity.Fingerprint {
		return false, &Conflict{Err: errors.New("replay checkpoint query identity mismatch")}
	}
	if b.Format != "kaferry.delivery/v1" || b.Version != 1 || b.Identity.ReplayJob == "" || b.Identity != s.Identity {
		return false, errors.New("replay binding identity or version changed")
	}
	return true, nil
}
