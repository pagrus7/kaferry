package archiveio

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/capture"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"os"
	"path/filepath"
)

// Recover publishes this disk's committed prefix after a visible replacement claim.
// Publication acknowledgements do not advance global source progress or fence its owner.
func (j *captureOutput) Recover(ctx context.Context, stop core.StopObserver) (result error) {
	if j.log == nil || j.session.Plan.Source.Kind != "TOPIC" || j.session.Plan.OneOff {
		return errors.New("local recovery requires coordinated capture and the original named group")
	}
	entries, err := os.ReadDir(filepath.Join(j.session.Plan.Destination.Name, ".kaferry", "writers"))
	if err != nil {
		return err
	}
	codec, err := avro.New(j.session.Limits.ArchiveBlockMaxBytes)
	if err != nil {
		return err
	}
	pending := []*archiveLease{}
	defer func() {
		for _, a := range pending {
			result = errors.Join(result, a.lock.Close())
		}
	}()
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) != 32 {
			continue
		}
		path := filepath.Join(j.session.Plan.Destination.Name, ".kaferry", "writers", entry.Name(), "state.json")
		data, err := (replay.NativeGroupFiles{}).Read(path, 96<<20)
		if err != nil {
			return err
		}
		envelope, err := checkpoint.Decode[archiveEnvelope](data, 96<<20)
		if err != nil {
			return err
		}
		if envelope.Identity.Group != j.session.Plan.Group {
			continue
		}
		if envelope.Version != delivery.Version || envelope.Identity != j.session.Identity {
			return errors.New("recovery query or source identity changed")
		}
		if envelope.Frame == nil {
			if len(envelope.Capture.Files) > 0 {
				return errors.New("incomplete archive checkpoint")
			}
			continue
		}
		if err = j.session.Validate(*envelope.Frame); err != nil {
			return err
		}
		partition := envelope.Frame.Cursor.Partition
		if partition != envelope.Partition {
			return errors.New("archive checkpoint partition changed")
		}
		logical := j.session.ArchiveNamespace(partition)
		lock, err := (replay.NativeGroupFiles{}).Acquire(filepath.Join(j.session.Plan.Destination.Name, ".kaferry", "eos-leases", logical+".lock"))
		if err != nil {
			return err
		}
		// Releasing the local lock does not settle the dead owner's broker
		// transaction. A different owner's committed claim proves it was fenced.
		claim, err := j.log.ReadValue(ctx, partition, j.session.Key("capture-owner"), false)
		if err != nil {
			_ = lock.Close()
			return err
		}
		writer, decodeErr := hex.DecodeString(string(claim))
		if decodeErr != nil || len(writer) != 16 || string(claim) == entry.Name() {
			_ = lock.Close()
			return errors.New("local recovery requires a replacement capture owner; resume capture on this disk or initialize a replacement first")
		}
		// A live writer may still be appending checkpoint chunks. Snapshot its
		// stable prefix; a new barrier could fall inside that transaction.
		frame, err := j.readFrame(ctx, partition, false)
		if err != nil {
			_ = lock.Close()
			return err
		}
		if frame == nil {
			_ = lock.Close()
			return errors.New("coordinator checkpoint missing during recovery")
		}
		owner, err := j.readOwner(ctx, partition, entry.Name(), false)
		if err != nil {
			_ = lock.Close()
			return err
		}
		if owner == nil {
			_ = lock.Close()
			return errors.New("coordinator owner manifest missing during recovery")
		}
		a := &archiveLease{journal: j, id: partition, writerID: entry.Name(), path: path, lock: lock, authority: owner, frame: frame, recoveryOnly: true}
		pending = append(pending, a)
	}
	for _, a := range pending {
		store, err := capture.Open(j.session.Plan.Destination.Name, codec, archiveFiles{writerID: a.writerID, journal: j, id: a.id}, a)
		if store != nil {
			stop.Stage("closing recovered archive files")
			err = errors.Join(err, store.Close())
		}
		if err != nil {
			return err
		}
	}
	return nil
}
