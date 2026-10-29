package archiveio

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/capture"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestRecoveryPreflightsEveryNamespaceBeforeChangingFiles(t *testing.T) {
	for _, invalid := range []string{"missing", "malformed", "version", "writer", "files"} {
		t.Run(invalid, func(t *testing.T) {
			// Given two ordered namespaces with approved private bytes; the later owner's authority is unusable.
			j, log, envelopes, paths := recoveryFixture(t)
			later := envelopes[1]
			key := authorityKey{later.Partition, j.session.Key("owner/" + later.Capture.WriterID)}
			original := log.values[key]
			if invalid == "missing" {
				delete(log.values, key)
			} else if invalid == "malformed" {
				log.values[key] = []byte("malformed manifest")
			} else {
				state := later.Capture
				switch invalid {
				case "version":
					state.Version++
				case "writer":
					state.WriterID = strings.Repeat("0", 32)
				case "files":
					state.Files = nil
				}
				data, err := checkpoint.Encode(state, 32<<20)
				require.NoError(t, err)
				log.values[key] = data
			}
			before, authority := archiveSnapshot(t, j.session.Plan.Destination.Name), cloneAuthority(log.values)
			acknowledged := map[string][]byte{}
			for _, envelope := range envelopes {
				for _, boundary := range envelope.Capture.Files {
					name := boundary.Identity.WriterID + "." + boundary.Identity.FileID
					data, err := os.ReadFile(filepath.Join(j.session.Plan.Destination.Name, "."+name+".partial"))
					require.NoError(t, err)
					acknowledged[name] = data[:boundary.Extent]
				}
			}
			// When recovery checks every namespace, then the later failure leaves all earlier files and authority unchanged.
			require.Error(t, j.Recover(t.Context(), core.NoopStopObserver{}))
			require.Equal(t, key, log.lastRead)
			require.Equal(t, before, archiveSnapshot(t, j.session.Plan.Destination.Name))
			require.Equal(t, authority, log.values)
			require.Zero(t, log.published)
			require.Zero(t, log.opened, "publication must never fence the active global owner")
			for _, path := range paths {
				lock, err := (replay.NativeGroupFiles{}).Acquire(path)
				require.NoError(t, err, "every preflight lock must be released")
				require.NoError(t, lock.Close())
			}
			// Then restored manifests allow publication, preserving acknowledged bytes and every global cursor.
			log.values[key] = original
			require.NoError(t, j.Recover(t.Context(), core.NoopStopObserver{}))
			require.Equal(t, 2, log.published)
			require.Zero(t, log.opened)
			for _, envelope := range envelopes {
				global := authorityKey{envelope.Partition, j.session.Key("state")}
				require.Equal(t, authority[global], log.values[global])
				for _, boundary := range envelope.Capture.Files {
					name := boundary.Identity.WriterID + "." + boundary.Identity.FileID
					completed, err := os.ReadFile(filepath.Join(j.session.Plan.Destination.Name, name+".avro"))
					require.NoError(t, err)
					require.Equal(t, acknowledged[name], completed)
					_, err = os.Stat(filepath.Join(j.session.Plan.Destination.Name, "."+name+".partial"))
					require.ErrorIs(t, err, os.ErrNotExist)
				}
			}
		})
	}
}

type authorityKey struct {
	partition int32
	key       string
}
type recoveryLog struct {
	values            map[authorityKey][]byte
	lastRead          authorityKey
	published, opened int
}

func (*recoveryLog) Ensure(context.Context, []int32) error { return nil }
func (l *recoveryLog) Open(context.Context, int32) (delivery.Transaction, error) {
	l.opened++
	return nil, errors.New("recovery may not fence the global owner")
}
func (l *recoveryLog) ReadValue(_ context.Context, id int32, key string, _ bool) ([]byte, error) {
	l.lastRead = authorityKey{id, key}
	return l.values[l.lastRead], nil
}
func (l *recoveryLog) Publish(_ context.Context, id int32, owner, key string, value []byte) error {
	if !strings.HasSuffix(key, "/owner/"+owner) {
		return errors.New("publication changed global authority")
	}
	l.published++
	l.values[authorityKey{id, key}] = append([]byte{}, value...)
	return nil
}
func cloneAuthority(values map[authorityKey][]byte) map[authorityKey][]byte {
	copy := make(map[authorityKey][]byte, len(values))
	for key, value := range values {
		copy[key] = append([]byte{}, value...)
	}
	return copy
}
func archiveSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			result[relative] = fmt.Sprintf("%x", sha256.Sum256(data))
		}
		return nil
	}))
	return result
}
func recoveryFixture(t *testing.T) (*captureOutput, *recoveryLog, []archiveEnvelope, []string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "archive")
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	files := capture.NativeFiles{}
	stores := []*capture.Store{}
	for id := range int32(2) {
		store, err := capture.Open(root, codec, files, capture.NewJournal(files, checkpoint.DurableWriter{}))
		require.NoError(t, err)
		stores = append(stores, store)
		t.Cleanup(func() { _ = store.Close() })
		source := avro.Source{Cluster: "source", Topic: "orders", TopicID: avro.TopicID{1}, Partition: id}
		segment, err := store.Create(source, 0)
		require.NoError(t, err)
		_, err = store.Append(segment.ID, avro.Record{Source: source, Offset: 0, Value: []byte("acknowledged"), TimestampKind: "unknown"})
		require.NoError(t, err)
		require.NoError(t, store.Checkpoint())
	}
	for _, store := range stores {
		require.NoError(t, store.Close())
	}
	p := plan.Plan{Group: "recovery", Source: plan.Endpoint{Kind: "TOPIC", Name: "orders"}, Destination: plan.Endpoint{Kind: "ARCHIVE", Name: root}}
	s := delivery.New(p, config.Defaults("").Worker)
	s.Identity = delivery.Identity{Group: p.Group, Fingerprint: "query", SourceCluster: "source", SourceTopic: [16]byte{1}, ProgressTopic: [16]byte{2}}
	log := &recoveryLog{values: map[authorityKey][]byte{}}
	j := NewCapture(s, core.RealClock{}, log, checkpoint.DurableWriter{})
	paths, err := filepath.Glob(filepath.Join(root, ".kaferry", "writers", "*", "state.json"))
	require.NoError(t, err)
	require.Len(t, paths, 2)
	require.NoError(t, os.Mkdir(filepath.Join(root, ".kaferry", "eos-leases"), 0700))
	envelopes := []archiveEnvelope{}
	locks := []string{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		state, err := checkpoint.Decode[capture.State](data, 32<<20)
		require.NoError(t, err)
		require.Len(t, state.Files, 1)
		var partition int32
		for _, boundary := range state.Files {
			partition = boundary.Identity.Source.Partition
		}
		frame := delivery.Frame{Version: delivery.Version, Identity: s.Identity, Cursor: core.Cursor{Partition: partition, Position: 1}, Metadata: "source metadata"}
		envelope := archiveEnvelope{Version: delivery.Version, Partition: partition, Identity: s.Identity, Frame: &frame, Capture: state}
		encoded, err := checkpoint.Encode(envelope, 96<<20)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, encoded, 0600))
		envelopes = append(envelopes, envelope)
		lockPath := filepath.Join(root, ".kaferry", "eos-leases", s.ArchiveNamespace(partition)+".lock")
		lock, err := (replay.NativeGroupFiles{}).Acquire(lockPath)
		require.NoError(t, err)
		require.NoError(t, lock.Close())
		locks = append(locks, lockPath)
		claim := "0" + state.WriterID[1:]
		if claim == state.WriterID {
			claim = "1" + state.WriterID[1:]
		}
		log.values[authorityKey{partition, s.Key("capture-owner")}] = []byte(claim)
		log.values[authorityKey{partition, s.Key("state")}], err = delivery.Encode(frame)
		require.NoError(t, err)
		log.values[authorityKey{partition, s.Key("owner/" + state.WriterID)}], err = checkpoint.Encode(state, 32<<20)
		require.NoError(t, err)
	}
	return j, log, envelopes, locks
}

func TestCaptureRejectsInvalidLocalWriterBindingBeforeAcquiringAuthority(t *testing.T) {
	for _, invalid := range []string{"malformed", "version", "hex", "length"} {
		t.Run(invalid, func(t *testing.T) {
			// Given an initialized disk whose local writer binding has invalid data with a valid checksum where applicable.
			j, log, envelopes, _ := recoveryFixture(t)
			e := envelopes[0]
			path := filepath.Join(j.session.Plan.Destination.Name, ".kaferry", "eos-leases", j.session.ArchiveNamespace(e.Partition)+".owner")
			owner := localWriterIdentity{Version: 1, WriterID: e.Capture.WriterID}
			if invalid == "version" {
				owner.Version++
			}
			if invalid == "hex" {
				owner.WriterID = strings.Repeat("z", 32)
			}
			if invalid == "length" {
				owner.WriterID = "ab"
			}
			data, err := checkpoint.Encode(owner, 1024)
			require.NoError(t, err)
			if invalid == "malformed" {
				data = []byte("malformed writer binding")
			}
			require.NoError(t, os.WriteFile(path, data, 0600))
			before, authority := archiveSnapshot(t, j.session.Plan.Destination.Name), cloneAuthority(log.values)
			// When capture acquires the partition, then it fails before fencing or changing record files or authority.
			partition, err := j.Acquire(t.Context(), e.Partition)
			require.Error(t, err)
			require.Nil(t, partition)
			require.Zero(t, log.opened)
			require.Equal(t, authority, log.values)
			require.Equal(t, before, archiveSnapshot(t, j.session.Plan.Destination.Name))
			// Then repairing the binding returns the original writer without allocating another namespace.
			valid, err := checkpoint.Encode(localWriterIdentity{Version: 1, WriterID: e.Capture.WriterID}, 1024)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, valid, 0600))
			writer, err := j.localWriter(j.session.Plan.Destination.Name, j.session.ArchiveNamespace(e.Partition), e.Partition)
			require.NoError(t, err)
			require.Equal(t, e.Capture.WriterID, writer)
		})
	}
}

func TestArchiveLoaderRejectsIncompatibleEnvelopeBeforeUsingBoundaries(t *testing.T) {
	for _, invalid := range []string{"version", "identity", "partition", "missing frame", "invalid frame", "missing owner"} {
		t.Run(invalid, func(t *testing.T) {
			// Given approved private files and either an incompatible checksummed envelope or missing owner authority.
			j, log, envelopes, _ := recoveryFixture(t)
			e := envelopes[0]
			path := filepath.Join(j.session.Plan.Destination.Name, ".kaferry", "writers", e.Capture.WriterID, "state.json")
			a := &archiveLease{journal: j, id: e.Partition, writerID: e.Capture.WriterID, path: path, authority: &e.Capture, recoveryOnly: true}
			switch invalid {
			case "version":
				e.Version++
			case "identity":
				e.Identity.Fingerprint = "another query"
			case "partition":
				e.Partition++
			case "missing frame":
				e.Frame = nil
			case "invalid frame":
				e.Frame.Version++
			case "missing owner":
				a.authority = nil
				delete(log.values, authorityKey{e.Partition, j.session.Key("owner/" + e.Capture.WriterID)})
			}
			data, err := checkpoint.Encode(e, 96<<20)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, data, 0600))
			before, authority := archiveSnapshot(t, j.session.Plan.Destination.Name), cloneAuthority(log.values)
			// When the archive loader checks saved state, then it refuses without returning boundaries or changing files or authority.
			loaded, err := a.Load(path)
			require.Error(t, err)
			require.Nil(t, loaded.Files)
			require.Equal(t, before, archiveSnapshot(t, j.session.Plan.Destination.Name))
			require.Equal(t, authority, log.values)
		})
	}
}
