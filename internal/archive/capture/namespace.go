package capture

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pagrus7/kaferry/internal/archive/avro"
)

type owner struct {
	path string
	lock io.Closer
}

// Open recovers every available writer namespace, then keeps one locked for use.
func Open(root string, codec *avro.Codec, files Filesystem, states StateStore) (store *Store, result error) {
	root, err := files.ResolveRoot(root)
	if err != nil {
		return nil, err
	}
	writers := filepath.Join(root, ".kaferry", "writers")
	for _, directory := range []string{root, filepath.Dir(writers), writers} {
		if err := ensureDirectory(files, directory); err != nil {
			return nil, err
		}
	}
	owners, err := acquireNamespaces(writers, files)
	if err != nil {
		return nil, err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, releaseOwners(owners))
		}
	}()
	if len(owners) == 0 {
		path, lock, err := initializeNamespace(writers, newID(), files, states)
		if err != nil {
			return nil, err
		}
		owners = append(owners, owner{path: path, lock: lock})
	} else {
		protected := make(map[string]bool, len(owners))
		for _, owned := range owners {
			id := filepath.Base(owned.path)
			if err := recoverNamespace(root, owned.path, id, codec, files, states); err != nil {
				return nil, fmt.Errorf("recover archive writer %s: %w", id, err)
			}
			protected[id] = true
		}
		if err := discardUnacknowledged(root, protected, files); err != nil {
			return nil, err
		}
	}
	if err := releaseOwners(owners[1:]); err != nil {
		return nil, err
	}
	keep := owners[0]
	return &Store{root: root, namespace: keep.path, lock: keep.lock, files: files, states: states,
		codec: codec, state: emptyState(filepath.Base(keep.path)), active: make(map[string]*activeFile)}, nil
}

func acquireNamespaces(writers string, files Filesystem) (owners []owner, result error) {
	defer func() {
		if result != nil {
			result = errors.Join(result, releaseOwners(owners))
			owners = nil
		}
	}()
	entries, err := files.ReadDir(writers)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !validID(entry.Name()) {
			continue
		}
		if !entry.IsDir() {
			return owners, errors.New("archive writer namespace must be a directory")
		}
		namespace := filepath.Join(writers, entry.Name())
		lock, err := files.TryLock(filepath.Join(namespace, "lock"))
		if errors.Is(err, ErrBusy) {
			continue
		}
		if err != nil {
			return owners, err
		}
		owners = append(owners, owner{path: namespace, lock: lock})
	}
	return owners, nil
}

func releaseOwners(owners []owner) error {
	var result error
	for i := range owners {
		if owners[i].lock != nil {
			result = errors.Join(result, owners[i].lock.Close())
			owners[i].lock = nil
		}
	}
	return result
}

func ensureDirectory(files Filesystem, path string) error {
	if err := files.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := files.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("archive path must be a directory")
	}
	return files.SyncDirectory(filepath.Dir(path))
}

func initializeNamespace(writers, id string, files Filesystem, states StateStore) (path string, lock io.Closer, result error) {
	temporary := filepath.Join(writers, ".init-"+id)
	if err := files.Mkdir(temporary, 0700); err != nil {
		return "", nil, err
	}
	f, err := files.OpenFile(filepath.Join(temporary, "lock"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return "", nil, err
	}
	if err = errors.Join(f.Sync(), f.Close()); err != nil {
		return "", nil, err
	}
	lock, err = files.TryLock(filepath.Join(temporary, "lock"))
	if err != nil {
		return "", nil, err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, lock.Close())
			lock = nil
		}
	}()
	if err = states.Save(filepath.Join(temporary, "state.json"), emptyState(id)); err != nil {
		return "", lock, err
	}
	if err = files.SyncDirectory(temporary); err != nil {
		return "", lock, err
	}
	path = filepath.Join(writers, id)
	if err = files.RenameExclusive(temporary, path); err != nil {
		return "", lock, err
	}
	return path, lock, files.SyncDirectory(writers)
}
