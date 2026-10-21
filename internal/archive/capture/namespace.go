package capture

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pagrus7/kaferry/internal/archive/avro"
)

// Open recovers every available writer namespace, then keeps one locked for use.
func Open(root string, codec *avro.Codec, files Filesystem, states StateStore) (*Store, error) {
	writers := filepath.Join(root, ".kaferry", "writers")
	for _, directory := range []string{root, filepath.Dir(writers), writers} {
		if err := ensureDirectory(files, directory); err != nil {
			return nil, err
		}
	}
	s := &Store{root: root, files: files, states: states, codec: codec, active: make(map[string]*activeFile)}
	entries, err := files.ReadDir(writers)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !validID(entry.Name()) {
			continue
		}
		if !entry.IsDir() {
			return nil, errors.Join(errors.New("archive writer namespace must be a directory"), s.Close())
		}
		namespace := filepath.Join(writers, entry.Name())
		lock, err := files.TryLock(filepath.Join(namespace, "lock"))
		if errors.Is(err, ErrBusy) {
			continue
		}
		if err != nil {
			return nil, errors.Join(err, s.Close())
		}
		if err = recoverNamespace(root, namespace, entry.Name(), codec, files, states); err != nil {
			return nil, errors.Join(fmt.Errorf("recover archive writer %s: %w", entry.Name(), err), lock.Close(), s.Close())
		}
		if s.lock == nil {
			s.namespace, s.lock, s.state = namespace, lock, emptyState(entry.Name())
		} else if err = lock.Close(); err != nil {
			return nil, errors.Join(err, s.Close())
		}
	}
	if s.lock == nil {
		id := newID()
		s.namespace, s.lock, err = initializeNamespace(writers, id, files, states)
		if err != nil {
			return nil, err
		}
		s.state = emptyState(id)
	}
	return s, nil
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
