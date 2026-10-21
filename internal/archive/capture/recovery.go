package capture

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pagrus7/kaferry/internal/archive/avro"
)

func recoverNamespace(root, namespace, owner string, codec *avro.Codec, files Filesystem, states StateStore) error {
	path := filepath.Join(namespace, "state.json")
	state, err := states.Load(path)
	if err != nil {
		return err
	}
	if state.WriterID != owner {
		return errors.New("archive state belongs to a different writer")
	}
	for _, id := range slices.Sorted(maps.Keys(state.Files)) {
		if err := recoverFile(root, state.Files[id], codec, files); err != nil {
			return fmt.Errorf("recover archive file %s: %w", id, err)
		}
	}
	if len(state.Files) > 0 {
		if err := states.Save(path, emptyState(owner)); err != nil {
			return err
		}
	}
	return discardUnacknowledged(root, owner, files)
}

func recoverFile(root string, boundary Boundary, codec *avro.Codec, files Filesystem) (result error) {
	completed := dataPath(root, boundary.Identity, false)
	f, err := files.OpenFile(completed, os.O_RDONLY, 0)
	if err == nil {
		defer func() { result = errors.Join(result, f.Close()) }()
		if err := validatePrefix(f, boundary, true, codec); err != nil {
			return err
		}
		if _, err := files.Lstat(dataPath(root, boundary.Identity, true)); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return err
			}
			return errors.New("both private and completed archive files exist")
		}
		return files.SyncDirectory(root)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	private := dataPath(root, boundary.Identity, true)
	f, err = files.OpenFile(private, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := validatePrefix(f, boundary, false, codec); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Truncate(boundary.Extent); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return err
	}
	if err := files.RenameExclusive(private, completed); err != nil {
		return err
	}
	return files.SyncDirectory(root)
}

func validatePrefix(f DataFile, boundary Boundary, completed bool, codec *avro.Codec) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < boundary.Extent || completed && info.Size() != boundary.Extent {
		return errors.New("acknowledged archive length or file type changed")
	}
	reader, err := codec.Open(f, boundary.Extent)
	if err != nil {
		return err
	}
	if reader.Identity != boundary.Identity {
		return errors.New("acknowledged archive identity changed")
	}
	stats, err := reader.Scan()
	if err != nil {
		return err
	}
	if stats.Records != boundary.Records {
		return errors.New("acknowledged archive record count changed")
	}
	return nil
}

func discardUnacknowledged(root, owner string, files Filesystem) error {
	entries, err := files.ReadDir(root)
	if err != nil {
		return err
	}
	removed := false
	prefix := "." + owner + "."
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) || !strings.HasSuffix(entry.Name(), ".partial") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), prefix), ".partial")
		if !validID(id) || !entry.Type().IsRegular() {
			return errors.New("unrecognized private archive file")
		}
		if err := files.Remove(filepath.Join(root, entry.Name())); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return files.SyncDirectory(root)
	}
	return nil
}
