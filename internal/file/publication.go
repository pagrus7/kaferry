package file

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// Publisher writes a durable header and publishes it without replacing a path.
type Publisher interface {
	Create(string, string, string, ArchiveEncoder) (*os.File, bool, error)
}

type publicationFS interface {
	CreateTemp(string, string) (*os.File, error)
	Write(*os.File, []byte) error
	Sync(*os.File) error
	Link(string, string) error
	Remove(string) error
	Open(string) (*os.File, error)
	SyncDirectory(string) error
}

type nativePublication struct{}

func (nativePublication) CreateTemp(dir, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}
func (nativePublication) Write(f *os.File, data []byte) error {
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}
func (nativePublication) Sync(f *os.File) error      { return f.Sync() }
func (nativePublication) Link(old, new string) error { return os.Link(old, new) }
func (nativePublication) Remove(path string) error   { return os.Remove(path) }
func (nativePublication) Open(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR, 0600)
}
func (nativePublication) SyncDirectory(path string) error { return syncDirectory(path) }

type publisher struct{ fs publicationFS }

func NewPublisher() Publisher { return &publisher{fs: nativePublication{}} }

func (p *publisher) Create(path, group, fp string, codec ArchiveEncoder) (*os.File, bool, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, false, err
	}
	temporary, err := p.fs.CreateTemp(filepath.Dir(path), temporaryPrefix(path)+"*")
	if err != nil {
		return nil, false, err
	}
	name := temporary.Name()
	defer p.fs.Remove(name)
	fail := func(err error) (*os.File, bool, error) { temporary.Close(); return nil, false, err }
	if err = lock(temporary); err != nil {
		return fail(err)
	}
	data, err := codec.EncodeHeader(Header{Version: 1, ID: hex.EncodeToString(id[:]), Group: group, Fingerprint: fp})
	if err != nil {
		return fail(err)
	}
	if err = p.fs.Write(temporary, data); err != nil {
		return fail(err)
	}
	if err = p.fs.Sync(temporary); err != nil {
		return fail(err)
	}
	if err = p.fs.Link(name, path); err != nil {
		temporary.Close()
		if errors.Is(err, os.ErrExist) {
			existing, err := p.fs.Open(path)
			return existing, false, err
		}
		return nil, false, err
	}
	if err = p.fs.Remove(name); err != nil {
		return fail(err)
	}
	if err = p.fs.SyncDirectory(path); err != nil {
		return fail(err)
	}
	return temporary, true, nil
}
