// Package replay reads fixed selections of completed Avro archive files.
package replay

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/plan"
	"golang.org/x/sys/unix"
)

type File struct {
	Identity avro.Identity `json:"identity"`
	Size     int64         `json:"size"`
	Start    int64         `json:"start"`
	SHA256   string        `json:"sha256"`
}

func (f File) name() string { return f.Identity.WriterID + "." + f.Identity.FileID + ".avro" }

type Selection struct {
	Files        []File
	PrivateFiles int
}
type Snapshot interface {
	Select(*plan.PartitionFilter) (Selection, error)
	Validate([]File) error
}
type InputFile interface {
	io.ReaderAt
	io.Closer
	Stat() (os.FileInfo, error)
}
type Directory interface {
	io.Closer
	ReadDir(int) ([]os.DirEntry, error)
}
type Filesystem interface {
	Open(string) (InputFile, error)
	OpenDirectory(string) (Directory, error)
}
type NativeFiles struct{}

func (NativeFiles) Open(path string) (InputFile, error) {
	return os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
}
func (NativeFiles) OpenDirectory(path string) (Directory, error) {
	return os.OpenFile(path, os.O_RDONLY|unix.O_DIRECTORY, 0)
}

type Catalog struct {
	root  string
	codec *avro.Codec
	files Filesystem
}

func NewCatalog(root string, codec *avro.Codec, files Filesystem) *Catalog {
	return &Catalog{root: root, codec: codec, files: files}
}

func (c *Catalog) Select(filter *plan.PartitionFilter) (Selection, error) {
	return c.selectFiles(true, filter)
}

// Preview supplies a snapshot that checks headers and sizes without hashing contents.
func (c *Catalog) Preview() Snapshot { return preview{c} }

type preview struct{ catalog *Catalog }

func (p preview) Select(filter *plan.PartitionFilter) (Selection, error) {
	return p.catalog.selectFiles(false, filter)
}

func (c *Catalog) selectFiles(hashContents bool, filter *plan.PartitionFilter) (selection Selection, result error) {
	selection.Files = make([]File, 0)
	dir, err := c.files.OpenDirectory(c.root)
	if err != nil {
		return selection, err
	}
	defer func() { result = errors.Join(result, dir.Close()) }()
	for {
		entries, readErr := dir.ReadDir(256)
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".partial") {
				selection.PrivateFiles++
				continue
			}
			if !strings.HasSuffix(entry.Name(), ".avro") {
				continue
			}
			if !entry.Type().IsRegular() {
				return selection, errors.New("completed archive input must be a regular file")
			}
			file, err := c.describe(entry.Name(), hashContents, filter)
			if err != nil {
				return selection, fmt.Errorf("select archive file %s: %w", entry.Name(), err)
			}
			selection.Files = append(selection.Files, file)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return selection, readErr
		}
	}
	slices.SortFunc(selection.Files, compareFile)
	seen := make(map[string]bool, len(selection.Files))
	for _, file := range selection.Files {
		if seen[file.Identity.FileID] {
			return selection, errors.New("duplicate archive file ID")
		}
		seen[file.Identity.FileID] = true
	}
	if filter != nil {
		ids := make([]int32, 0, len(selection.Files))
		for _, file := range selection.Files {
			ids = append(ids, file.Identity.Source.Partition)
		}
		if _, err := filter.Select(ids); err != nil {
			return selection, err
		}
		selection.Files = slices.DeleteFunc(selection.Files, func(f File) bool { return !filter.Matches(f.Identity.Source.Partition) })
	}
	return selection, nil
}

func (c *Catalog) describe(name string, hashContents bool, filter *plan.PartitionFilter) (descriptor File, result error) {
	f, err := c.files.Open(filepath.Join(c.root, name))
	if err != nil {
		return descriptor, err
	}
	defer func() { result = errors.Join(result, f.Close()) }()
	reader, size, err := c.header(f)
	if err != nil {
		return descriptor, err
	}
	descriptor = File{Identity: reader.Identity, Size: size, Start: reader.Start}
	if descriptor.name() != name {
		return descriptor, errors.New("archive filename disagrees with file identity")
	}
	if !hashContents || !filter.Matches(descriptor.Identity.Source.Partition) {
		return descriptor, nil
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.NewSectionReader(f, 0, size))
	if err != nil {
		return descriptor, err
	}
	if n != size {
		return descriptor, io.ErrUnexpectedEOF
	}
	descriptor.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return descriptor, nil
}

// Validate checks every selected file, including those already consumed, on resume.
func (c *Catalog) Validate(files []File) error {
	for _, file := range files {
		current, err := c.describe(file.name(), true, nil)
		if err != nil {
			return fmt.Errorf("validate archive file %s: %w", file.name(), err)
		}
		if current != file {
			return fmt.Errorf("selected archive file %s changed", file.name())
		}
	}
	return nil
}

func (c *Catalog) header(f InputFile) (*avro.Reader, int64, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, 0, errors.New("archive input must be a regular file")
	}
	reader, err := c.codec.Open(f, info.Size())
	if err != nil {
		return nil, 0, err
	}
	if reader.Start >= info.Size() {
		return nil, 0, errors.New("completed archive file contains no records")
	}
	return reader, info.Size(), nil
}

type Handle interface {
	Block(int64) (*avro.Block, error)
	Close() error
}
type Opener interface{ Open(File) (Handle, error) }
type inputHandle struct {
	*avro.Reader
	file InputFile
}

func (h *inputHandle) Close() error { return h.file.Close() }

// Open rechecks header and size. Full contents were verified before replay starts.
func (c *Catalog) Open(expected File) (handle Handle, result error) {
	if err := expected.validate(); err != nil {
		return nil, err
	}
	f, err := c.files.Open(filepath.Join(c.root, expected.name()))
	if err != nil {
		return nil, err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, f.Close())
		}
	}()
	reader, size, err := c.header(f)
	if err != nil {
		return nil, err
	}
	if reader.Identity != expected.Identity || size != expected.Size || reader.Start != expected.Start {
		return nil, errors.New("selected archive input changed")
	}
	return &inputHandle{Reader: reader, file: f}, nil
}

func compareFile(a, b File) int {
	if n := compareSource(a.Identity.Source, b.Identity.Source); n != 0 {
		return n
	}
	if n := cmp.Compare(a.Identity.FirstOffset, b.Identity.FirstOffset); n != 0 {
		return n
	}
	return strings.Compare(a.Identity.FileID, b.Identity.FileID)
}

func compareSource(a, b avro.Source) int {
	if n := strings.Compare(a.Cluster, b.Cluster); n != 0 {
		return n
	}
	if n := strings.Compare(a.Topic, b.Topic); n != 0 {
		return n
	}
	if n := slices.Compare(a.TopicID[:], b.TopicID[:]); n != 0 {
		return n
	}
	return cmp.Compare(a.Partition, b.Partition)
}

func (p preview) Validate(files []File) error {
	for _, file := range files {
		h, err := p.catalog.Open(file)
		if err != nil {
			return fmt.Errorf("inspect archive file %s: %w", file.name(), err)
		}
		if err = h.Close(); err != nil {
			return err
		}
	}
	return nil
}
