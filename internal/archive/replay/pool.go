package replay

import (
	"errors"
	"fmt"
	"io"

	"github.com/pagrus7/kaferry/internal/archive/avro"
)

type Limits struct{ OpenFiles, Blocks int }
type handleEntry struct {
	handle  Handle
	touched uint64
}
type blockKey struct {
	file     File
	position int64
}
type blockEntry struct {
	block   *avro.Block
	index   int64
	record  avro.Record
	touched uint64
}

// Pool bounds open files and resident decoded blocks independently.
type Pool struct {
	opener   Opener
	limits   Limits
	handles  map[File]handleEntry
	blocks   map[blockKey]*blockEntry
	sequence uint64
	failed   error
	closed   bool
}

func NewPool(opener Opener, limits Limits) (*Pool, error) {
	if limits.OpenFiles == 0 {
		limits.OpenFiles = 32
	}
	if limits.Blocks == 0 {
		limits.Blocks = 8
	}
	if limits.OpenFiles < 1 || limits.Blocks < 1 {
		return nil, errors.New("replay file and block budgets must be positive")
	}
	return &Pool{opener: opener, limits: limits, handles: make(map[File]handleEntry), blocks: make(map[blockKey]*blockEntry)}, nil
}

func (p *Pool) Read(file File, pos Position) (record avro.Record, next Position, result error) {
	if p.failed != nil {
		return record, next, p.failed
	}
	if p.closed {
		return record, next, errors.New("archive replay pool is closed")
	}
	if pos.Block == file.Size && pos.Record == 0 {
		return record, next, io.EOF
	}
	defer func() {
		if result != nil {
			if errors.Is(result, io.EOF) {
				result = io.ErrUnexpectedEOF
			}
			result = fmt.Errorf("read archive file %s at block byte %d, record %d: %w", file.name(), pos.Block, pos.Record, result)
			p.failed = result
		}
	}()
	if pos.Record < 0 {
		return record, next, errors.New("negative archive record cursor")
	}
	p.sequence++
	key := blockKey{file: file, position: pos.Block}
	entry := p.blocks[key]
	if entry == nil || pos.Record < entry.index-1 {
		var err error
		entry, err = p.load(key)
		if err != nil {
			return record, next, err
		}
	}
	entry.touched = p.sequence
	if pos.Record >= entry.block.Count {
		return record, next, errors.New("archive cursor exceeds block record count")
	}
	for entry.index <= pos.Record {
		var err error
		entry.record, err = entry.block.Next()
		if err != nil {
			return record, next, err
		}
		entry.index++
	}
	record = entry.record
	if record.Offset < pos.Previous {
		return record, next, errors.New("source offset decreased within archive file")
	}
	next = Position{Block: entry.block.Start, Record: pos.Record + 1, Previous: record.Offset}
	if next.Record == entry.block.Count {
		next.Block, next.Record = entry.block.End, 0
	}
	return record, next, nil
}

func (p *Pool) load(key blockKey) (*blockEntry, error) {
	delete(p.blocks, key)
	if len(p.blocks) >= p.limits.Blocks {
		var oldest blockKey
		var age uint64 = ^uint64(0)
		for key, entry := range p.blocks {
			if entry.touched < age {
				oldest, age = key, entry.touched
			}
		}
		delete(p.blocks, oldest)
	}
	handle, err := p.handle(key.file)
	if err != nil {
		return nil, err
	}
	block, err := handle.Block(key.position)
	if err != nil {
		return nil, err
	}
	entry := &blockEntry{block: block, touched: p.sequence}
	p.blocks[key] = entry
	return entry, nil
}

func (p *Pool) handle(file File) (Handle, error) {
	if entry, ok := p.handles[file]; ok {
		entry.touched = p.sequence
		p.handles[file] = entry
		return entry.handle, nil
	}
	if len(p.handles) >= p.limits.OpenFiles {
		var oldest File
		var age uint64 = ^uint64(0)
		for key, entry := range p.handles {
			if entry.touched < age {
				oldest, age = key, entry.touched
			}
		}
		entry := p.handles[oldest]
		delete(p.handles, oldest)
		if err := entry.handle.Close(); err != nil {
			return nil, err
		}
	}
	handle, err := p.opener.Open(file)
	if err != nil {
		return nil, err
	}
	p.handles[file] = handleEntry{handle: handle, touched: p.sequence}
	return handle, nil
}

func (p *Pool) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	var err error
	for key, entry := range p.handles {
		err = errors.Join(err, entry.handle.Close())
		delete(p.handles, key)
	}
	clear(p.blocks)
	return err
}
