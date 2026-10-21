package replay

import (
	"container/heap"
	"errors"
	"fmt"
	"io"

	"github.com/pagrus7/kaferry/internal/archive/avro"
)

type Blocks interface {
	Read(File, Position) (avro.Record, Position, error)
	Close() error
}
type Entry struct {
	Record avro.Record
	File   int
	Next   Position
}
type head struct {
	file   int
	offset int64
	source avro.Source
	id     string
}
type heads []head

func (h heads) Len() int      { return len(h) }
func (h heads) Swap(a, b int) { h[a], h[b] = h[b], h[a] }
func (h heads) Less(a, b int) bool {
	x, y := h[a], h[b]
	if n := compareSource(x.source, y.source); n != 0 {
		return n < 0
	}
	if x.offset != y.offset {
		return x.offset < y.offset
	}
	return x.id < y.id
}
func (h *heads) Push(v any) { *h = append(*h, v.(head)) }
func (h *heads) Pop() any {
	old := *h
	n := len(old) - 1
	x := old[n]
	old[n] = head{}
	*h = old[:n]
	return x
}

// Reader retains scalar heads only; Blocks owns the bounded payload cache.
type Reader struct {
	blocks  Blocks
	files   []Progress
	queue   heads
	pending int
	failed  error
	closed  bool
}

func NewReader(files []Progress, blocks Blocks) (*Reader, error) {
	if err := validateProgress(files); err != nil {
		return nil, err
	}
	r := &Reader{blocks: blocks, files: append([]Progress(nil), files...), pending: -1}
	for i, p := range r.files {
		if p.ended() {
			continue
		}
		if p.Next == initial(p.File) {
			r.push(i, p.File.Identity.FirstOffset)
		} else {
			record, _, err := readAt(blocks, p)
			if err != nil {
				return nil, err
			}
			r.push(i, record.Offset)
		}
	}
	return r, nil
}

func (r *Reader) push(i int, offset int64) {
	p := r.files[i]
	heap.Push(&r.queue, head{file: i, offset: offset, source: p.File.Identity.Source, id: p.File.Identity.FileID})
}

func (r *Reader) Next() (entry Entry, err error) {
	if r.failed != nil {
		return entry, r.failed
	}
	if r.closed {
		return entry, errors.New("archive replay reader is closed")
	}
	defer func() {
		if err != nil && !errors.Is(err, io.EOF) {
			r.failed = err
		}
	}()
	if r.pending >= 0 {
		i := r.pending
		r.pending = -1
		p := r.files[i]
		if !p.ended() {
			record, _, err := readAt(r.blocks, p)
			if err != nil {
				return entry, err
			}
			r.push(i, record.Offset)
		}
	}
	if len(r.queue) == 0 {
		return entry, io.EOF
	}
	h := heap.Pop(&r.queue).(head)
	p := r.files[h.file]
	record, next, err := readAt(r.blocks, p)
	if err != nil {
		return entry, err
	}
	if record.Offset != h.offset {
		return entry, fmt.Errorf("archive file %s: expected source offset %d, got %d", p.File.name(), h.offset, record.Offset)
	}
	r.files[h.file].Next = next
	r.pending = h.file
	return Entry{Record: record, File: h.file, Next: next}, nil
}

func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.blocks.Close()
}

// readAt is used only for unfinished cursors; EOF here means a shortened input.
func readAt(blocks Blocks, p Progress) (avro.Record, Position, error) {
	record, next, err := blocks.Read(p.File, p.Next)
	if errors.Is(err, io.EOF) {
		err = fmt.Errorf("archive file %s ended before block byte %d, record %d: %w", p.File.name(), p.Next.Block, p.Next.Record, io.ErrUnexpectedEOF)
	}
	return record, next, err
}
