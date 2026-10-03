package copy

import "fmt"

type entry struct {
	token     Token
	next      int64
	bytes     int64
	resolved  bool
	submitted bool
	position  int64
}
type partitionProgress struct {
	generation uint64
	sequence   uint64
	committed  int64
	entries    []*entry
	pending    map[uint64]*entry
	bytes      int64
}
type Tracker struct {
	partitions map[int32]*partitionProgress
	generation uint64
}

func NewTracker() *Tracker { return &Tracker{partitions: make(map[int32]*partitionProgress)} }
func (t *Tracker) Assign(cursor Cursor) {
	t.generation++
	t.partitions[cursor.Partition] = &partitionProgress{generation: t.generation, committed: cursor.Position, pending: make(map[uint64]*entry)}
}
func (t *Tracker) Revoke(partition int32) { delete(t.partitions, partition) }
func (t *Tracker) Observe(cursor Cursor, next, bytes int64, resolved bool) (Token, error) {
	p := t.partitions[cursor.Partition]
	if p == nil {
		return Token{}, fmt.Errorf("record for an unassigned partition")
	}
	previous := p.committed
	if n := len(p.entries); n > 0 {
		previous = p.entries[n-1].next
	}
	if cursor.Position < previous || next < cursor.Position {
		return Token{}, fmt.Errorf("source cursor moved backwards")
	}
	if bytes == 0 && resolved && next == previous {
		return Token{}, nil
	}
	p.sequence++
	token := Token{cursor.Partition, p.generation, p.sequence}
	e := &entry{token: token, next: next, bytes: bytes, resolved: resolved, position: cursor.Position}
	p.entries = append(p.entries, e)
	p.pending[token.Sequence] = e
	p.bytes += bytes
	return token, nil
}
func (t *Tracker) Resolve(token Token) bool {
	p := t.partitions[token.Partition]
	if p == nil || p.generation != token.Generation {
		return false
	}
	e := p.pending[token.Sequence]
	if e == nil {
		return false
	}
	e.resolved = true
	e.submitted = false
	return true
}
func (t *Tracker) Submitted(token Token) {
	if p := t.partitions[token.Partition]; p != nil && p.generation == token.Generation {
		if e := p.pending[token.Sequence]; e != nil {
			e.submitted = true
		}
	}
}
func (t *Tracker) InFlight() bool {
	for _, p := range t.partitions {
		for _, e := range p.entries {
			if e.submitted && !e.resolved {
				return true
			}
		}
	}
	return false
}
func (t *Tracker) PositionOf(token Token) int64 {
	if p := t.partitions[token.Partition]; p != nil {
		if e := p.pending[token.Sequence]; e != nil {
			return e.position
		}
	}
	return -1
}
func (t *Tracker) ResolveIfOwned(token Token, resolve bool) bool {
	p := t.partitions[token.Partition]
	if p == nil || p.generation != token.Generation || p.pending[token.Sequence] == nil {
		return false
	}
	e := p.pending[token.Sequence]
	e.submitted = false
	if resolve {
		e.resolved = true
	}
	return true
}
func (t *Tracker) Confirmed(partition int32) Cursor {
	p := t.partitions[partition]
	position := p.committed
	for _, e := range p.entries {
		if !e.resolved {
			break
		}
		position = e.next
	}
	return Cursor{partition, position}
}
func (t *Tracker) HasProgress(partition int32) bool {
	p := t.partitions[partition]
	return p != nil && t.Confirmed(partition).Position > p.committed
}
func (t *Tracker) Committed(cursor Cursor) {
	p := t.partitions[cursor.Partition]
	if p == nil {
		return
	}
	n := 0
	for _, e := range p.entries {
		if !e.resolved || e.next > cursor.Position {
			break
		}
		p.bytes -= e.bytes
		delete(p.pending, e.token.Sequence)
		n++
	}
	for i := 0; i < n; i++ {
		p.entries[i] = nil
	}
	p.entries = p.entries[n:]
	p.committed = cursor.Position
}
func (t *Tracker) Work(partition int32) (int, int64) {
	p := t.partitions[partition]
	if p == nil {
		return 0, 0
	}
	return len(p.entries), p.bytes
}
func (t *Tracker) Total() (int, int64) {
	var records int
	var bytes int64
	for _, p := range t.partitions {
		records += len(p.entries)
		bytes += p.bytes
	}
	return records, bytes
}
func (t *Tracker) Resolved(partition int32) bool {
	p := t.partitions[partition]
	if p == nil {
		return true
	}
	for _, e := range p.entries {
		if !e.resolved {
			return false
		}
	}
	return true
}
