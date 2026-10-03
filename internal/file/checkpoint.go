package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	copy "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
)

type FrameIdentity struct {
	Position int64  `json:"position"`
	CRC      uint32 `json:"crc"`
}
type Checkpoint struct {
	Version       int            `json:"version"`
	Fingerprint   string         `json:"fingerprint"`
	SourceID      string         `json:"source_id"`
	Position      int64          `json:"position"`
	LastFrame     *FrameIdentity `json:"last_frame"`
	Window        plan.Window    `json:"window"`
	DestinationID string         `json:"destination_id"`
	Extent        int64          `json:"extent"`
}
type AtomicWriter interface{ Replace(string, []byte) error }
type DurableWriter struct{}

func (DurableWriter) Replace(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".checkpoint-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	return syncDirectory(path)
}

type Checkpoints struct {
	path    string
	lock    *os.File
	writer  AtomicWriter
	saved   *Checkpoint
	current Checkpoint
	frames  map[int64]*FrameIdentity
	output  copy.OutputProgress
}

func NewCheckpoints(directory, group string, writer AtomicWriter) (*Checkpoints, error) {
	if err := createDirectory(directory); err != nil {
		return nil, err
	}
	var slug strings.Builder
	for _, r := range group {
		if slug.Len() >= 48 {
			break
		}
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			slug.WriteRune(r)
		} else {
			slug.WriteByte('_')
		}
	}
	hash := sha256.Sum256([]byte(group))
	path := filepath.Join(directory, slug.String()+"-"+hex.EncodeToString(hash[:8])+".json")
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lock(f); err != nil {
		f.Close()
		return nil, err
	}
	if err = syncDirectory(path + ".lock"); err != nil {
		f.Close()
		return nil, err
	}
	c := &Checkpoints{path: path, lock: f, writer: writer, frames: make(map[int64]*FrameIdentity)}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return c, nil
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	if len(data) > 65536 {
		f.Close()
		return nil, conflict("checkpoint too large")
	}
	var saved Checkpoint
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&saved); err != nil || decoder.Decode(new(any)) != io.EOF || saved.Version != 1 || saved.Fingerprint == "" || saved.SourceID == "" || saved.Position < 0 || saved.Extent < 0 || (saved.DestinationID == "") != (saved.Extent == 0) {
		f.Close()
		return nil, conflict("invalid checkpoint")
	}
	c.saved = &saved
	c.current = saved
	c.frames[saved.Position] = saved.LastFrame
	return c, nil
}
func (c *Checkpoints) Existing() *Checkpoint                 { return c.saved }
func (c *Checkpoints) BindOutput(output copy.OutputProgress) { c.output = output }
func (c *Checkpoints) Save(ctx context.Context, cursors []copy.Cursor) (map[int32]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(cursors) != 1 || cursors[0].Partition != 0 {
		return nil, fmt.Errorf("file checkpoints require one source cursor")
	}
	position := cursors[0].Position
	last, present := c.frames[position]
	if !present {
		return nil, fmt.Errorf("checkpoint position is not a known frame boundary")
	}
	next := c.current
	next.Position = position
	next.LastFrame = last
	if c.output != nil {
		next.DestinationID, next.Extent = c.output.DurableOutput()
	}
	data, err := json.Marshal(next)
	if err != nil {
		return nil, err
	}
	if err = c.writer.Replace(c.path, data); err != nil {
		return nil, err
	}
	c.current = next
	c.saved = &next
	for offset := range c.frames {
		if offset < position {
			delete(c.frames, offset)
		}
	}
	return map[int32]error{0: nil}, nil
}
func (c *Checkpoints) Delete() error {
	if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDirectory(c.path)
}
func (c *Checkpoints) Close() { _ = c.lock.Close() }
