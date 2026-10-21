package replay

import (
	"encoding/hex"
	"errors"
)

// Position identifies the next physical record, including duplicate source offsets.
type Position struct {
	Block    int64 `json:"block"`
	Record   int64 `json:"record"`
	Previous int64 `json:"previous"`
}
type Progress struct {
	File File     `json:"file"`
	Next Position `json:"next"`
}

func initial(file File) Position { return Position{Block: file.Start, Previous: -1} }
func (p Progress) ended() bool   { return p.Next.Block == p.File.Size }

func (f File) validate() error {
	if err := f.Identity.Validate(); err != nil {
		return err
	}
	digest, err := hex.DecodeString(f.SHA256)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != f.SHA256 || f.Start < 1 || f.Size <= f.Start {
		return errors.New("invalid selected archive file")
	}
	return nil
}

func (p Progress) validate() error {
	if err := p.File.validate(); err != nil {
		return err
	}
	pos := p.Next
	if pos.Block < p.File.Start || pos.Block > p.File.Size || pos.Record < 0 || pos.Record >= 1_000_000 || pos.Previous < -1 {
		return errors.New("invalid archive replay cursor")
	}
	if p.ended() && pos.Record != 0 {
		return errors.New("invalid archive EOF cursor")
	}
	isStart := pos.Block == p.File.Start && pos.Record == 0
	if isStart != (pos.Previous == -1) || !isStart && pos.Previous < p.File.Identity.FirstOffset {
		return errors.New("invalid previous source offset in replay cursor")
	}
	return nil
}
