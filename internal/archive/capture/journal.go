package capture

import (
	"encoding/hex"
	"errors"
	"io"
	"os"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/checkpoint"
)

const maxStateBytes = 16 << 20

type Boundary struct {
	Identity avro.Identity `json:"identity"`
	Extent   int64         `json:"extent"`
	Records  int64         `json:"records"`
}

type State struct {
	Version  int                 `json:"version"`
	WriterID string              `json:"writer_id"`
	Files    map[string]Boundary `json:"files"`
}

type StateStore interface {
	Load(string) (State, error)
	Save(string, State) error
}

type AtomicWriter interface{ Replace(string, []byte) error }
type stateFiles interface {
	OpenFile(string, int, os.FileMode) (DataFile, error)
}

// Journal saves required recovery state, independently of source consumer commits.
type Journal struct {
	files  stateFiles
	writer AtomicWriter
}

func NewJournal(files stateFiles, writer AtomicWriter) *Journal {
	return &Journal{files: files, writer: writer}
}

func (j *Journal) Load(path string) (state State, result error) {
	f, err := j.files.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return State{}, err
	}
	defer func() { result = errors.Join(result, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return State{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxStateBytes {
		return State{}, errors.New("archive writer state is not a regular file within the size limit")
	}
	data, err := io.ReadAll(io.NewSectionReader(f, 0, info.Size()))
	if err != nil {
		return State{}, err
	}
	if int64(len(data)) != info.Size() {
		return State{}, io.ErrUnexpectedEOF
	}
	return decodeState(data)
}

func (j *Journal) Save(path string, state State) error {
	data, err := encodeState(state)
	if err != nil {
		return err
	}
	return j.writer.Replace(path, data)
}

func encodeState(state State) ([]byte, error) {
	if err := state.validate(); err != nil {
		return nil, err
	}
	return checkpoint.Encode(state, maxStateBytes)
}

func decodeState(data []byte) (State, error) {
	state, err := checkpoint.Decode[State](data, maxStateBytes)
	if err != nil {
		return state, err
	}
	return state, state.validate()
}

func (s State) validate() error {
	if s.Version != 1 || !validID(s.WriterID) || s.Files == nil {
		return errors.New("invalid or unsupported archive writer state")
	}
	for id, boundary := range s.Files {
		if !validID(id) || boundary.Identity.FileID != id || boundary.Identity.WriterID != s.WriterID || boundary.Extent <= 0 || boundary.Records <= 0 {
			return errors.New("invalid archive file boundary in writer state")
		}
	}
	return nil
}

func emptyState(writer string) State {
	return State{Version: 1, WriterID: writer, Files: make(map[string]Boundary)}
}

func validID(id string) bool {
	data, err := hex.DecodeString(id)
	return err == nil && len(data) == 16 && hex.EncodeToString(data) == id
}
