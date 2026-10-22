package checkpoint

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

type envelope struct {
	State  json.RawMessage `json:"state"`
	SHA256 string          `json:"sha256"`
}

// Encode preserves canonical JSON and checks its size before any filesystem write.
func Encode[T any](state T, limit int) ([]byte, error) {
	payload, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(payload)
	// The envelope contains valid JSON from Marshal above and a hexadecimal string.
	data, _ := json.Marshal(envelope{State: payload, SHA256: hex.EncodeToString(hash[:])})
	if len(data) > limit {
		return nil, fmt.Errorf("state exceeds %d MiB", limit>>20)
	}
	return data, nil
}

func Decode[T any](data []byte, limit int) (T, error) {
	var state T
	if len(data) > limit {
		return state, fmt.Errorf("state exceeds %d MiB", limit>>20)
	}
	var saved envelope
	if err := json.Unmarshal(data, &saved); err != nil {
		return state, err
	}
	hash := sha256.Sum256(saved.State)
	if saved.SHA256 != hex.EncodeToString(hash[:]) {
		return state, errors.New("state checksum mismatch")
	}
	if err := json.Unmarshal(saved.State, &state); err != nil {
		return state, err
	}
	canonical, err := Encode(state, limit)
	if err != nil {
		return state, err
	}
	if !bytes.Equal(data, canonical) {
		return state, errors.New("state JSON is not canonical")
	}
	return state, nil
}
