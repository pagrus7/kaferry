package franz

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCheckpointChunksRejectMissingAndCorruptData(t *testing.T) {
	// Given a checkpoint whose independently authored head covers two chunks.
	value := []byte("abcdef")
	head, _ := json.Marshal(valueHead{Version: 1, Size: 6, Parts: 2, SHA256: valueDigest(value)})
	for _, test := range []struct {
		name   string
		chunks map[int]string
		valid  bool
	}{
		{"complete", map[int]string{0: "abc", 1: "def"}, true},
		{"missing", map[int]string{0: "abc"}, false},
		{"corrupt", map[int]string{0: "abc", 1: "xyz"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reader valueReader
			for _, n := range slices.Sorted(maps.Keys(test.chunks)) {
				v := test.chunks[n]
				require.NoError(t, reader.accept("state", &kgo.Record{Key: []byte(fmt.Sprintf("state/chunk/%d", n)), Value: []byte(v)}))
			}
			require.NoError(t, reader.accept("state", &kgo.Record{Key: []byte("state"), Value: head}))
			// When read, then missing/corrupt chunks fail instead of returning fresh progress.
			got, err := reader.value()
			if test.valid {
				require.NoError(t, err)
				require.Equal(t, value, got)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestCheckpointChunkHeaderRejectsMalformedOrUnsupportedState(t *testing.T) {
	for _, head := range []string{`{`, `{"Version":2,"Size":3,"Parts":1}`, `{"Version":1,"Size":0,"Parts":1}`, `{"Version":1,"Size":3,"Parts":4}`} {
		t.Run(head, func(t *testing.T) {
			// Given a saved chunk prefix with an invalid or unsupported header.
			var reader valueReader
			require.NoError(t, reader.accept("state", &kgo.Record{Key: []byte("state/chunk/0"), Value: []byte("abc")}))
			// When the header arrives, then it fails without accepting authority or changing the saved chunk bytes.
			require.Error(t, reader.accept("state", &kgo.Record{Key: []byte("state"), Value: []byte(head)}))
			require.Nil(t, reader.head)
			require.Equal(t, []byte("abc"), reader.chunks[0])
			_, err := reader.value()
			require.ErrorContains(t, err, "lack a committed header")
		})
	}
}

func TestCheckpointChunkSizeChangesPreserveLatestValue(t *testing.T) {
	// Given valid large checkpoints written with different producer batch limits.
	value := bytes.Repeat([]byte("checkpoint"), (60<<20)/10)
	var history []*kgo.Record
	for _, size := range []int{64 << 10, 256 << 10, 512 << 10} {
		for start, part := 0, 0; start < len(value); start, part = start+size, part+1 {
			history = append(history, &kgo.Record{Key: []byte(fmt.Sprintf("state/chunk/%d", part)), Value: value[start:min(start+size, len(value))]})
		}
		head, err := json.Marshal(valueHead{Version: 1, Size: len(value), Parts: (len(value) + size - 1) / size, SHA256: valueDigest(value)})
		require.NoError(t, err)
		history = append(history, &kgo.Record{Key: []byte("state"), Value: head})
	}
	for _, compacted := range []bool{false, true} {
		t.Run(fmt.Sprint("compacted=", compacted), func(t *testing.T) {
			latest := make(map[string]int)
			for i, record := range history {
				latest[string(record.Key)] = i
			}
			// When scanning the ordinary log or its last-record-per-key compacted prefix.
			var reader valueReader
			for i, record := range history {
				if !compacted || latest[string(record.Key)] == i {
					require.NoError(t, reader.accept("state", record))
				}
			}
			// Then obsolete high indices cannot prevent resuming the latest checkpoint.
			got, err := reader.value()
			require.NoError(t, err)
			require.Equal(t, value, got)
			require.Len(t, reader.chunks, (len(value)+(512<<10)-1)/(512<<10))
		})
	}
}

func TestCheckpointReaderBoundsIncompleteGenerations(t *testing.T) {
	// Given a persisted generation whose accumulated chunks exceed the wire limit.
	var reader valueReader
	chunk := make([]byte, 512<<10)
	for part := range maxValueBytes/len(chunk) + 1 {
		require.NoError(t, reader.accept("state", &kgo.Record{Key: []byte(fmt.Sprintf("state/chunk/%d", part)), Value: chunk}))
	}
	// When that prefix ends, then it fails without retaining the oversized buffers.
	_, err := reader.value()
	require.ErrorContains(t, err, "exceed 96 MiB")
	require.Nil(t, reader.chunks)
	// When a later complete transaction replaces it, then its valid state is readable.
	value := []byte("current")
	require.NoError(t, reader.accept("state", &kgo.Record{Key: []byte("state/chunk/0"), Value: value}))
	_, err = reader.value()
	require.ErrorContains(t, err, "lack a committed header")
	head, err := json.Marshal(valueHead{Version: 1, Size: len(value), Parts: 1, SHA256: valueDigest(value)})
	require.NoError(t, err)
	require.NoError(t, reader.accept("state", &kgo.Record{Key: []byte("state"), Value: head}))
	got, err := reader.value()
	require.NoError(t, err)
	require.Equal(t, value, got)
}
