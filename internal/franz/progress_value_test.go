package franz

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
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
			require.NoError(t, reader.accept("state", &kgo.Record{Key: []byte("state"), Value: head}))
			for n, v := range test.chunks {
				require.NoError(t, reader.accept("state", &kgo.Record{Key: []byte(fmt.Sprintf("state/chunk/%d", n)), Value: []byte(v)}))
			}
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
