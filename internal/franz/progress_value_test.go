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
