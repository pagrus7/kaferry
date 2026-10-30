package kafkaport

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/stretchr/testify/require"
)

func TestProgressPreservesIdentity(t *testing.T) {
	// Given committed metadata in the documented format.
	metadata := `kaferry:2;{"fp":"fp","tid":"topic-id"}`

	// When Kaferry reads and writes that progress.
	progress, err := DecodeProgress(commitmeta.NewCodec(), &metadata, "fp", "topic-id", false)

	// Then the persisted identity and wire format remain unchanged.
	require.NoError(t, err)
	require.Equal(t, metadata, encodeMetadata(progress))
}

func TestInvalidProgressNeverBecomesAStartFallback(t *testing.T) {
	// Given
	valid := encodeMetadata(Progress{Fingerprint: "fingerprint", TopicID: "topic"})

	// When
	_, decodeProgressErr := DecodeProgress(commitmeta.NewCodec(), &valid, "fingerprint", "topic", false)

	// Then
	require.NoError(t, decodeProgressErr)
	for _, text := range []string{"", "foreign", valid + "{}", `kaferry:2;{"fp":"fingerprint","tid":"topic","unknown":1}`, encodeMetadata(Progress{Fingerprint: "other", TopicID: "topic"}), encodeMetadata(Progress{Fingerprint: "fingerprint", TopicID: "recreated"}), `kaferry:2;{"fp":"fingerprint","tid":"topic","fid":"file","extent":-1}`} {
		_, err := DecodeProgress(commitmeta.NewCodec(), &text, "fingerprint", "topic", false)
		require.Error(t, err, "accepted invalid progress: %s", text)
	}
	_, err := DecodeProgress(commitmeta.NewCodec(), &valid, "fingerprint", "topic", true)
	require.Error(t, err, "relative query accepted missing window")
}
