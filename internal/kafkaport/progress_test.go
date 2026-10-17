package kafkaport

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/commitmeta"
)

func TestProgressPreservesIdentity(t *testing.T) {
	// Given committed metadata in the documented format.
	metadata := `kaferry:1;{"fp":"fp","tid":"topic-id"}`

	// When Kaferry reads and writes that progress.
	progress, err := DecodeProgress(commitmeta.NewCodec(), &metadata, "fp", "topic-id", false)

	// Then the persisted identity and wire format remain unchanged.
	if err != nil || encodeMetadata(progress) != metadata {
		t.Fatal("Kafka progress changed", progress, err)
	}
}

func TestInvalidProgressNeverBecomesAStartFallback(t *testing.T) {
	// Given
	valid := encodeMetadata(Progress{Fingerprint: "fingerprint", TopicID: "topic"})

	// When
	_, decodeProgressErr := DecodeProgress(commitmeta.NewCodec(), &valid, "fingerprint", "topic", false)

	// Then
	if decodeProgressErr != nil {
		t.Fatal(decodeProgressErr)
	}
	for _, text := range []string{"", "foreign", valid + "{}", `kaferry:1;{"fp":"fingerprint","tid":"topic","unknown":1}`, encodeMetadata(Progress{Fingerprint: "other", TopicID: "topic"}), encodeMetadata(Progress{Fingerprint: "fingerprint", TopicID: "recreated"}), `kaferry:1;{"fp":"fingerprint","tid":"topic","fid":"file","extent":-1}`} {
		if _, err := DecodeProgress(commitmeta.NewCodec(), &text, "fingerprint", "topic", false); err == nil {
			t.Fatalf("accepted invalid progress: %s", text)
		}
	}
	if _, err := DecodeProgress(commitmeta.NewCodec(), &valid, "fingerprint", "topic", true); err == nil {
		t.Fatal("relative query accepted missing window")
	}
}
