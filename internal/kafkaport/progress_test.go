package kafkaport

import "testing"

func TestReleasedProgressSurvivesRename(t *testing.T) {
	// Given committed metadata in the released v1 format.
	metadata := `kfwd2:1;{"fp":"fp","tid":"topic-id"}`

	// When Kaferry reads and writes that progress.
	progress, err := DecodeProgress(&metadata, "fp", "topic-id", false)

	// Then the persisted identity and wire format remain unchanged.
	if err != nil || progress.Encode() != metadata {
		t.Fatal("released Kafka progress changed", progress, err)
	}
}

func TestInvalidProgressNeverBecomesAStartFallback(t *testing.T) {
	// Given
	valid := Progress{Fingerprint: "fingerprint", TopicID: "topic"}.Encode()

	// When
	_, decodeProgressErr := DecodeProgress(&valid, "fingerprint", "topic", false)

	// Then
	if decodeProgressErr != nil {
		t.Fatal(decodeProgressErr)
	}
	for _, text := range []string{"", "foreign", valid + "{}", `kfwd2:1;{"fp":"fingerprint","tid":"topic","unknown":1}`, Progress{Fingerprint: "other", TopicID: "topic"}.Encode(), Progress{Fingerprint: "fingerprint", TopicID: "recreated"}.Encode(), Progress{Fingerprint: "fingerprint", TopicID: "topic", FileID: "file", Extent: -1}.Encode()} {
		if _, err := DecodeProgress(&text, "fingerprint", "topic", false); err == nil {
			t.Fatalf("accepted invalid progress: %s", text)
		}
	}
	if _, err := DecodeProgress(&valid, "fingerprint", "topic", true); err == nil {
		t.Fatal("relative query accepted missing window")
	}
}
