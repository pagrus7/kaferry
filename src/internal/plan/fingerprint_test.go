package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func (p Plan) testFingerprint(sourceClusterID, destinationClusterID string) (string, error) {
	destinationTopicID := ""
	if p.Destination.Kind == "TOPIC" {
		destinationTopicID = "destination-topic-id"
	}
	return p.Fingerprint(sourceClusterID, destinationClusterID, destinationTopicID)
}

//kaferry:guarantee identity
func TestSnapshotAndFollowFingerprints(t *testing.T) {
	const base = "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' "
	fingerprint := func(tail string) string {
		got, err := build(t, base+tail).testFingerprint("source-cluster", "destination-cluster")
		require.NoError(t, err)
		return got
	}
	// Given equivalent spellings, when fingerprinted, then each shares a saved range.
	snapshot := fingerprint("")
	require.Equal(t, snapshot, fingerprint("WITH(FOLLOW=OFF) WHERE RECORD_OFFSET<LATEST"))
	require.Equal(t, fingerprint("WHERE RECORD_TIMESTAMP<NOW()"), fingerprint("WITH(MAX_OUT_OF_ORDER=OFF) WHERE RECORD_TIMESTAMP<NOW() AND RECORD_OFFSET<LATEST"))
	// Following and an early timestamp stop have distinct completion semantics.
	require.NotEqual(t, snapshot, fingerprint("WITH(FOLLOW=ON)"))
	require.NotEqual(t, fingerprint("WHERE RECORD_TIMESTAMP<NOW()"), fingerprint("WITH(MAX_OUT_OF_ORDER=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP<NOW()"))
	require.Equal(t, "1226e1073884b68c86d8e6eaed90455d7b6267ff5033885c57645c67bc9c6b16", snapshot)
	require.Equal(t, "6f705b9d517ca03139cb5a83c015a27f7c7136034c3ee64e952fc2b9e11b0177", fingerprint("WITH(FOLLOW=ON)"))
}

func TestFingerprintRequiresAndPinsDestinationIdentity(t *testing.T) {
	for _, source := range []string{"TOPIC 'a'", "ARCHIVE '/tmp/input'"} {
		t.Run(source, func(t *testing.T) {
			// Given the same query and endpoint names across a destination replacement.
			p := build(t, "INSERT INTO TOPIC 'b' SELECT * FROM "+source)
			// When fingerprinting the verified original and replacement UUIDs.
			first, err := p.Fingerprint("source", "destination", "original")
			require.NoError(t, err)
			replacement, err := p.Fingerprint("source", "destination", "replacement")
			require.NoError(t, err)
			// Then identity differs, and missing verification cannot adopt legacy progress.
			require.NotEqual(t, first, replacement)
			_, err = p.Fingerprint("source", "destination", "")
			require.ErrorContains(t, err, "verified destination topic ID")
		})
	}
	// Given an archive destination, when it is assigned a Kafka UUID, then reject the inconsistent endpoint identity.
	p := build(t, "INSERT INTO ARCHIVE '/tmp/output' SELECT * FROM TOPIC 'a'")
	_, err := p.Fingerprint("source", "", "unexpected")
	require.ErrorContains(t, err, "archive destination")
}
