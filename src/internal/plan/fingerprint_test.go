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

// These digests are persisted in commits and checkpoints; equivalence tests
// alone cannot detect a change that rewrites every query's identity.
func TestPersistedFingerprintCompatibility(t *testing.T) {
	for _, tc := range []struct{ statement, digest string }{
		{"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WITH(GROUP='g',START=EARLIEST)", "98fdb91f8d8cd93a2fa7e0ed527e4d401adaca3aeb0013696a8c9949c3d93795"},
		{"INSERT INTO TOPIC 'b' SELECT * FROM ARCHIVE '/tmp/in' WITH(GROUP='g')", "827647b4c2ec7233380002ac58880f7df7ed5208905f1bc0cc6530890807a614"},
		{"INSERT INTO TOPIC 'b' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC 'a' WITH (GROUP='g', START=EARLIEST) ", "ba717db1e9299f6fafbd828a84e212d00d4c88883161b239142910af0998afd2"},
		{"INSERT INTO ARCHIVE '/tmp/out' SELECT * FROM TOPIC 'a' WITH (GROUP='g') WHERE RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T00:00:00Z' AND TIMESTAMP '2026-10-01T01:00:00Z' ", "e0586230a1dd5824e068f4382924e018e72298ca452453ff7740a7221e4e1967"},
		{"INSERT INTO TOPIC 'b' WITH (PARTITIONING = KEY) SELECT * FROM ARCHIVE '/tmp/in' WITH (GROUP='g') ", "ef8573347c303ee6cab1b5e2f2ba5614c470a75dec59af2327a73ec93e950908"},
		{"INSERT INTO ARCHIVE '/tmp/out' SELECT * FROM TOPIC 'a' WITH (GROUP='g')", "48dec5403cf6de382de5067b99c71d24b9d001a0a4df17d6fc0f7063699926b7"},
		{"INSERT INTO ARCHIVE '/tmp/out' SELECT * FROM ARCHIVE '/tmp/in' WITH (GROUP='g') ", "4a15751638b452ad9455eba8d29f80571add2978699664eb4b0e546e48aa81bb"},
		{"INSERT INTO TOPIC 'b' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC 'a' WITH (GROUP='g') WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR AND RECORD_TIMESTAMP < NOW() ", "778cea5a3b644122a14bcc4d3953654db50e897da318701f0b12add9fe402352"},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			// Given a statement with a fixed persisted fingerprint digest.
			p := build(t, tc.statement)

			// When its canonical identity is fingerprinted.
			got, err := p.testFingerprint("source-cluster", "destination-cluster")

			// Then Kafka destination identities differ and unaffected archive identities remain compatible.
			require.NoError(t, err)
			if p.Destination.Kind == "TOPIC" || p.Source.Kind == "TOPIC" && p.Window.Upper != nil {
				require.NotEqual(t, tc.digest, got)
			} else {
				require.Equal(t, tc.digest, got)
			}
		})
	}
}

func TestBoundedFingerprintPinsCompletionSemantics(t *testing.T) {
	// Given a bounded query using the current default.
	query := "INSERT INTO TOPIC 'b' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC 'a' WITH (GROUP='g') WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR AND RECORD_TIMESTAMP < NOW()"
	p := build(t, query)
	// When fingerprinting, then the canonical identity is independently fixed.
	fp, err := p.testFingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	require.Equal(t, "ea87e63ab8891c45673de4fe8eca8d60938214f23c737cb6223c9315b4f60210", fp)
	// Then equivalent explicit settings retain identity and different completion
	// allowances have independently fixed identities.
	p.MaxOutOfOrderMS = new(int64(-1))
	explicitOff, err := p.testFingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	require.Equal(t, fp, explicitOff)
	p.MaxOutOfOrderMS = new(int64(300000))
	explicitOld, err := p.testFingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	require.Equal(t, "3457d71b29583614104e61084d6c9f442aabf493d522eaf90a814c32c0c18a17", explicitOld)
	require.NotEqual(t, fp, explicitOld)
	// Given an explicit one-minute allowance, bounded-query identity rejects incompatible completion semantics.
	p.MaxStalenessMS = 60000
	fp, err = p.testFingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	require.NotEqual(t, "778cea5a3b644122a14bcc4d3953654db50e897da318701f0b12add9fe402352", fp)
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
