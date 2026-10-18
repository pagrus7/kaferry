package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These digests are persisted in commits and checkpoints; equivalence tests
// alone cannot detect a change that rewrites every query's identity.
func TestPersistedFingerprintCompatibility(t *testing.T) {
	for _, tc := range []struct{ statement, digest string }{
		{"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' OPTIONS(GROUP='g', START=EARLIEST)", "ba717db1e9299f6fafbd828a84e212d00d4c88883161b239142910af0998afd2"},
		{"INSERT INTO FILE cbor('/tmp/out') SELECT * FROM TOPIC 'a' WHERE RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T00:00:00Z' AND TIMESTAMP '2026-10-01T01:00:00Z' OPTIONS(GROUP='g')", "ca81c0491763d7179c0f48f37f4f626b51ae280feb228e0f51c5473db76b8682"},
		{"INSERT INTO TOPIC 'b' SELECT * FROM FILE cbor('/tmp/in') OPTIONS(GROUP='g')", "e8941852551678373fe80e3262dacfbcae86f0ce98201409d51c094c5c5d8590"},
		{"INSERT INTO FILE cbor('/tmp/out') SELECT * FROM FILE cbor('/tmp/in') OPTIONS(GROUP='g')", "f5b3b2165f6512e268221251bb82e0750e35884b3eae9c4b45fc31a621656f17"},
		{"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR AND RECORD_TIMESTAMP < NOW() OPTIONS(GROUP='g')", "778cea5a3b644122a14bcc4d3953654db50e897da318701f0b12add9fe402352"},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			// Given a statement with a fixed persisted fingerprint digest.
			p := build(t, tc.statement)

			// When its canonical identity is fingerprinted.
			got, err := p.Fingerprint("source-cluster", "destination-cluster")

			// Then existing consumer groups and checkpoints keep the same identity.
			require.NoError(t, err)
			require.Equal(t, tc.digest, got)
		})
	}
}
