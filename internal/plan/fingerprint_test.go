package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These digests are persisted in commits and checkpoints; equivalence tests
// alone cannot detect a change that rewrites every query's identity.
func TestPersistedFingerprintCompatibility(t *testing.T) {
	for _, tc := range []struct{ statement, digest string }{
		{"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WITH(GROUP='g',START=EARLIEST)", "98fdb91f8d8cd93a2fa7e0ed527e4d401adaca3aeb0013696a8c9949c3d93795"},
		{"INSERT INTO TOPIC 'b' SELECT * FROM FILE avro('/tmp/in') WITH(GROUP='g')", "827647b4c2ec7233380002ac58880f7df7ed5208905f1bc0cc6530890807a614"},
		{"INSERT INTO TOPIC 'b' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC 'a' WITH (GROUP='g', START=EARLIEST) ", "ba717db1e9299f6fafbd828a84e212d00d4c88883161b239142910af0998afd2"},
		{"INSERT INTO FILE avro('/tmp/out') SELECT * FROM TOPIC 'a' WITH (GROUP='g') WHERE RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T00:00:00Z' AND TIMESTAMP '2026-10-01T01:00:00Z' ", "e0586230a1dd5824e068f4382924e018e72298ca452453ff7740a7221e4e1967"},
		{"INSERT INTO TOPIC 'b' WITH (PARTITIONING = KEY) SELECT * FROM FILE avro('/tmp/in') WITH (GROUP='g') ", "ef8573347c303ee6cab1b5e2f2ba5614c470a75dec59af2327a73ec93e950908"},
		{"INSERT INTO FILE avro('/tmp/out') SELECT * FROM FILE avro('/tmp/in') WITH (GROUP='g') ", "4a15751638b452ad9455eba8d29f80571add2978699664eb4b0e546e48aa81bb"},
		{"INSERT INTO TOPIC 'b' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC 'a' WITH (GROUP='g') WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR AND RECORD_TIMESTAMP < NOW() ", "778cea5a3b644122a14bcc4d3953654db50e897da318701f0b12add9fe402352"},
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
