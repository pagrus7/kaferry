package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOffsetSyntaxErrorsLocateTokensWithoutExposingValues(t *testing.T) {
	const prefix = "INSERT INTO TOPIC 'b'\nSELECT * FROM TOPIC 'a'\n"
	for _, tc := range []struct{ query, marker string }{
		{"WHERE RECORD_OFFSET >= secret_SENTINEL", "secret_SENTINEL"},
		{"WHERE (PARTITION IN(0) AND RECORD_OFFSET < )", ")"},
		{"WITH(MAX_OUT_OF_ORDER='secret_SENTINEL') WHERE RECORD_TIMESTAMP<NOW()", "'secret_SENTINEL'"},
		{"WHERE (PARTITION IN(0) AND RECORD_OFFSET<10) OR secret_SENTINEL", "secret_SENTINEL"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			// Given malformed offset criteria, when parsing, then report the safe category at the original token.
			_, err := NewParser().Parse(prefix + tc.query)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret_SENTINEL")
			var diagnostic *Diagnostic
			require.ErrorAs(t, err, &diagnostic)
			index := strings.Index(tc.query, tc.marker)
			if tc.marker == ")" {
				index = strings.LastIndex(tc.query, ")")
			}
			require.Equal(t, Position{Line: 3, Column: index + 1}, diagnostic.Position)
		})
	}
}
