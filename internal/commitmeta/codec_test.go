package commitmeta

import (
	"math"
	"strings"
	"testing"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestCodecContract(t *testing.T) {
	zero, upper := int64(0), int64(math.MaxInt64)
	for _, tc := range []struct {
		name     string
		progress Progress
		golden   string
	}{
		{"minimal", Progress{Fingerprint: "fp", TopicID: "/////////////////////w"}, `kaferry:2;{"fp":"fp","tid":"/////////////////////w"}`},
		{"worker stop", Progress{Fingerprint: "fp", TopicID: "id", Stop: &core.Stop{Reason: "worker_record_cap", At: 42}}, `kaferry:2;{"fp":"fp","tid":"id","stop":{"reason":"worker_record_cap","at":42}}`},
		{"full", Progress{Fingerprint: "fp", TopicID: "id", Window: &plan.Window{Lower: &zero, Upper: &upper}, Stop: &core.Stop{Reason: "tip", At: 0}}, `kaferry:2;{"fp":"fp","tid":"id","win":{"lower":0,"upper":9223372036854775807},"stop":{"reason":"tip","at":0}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given logical progress independent of serialized field names.
			codec := NewCodec()
			// When encoding and decoding it.
			encoded, err := codec.Encode(tc.progress)
			require.NoError(t, err)
			decoded, err := codec.Decode(encoded)
			// Then the documented bytes and all null/zero distinctions survive.
			require.NoError(t, err)
			require.Equal(t, tc.golden, encoded)
			require.Equal(t, tc.progress, decoded)
		})
	}
}

func TestCodecAcceptsJSONWhitespaceAndFieldOrder(t *testing.T) {
	// Given valid JSON with reordered fields and a negative millisecond bound.
	text := `kaferry:2; {"tid":"id", "win":{"upper":null,"lower":-9223372036854775808}, "fp":"fp"} `
	// When decoding without requiring canonical JSON bytes.
	p, err := NewCodec().Decode(text)
	// Then integer milliseconds remain exact and absence stays absent.
	require.NoError(t, err)
	require.NotNil(t, p.Window)
	require.NotNil(t, p.Window.Lower)
	require.Equal(t, int64(math.MinInt64), *p.Window.Lower)
	require.Nil(t, p.Window.Upper)
	require.Nil(t, p.Stop)
}

func TestCodecRejectsMalformedOrUnsupportedData(t *testing.T) {
	for _, tc := range []struct{ name, text, kind string }{
		{"foreign", "other;{}", "foreign"},
		{"version", "kaferry:3;opaque", "unsupported version of"},
		{"previous version", `kaferry:1;{"fp":"fp","tid":"id"}`, "unsupported version of"},
		{"missing version", "kaferry:;{}", "unsupported version of"},
		{"oversize", strings.Repeat("x", MaxBytes+1), "oversized"},
		{"missing identity", `kaferry:2;{}`, "invalid"},
		{"null root", `kaferry:2;null`, "invalid"},
		{"array root", `kaferry:2;[]`, "invalid"},
		{"broken JSON", `kaferry:2;{"fp":`, "invalid"},
		{"trailing value", `kaferry:2;{"fp":"fp","tid":"id"} {}`, "invalid"},
		{"duplicate field", `kaferry:2;{"fp":"fp","fp":"other","tid":"id"}`, "invalid"},
		{"Unicode stop name", `kaferry:2;{"fp":"fp","tid":"id","ſtop":{"reason":"tip","at":0,"at":9}}`, "invalid"},
		{"Unicode reason name", `kaferry:2;{"fp":"fp","tid":"id","stop":{"reaſon":"tip","at":0}}`, "invalid"},
		{"duplicate window", `kaferry:2;{"fp":"fp","tid":"id","win":{"lower":null,"upper":1},"win":{"lower":0,"upper":1}}`, "invalid"},
		{"case variant", `kaferry:2;{"FP":"fp","tid":"id"}`, "invalid"},
		{"removed file fields", `kaferry:2;{"fp":"fp","tid":"id","fid":"file","extent":128}`, "invalid"},
		{"unknown field", `kaferry:2;{"fp":"fp","tid":"id","future":1}`, "invalid"},
		{"unknown nested field", `kaferry:2;{"fp":"fp","tid":"id","win":{"lower":0,"extra":1}}`, "invalid"},
		{"duplicate nested field", `kaferry:2;{"fp":"fp","tid":"id","stop":{"reason":"tip","at":0,"at":1}}`, "invalid"},
		{"null window", `kaferry:2;{"fp":"fp","tid":"id","win":null}`, "invalid"},
		{"reversed window", `kaferry:2;{"fp":"fp","tid":"id","win":{"lower":1,"upper":0}}`, "invalid"},
		{"fractional bound", `kaferry:2;{"fp":"fp","tid":"id","win":{"lower":1.5}}`, "invalid"},
		{"overflow", `kaferry:2;{"fp":"fp","tid":"id","win":{"upper":9223372036854775808}}`, "invalid"},
		{"unknown reason", `kaferry:2;{"fp":"fp","tid":"id","stop":{"reason":"eof","at":0}}`, "invalid"},
		{"missing stop position", `kaferry:2;{"fp":"fp","tid":"id","stop":{"reason":"tip"}}`, "invalid"},
		{"negative stop position", `kaferry:2;{"fp":"fp","tid":"id","stop":{"reason":"tip","at":-1}}`, "invalid"},
		{"invalid UTF8", "kaferry:2;{\"fp\":\"\xff\",\"tid\":\"id\"}", "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given untrusted broker metadata.
			// When decoding it through the independent codec.
			_, err := NewCodec().Decode(tc.text)
			// Then callers can distinguish foreign, unsupported and invalid data without echoing it.
			var problem *Error
			require.ErrorAs(t, err, &problem)
			require.Equal(t, tc.kind, problem.Kind)
		})
	}
}

func TestCodecValidatesWritesAndSizeLimit(t *testing.T) {
	for _, p := range []Progress{
		{},
		{Fingerprint: "fp", TopicID: "id", Stop: &core.Stop{Reason: "unknown"}},
		{Fingerprint: strings.Repeat("a", MaxBytes), TopicID: "id"},
		{Fingerprint: "\xff", TopicID: "id"},
	} {
		// Given invalid logical progress, when encoding it, then no bytes are returned.
		text, err := NewCodec().Encode(p)
		require.Error(t, err)
		require.Empty(t, text)
	}
	// Given a minimal envelope whose payload fills exactly the byte budget.
	base, _ := NewCodec().Encode(Progress{Fingerprint: "fp", TopicID: "id"})
	p := Progress{Fingerprint: strings.Repeat("a", MaxBytes-len(base)+2), TopicID: "id"}
	// When encoding and reading the maximum supported metadata size.
	text, err := NewCodec().Encode(p)
	require.NoError(t, err)
	require.Len(t, text, MaxBytes)
	// Then the reader accepts that exact limit.
	_, err = NewCodec().Decode(text)
	require.NoError(t, err)
}
