package commitmeta

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

func TestCodecContract(t *testing.T) {
	zero, upper := int64(0), int64(math.MaxInt64)
	for _, tc := range []struct {
		name     string
		progress Progress
		golden   string
	}{
		{"minimal", Progress{Fingerprint: "fp", TopicID: "/////////////////////w"}, `kaferry:1;{"fp":"fp","tid":"/////////////////////w"}`},
		{"full", Progress{Fingerprint: "fp", TopicID: "id", Window: &plan.Window{Lower: &zero, Upper: &upper}, FileID: "file", Extent: 128, Stop: &core.Stop{Reason: "tip", At: 0}}, `kaferry:1;{"fp":"fp","tid":"id","win":{"lower":0,"upper":9223372036854775807},"fid":"file","extent":128,"stop":{"reason":"tip","at":0}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given logical progress independent of serialized field names.
			codec := NewCodec()
			// When encoding and decoding it.
			encoded, err := codec.Encode(tc.progress)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := codec.Decode(encoded)
			// Then the documented bytes and all null/zero distinctions survive.
			if err != nil || encoded != tc.golden || !reflect.DeepEqual(decoded, tc.progress) {
				t.Fatal(encoded, decoded, err)
			}
		})
	}
}

func TestCodecAcceptsJSONWhitespaceAndFieldOrder(t *testing.T) {
	// Given valid JSON with reordered fields and a negative millisecond bound.
	text := `kaferry:1; {"tid":"id", "win":{"upper":null,"lower":-9223372036854775808}, "fp":"fp"} `
	// When decoding without requiring canonical JSON bytes.
	p, err := NewCodec().Decode(text)
	// Then integer milliseconds remain exact and absence stays absent.
	if err != nil || p.Window == nil || p.Window.Lower == nil || *p.Window.Lower != math.MinInt64 || p.Window.Upper != nil || p.Stop != nil {
		t.Fatal(p, err)
	}
}

func TestCodecRejectsMalformedOrUnsupportedData(t *testing.T) {
	for _, tc := range []struct{ name, text, kind string }{
		{"foreign", "other;{}", "foreign"},
		{"version", "kaferry:2;opaque", "unsupported version of"},
		{"missing version", "kaferry:;{}", "unsupported version of"},
		{"oversize", strings.Repeat("x", MaxBytes+1), "oversized"},
		{"missing identity", `kaferry:1;{}`, "invalid"},
		{"null root", `kaferry:1;null`, "invalid"},
		{"array root", `kaferry:1;[]`, "invalid"},
		{"broken JSON", `kaferry:1;{"fp":`, "invalid"},
		{"trailing value", `kaferry:1;{"fp":"fp","tid":"id"} {}`, "invalid"},
		{"duplicate field", `kaferry:1;{"fp":"fp","fp":"other","tid":"id"}`, "invalid"},
		{"case variant", `kaferry:1;{"FP":"fp","tid":"id"}`, "invalid"},
		{"unknown field", `kaferry:1;{"fp":"fp","tid":"id","future":1}`, "invalid"},
		{"unknown nested field", `kaferry:1;{"fp":"fp","tid":"id","win":{"lower":0,"extra":1}}`, "invalid"},
		{"duplicate nested field", `kaferry:1;{"fp":"fp","tid":"id","stop":{"reason":"tip","at":0,"at":1}}`, "invalid"},
		{"null window", `kaferry:1;{"fp":"fp","tid":"id","win":null}`, "invalid"},
		{"reversed window", `kaferry:1;{"fp":"fp","tid":"id","win":{"lower":1,"upper":0}}`, "invalid"},
		{"fractional bound", `kaferry:1;{"fp":"fp","tid":"id","win":{"lower":1.5}}`, "invalid"},
		{"overflow", `kaferry:1;{"fp":"fp","tid":"id","win":{"upper":9223372036854775808}}`, "invalid"},
		{"file without extent", `kaferry:1;{"fp":"fp","tid":"id","fid":"file"}`, "invalid"},
		{"empty file pair", `kaferry:1;{"fp":"fp","tid":"id","fid":"","extent":0}`, "invalid"},
		{"zero extent without file", `kaferry:1;{"fp":"fp","tid":"id","extent":0}`, "invalid"},
		{"null extent", `kaferry:1;{"fp":"fp","tid":"id","extent":null}`, "invalid"},
		{"extent without file", `kaferry:1;{"fp":"fp","tid":"id","extent":1}`, "invalid"},
		{"negative extent", `kaferry:1;{"fp":"fp","tid":"id","fid":"file","extent":-1}`, "invalid"},
		{"unknown reason", `kaferry:1;{"fp":"fp","tid":"id","stop":{"reason":"eof","at":0}}`, "invalid"},
		{"missing stop position", `kaferry:1;{"fp":"fp","tid":"id","stop":{"reason":"tip"}}`, "invalid"},
		{"negative stop position", `kaferry:1;{"fp":"fp","tid":"id","stop":{"reason":"tip","at":-1}}`, "invalid"},
		{"invalid UTF8", "kaferry:1;{\"fp\":\"\xff\",\"tid\":\"id\"}", "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given untrusted broker metadata.
			// When decoding it through the independent codec.
			_, err := NewCodec().Decode(tc.text)
			// Then callers can distinguish foreign, unsupported and invalid data without echoing it.
			var problem *Error
			if !errors.As(err, &problem) || problem.Kind != tc.kind {
				t.Fatal(err)
			}
		})
	}
}

func TestCodecValidatesWritesAndSizeLimit(t *testing.T) {
	for _, p := range []Progress{
		{}, {Fingerprint: "fp", TopicID: "id", Extent: 1},
		{Fingerprint: "fp", TopicID: "id", Stop: &core.Stop{Reason: "unknown"}},
		{Fingerprint: strings.Repeat("a", MaxBytes), TopicID: "id"},
		{Fingerprint: "\xff", TopicID: "id"},
	} {
		// Given invalid logical progress, when encoding it, then no bytes are returned.
		if text, err := NewCodec().Encode(p); err == nil || text != "" {
			t.Fatal(text, err)
		}
	}
	// Given a minimal envelope whose payload fills exactly the byte budget.
	base, _ := NewCodec().Encode(Progress{Fingerprint: "fp", TopicID: "id"})
	p := Progress{Fingerprint: strings.Repeat("a", MaxBytes-len(base)+2), TopicID: "id"}
	// When encoding and reading the maximum supported metadata size.
	text, err := NewCodec().Encode(p)
	if err != nil || len(text) != MaxBytes {
		t.Fatal(len(text), err)
	}
	// Then the reader accepts that exact limit.
	if _, err := NewCodec().Decode(text); err != nil {
		t.Fatal(err)
	}
}
