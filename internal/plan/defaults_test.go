package plan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMinimalStatementsMatchExplicitDefaults(t *testing.T) {
	for _, source := range []string{"TOPIC 'a'", "FILE avro('in')"} {
		for _, destination := range []string{"TOPIC 'b'", "FILE avro('out')"} {
			t.Run(source+" to "+destination, func(t *testing.T) {
				// Given each endpoint combination with no settings clauses.
				query := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", destination, source)
				settings := "GROUP=ONE_OFF"
				if source == "TOPIC 'a'" {
					settings += ", START=EARLIEST"
				}
				// When planning implicit and explicit defaults.
				implicit := build(t, query)
				explicit := build(t, query+" WITH("+settings+")")
				// Then both execute the same copy and retain its persisted identity.
				require.Equal(t, explicit, implicit)
				require.True(t, implicit.OneOff, "bare SQL must start a fresh invocation")
				implicitID, err := implicit.Fingerprint("source", "destination")
				require.NoError(t, err)
				explicitID, err := explicit.Fingerprint("source", "destination")
				require.NoError(t, err)
				require.Equal(t, explicitID, implicitID, "spelling a default must not change saved identity")
			})
		}
	}
}

func TestOptionalSourceSettingsOverrideOnlyTheirDefaults(t *testing.T) {
	for _, tc := range []struct {
		settings, group, start string
		oneOff                 bool
	}{
		{"GROUP='resume'", "resume", "EARLIEST", false},
		{"START=LATEST", "ONE_OFF", "LATEST", true},
		{"GROUP='resume', START=LATEST", "resume", "LATEST", false},
	} {
		t.Run(tc.settings, func(t *testing.T) {
			// Given a source overriding its group, start, or both.
			query := "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WITH(" + tc.settings + ")"
			// When planning, then unspecified source settings keep their defaults.
			p := build(t, query)
			require.Equal(t, tc.group, p.Group)
			require.Equal(t, tc.oneOff, p.OneOff)
			require.Equal(t, tc.start, p.Start)
		})
	}
}

func TestTimeFiltersDoNotBecomeExplicitStartRules(t *testing.T) {
	for _, source := range []string{"TOPIC 'a'", "FILE avro('in')"} {
		// Given an upper-only filter without source settings.
		query := "INSERT INTO FILE avro('out') SELECT * FROM " + source + " WHERE RECORD_TIMESTAMP < NOW()"
		// When planning, then leave positioning to the filter rather than adding a conflicting START.
		p := build(t, query)
		require.True(t, p.OneOff)
		require.Empty(t, p.Start)
		require.Nil(t, p.Window.Lower)
		require.NotNil(t, p.Window.Upper)
		if p.Source.Kind == "TOPIC" {
			require.EqualValues(t, 60000, p.CutoffMS)
		} else {
			require.Zero(t, p.CutoffMS)
		}
	}
	// Given only an explicit cutoff, when planned, then it overrides the cutoff without requiring GROUP.
	p := build(t, "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WITH(CUTOFF=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < NOW() OPTIONS(ON_ERROR=SKIP)")
	require.True(t, p.OneOff)
	require.Zero(t, p.CutoffMS)
	require.Equal(t, (Policies{"SKIP", "SKIP", "SKIP"}), p.Policies)
}
