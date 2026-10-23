package plan

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionLayoutsAllowExpansionButNotMissingIDs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ids   []int32
		count int
		want  string
	}{
		{"larger destination", []int32{0, 1, 2, 3}, 8, ""},
		{"same layout", []int32{0, 1}, 2, ""},
		{"smaller destination", []int32{0, 4}, 4, "original partition 4"},
		{"negative provenance", []int32{-1}, 4, "original partition -1"},
		{"empty destination", nil, 0, "no partitions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given original partition IDs and a destination layout.
			// When validating routing, then every original ID must exist.
			err := CheckPartitions(tc.ids, tc.count)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestPartitioningIdentityAndDefault(t *testing.T) {
	// Given equivalent default and explicit preserved routing, and explicit key routing.
	const query = "INSERT INTO TOPIC 'out' %s SELECT * FROM TOPIC 'in' WITH(GROUP='g', START=EARLIEST)"
	defaultPlan := build(t, strings.Replace(query, "%s", "", 1))
	preservePlan := build(t, strings.Replace(query, "%s", "WITH(PARTITIONING=PRESERVE)", 1))
	keyPlan := build(t, strings.Replace(query, "%s", "WITH(PARTITIONING=KEY)", 1))
	// When calculating saved query identities, then defaults normalize and a routing change requires a new group.
	defaultFP, err := defaultPlan.Fingerprint("source", "destination")
	require.NoError(t, err)
	preserveFP, err := preservePlan.Fingerprint("source", "destination")
	require.NoError(t, err)
	keyFP, err := keyPlan.Fingerprint("source", "destination")
	require.NoError(t, err)
	require.Equal(t, Preserve, defaultPlan.Destination.Partitioning)
	require.Equal(t, defaultFP, preserveFP)
	require.NotEqual(t, defaultFP, keyFP)
	for _, p := range []Plan{defaultPlan, keyPlan} {
		p.Destination.Name = p.Source.Name
		_, err = p.Fingerprint("same", "same")
		require.ErrorContains(t, err, "same Kafka topic", "routing must not bypass the self-copy guard")
	}
	defaultPlan.Destination.Partitioning = "invalid"
	_, err = defaultPlan.Fingerprint("source", "destination")
	require.ErrorContains(t, err, "PARTITIONING must be PRESERVE or KEY")
}
