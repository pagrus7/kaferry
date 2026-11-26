package copy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

//kaferry:guarantee completion.offset-end
func TestFixedEndWaitsForAcknowledgementAcrossGap(t *testing.T) {
	// Given a fixed end and an outstanding write below it.
	r, _, sink, store, _ := fixture(t)
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{End: new(int64(3))}}}))
	require.NoError(t, r.input(t.Context(), incoming(0, 0, 1)))
	// When a later record proves the gap reaches the saved end, progress still waits for acknowledgement.
	require.NoError(t, r.input(t.Context(), incoming(0, 5, 1)))
	require.NoError(t, r.advance(t.Context()))
	require.NoError(t, r.commit(r.ids()))
	require.Empty(t, store.saves)
	require.False(t, r.readyToExit())
	// Then the confirmed position and stop are saved at the original end.
	sink.reports <- Delivery{Token: sink.writes[0]}
	require.NoError(t, r.advance(t.Context()))
	require.NoError(t, r.commit(r.ids()))
	require.True(t, r.readyToExit())
	require.Equal(t, [][]Cursor{{{Position: 3, End: new(int64(3)), Stop: &Stop{Reason: "tip", At: 3}}}}, store.saves)
}
