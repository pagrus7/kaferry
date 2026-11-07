package copy

// ProgressTracker owns consumed positions, delivery outcomes and work accounting
// on one event loop. Assign starts a fresh token generation; Revoke discards it.
// Confirmed stops before the first unresolved input, even if later writes finish.
// Work includes resolved entries until Committed releases their saved prefix.
// Observe, Advance, Confirmed, Saved, Committed, Work and Resolved require a
// currently assigned partition; callers maintain assignments on this same loop.
type ProgressTracker interface {
	Assign(Cursor)
	Revoke(int32)
	// Observe admits an input with its next position and estimated bytes.
	Observe(Cursor, int64, int64) (Token, error)
	// Advance records a consumed gap or boundary without a destination write.
	Advance(Cursor) error
	// Submitted marks an accepted sink write as awaiting its delivery outcome.
	Submitted(Token)
	InFlight() bool
	// PositionOf requires a token still tracked, before commit or revocation.
	PositionOf(Token) int64
	// Settle rejects tokens from revoked assignments. A failed outcome remains
	// unresolved; it cannot advance the confirmed prefix.
	Settle(Token, bool) bool
	Confirmed(int32) Cursor
	Saved(int32) int64
	HasProgress(int32) bool
	// Committed follows a successful save of this confirmed cursor.
	Committed(Cursor)
	Work(int32) (int, int64)
	Total() (int, int64)
	Resolved(int32) bool
}

type ProgressFactory interface{ New() ProgressTracker }
type trackerFactory struct{}

func NewProgressFactory() ProgressFactory   { return trackerFactory{} }
func (trackerFactory) New() ProgressTracker { return NewTracker() }
