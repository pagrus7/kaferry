package plan

// TimestampStopEnabled selects the timestamp-disorder rule. The source applies
// it to each original partition using that partition's resolved upper bound.
func (p Plan) TimestampStopEnabled() bool {
	return !p.HasOffsetEnd() && p.DisorderMS() >= 0
}

// ClockCaptureEnabled selects wall-clock sampling of a Kafka end. Sampling
// requires a resolved upper bound; an already saved end is always reused.
func (p Plan) ClockCaptureEnabled() bool {
	return p.Source.Kind == "TOPIC" && !p.HasOffsetEnd() && p.MaxStalenessMS >= 0
}

func (p Plan) Finite() bool {
	return p.Source.Kind == "ARCHIVE" || p.WorkerStopAfterRecords > 0 || p.HasOffsetEnd() ||
		p.Window.Upper != nil && (p.ClockCaptureEnabled() || p.TimestampStopEnabled())
}

// StopUsesTime classifies a validated saved stop. A tip without an offset end
// was captured under timestamp assumptions; archive EOF can include streams
// skipped under the timestamp-disorder rule.
func (p Plan) StopUsesTime(reason string) bool {
	switch reason {
	case "timestamp":
		return true
	case "tip":
		return !p.HasOffsetEnd()
	case "eof":
		return p.TimestampStopEnabled()
	default:
		return false
	}
}
