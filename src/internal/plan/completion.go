package plan

func (p Plan) TimestampStopEnabled() bool { return p.DisorderMS() >= 0 }

func (p Plan) Finite() bool {
	return p.Source.Kind == "ARCHIVE" || p.HasOffsetEnd() || p.WorkerStopAfterRecords > 0
}
