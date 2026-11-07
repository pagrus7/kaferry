package avro

import "errors"

type Statistics struct {
	Records           int64  `json:"records"`
	MinOffset         int64  `json:"min_offset"`
	MaxOffset         int64  `json:"max_offset"`
	MinTimestamp      *int64 `json:"min_timestamp"`
	MaxTimestamp      *int64 `json:"max_timestamp"`
	MissingTimestamps int64  `json:"missing_timestamps"`
}

// Scan validates the whole selected prefix with one resident decompressed block.
func (r *Reader) Scan() (Statistics, error) {
	var stats Statistics
	for position := r.Start; position < r.size; {
		block, err := r.Block(position)
		if err != nil {
			return stats, err
		}
		for range block.Count {
			record, err := block.Next()
			if err != nil {
				return stats, err
			}
			if stats.Records > 0 && record.Offset < stats.MaxOffset {
				return stats, errors.New("source offset decreased between Avro blocks")
			}
			stats.Observe(record)
		}
		position = block.End
	}
	return stats, nil
}

func (s *Statistics) Observe(record Record) {
	if s.Records == 0 {
		s.MinOffset = record.Offset
	}
	s.Records++
	s.MaxOffset = record.Offset
	if record.TimestampMS == nil {
		s.MissingTimestamps++
		return
	}
	timestamp := *record.TimestampMS
	if s.MinTimestamp == nil || timestamp < *s.MinTimestamp {
		s.MinTimestamp = new(timestamp)
	}
	if s.MaxTimestamp == nil || timestamp > *s.MaxTimestamp {
		s.MaxTimestamp = new(timestamp)
	}
}
