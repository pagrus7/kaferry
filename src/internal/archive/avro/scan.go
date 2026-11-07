package avro

import "errors"

// Scan validates the whole selected prefix with one resident decompressed block.
func (r *Reader) Scan() (int64, error) {
	var count, previous int64
	for position := r.Start; position < r.size; {
		block, err := r.Block(position)
		if err != nil {
			return count, err
		}
		for range block.Count {
			record, err := block.Next()
			if err != nil {
				return count, err
			}
			if count > 0 && record.Offset < previous {
				return count, errors.New("source offset decreased between Avro blocks")
			}
			count++
			previous = record.Offset
		}
		position = block.End
	}
	return count, nil
}
