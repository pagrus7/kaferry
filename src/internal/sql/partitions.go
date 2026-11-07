package sql

func (p *parser) partitionPredicate(position Position) *PartitionFilter {
	filter := &PartitionFilter{Position: position, Exclude: p.take("NOT")}
	if p.take("BETWEEN") {
		lower := PartitionID{Position: p.current.Position}
		lower.Value = p.text("integer")
		p.expect("AND")
		upper := PartitionID{Position: p.current.Position}
		upper.Value = p.text("integer")
		filter.Range = &PartitionRange{Lower: lower, Upper: upper}
		return filter
	}
	p.expect("IN")
	p.expect("(")
	for p.err == nil {
		position := p.current.Position
		filter.IDs = append(filter.IDs, PartitionID{Position: position, Value: p.text("integer")})
		if !p.take(",") {
			break
		}
	}
	p.expect(")")
	return filter
}
