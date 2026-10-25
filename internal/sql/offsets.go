package sql

import "fmt"

// OffsetBound retains diagnostic positions separately from planned criteria.
type OffsetBound struct {
	Position      Position `json:"-"`
	ValuePosition Position `json:"-"`
	Operator      string
	Value         string
	Latest        bool
}
type OffsetBranch struct {
	Position   Position `json:"-"`
	Partitions *PartitionFilter
	Bounds     []OffsetBound
}

func (p *parser) offsetBound() OffsetBound {
	b := OffsetBound{Position: p.current.Position}
	b.Operator = p.oneOf(">=", ">", "<", "<=")
	b.ValuePosition = p.current.Position
	if p.take("LATEST") {
		b.Latest = true
	} else {
		b.Value = p.text("integer")
	}
	return b
}

// Grouped OR is deliberately limited to disjoint partition offset ranges.
func (p *parser) where(s *Statement) {
	if !p.is("(") {
		p.predicates(s)
		return
	}
	for p.err == nil {
		position := p.current.Position
		p.expect("(")
		var branch Statement
		p.predicates(&branch)
		p.expect(")")
		if p.err != nil {
			return
		}
		if len(branch.Bounds) != 0 {
			p.err = At(branch.Bounds[0].Position, fmt.Errorf("partition range branches accept PARTITION IN and RECORD_OFFSET bounds; put shared timestamp bounds after the branches"))
			return
		}
		s.OffsetBranches = append(s.OffsetBranches, OffsetBranch{Position: position, Partitions: branch.Partitions, Bounds: branch.OffsetBounds})
		if !p.take("OR") {
			break
		}
	}
	if p.take("AND") {
		p.predicates(s)
	}
}
