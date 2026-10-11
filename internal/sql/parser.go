package sql

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type parser struct {
	lexer   lexer
	current token
	err     error
}

func (s *StatementParser) Parse(input string) (Statement, error) {
	l := lexer{input: input, line: 1, column: 1}
	if !utf8.ValidString(input) {
		for l.index < len(input) {
			_, width := utf8.DecodeRuneInString(input[l.index:])
			if width == 1 && input[l.index] >= utf8.RuneSelf {
				return Statement{}, At(l.position(), fmt.Errorf("statement is not valid UTF-8; save it as UTF-8"))
			}
			l.advance()
		}
	}
	p := parser{lexer: l}
	p.next()
	statement := p.statement()
	if p.err != nil {
		return Statement{}, p.err
	}
	return statement, nil
}

func (p *parser) next() {
	if p.err == nil {
		p.current, p.err = p.lexer.next()
	}
}
func (p *parser) is(word string) bool {
	return p.err == nil && p.current.kind != "string" && strings.EqualFold(p.current.text, word)
}
func (p *parser) take(word string) bool {
	if !p.is(word) {
		return false
	}
	p.next()
	return true
}
func (p *parser) fail(expected string) {
	if p.err == nil {
		p.err = At(p.current.Position, fmt.Errorf("expected %s; found %s", expected, p.current.description()))
	}
}
func (p *parser) expect(word string) {
	if !p.take(word) {
		p.fail(word)
	}
}
func (p *parser) text(kind string) string {
	if p.err != nil {
		return ""
	}
	if p.current.kind != kind {
		p.fail(kind)
		return ""
	}
	value := p.current.text
	p.next()
	return value
}

func (p *parser) statement() Statement {
	var s Statement
	if p.take("WITH") {
		for p.err == nil {
			s.Connections = append(s.Connections, p.connection())
			if !p.take(",") {
				break
			}
		}
	}
	p.expect("INSERT")
	p.expect("INTO")
	s.Destination = p.endpoint()
	p.expect("SELECT")
	p.expect("*")
	p.expect("FROM")
	s.Source = p.endpoint()
	if p.is("WHERE") {
		s.WherePosition = p.current.Position
		p.next()
		s.Bounds = p.bounds()
		if len(s.Bounds) == 1 && !p.is("OPTIONS") {
			p.fail("AND or OPTIONS")
		}
	}
	s.OptionsPosition = p.current.Position
	p.expect("OPTIONS")
	p.expect("(")
	for p.err == nil {
		s.Options = append(s.Options, p.option())
		if !p.take(",") {
			break
		}
	}
	p.expect(")")
	p.take(";")
	if p.err == nil && p.current.kind != "end of statement" {
		p.fail("end of statement")
	}
	return s
}

func (p *parser) endpoint() Endpoint {
	if p.take("TOPIC") {
		position := p.current.Position
		return Endpoint{Position: position, Kind: "TOPIC", Name: p.text("string")}
	}
	if !p.take("FILE") {
		p.fail("TOPIC or FILE")
		return Endpoint{}
	}
	p.expect("cbor")
	p.expect("(")
	position := p.current.Position
	name := p.text("string")
	p.expect(")")
	return Endpoint{Position: position, Kind: "FILE", Name: name, Format: "cbor"}
}

func (p *parser) connection() Connection {
	p.expect("CONNECTION")
	position := p.current.Position
	c := Connection{Position: position, Alias: p.text("identifier")}
	p.expect("AS")
	p.expect("(")
	for p.err == nil {
		position := p.current.Position
		property := Property{Position: position, Name: p.text("string")}
		p.expect("=")
		if p.take("ENV") {
			p.expect("(")
			property.ValuePosition = p.current.Position
			property.Value = p.text("string")
			p.expect(")")
			property.Environment = true
		} else {
			property.ValuePosition = p.current.Position
			property.Value = p.text("string")
		}
		c.Properties = append(c.Properties, property)
		if !p.take(",") {
			break
		}
	}
	p.expect(")")
	return c
}

func (p *parser) bounds() []Bound {
	p.expect("RECORD_TIMESTAMP")
	if p.take("BETWEEN") {
		lo := p.time()
		p.expect("AND")
		hi := p.time()
		return []Bound{{Position: lo.Position, Operator: ">=", Time: lo}, {Position: hi.Position, Operator: "<=", Time: hi}}
	}
	bounds := []Bound{p.bound()}
	if p.take("AND") {
		p.expect("RECORD_TIMESTAMP")
		bounds = append(bounds, p.bound())
	}
	return bounds
}

func (p *parser) bound() Bound {
	op := p.current.text
	if p.current.kind != "symbol" || (op != ">" && op != ">=" && op != "<" && op != "<=") {
		p.fail("<, <=, >, or >=")
		return Bound{}
	}
	position := p.current.Position
	p.next()
	return Bound{Position: position, Operator: op, Time: p.time()}
}

func (p *parser) time() Time {
	t := Time{Position: p.current.Position}
	if p.take("NOW") {
		p.expect("(")
		p.expect(")")
		t.Now = true
	} else if p.take("TIMESTAMP") {
		t.Position = p.current.Position
		t.Literal = p.text("string")
	} else {
		p.fail("TIMESTAMP or NOW()")
	}
	for p.is("+") || p.is("-") {
		subtract := p.take("-")
		if !subtract {
			p.expect("+")
		}
		interval := p.interval()
		interval.Subtract = subtract
		t.Adjustments = append(t.Adjustments, interval)
	}
	return t
}

func (p *parser) interval() Interval {
	p.expect("INTERVAL")
	position := p.current.Position
	i := Interval{Position: position, Magnitude: p.text("string")}
	i.Unit = strings.ToUpper(p.current.text)
	if p.current.kind != "identifier" || (i.Unit != "SECOND" && i.Unit != "MINUTE" && i.Unit != "HOUR" && i.Unit != "DAY") {
		p.fail("SECOND, MINUTE, HOUR, or DAY")
	} else {
		p.next()
	}
	return i
}

func (p *parser) option() Option {
	o := Option{Position: p.current.Position, Name: strings.ToUpper(p.current.text)}
	if p.current.kind != "identifier" {
		p.fail("GROUP, START, CUTOFF, ON_ERROR, ON_MISSING_TIMESTAMP, or ON_REJECTED_TIMESTAMP")
		return o
	}
	switch o.Name {
	case "CUTOFF":
		p.optionValue(&o)
		i := p.interval()
		o.Interval = &i
	case "GROUP":
		p.optionValue(&o)
		if p.current.kind == "string" {
			o.Quoted = true
			o.Value = p.text("string")
		} else {
			p.expect("ONE_OFF")
			o.Value = "ONE_OFF"
		}
	case "START", "ON_ERROR", "ON_MISSING_TIMESTAMP", "ON_REJECTED_TIMESTAMP":
		p.optionValue(&o)
		o.Value = strings.ToUpper(p.text("identifier"))
	default:
		p.fail("GROUP, START, CUTOFF, ON_ERROR, ON_MISSING_TIMESTAMP, or ON_REJECTED_TIMESTAMP")
	}
	return o
}

func (p *parser) optionValue(o *Option) {
	p.next()
	p.expect("=")
	o.ValuePosition = p.current.Position
}
