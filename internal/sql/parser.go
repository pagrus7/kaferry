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
	if !utf8.ValidString(input) {
		return Statement{}, fmt.Errorf("statement is not valid UTF-8")
	}
	p := parser{lexer: lexer{input: input, line: 1, column: 1}}
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
		p.err = fmt.Errorf("line %d, column %d: expected %s", p.current.line, p.current.column, expected)
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
	if p.take("WHERE") {
		s.Bounds = p.bounds()
	}
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
		return Endpoint{Kind: "TOPIC", Name: p.text("string")}
	}
	p.expect("FILE")
	format := p.text("identifier")
	if !strings.EqualFold(format, "cbor") {
		p.fail("cbor")
	}
	p.expect("(")
	name := p.text("string")
	p.expect(")")
	return Endpoint{Kind: "FILE", Name: name, Format: "cbor"}
}

func (p *parser) connection() Connection {
	p.expect("CONNECTION")
	c := Connection{Alias: p.text("identifier")}
	p.expect("AS")
	p.expect("(")
	for p.err == nil {
		property := Property{Name: p.text("string")}
		p.expect("=")
		if p.take("ENV") {
			p.expect("(")
			property.Value = p.text("string")
			p.expect(")")
			property.Environment = true
		} else {
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
		return []Bound{{Operator: ">=", Time: lo}, {Operator: "<=", Time: hi}}
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
		p.fail("timestamp comparison")
		return Bound{}
	}
	p.next()
	return Bound{Operator: op, Time: p.time()}
}

func (p *parser) time() Time {
	var t Time
	if p.take("NOW") {
		p.expect("(")
		p.expect(")")
		t.Now = true
	} else {
		p.expect("TIMESTAMP")
		t.Literal = p.text("string")
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
	i := Interval{Magnitude: p.text("string"), Unit: strings.ToUpper(p.text("identifier"))}
	if i.Unit != "SECOND" && i.Unit != "MINUTE" && i.Unit != "HOUR" && i.Unit != "DAY" {
		p.fail("SECOND, MINUTE, HOUR, or DAY")
	}
	return i
}

func (p *parser) option() Option {
	o := Option{Name: strings.ToUpper(p.text("identifier"))}
	p.expect("=")
	switch o.Name {
	case "CUTOFF":
		i := p.interval()
		o.Interval = &i
	case "GROUP":
		if p.current.kind == "string" {
			o.Quoted = true
			o.Value = p.text("string")
		} else {
			p.expect("ONE_OFF")
			o.Value = "ONE_OFF"
		}
	case "START", "ON_ERROR", "ON_MISSING_TIMESTAMP", "ON_REJECTED_TIMESTAMP":
		o.Value = strings.ToUpper(p.text("identifier"))
	default:
		p.fail("supported option name")
	}
	return o
}
