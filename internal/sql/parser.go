package sql

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

type parser struct {
	lexer    lexer
	current  token
	err      error
	expected []string // alternatives tried at the current token
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
		p.expected = p.expected[:0]
	}
}

// want records an alternative accepted at the current token.
func (p *parser) want(alternative string) {
	if !slices.Contains(p.expected, alternative) {
		p.expected = append(p.expected, alternative)
	}
}
func (p *parser) is(word string) bool {
	if p.err != nil {
		return false
	}
	p.want(word)
	return p.current.kind != "string" && strings.EqualFold(p.current.text, word)
}
func (p *parser) take(word string) bool {
	if !p.is(word) {
		return false
	}
	p.next()
	return true
}
func (p *parser) fail() {
	if p.err == nil {
		p.err = At(p.current.Position, fmt.Errorf("expected %s; found %s", alternatives(p.expected), p.current.description()))
	}
}
func (p *parser) expect(word string) {
	if !p.take(word) {
		p.fail()
	}
}
func (p *parser) oneOf(words ...string) string {
	for _, word := range words {
		if p.take(word) {
			return word
		}
	}
	p.fail()
	return ""
}
func (p *parser) text(kind string) string {
	if p.err != nil {
		return ""
	}
	if p.current.kind != kind {
		p.want(kind)
		p.fail()
		return ""
	}
	value := p.current.text
	p.next()
	return value
}

func alternatives(words []string) string {
	shown := make([]string, len(words))
	for i, word := range words {
		shown[i] = word
		if word == "string" {
			shown[i] = "single-quoted string"
		}
		if r, _ := utf8.DecodeRuneInString(word); !identifierStart(r) {
			shown[i] = strconv.Quote(word)
		}
	}
	if len(shown) < 2 {
		return strings.Join(shown, "")
	}
	return strings.Join(shown[:len(shown)-1], ", ") + " or " + shown[len(shown)-1]
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
	if p.err == nil && p.current.kind != "end of input" {
		p.want("end of input")
		p.fail()
	}
	return s
}

func (p *parser) endpoint() Endpoint {
	if p.take("TOPIC") {
		position := p.current.Position
		return Endpoint{Position: position, Kind: "TOPIC", Name: p.text("string")}
	}
	if !p.take("FILE") {
		p.fail()
		return Endpoint{}
	}
	format := strings.ToLower(p.oneOf("cbor", "avro"))
	p.expect("(")
	position := p.current.Position
	name := p.text("string")
	e := Endpoint{Position: position, Kind: "FILE", Name: name, Format: format}
	for p.take(",") {
		o := Option{Position: p.current.Position}
		o.Name = p.oneOf("ROTATE_AFTER_SIZE", "ROTATE_AFTER_AGE")
		p.expect("=")
		o.ValuePosition = p.current.Position
		if o.Name == "ROTATE_AFTER_AGE" {
			i := p.interval()
			o.Interval = &i
		} else {
			o.Value = p.text("integer")
			if p.is("k") || p.is("m") || p.is("g") {
				o.Value += strings.ToLower(p.current.text)
				p.next()
			}
		}
		e.Options = append(e.Options, o)
	}
	p.expect(")")
	return e
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
	position := p.current.Position
	op := p.oneOf(">=", ">", "<", "<=")
	if op == "" {
		return Bound{}
	}
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
		p.fail()
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
	i.Unit = p.oneOf("SECOND", "MINUTE", "HOUR", "DAY")
	return i
}

func (p *parser) option() Option {
	o := Option{Position: p.current.Position}
	if o.Name = p.oneOf("GROUP", "START", "CUTOFF", "WORKER_STOP_AFTER_RECORDS", "ON_ERROR", "ON_MISSING_TIMESTAMP", "ON_REJECTED_TIMESTAMP"); o.Name == "" {
		return o
	}
	p.expect("=")
	o.ValuePosition = p.current.Position
	switch o.Name {
	case "CUTOFF":
		i := p.interval()
		o.Interval = &i
	case "GROUP":
		p.want("string")
		if p.current.kind == "string" {
			o.Quoted = true
			o.Value = p.text("string")
		} else {
			p.expect("ONE_OFF")
			o.Value = "ONE_OFF"
		}
	case "START":
		o.Value = p.oneOf("EARLIEST", "LATEST")
	case "WORKER_STOP_AFTER_RECORDS":
		o.Value = p.text("integer")
	default:
		o.Value = p.oneOf("FAIL", "SKIP")
	}
	return o
}
