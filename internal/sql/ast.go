// Package sql parses the copy statement language. It does not access endpoints.
package sql

type Endpoint struct {
	Kind   string // TOPIC or FILE
	Name   string
	Format string
}

type Property struct {
	Name        string
	Value       string
	Environment bool
}

type Connection struct {
	Alias      string
	Properties []Property
}

type Interval struct {
	Magnitude string
	Unit      string
	Subtract  bool
}

type Time struct {
	Now         bool
	Literal     string
	Adjustments []Interval
}

type Bound struct {
	Operator string
	Time     Time
}

type Option struct {
	Name     string
	Value    string
	Quoted   bool
	Interval *Interval
}

type Statement struct {
	Connections []Connection
	Source      Endpoint
	Destination Endpoint
	Bounds      []Bound
	Options     []Option
}

type Parser interface {
	Parse(string) (Statement, error)
}

type StatementParser struct{}

func NewParser() *StatementParser { return &StatementParser{} }
