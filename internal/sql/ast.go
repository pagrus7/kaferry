// Package sql parses the copy statement language. It does not access endpoints.
package sql

type Endpoint struct {
	Position        Position `json:"-"`
	OptionsPosition Position `json:"-"`
	Kind            string   // TOPIC or ARCHIVE
	Name            string
	Format          string
	Options         []Option
}

type Property struct {
	Position      Position `json:"-"`
	ValuePosition Position `json:"-"`
	Name          string
	Value         string
	Environment   bool
}

type Connection struct {
	Position   Position `json:"-"`
	Alias      string
	Properties []Property
}

type Interval struct {
	Position  Position `json:"-"`
	Magnitude string
	Unit      string
	Subtract  bool
}

type Time struct {
	Position    Position `json:"-"`
	Now         bool
	Literal     string
	Adjustments []Interval
}

type Bound struct {
	Position Position `json:"-"`
	Operator string
	Time     Time
}

type Option struct {
	Position      Position `json:"-"`
	ValuePosition Position `json:"-"`
	Name          string
	Value         string
	Quoted        bool
	Interval      *Interval
}

type Statement struct {
	OptionsPosition Position `json:"-"`
	WherePosition   Position `json:"-"`
	Connections     []Connection
	Source          Endpoint
	Destination     Endpoint
	OffsetBounds    []OffsetBound
	OffsetBranches  []OffsetBranch
	Bounds          []Bound
	Partitions      *PartitionFilter
	Options         []Option
}

type PartitionID struct {
	Position Position `json:"-"`
	Value    string
}

type PartitionFilter struct {
	Position Position `json:"-"`
	Exclude  bool
	IDs      []PartitionID
	Range    *PartitionRange
}

type PartitionRange struct {
	Lower PartitionID
	Upper PartitionID
}

type Parser interface {
	Parse(string) (Statement, error)
}

type StatementParser struct{}

func NewParser() *StatementParser { return &StatementParser{} }
