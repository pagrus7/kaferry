package plan

import (
	"maps"

	"github.com/pagrus7/kaferry/internal/sql"
)

type Environment interface{ LookupEnv(string) (string, bool) }

// Builder validates and normalizes SQL using profiles and environment values;
// it does not open source or destination resources. Diagnostics retain SQL source
// locations, while the resulting Plan and its fingerprint exclude them.
type Builder interface {
	Build(sql.Statement, Profiles) (Plan, error)
}

type Profiles map[string]map[string]string

type Properties struct{ values map[string]string }

func (p Properties) Get(name string) string { return p.values[name] }
func (p Properties) Values() map[string]string {
	values := make(map[string]string, len(p.values))
	maps.Copy(values, p.values)
	return values
}

// Do not expose credentials through fmt, JSON, or error reporting.
func (p Properties) String() string   { return "[connection properties redacted]" }
func (p Properties) GoString() string { return p.String() }

const (
	Preserve = "PRESERVE"
	Key      = "KEY"
)

type Endpoint struct {
	Kind             string
	Name             string
	Format           string
	Partitioning     string
	RotateAfterSize  int64
	RotateAfterAgeMS int64
	Alias            string
	Properties       Properties
}

type Policies struct {
	MissingTimestamp  string
	RejectedTimestamp string
	OtherRecord       string
}

// Plan contains execution settings after validation. Callers that construct plans
// directly must satisfy the same constraints. Fingerprint defines which settings
// identify saved progress; tuning and SQL source locations do not enter it.
type Plan struct {
	Source                 Endpoint
	Destination            Endpoint
	Group                  string
	OneOff                 bool
	Start                  string
	Window                 WindowSpec
	Partitions             *PartitionFilter
	MaxOutOfOrderMS        *int64
	Offsets                *OffsetRange
	PartitionOffsets       []PartitionOffsetRange
	MaxStalenessMS         int64
	WorkerStopAfterRecords uint64
	Policies               Policies
}
